//! Горячее резервирование: выборы активного экземпляра через группу потребителей Kafka.
//!
//! Оба экземпляра пары подписаны одной группой (`gateway.ha.group-id`) на служебный топик из одной
//! партиции (`gateway.ha.topic`). Kafka отдаёт партицию ровно одному члену группы — он и активный.
//! Упал активный — брокер перестаёт получать от него heartbeat и через `session-timeout-ms` отдаёт
//! партицию резервному; остановили штатно — экземпляр покидает группу сразу, и резерв подхватывает за
//! доли секунды.
//!
//! **Почему Kafka, а не отдельный арбитр.** Kafka — единственный канал шлюза к монитору (телеметрия,
//! события, команды), поэтому выборы через неё не добавляют точки отказа. И split-brain здесь
//! безвреден: экземпляр, потерявший связь с брокером и не узнавший, что лидерство ушло, всё равно
//! ничего не может опубликовать или принять.
//!
//! **Защиты.**
//! * `cooperative-sticky`: перезапущенный (вернувшийся) резерв не отбирает лидерство у живого
//!   активного — лишних переключений нет.
//! * Самоотключение: librdkafka сам снимает назначение, если сессия с координатором группы не
//!   подтверждалась дольше `session-timeout-ms` (изоляция от сети) — активный уходит в резерв.
//! * Потеря партиции ловится и колбэком ребаланса (до его завершения), и опросом назначения
//!   раз в 100 мс.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, Result};
use rdkafka::ClientConfig;
use rdkafka::client::ClientContext;
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext, Rebalance};
use rdkafka::error::KafkaError;
use rdkafka::topic_partition_list::TopicPartitionList;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::config::{HaSettings, KafkaSettings};
use crate::kafka;
use crate::leadership::Leadership;

/// Как часто опрашиваем группу: за столько же замечаем потерю или получение лидерства.
const POLL: Duration = Duration::from_millis(100);
/// Пауза перед повтором после ошибки выборов (брокер недоступен, неверная настройка).
const RETRY_BACKOFF: Duration = Duration::from_secs(2);

struct ElectionContext {
    leadership: Arc<Leadership>,
    topic: String,
}

impl ElectionContext {
    fn holds_leader_partition(&self, list: &TopicPartitionList) -> bool {
        list.elements().iter().any(|e| e.topic() == self.topic && e.partition() == 0)
    }
}

impl ClientContext for ElectionContext {}

impl ConsumerContext for ElectionContext {
    /// Потеря партиции — уходим в резерв ДО того, как её получит другой экземпляр (колбэк идёт внутри
    /// poll, до завершения ребаланса). Получение лидерства решает цикл выборов по `assignment()`.
    fn pre_rebalance(&self, _consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        match rebalance {
            Rebalance::Revoke(list) if self.holds_leader_partition(list) => {
                self.leadership.set_active(false, "лидерство передано другому экземпляру");
            }
            Rebalance::Error(e) => {
                self.leadership.set_active(false, &format!("ошибка ребаланса: {e}"));
            }
            _ => {}
        }
    }
}

/// Запустить выборы. Для одиночного экземпляра (`enabled=false`) ничего не делает — он активен всегда.
/// Возвращает поток выборов; остановка — через `cancel` (сначала роль → резерв, потом выход из группы).
pub fn spawn(
    ha: HaSettings,
    kafka_settings: KafkaSettings,
    leadership: Arc<Leadership>,
    cancel: CancellationToken,
) -> Option<std::thread::JoinHandle<()>> {
    if !ha.enabled {
        return None;
    }
    info!(
        "🛡 Горячий резерв: экземпляр {} (группа {}, топик {}, session {} мс, heartbeat {} мс)",
        ha.instance_id, ha.group_id, ha.topic, ha.session_timeout_ms, ha.heartbeat_interval_ms
    );
    let runtime = tokio::runtime::Handle::current();
    Some(
        std::thread::Builder::new()
            .name("ha-elector".into())
            .spawn(move || election_loop(ha, kafka_settings, leadership, cancel, runtime))
            .expect("поток выборов"),
    )
}

fn election_loop(
    ha: HaSettings,
    kafka_settings: KafkaSettings,
    leadership: Arc<Leadership>,
    cancel: CancellationToken,
    runtime: tokio::runtime::Handle,
) {
    while !cancel.is_cancelled() {
        match run_session(&ha, &kafka_settings, &leadership, &cancel, &runtime) {
            Ok(()) => {}
            Err(e) => {
                leadership.set_active(false, &format!("ошибка выборов: {e:#}"));
                error!(
                    "🛡 Выборы активного экземпляра прерваны: {e:#}. Проверьте брокер и настройки: session-timeout-ms ({}) \
                     должен быть в пределах group.min/max.session.timeout.ms брокера. Повтор через {} мс",
                    ha.session_timeout_ms,
                    RETRY_BACKOFF.as_millis()
                );
                sleep_or_cancel(&cancel, RETRY_BACKOFF);
            }
        }
    }
    leadership.set_active(false, "выборы остановлены");
}

fn run_session(
    ha: &HaSettings,
    kafka_settings: &KafkaSettings,
    leadership: &Arc<Leadership>,
    cancel: &CancellationToken,
    runtime: &tokio::runtime::Handle,
) -> Result<()> {
    runtime.block_on(kafka::ensure_topic(kafka_settings, &ha.topic, 1));
    let consumer: BaseConsumer<ElectionContext> = ClientConfig::new()
        .set("bootstrap.servers", &kafka_settings.bootstrap_servers)
        .set("group.id", &ha.group_id)
        .set("client.id", format!("scada-gateway-ha-{}", ha.instance_id))
        .set("session.timeout.ms", ha.session_timeout_ms.to_string())
        .set("heartbeat.interval.ms", ha.heartbeat_interval_ms.to_string())
        // Поток выборов опрашивает раз в 100 мс; если он завис на 30 с — пусть выпадет из группы и
        // отдаст лидерство (зависший экземпляр не должен оставаться активным).
        .set("max.poll.interval.ms", "30000")
        .set("partition.assignment.strategy", "cooperative-sticky")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "latest")
        .set("allow.auto.create.topics", "false")
        .create_with_context(ElectionContext { leadership: leadership.clone(), topic: ha.topic.clone() })
        .context("не создан консьюмер выборов")?;
    consumer.subscribe(&[&ha.topic]).context("подписка на служебный топик выборов")?;

    let mut last_error_log = std::time::Instant::now() - Duration::from_secs(60);
    while !cancel.is_cancelled() {
        if let Some(Err(e)) = consumer.poll(POLL) {
            // Брокер недоступен — не фатально: librdkafka переподключается, а если сессия истекла,
            // сам снимет назначение, и мы уйдём в резерв (ниже).
            if is_fatal(&e) {
                return Err(e.into());
            }
            if last_error_log.elapsed() > Duration::from_secs(30) {
                last_error_log = std::time::Instant::now();
                warn!("🛡 Kafka (выборы): {e}");
            }
        }
        let owner = consumer
            .assignment()
            .map(|a| a.elements().iter().any(|e| e.topic() == ha.topic && e.partition() == 0))
            .unwrap_or(false);
        if owner {
            leadership.set_active(true, &format!("получено лидерство в группе {}", ha.group_id));
        } else {
            leadership.set_active(false, "партиция выборов у другого экземпляра или потеряна связь с брокером");
        }
    }
    // Штатная остановка: сначала перестаём публиковать, потом покидаем группу (drop → LeaveGroup) —
    // резерв получает лидерство без ожидания session-timeout.
    leadership.set_active(false, "остановка экземпляра");
    drop(consumer);
    Ok(())
}

fn is_fatal(e: &KafkaError) -> bool {
    matches!(e, KafkaError::MessageConsumptionFatal(_) | KafkaError::ClientCreation(_))
}

fn sleep_or_cancel(cancel: &CancellationToken, d: Duration) {
    let until = std::time::Instant::now() + d;
    while std::time::Instant::now() < until && !cancel.is_cancelled() {
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Смена роли — в журнал (event_log) и, если экземпляр стал активным, в Kafka (`scada-events`):
/// монитор и оператор видят, что произошло переключение и кто теперь ведёт.
pub async fn record_events(leadership: Arc<Leadership>, events: crate::events::EventSink, cancel: CancellationToken) {
    if !leadership.is_ha_enabled() {
        return;
    }
    let mut rx = leadership.subscribe();
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return,
            changed = rx.changed() => if changed.is_err() { return },
        }
        let change = rx.borrow_and_update().clone();
        let me = leadership.instance_id();
        let message = if change.active {
            format!("Экземпляр {me} стал активным: {}", change.reason)
        } else {
            format!("Экземпляр {me} перешёл в резерв: {}", change.reason)
        };
        events.emit(
            crate::events::Event::new("HA", "HotStandby", if change.active { "INFO" } else { "WARNING" }, message).details(
                serde_json::json!({"instance": me, "role": if change.active { "ACTIVE" } else { "STANDBY" }, "reason": change.reason}),
            ),
        );
    }
}
