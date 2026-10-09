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
//! * «Слепой» активный: если связь потеряна со ВСЕМИ контроллерами дольше `yield-after-ms`, а партнёр
//!   в группе есть, активный выходит из группы и отдаёт лидерство (партнёр мог сохранить связь —
//!   у него другой сетевой путь к ПЛК). Возвращается резервным: `cooperative-sticky` лидерство не
//!   отбирает. Без партнёра в группе ничего не меняется — BAD-кадры монитору всё равно нужны.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result};
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
/// Не чаще: проверка наличия партнёра в группе — запрос к брокеру.
const PEER_CHECK_EVERY: Duration = Duration::from_secs(5);
/// Минимальное время вне группы после передачи лидерства — партнёру нужно успеть его получить.
const MIN_YIELD_PAUSE: Duration = Duration::from_secs(5);
/// Пауза перед повтором после ошибки выборов (брокер недоступен, неверная настройка).
const RETRY_BACKOFF: Duration = Duration::from_secs(2);

/// Контекст консьюмера выборов: колбэк ребаланса переводит роль в резерв ДО того, как партицию получит другой экземпляр.
struct ElectionContext {
    /// Роль экземпляра.
    leadership: Arc<Leadership>,
    /// Служебный топик выборов.
    topic: String,
}

impl ElectionContext {
    /// Есть ли в списке партиция выборов (нулевая партиция служебного топика).
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
    blind: Blind,
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
            .spawn(move || election_loop(ha, kafka_settings, leadership, blind, cancel, runtime))
            .expect("поток выборов"),
    )
}

/// «Слепота» шлюза: связь потеряна со всеми контроллерами (см. `App::all_links_down`).
pub type Blind = Arc<dyn Fn() -> bool + Send + Sync>;

/// Сколько непрерывно длится слепота.
#[derive(Default)]
struct BlindTracker {
    since: Option<Instant>,
}

impl BlindTracker {
    /// Сколько непрерывно длится слепота; любая связь с контроллером обнуляет отсчёт.
    fn update(&mut self, blind: bool, now: Instant) -> Duration {
        if !blind {
            self.since = None;
            return Duration::ZERO;
        }
        now.saturating_duration_since(*self.since.get_or_insert(now))
    }
}

/// Цикл выборов: сессия за сессией до остановки; ошибка — роль «резерв», пауза и повтор.
fn election_loop(
    ha: HaSettings,
    kafka_settings: KafkaSettings,
    leadership: Arc<Leadership>,
    blind: Blind,
    cancel: CancellationToken,
    runtime: tokio::runtime::Handle,
) {
    while !cancel.is_cancelled() {
        match run_session(&ha, &kafka_settings, &leadership, &blind, &cancel, &runtime) {
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

/// Одна сессия в группе выборов. Возвращается, когда шлюз остановлен или лидерство передано партнёру (после паузы вне группы); ошибка — `Err`, сессию пересоздаёт [`election_loop`].
fn run_session(
    ha: &HaSettings,
    kafka_settings: &KafkaSettings,
    leadership: &Arc<Leadership>,
    blind: &Blind,
    cancel: &CancellationToken,
    runtime: &tokio::runtime::Handle,
) -> Result<()> {
    runtime.block_on(kafka::ensure_topic(kafka_settings, &ha.topic, 1));
    let consumer: BaseConsumer<ElectionContext> = kafka_settings
        .client_config()
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

    let mut last_error_log = Instant::now() - Duration::from_secs(60);
    let mut tracker = BlindTracker::default();
    let mut last_peer_check = Instant::now() - PEER_CHECK_EVERY;
    while !cancel.is_cancelled() {
        if let Some(Err(e)) = consumer.poll(POLL) {
            // Брокер недоступен — не фатально: librdkafka переподключается, а если сессия истекла,
            // сам снимет назначение, и мы уйдём в резерв (ниже).
            if is_fatal(&e) {
                return Err(e.into());
            }
            if last_error_log.elapsed() > Duration::from_secs(30) {
                last_error_log = Instant::now();
                warn!("🛡 Kafka (выборы): {e}");
            }
        }
        let owner = consumer
            .assignment()
            .map(|a| a.elements().iter().any(|e| e.topic() == ha.topic && e.partition() == 0))
            .unwrap_or(false);
        if owner {
            leadership.set_active(true, &format!("получено лидерство в группе {}", ha.group_id));
            let blind_for = tracker.update(!ha.yield_after.is_zero() && blind(), Instant::now());
            if !ha.yield_after.is_zero() && blind_for >= ha.yield_after && last_peer_check.elapsed() >= PEER_CHECK_EVERY
            {
                last_peer_check = Instant::now();
                if has_peer(&consumer, &ha.group_id) {
                    let reason = format!(
                        "нет связи ни с одним контроллером {} с — лидерство передано партнёру",
                        blind_for.as_secs()
                    );
                    warn!("🛡 {reason}");
                    leadership.set_active(false, &reason);
                    drop(consumer); // выход из группы: партнёр получает партицию сразу
                    sleep_or_cancel(cancel, ha.yield_after.max(MIN_YIELD_PAUSE));
                    return Ok(());
                }
            }
        } else {
            tracker.update(false, Instant::now());
            leadership.set_active(false, "партиция выборов у другого экземпляра или потеряна связь с брокером");
        }
    }
    // Штатная остановка: сначала перестаём публиковать, потом покидаем группу (drop → LeaveGroup) —
    // резерв получает лидерство без ожидания session-timeout.
    leadership.set_active(false, "остановка экземпляра");
    drop(consumer);
    Ok(())
}

/// В группе выборов есть ещё участник (кроме нас): иначе отдавать лидерство некому.
fn has_peer(consumer: &BaseConsumer<ElectionContext>, group: &str) -> bool {
    match consumer.fetch_group_list(Some(group), Duration::from_secs(5)) {
        Ok(list) => list.groups().iter().find(|g| g.name() == group).is_some_and(|g| g.members().len() >= 2),
        Err(e) => {
            warn!("🛡 Состав группы выборов не получен: {e}");
            false
        }
    }
}

/// Ошибка, после которой сессию надо пересоздавать (а не временная недоступность брокера, которую librdkafka переживает сама).
fn is_fatal(e: &KafkaError) -> bool {
    matches!(e, KafkaError::MessageConsumptionFatal(_) | KafkaError::ClientCreation(_))
}

/// Спать не дольше `d`, просыпаясь при остановке (поток выборов — обычный, не async).
fn sleep_or_cancel(cancel: &CancellationToken, d: Duration) {
    let until = Instant::now() + d;
    while Instant::now() < until && !cancel.is_cancelled() {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blindness_is_measured_from_its_start_and_reset_by_any_sight() {
        let mut t = BlindTracker::default();
        let t0 = Instant::now();
        assert_eq!(t.update(false, t0), Duration::ZERO);
        assert_eq!(t.update(true, t0), Duration::ZERO);
        assert_eq!(t.update(true, t0 + Duration::from_secs(7)), Duration::from_secs(7));
        // Хотя бы одна связь вернулась — отсчёт заново.
        assert_eq!(t.update(false, t0 + Duration::from_secs(8)), Duration::ZERO);
        assert_eq!(t.update(true, t0 + Duration::from_secs(9)), Duration::ZERO);
        assert_eq!(t.update(true, t0 + Duration::from_secs(12)), Duration::from_secs(3));
    }
}
