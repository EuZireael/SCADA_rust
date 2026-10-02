//! Горячее резервирование на двух настоящих процессах шлюза: общие топики, одна группа выборов,
//! настоящий Kafka и симулятор. Проверяется то, что видит монитор:
//!   * активный один, резервный держит связь с ПЛК, но наружу молчит;
//!   * `kill -9` активного — резерв подхватывает за session-timeout, заново отправляет ВСЕ теги
//!     (прежний мог упасть, потребитель значений не знает) и принимает команды сразу;
//!   * вернувшийся экземпляр лидерство не отбирает;
//!   * штатная остановка активного — резерв подхватывает за доли секунды.
//!
//! Брокеру нужен `group.min.session.timeout.ms ≤ 1000` (docker-compose.yml и CI это задают).
//!
//!   cargo test --test ha -- --ignored      (симулятор + Kafka, см. tests/common)

mod common;

use std::collections::HashSet;
use std::time::{Duration, Instant};

use rdkafka::consumer::StreamConsumer;
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde_json::{Value, json};

use common::gateway::{Gateway, consumer_from_beginning, kafka_config, new_prefix};
use common::{controllers, controllers_path};

fn session_ms() -> u64 {
    std::env::var("IT_HA_SESSION_MS").ok().and_then(|v| v.parse().ok()).unwrap_or(2000)
}

fn ha_env(prefix: &str, instance: &str) -> Vec<(String, String)> {
    vec![
        ("GATEWAY_HA_ENABLED".into(), "true".into()),
        ("GATEWAY_HA_INSTANCE_ID".into(), instance.into()),
        ("GATEWAY_HA_TOPIC".into(), format!("{prefix}.ha")),
        ("GATEWAY_HA_GROUP_ID".into(), format!("{prefix}.group")),
        ("GATEWAY_HA_SESSION_TIMEOUT_MS".into(), session_ms().to_string()),
        ("GATEWAY_HA_HEARTBEAT_INTERVAL_MS".into(), (session_ms() / 4).to_string()),
    ]
}

fn start(prefix: &str, instance: &str) -> Gateway {
    let env = ha_env(prefix, instance);
    let refs: Vec<(&str, &str)> = env.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
    Gateway::start_on(prefix, &controllers_path(), &refs)
}

/// Теги, записанные в Kafka не раньше `since_ms` (по метке записи), пока не наберутся все `expected`.
async fn tags_since(
    consumer: &StreamConsumer,
    expected: &HashSet<String>,
    since_ms: i64,
    within: Duration,
) -> HashSet<String> {
    let mut seen = HashSet::new();
    let deadline = Instant::now() + within;
    while seen.len() < expected.len() {
        let left = deadline.saturating_duration_since(Instant::now());
        let Ok(Ok(m)) = tokio::time::timeout(left, consumer.recv()).await else { break };
        if m.timestamp().to_millis().unwrap_or(0) >= since_ms {
            let key = String::from_utf8_lossy(m.key().unwrap_or_default()).to_string();
            if expected.contains(&key) {
                seen.insert(key);
            }
        }
    }
    seen
}

async fn command(
    producer: &FutureProducer,
    results: &StreamConsumer,
    prefix: &str,
    tag: &str,
    value: Value,
) -> (Value, Duration) {
    let id = uuid::Uuid::new_v4().to_string();
    let body = json!({"commandId": id, "tagName": tag, "value": value, "requestedBy": "ha-it"}).to_string();
    let sent = Instant::now();
    producer
        .send(FutureRecord::to(&format!("{prefix}.commands")).key(tag).payload(&body), Duration::from_secs(5))
        .await
        .expect("команда не отправлена");
    while let Ok(Ok(m)) = tokio::time::timeout(Duration::from_secs(15), results.recv()).await {
        let r: Value = serde_json::from_slice(m.payload().unwrap_or_default()).unwrap();
        if r["commandId"] == id {
            return (r, sent.elapsed());
        }
    }
    panic!("нет результата команды {tag} за 15 с");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "нужны симулятор и Kafka с group.min.session.timeout.ms ≤ 1000: cargo test -- --ignored"]
async fn hot_standby_failover() {
    let session = Duration::from_millis(session_ms());
    let prefix = new_prefix();
    let expected: HashSet<String> = controllers().iter().flat_map(|c| c.tags.iter().map(|t| t.name.clone())).collect();
    // Записываемый тег: по умолчанию — OPC UA-канал симулятора; на стенде станции — IT_RW_TAG.
    let rw_tag = std::env::var("IT_RW_TAG").unwrap_or_else(|_| "Барановичи-1.BN1_MCA1.M_M.M3.M".into());
    let rw_tag = rw_tag.as_str();

    // --- Два экземпляра: активный один, резервный держит связь с ПЛК ---
    let mut a = start(&prefix, "a");
    a.wait_role("ACTIVE", Duration::from_secs(40)).await;
    let mut b = start(&prefix, "b");
    b.wait_role("STANDBY", Duration::from_secs(40)).await;
    a.wait_links("UP", Duration::from_secs(40)).await;
    b.wait_links("UP", Duration::from_secs(40)).await; // резерв — горячий: контроллеры опрашивает
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert!(a.metric("scada_telemetry_sent_total").unwrap_or(0.0) > 0.0, "активный публикует");
    assert_eq!(b.metric("scada_telemetry_sent_total"), Some(0.0), "резервный наружу молчит\n{}", b.log_tail());

    let telemetry = consumer_from_beginning(&a.topic("tags")).await;
    let results = consumer_from_beginning(&a.topic("results")).await;
    let producer: FutureProducer = kafka_config().create().unwrap();
    let (r, _) = command(&producer, &results, &prefix, rw_tag, json!(0)).await;
    assert_eq!(r["status"], "APPLIED", "{r}");

    // --- kill -9 активного ---
    let killed_at_ms = chrono::Utc::now().timestamp_millis();
    let t0 = Instant::now();
    a.child.kill().unwrap();
    let _ = a.child.wait();
    let takeover = b.wait_role("ACTIVE", session + Duration::from_secs(6)).await;
    println!("HA: kill -9 активного → резерв активен через {} мс", t0.elapsed().as_millis());
    assert!(takeover < session + Duration::from_secs(3), "переключение {takeover:?} при session-timeout {session:?}");

    // Новый активный заново отправляет значения ВСЕХ тегов: прежний мог упасть, потребитель их не знает.
    let seen = tags_since(&telemetry, &expected, killed_at_ms, Duration::from_secs(20)).await;
    assert_eq!(seen.len(), expected.len(), "после переключения пришло {}/{} тегов", seen.len(), expected.len());
    println!("HA: полная отправка после переключения — {} тегов, {} мс от kill", seen.len(), t0.elapsed().as_millis());

    // Команда принимается сразу: консьюмер команд не ждёт, пока брокер вычеркнет мёртвого члена группы.
    let (r, took) = command(&producer, &results, &prefix, rw_tag, json!(1)).await;
    assert_eq!(r["status"], "APPLIED", "{r}\n{}", b.log_tail());
    println!("HA: команда после переключения → {} за {} мс", r["status"], took.as_millis());
    assert!(took < Duration::from_secs(8));

    // --- Вернувшийся экземпляр лидерство не отбирает ---
    let a2 = start(&prefix, "a2");
    a2.wait_role("STANDBY", Duration::from_secs(40)).await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(b.role().as_deref(), Some("ACTIVE"), "перезапущенный резерв не отбирает лидерство");
    assert_eq!(a2.role().as_deref(), Some("STANDBY"));

    // --- Штатная остановка активного: резерв подхватывает за доли секунды ---
    let t1 = Instant::now();
    let stopper = std::thread::spawn(move || {
        b.terminate();
        b
    });
    let graceful = a2.wait_role("ACTIVE", Duration::from_secs(10)).await;
    println!("HA: штатная остановка активного → резерв активен через {} мс", t1.elapsed().as_millis());
    assert!(graceful < session, "штатная остановка не должна ждать session-timeout: {graceful:?}");
    let _b = stopper.join().unwrap();

    let (r, _) = command(&producer, &results, &prefix, rw_tag, json!(0)).await;
    assert_eq!(r["status"], "APPLIED", "{r}");
    a2.delete_topics().await;
}
