//! Сквозной тест: собранный шлюз как отдельный процесс против PLC-симулятора и Kafka.
//! Проверяется то, что видит монитор, — Kafka и HTTP, — на уникальных топиках `it-<id>.*`
//! (не мешает стенду):
//!   * каждый тег controllers.yaml приходит в телеметрию GOOD, тело — ровно
//!     {value, quality, timestamp}, value типизирован по dataType, timestamp — число;
//!   * команды через Kafka: запись по OPC UA и PAC применяется и доходит до телеметрии,
//!     запрещённое отклоняется с тем же статусом, что у Java-шлюза;
//!   * с БД (IT_DATABASE_URL): журнал событий, REST; метрики; остановка по SIGTERM.
//!
//!   cargo test --test e2e -- --ignored      (симулятор + Kafka, см. tests/common)

mod common;

use std::collections::{HashMap, HashSet};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use serde_json::{Value, json};

use common::{controllers, json_type_matches};

/// Шлюз-процесс; убивается при выходе из теста (в том числе по панике).
struct Gateway {
    child: Child,
    port: u16,
    prefix: String,
    log: std::path::PathBuf,
}

impl Gateway {
    fn start() -> Self {
        let prefix = format!("it-{}", &uuid::Uuid::new_v4().simple().to_string()[..8]);
        let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
        let log = std::env::temp_dir().join(format!("scada-gateway-{prefix}.log"));
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_scada-gateway"));
        cmd.env("CONTROLLERS_YAML", common::controllers_path())
            .env("SIM_HOST", common::sim_host())
            .env("SPRING_KAFKA_BOOTSTRAP_SERVERS", common::kafka_bootstrap())
            .env("SERVER_PORT", port.to_string())
            .env("GATEWAY_HEARTBEAT_INTERVAL_MS", "2000");
        for (var, suffix) in [
            ("KAFKA_TOPICS_TELEMETRY", "tags"),
            ("KAFKA_TOPICS_COMMANDS", "commands"),
            ("KAFKA_TOPICS_COMMAND_RESULTS", "results"),
            ("KAFKA_TOPICS_EVENTS", "events"),
            ("KAFKA_TOPICS_ALARMS", "alarms"),
        ] {
            cmd.env(var, format!("{prefix}.{suffix}"));
        }
        match std::env::var("IT_DATABASE_URL") {
            Ok(url) => {
                cmd.env("SPRING_DATASOURCE_URL", url)
                    .env(
                        "SPRING_DATASOURCE_USERNAME",
                        std::env::var("IT_DATABASE_USERNAME").unwrap_or("scada_user".into()),
                    )
                    .env(
                        "SPRING_DATASOURCE_PASSWORD",
                        std::env::var("IT_DATABASE_PASSWORD").unwrap_or("scada_password".into()),
                    );
            }
            Err(_) => {
                cmd.env("DB_ENABLED", "false");
            }
        }
        let out = std::fs::File::create(&log).unwrap();
        let child =
            cmd.stdout(Stdio::from(out.try_clone().unwrap())).stderr(Stdio::from(out)).spawn().expect("запуск шлюза");
        Gateway { child, port, prefix, log }
    }

    fn topic(&self, suffix: &str) -> String {
        format!("{}.{suffix}", self.prefix)
    }

    fn get(&self, path: &str) -> Option<(u16, String)> {
        let mut s = TcpStream::connect_timeout(&([127, 0, 0, 1], self.port).into(), Duration::from_secs(2)).ok()?;
        s.set_read_timeout(Some(Duration::from_secs(5))).ok()?;
        write!(s, "GET {path} HTTP/1.0\r\nHost: localhost\r\n\r\n").ok()?;
        let mut resp = String::new();
        s.read_to_string(&mut resp).ok()?;
        let status = resp.split_whitespace().nth(1)?.parse().ok()?;
        Some((status, resp.split_once("\r\n\r\n").map(|(_, b)| b.to_string()).unwrap_or_default()))
    }

    fn log_tail(&self) -> String {
        let text = std::fs::read_to_string(&self.log).unwrap_or_default();
        text.lines().rev().take(30).collect::<Vec<_>>().into_iter().rev().collect::<Vec<_>>().join("\n")
    }
}

impl Drop for Gateway {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn kafka_config() -> ClientConfig {
    let mut c = ClientConfig::new();
    c.set("bootstrap.servers", common::kafka_bootstrap());
    c
}

/// Консьюмер всех партиций топика с начала (топик создаёт шлюз при старте — ждём его).
async fn consumer_from_beginning(topic: &str) -> StreamConsumer {
    let consumer: StreamConsumer =
        kafka_config().set("group.id", "scada-gateway-it").set("enable.auto.commit", "false").create().unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let md = consumer.fetch_metadata(Some(topic), Duration::from_secs(5)).unwrap();
        let parts: Vec<i32> = md
            .topics()
            .iter()
            .filter(|t| t.name() == topic)
            .flat_map(|t| t.partitions().iter().map(|p| p.id()))
            .collect();
        if !parts.is_empty() {
            let mut tpl = TopicPartitionList::new();
            for p in parts {
                tpl.add_partition_offset(topic, p, Offset::Beginning).unwrap();
            }
            consumer.assign(&tpl).unwrap();
            return consumer;
        }
        assert!(Instant::now() < deadline, "топик {topic} не появился за 30 с");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Отправить команду и дождаться её результата (повтор с тем же commandId безопасен —
/// шлюз отсекает дубли; нужен, если консьюмер команд шлюза ещё не назначен).
async fn command(gw: &Gateway, producer: &FutureProducer, results: &StreamConsumer, tag: &str, value: Value) -> Value {
    let id = uuid::Uuid::new_v4().to_string();
    let body = json!({"commandId": id, "tagName": tag, "value": value, "requestedBy": "it"}).to_string();
    for _ in 0..5 {
        producer
            .send(FutureRecord::to(&gw.topic("commands")).key(tag).payload(&body), Duration::from_secs(5))
            .await
            .expect("команда не отправлена");
        let deadline = Instant::now() + Duration::from_secs(3);
        while let Ok(Ok(m)) =
            tokio::time::timeout(deadline.saturating_duration_since(Instant::now()), results.recv()).await
        {
            let result: Value = serde_json::from_slice(m.payload().unwrap_or_default()).unwrap();
            if result["commandId"] == id {
                return result;
            }
        }
    }
    panic!("нет результата команды {tag} = {value}\n{}", gw.log_tail());
}

/// Последнее значение тега в телеметрии, дождавшись `expected` (или последнее увиденное).
async fn await_value(telemetry: &StreamConsumer, tag: &str, expected: &Value) -> Value {
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut last = Value::Null;
    while let Ok(Ok(m)) =
        tokio::time::timeout(deadline.saturating_duration_since(Instant::now()), telemetry.recv()).await
    {
        if m.key() == Some(tag.as_bytes()) {
            last = serde_json::from_slice::<Value>(m.payload().unwrap()).unwrap()["value"].clone();
            if &last == expected {
                break;
            }
        }
    }
    last
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "нужны симулятор и Kafka: cargo test -- --ignored"]
async fn gateway_end_to_end() {
    let expected: HashMap<String, String> =
        controllers().iter().flat_map(|c| c.tags.iter().map(|t| (t.name.clone(), t.data_type.clone()))).collect();
    let gw = Gateway::start();

    // --- Шлюз поднялся и связан со всеми контроллерами ---
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some((200, body)) = gw.get("/actuator/health") {
            let health: Value = serde_json::from_str(&body).unwrap();
            let ctrls = health["components"]["controllers"].as_object().cloned().unwrap_or_default();
            if !ctrls.is_empty() && ctrls.values().all(|s| s == "UP") {
                break;
            }
        }
        assert!(Instant::now() < deadline, "шлюз не связался с контроллерами за 30 с\n{}", gw.log_tail());
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // --- Телеметрия: каждый тег GOOD, контракт тела ---
    let telemetry = consumer_from_beginning(&gw.topic("tags")).await;
    let mut good: HashSet<String> = HashSet::new();
    let mut problems: Vec<String> = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(30);
    while good.len() < expected.len() && Instant::now() < deadline {
        let Ok(Ok(m)) = tokio::time::timeout(Duration::from_secs(5), telemetry.recv()).await else { continue };
        let key = String::from_utf8_lossy(m.key().unwrap_or_default()).to_string();
        let body: Value = serde_json::from_slice(m.payload().unwrap_or_default()).expect("тело — JSON");
        let fields: Vec<&String> = body.as_object().map(|o| o.keys().collect()).unwrap_or_default();
        if fields.len() != 3 || !body["timestamp"].is_number() {
            problems.push(format!("{key}: форма тела {body}"));
        }
        match expected.get(&key) {
            None => problems.push(format!("лишний ключ {key}")),
            Some(dt) if body["quality"] == "GOOD" => {
                if json_type_matches(dt, &body["value"]) {
                    good.insert(key);
                } else {
                    problems.push(format!("{key} ({dt}): value {}", body["value"]));
                }
            }
            Some(_) => {}
        }
    }
    problems.dedup();
    assert!(problems.is_empty(), "нарушения контракта: {:?}", &problems[..problems.len().min(10)]);
    let missing: Vec<&String> = expected.keys().filter(|k| !good.contains(*k)).take(10).collect();
    assert!(missing.is_empty(), "GOOD получено {}/{}; нет, например: {missing:?}", good.len(), expected.len());

    // --- Команды через Kafka ---
    let producer: FutureProducer = kafka_config().create().unwrap();
    let results = consumer_from_beginning(&gw.topic("results")).await;
    let opc_rw = "Барановичи-1.BN1_MCA1.M_M.M3.M";
    let pac_rw = "Барановичи-1.BN1_MCA1.V_M_1.LINE1V0.M";
    for tag in [opc_rw, pac_rw] {
        let r = command(&gw, &producer, &results, tag, json!(1)).await;
        assert_eq!(r["status"], "APPLIED", "{tag}: {r}");
        assert_eq!(r["success"], true);
        // Сквозной цикл «команда → ПЛК → телеметрия».
        assert_eq!(await_value(&telemetry, tag, &json!(1)).await, json!(1), "{tag}: записанное не дошло до телеметрии");
        let r = command(&gw, &producer, &results, tag, json!(0)).await;
        assert_eq!(r["status"], "APPLIED", "{tag}: {r}");
    }
    for (tag, value, status) in [
        ("Барановичи-1.BN1_MCA1.M_RPM.LINE1M1.RPM", json!(5), "REJECTED_NOT_WRITABLE"), // датчик
        ("Барановичи-1.BN1_MCA1.Статистика_линии_1.OBJECT1.RT_PAR_F[119]", json!(1), "REJECTED_NOT_WRITABLE"), // Modbus
        ("Нет.Такого.Тега", json!(1), "REJECTED_UNKNOWN_TAG"),
        (opc_rw, json!("abc"), "REJECTED_TYPE_MISMATCH"),
    ] {
        let r = command(&gw, &producer, &results, tag, value).await;
        assert_eq!(r["status"], status, "{tag}: {r}");
        assert_eq!(r["success"], false);
    }

    // --- Метрики ---
    let (_, metrics) = gw.get("/actuator/prometheus").expect("метрики");
    assert!(metrics.contains("scada_controllers_connected{application=\"scada-gateway\"} 3"), "{metrics}");
    assert!(metrics.contains("scada_commands_total{status=\"APPLIED\""), "нет счётчика команд");

    // --- Журнал в БД и REST (если шлюз запущен с БД) ---
    if std::env::var("IT_DATABASE_URL").is_ok() {
        tokio::time::sleep(Duration::from_secs(1)).await; // писатель журнала — пачками
        let (status, body) = gw.get("/api/events/type/CONNECTION?limit=500").expect("REST журнала");
        assert_eq!(status, 200, "{body}");
        let events: Vec<Value> = serde_json::from_str(&body).unwrap();
        for c in controllers() {
            assert!(
                events
                    .iter()
                    .any(|e| e["message"].as_str().is_some_and(|m| m.contains(&c.name) && m.contains("установлена"))),
                "в журнале нет подключения {}",
                c.name
            );
        }
        let (_, body) = gw.get("/api/events/type/COMMAND?limit=50").unwrap();
        assert!(serde_json::from_str::<Vec<Value>>(&body).unwrap().len() >= 8, "команды не попали в журнал");
        let (_, body) = gw.get("/api/status").unwrap();
        let status: Value = serde_json::from_str(&body).unwrap();
        let tags: u64 = status["controllers"].as_array().unwrap().iter().map(|c| c["tags"].as_u64().unwrap()).sum();
        assert_eq!(tags as usize, expected.len());
        assert!(status["controllers"].as_array().unwrap().iter().all(|c| c["id"].as_i64().unwrap() > 0), "id из БД");
    }

    // --- Остановка по SIGTERM ---
    let mut gw = gw;
    let stop = Instant::now();
    Command::new("kill").args(["-TERM", &gw.child.id().to_string()]).status().unwrap();
    let exit = loop {
        if let Some(status) = gw.child.try_wait().unwrap() {
            break status;
        }
        assert!(stop.elapsed() < Duration::from_secs(10), "шлюз не остановился за 10 с по SIGTERM");
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert!(exit.success(), "код выхода {exit}\n{}", gw.log_tail());

    // Уборка тестовых топиков (не критично при неудаче).
    let admin: AdminClient<DefaultClientContext> = kafka_config().create().unwrap();
    let topics: Vec<String> = ["tags", "commands", "results", "events", "alarms"].iter().map(|s| gw.topic(s)).collect();
    let refs: Vec<&str> = topics.iter().map(String::as_str).collect();
    let _ = admin.delete_topics(&refs, &AdminOptions::new()).await;
}
