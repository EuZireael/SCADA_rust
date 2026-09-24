//! Настройки шлюза и конфигурация контроллеров.
//!
//! Настройки берутся из env с теми же именами, что у Java-шлюза (Spring relaxed binding):
//! `SPRING_DATASOURCE_URL`, `SPRING_KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPICS_TELEMETRY`,
//! `GATEWAY_SEND_BAD_FRAMES`… — поэтому Rust-шлюз встаёт в те же docker-compose без правок.
//! Контроллеры и теги — тот же `controllers.yaml`, с подстановкой `${VAR:default}`.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde::Deserialize;

/// Все настройки процесса.
#[derive(Debug, Clone)]
pub struct Settings {
    pub http_port: u16,
    pub controllers_path: PathBuf,
    /// `None` — работа без БД (журнал и история не пишутся, команды только по имени тега).
    pub db: Option<DbSettings>,
    pub kafka: KafkaSettings,
    pub gateway: GatewaySettings,
}

#[derive(Debug, Clone)]
pub struct DbSettings {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub username: String,
    pub password: String,
}

#[derive(Debug, Clone)]
pub struct KafkaSettings {
    pub enabled: bool,
    pub bootstrap_servers: String,
    pub publish_events: bool,
    pub publish_alarms: bool,
    pub topics: Topics,
}

#[derive(Debug, Clone)]
pub struct Topics {
    pub telemetry: String,
    pub alarms: String,
    pub events: String,
    pub commands: String,
    pub command_results: String,
}

/// Флаги горячего пути (секция `gateway.*` Java-шлюза).
#[derive(Debug, Clone)]
pub struct GatewaySettings {
    /// Алармы по уставкам minValue/maxValue. По умолчанию их считает монитор.
    pub alarms_enabled: bool,
    /// Писать каждую точку в таблицу telemetry. По умолчанию выкл: ~15 ГБ/сут на станцию.
    pub persist_telemetry: bool,
    /// Сколько хранить историю telemetry (если она пишется).
    pub telemetry_retention: Duration,
    /// Кадр value=null/quality=BAD при обрыве. Монитор (runtime) к нему готов.
    pub send_bad_frames: bool,
    pub opcua_op_timeout: Duration,
    pub modbus_op_timeout: Duration,
    pub pac_op_timeout: Duration,
    /// Нет удачных чтений дольше — связь считается мёртвой, сессия пересоздаётся.
    pub stale_after: Duration,
    /// Пауза между попытками подключения.
    pub reconnect_interval: Duration,
    pub health_log_interval: Duration,
    pub heartbeat_interval: Duration,
}

impl Settings {
    /// Настройки из окружения; значения по умолчанию — как в application.yaml Java-шлюза.
    pub fn from_env() -> Result<Self> {
        let db = if env_bool(&["DB_ENABLED"], true) { Some(DbSettings::from_env()?) } else { None };
        Ok(Settings {
            http_port: env_parse(&["SERVER_PORT"], 8888)?,
            controllers_path: PathBuf::from(
                env_any(&["CONTROLLERS_YAML"]).unwrap_or_else(|| "config/controllers.yaml".into()),
            ),
            db,
            kafka: KafkaSettings {
                enabled: env_bool(&["KAFKA_ENABLED"], true),
                bootstrap_servers: env_any(&["SPRING_KAFKA_BOOTSTRAP_SERVERS", "KAFKA_BOOTSTRAP_SERVERS"])
                    .unwrap_or_else(|| "localhost:9092".into()),
                publish_events: env_bool(&["KAFKA_PUBLISH_EVENTS"], true),
                publish_alarms: env_bool(&["KAFKA_PUBLISH_ALARMS"], true),
                topics: Topics {
                    telemetry: env_or(&["KAFKA_TOPICS_TELEMETRY"], "scada.tags"),
                    alarms: env_or(&["KAFKA_TOPICS_ALARMS"], "scada-alarms"),
                    events: env_or(&["KAFKA_TOPICS_EVENTS"], "scada-events"),
                    commands: env_or(&["KAFKA_TOPICS_COMMANDS"], "scada-commands"),
                    command_results: env_or(
                        &["KAFKA_TOPICS_COMMAND_RESULTS", "KAFKA_TOPICS_COMMANDRESULTS"],
                        "scada-command-results",
                    ),
                },
            },
            gateway: GatewaySettings {
                alarms_enabled: env_bool(&["GATEWAY_ALARMS_ENABLED"], false),
                persist_telemetry: env_bool(&["GATEWAY_PERSIST_TELEMETRY", "GATEWAY_PERSISTTELEMETRY"], false),
                telemetry_retention: Duration::from_secs(
                    env_parse::<u64>(&["GATEWAY_TELEMETRY_RETENTION_HOURS"], 72)? * 3600,
                ),
                send_bad_frames: env_bool(&["GATEWAY_SEND_BAD_FRAMES", "GATEWAY_SENDBADFRAMES"], true),
                opcua_op_timeout: env_ms(&["GATEWAY_OPCUA_OP_TIMEOUT_MS", "GATEWAY_OPCUAOPTIMEOUTMS"], 5000)?,
                modbus_op_timeout: env_ms(&["GATEWAY_MODBUS_OP_TIMEOUT_MS", "GATEWAY_MODBUSOPTIMEOUTMS"], 3000)?,
                pac_op_timeout: env_ms(&["GATEWAY_PAC_OP_TIMEOUT_MS", "GATEWAY_PACOPTIMEOUTMS"], 3000)?,
                stale_after: env_ms(&["GATEWAY_STALE_AFTER_MS"], 30_000)?,
                reconnect_interval: env_ms(&["GATEWAY_SUPERVISE_INTERVAL_MS", "GATEWAY_SUPERVISEINTERVALMS"], 10_000)?,
                health_log_interval: env_ms(
                    &["GATEWAY_HEALTH_LOG_INTERVAL_MS", "GATEWAY_HEALTHLOGINTERVALMS"],
                    60_000,
                )?,
                heartbeat_interval: env_ms(&["GATEWAY_HEARTBEAT_INTERVAL_MS", "GATEWAY_HEARTBEATINTERVALMS"], 30_000)?,
            },
        })
    }
}

impl DbSettings {
    fn from_env() -> Result<Self> {
        let url = env_any(&["SPRING_DATASOURCE_URL", "DB_URL"])
            .unwrap_or_else(|| "jdbc:postgresql://localhost:5433/scada_db".into());
        let (host, port, database) = parse_jdbc_url(&url)?;
        Ok(DbSettings {
            host,
            port,
            database,
            username: env_or(&["SPRING_DATASOURCE_USERNAME", "DB_USERNAME"], "scada_user"),
            password: env_or(&["SPRING_DATASOURCE_PASSWORD", "DB_PASSWORD"], "scada_password"),
        })
    }
}

/// `jdbc:postgresql://host:port/db?params` (или `postgres://…`) → (host, port, db).
pub fn parse_jdbc_url(url: &str) -> Result<(String, u16, String)> {
    let rest = url
        .strip_prefix("jdbc:postgresql://")
        .or_else(|| url.strip_prefix("postgresql://"))
        .or_else(|| url.strip_prefix("postgres://"))
        .with_context(|| format!("неподдерживаемый адрес БД: {url}"))?;
    let rest = rest.rsplit('@').next().unwrap_or(rest); // user:pass@ в URL не используем
    let (authority, path) = rest.split_once('/').unwrap_or((rest, "scada_db"));
    let database = path.split('?').next().unwrap_or("scada_db").to_string();
    let (host, port) = match authority.rsplit_once(':') {
        Some((h, p)) => (h.to_string(), p.parse().with_context(|| format!("порт БД: {p}"))?),
        None => (authority.to_string(), 5432),
    };
    if host.is_empty() || database.is_empty() {
        bail!("неполный адрес БД: {url}");
    }
    Ok((host, port, database))
}

fn env_any(names: &[&str]) -> Option<String> {
    names.iter().find_map(|n| std::env::var(n).ok().filter(|v| !v.is_empty()))
}

fn env_or(names: &[&str], default: &str) -> String {
    env_any(names).unwrap_or_else(|| default.to_string())
}

fn env_bool(names: &[&str], default: bool) -> bool {
    match env_any(names) {
        Some(v) => matches!(v.trim().to_ascii_lowercase().as_str(), "true" | "1" | "yes" | "on"),
        None => default,
    }
}

fn env_parse<T: std::str::FromStr>(names: &[&str], default: T) -> Result<T>
where
    T::Err: std::fmt::Display,
{
    match env_any(names) {
        Some(v) => v.trim().parse().map_err(|e| anyhow::anyhow!("{}={v}: {e}", names[0])),
        None => Ok(default),
    }
}

fn env_ms(names: &[&str], default: u64) -> Result<Duration> {
    Ok(Duration::from_millis(env_parse(names, default)?))
}

// ---------------------------------------------------------------- controllers.yaml --

#[derive(Debug, Deserialize)]
pub struct ControllersFile {
    pub opcua: ControllersSection,
}

#[derive(Debug, Deserialize)]
pub struct ControllersSection {
    #[serde(default)]
    pub servers: Vec<ServerConfig>,
}

/// Один контроллер из YAML (OPC UA / Modbus / PAC — по схеме endpoint).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServerConfig {
    // id/security/username/password разбираются для совместимости формата; стенд работает
    // с политикой None и анонимно.
    #[allow(dead_code)]
    pub id: Option<String>,
    pub name: String,
    pub endpoint: String,
    #[allow(dead_code)]
    pub security: Option<String>,
    #[allow(dead_code)]
    pub username: Option<String>,
    #[allow(dead_code)]
    pub password: Option<String>,
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub tags: Vec<TagConfig>,
}

/// Один тег/канал из YAML — поля как у TagConfig Java-шлюза.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TagConfig {
    pub name: String,
    pub node_id: String,
    pub channel_id: Option<i64>,
    pub device_name: Option<String>,
    pub field_name: Option<String>,
    pub device_type: Option<String>,
    pub protocol: Option<String>,
    pub data_type: String,
    #[serde(default)]
    pub polling_rate: u64,
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub writable: bool,
    pub unit: Option<String>,
    pub min_value: Option<f64>,
    pub max_value: Option<f64>,
    pub modbus_address: Option<i32>,
    pub modbus_type: Option<String>,
    pub modbus_unit_id: Option<u8>,
}

/// Прочитать controllers.yaml с подстановкой `${VAR:default}` из окружения.
pub fn load_controllers(path: &std::path::Path) -> Result<Vec<ServerConfig>> {
    let raw = std::fs::read_to_string(path).with_context(|| format!("не прочитан {}", path.display()))?;
    parse_controllers(&expand_placeholders(&raw, |k| std::env::var(k).ok()))
}

pub fn parse_controllers(yaml: &str) -> Result<Vec<ServerConfig>> {
    let file: ControllersFile = serde_yaml_ng::from_str(yaml).context("controllers.yaml не разобран")?;
    Ok(file.opcua.servers)
}

/// Подстановка в стиле Spring: `${NAME}` или `${NAME:default}`.
pub fn expand_placeholders(text: &str, lookup: impl Fn(&str) -> Option<String>) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(start) = rest.find("${") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        match after.find('}') {
            Some(end) => {
                let expr = &after[..end];
                let (name, default) = expr.split_once(':').unwrap_or((expr, ""));
                out.push_str(&lookup(name).unwrap_or_else(|| default.to_string()));
                rest = &after[end + 1..];
            }
            None => {
                out.push_str(&rest[start..]);
                rest = "";
            }
        }
    }
    out.push_str(rest);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placeholders_use_env_or_default() {
        let env = |k: &str| (k == "SIM_HOST").then(|| "simulator".to_string());
        assert_eq!(
            expand_placeholders("opc.tcp://${SIM_HOST:127.0.0.1}:4840 ${NOPE:x} ${EMPTY}", env),
            "opc.tcp://simulator:4840 x "
        );
    }

    #[test]
    fn jdbc_url_is_parsed() {
        assert_eq!(
            parse_jdbc_url("jdbc:postgresql://postgres:5432/scada_db").unwrap(),
            ("postgres".into(), 5432, "scada_db".into())
        );
        assert_eq!(
            parse_jdbc_url("jdbc:postgresql://localhost/scada_db?ssl=false").unwrap(),
            ("localhost".into(), 5432, "scada_db".into())
        );
        assert!(parse_jdbc_url("mysql://x/y").is_err());
    }

    #[test]
    fn controllers_yaml_flow_mapping() {
        let yaml = r#"
opcua:
  servers:
    - id: pac-demo-001
      name: "PAC Savushkin"
      endpoint: "pac://127.0.0.1:10000"
      enabled: true
      tags:
        - {name: "A.B.LINE1V0.ST", nodeId: "pac:385", channelId: 385, deviceName: "LINE1V0", fieldName: "ST", deviceType: "V", protocol: pac, dataType: INT32, pollingRate: 2000, enabled: true, writable: true}
"#;
        let servers = parse_controllers(yaml).unwrap();
        assert_eq!(servers.len(), 1);
        let tag = &servers[0].tags[0];
        assert_eq!(tag.channel_id, Some(385));
        assert_eq!(tag.protocol.as_deref(), Some("pac"));
        assert!(tag.writable && tag.enabled);
        assert_eq!(tag.polling_rate, 2000);
    }
}
