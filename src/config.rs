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
    /// Адрес привязки HTTP (`GATEWAY_HTTP_BIND`, по умолчанию все интерфейсы).
    pub http_bind: String,
    /// Токен доступа к `/api/*` (`GATEWAY_API_TOKEN` или файл `GATEWAY_API_TOKEN_FILE`); `None` — без проверки.
    /// `/actuator/health` и `/actuator/prometheus` токеном не закрываются (проверка контейнера, сбор метрик).
    pub api_token: Option<String>,
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
    /// Фактор репликации создаваемых шлюзом топиков (на стенде 1, на кластере — по числу брокеров).
    pub replication: i32,
    /// Свойства librdkafka для всех клиентов шлюза (безопасность: TLS, SASL) — см. [`client_properties`].
    pub client: ClientProperties,
    pub topics: Topics,
}

impl KafkaSettings {
    /// Заготовка конфигурации клиента: брокеры и свойства безопасности. Остальное клиент задаёт сам.
    pub fn client_config(&self) -> rdkafka::ClientConfig {
        let mut c = rdkafka::ClientConfig::new();
        c.set("bootstrap.servers", &self.bootstrap_servers);
        for (k, v) in &self.client.0 {
            c.set(k, v);
        }
        c
    }
}

/// Дополнительные свойства librdkafka (`security.protocol`, `sasl.*`, `ssl.*`…). В `Debug` значения
/// паролей и ключей скрыты, чтобы они не попали в журнал.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct ClientProperties(pub Vec<(String, String)>);

impl std::fmt::Debug for ClientProperties {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_map().entries(self.0.iter().map(|(k, v)| (k, if is_secret(k) { "***" } else { v.as_str() }))).finish()
    }
}

impl ClientProperties {
    /// Сводка для журнала: свойства без секретов.
    pub fn summary(&self) -> String {
        self.0.iter().filter(|(k, _)| !is_secret(k)).map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>().join(", ")
    }
}

fn is_secret(key: &str) -> bool {
    key.contains("password") || key.contains("secret") || key.contains("key.pem")
}

/// Свойства Kafka-клиентов из окружения.
///
/// Именованные: `KAFKA_SECURITY_PROTOCOL`, `KAFKA_SASL_MECHANISM`, `KAFKA_SASL_USERNAME`,
/// `KAFKA_SASL_PASSWORD`, `KAFKA_SSL_CA_LOCATION`, `KAFKA_SSL_CERTIFICATE_LOCATION`,
/// `KAFKA_SSL_KEY_LOCATION`, `KAFKA_SSL_KEY_PASSWORD`. Любое другое свойство librdkafka —
/// `KAFKA_CLIENT_<ИМЯ>` (`KAFKA_CLIENT_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM=none` →
/// `ssl.endpoint.identification.algorithm=none`); оно перекрывает именованное. Для совместимости с
/// Spring понимаются `SPRING_KAFKA_PROPERTIES_SECURITY_PROTOCOL`, `…_SASL_MECHANISM` и
/// `…_SASL_JAAS_CONFIG` (имя и пароль берутся из `username="…" password="…"`).
pub fn client_properties(vars: impl IntoIterator<Item = (String, String)>) -> ClientProperties {
    let vars: Vec<(String, String)> = vars.into_iter().filter(|(_, v)| !v.is_empty()).collect();
    let get = |name: &str| vars.iter().find(|(k, _)| k == name).map(|(_, v)| v.clone());
    let mut props: Vec<(String, String)> = Vec::new();
    let mut set = |key: &str, value: String| {
        props.retain(|(k, _)| k != key);
        props.push((key.to_string(), value));
    };
    for (key, names) in [
        ("security.protocol", &["KAFKA_SECURITY_PROTOCOL", "SPRING_KAFKA_PROPERTIES_SECURITY_PROTOCOL"][..]),
        ("sasl.mechanism", &["KAFKA_SASL_MECHANISM", "SPRING_KAFKA_PROPERTIES_SASL_MECHANISM"][..]),
        ("sasl.username", &["KAFKA_SASL_USERNAME"][..]),
        ("sasl.password", &["KAFKA_SASL_PASSWORD"][..]),
        ("ssl.ca.location", &["KAFKA_SSL_CA_LOCATION"][..]),
        ("ssl.certificate.location", &["KAFKA_SSL_CERTIFICATE_LOCATION"][..]),
        ("ssl.key.location", &["KAFKA_SSL_KEY_LOCATION"][..]),
        ("ssl.key.password", &["KAFKA_SSL_KEY_PASSWORD"][..]),
    ] {
        if let Some(v) = names.iter().find_map(|n| get(n)) {
            set(key, v.trim().to_string());
        }
    }
    if let Some(jaas) = get("SPRING_KAFKA_PROPERTIES_SASL_JAAS_CONFIG") {
        for (key, field) in [("sasl.username", "username"), ("sasl.password", "password")] {
            if let Some(v) = jaas_field(&jaas, field) {
                set(key, v);
            }
        }
    }
    for (name, value) in &vars {
        if let Some(rest) = name.strip_prefix("KAFKA_CLIENT_") {
            set(&rest.to_ascii_lowercase().replace('_', "."), value.trim().to_string());
        }
    }
    ClientProperties(props)
}

/// `username="alice" password="s3"` → значение поля в кавычках (или без них, до пробела/`;`).
fn jaas_field(jaas: &str, field: &str) -> Option<String> {
    let rest = &jaas[jaas.find(&format!("{field}="))? + field.len() + 1..];
    Some(match rest.strip_prefix('"') {
        Some(quoted) => quoted[..quoted.find('"')?].to_string(),
        None => rest.split(|c: char| c.is_whitespace() || c == ';').next()?.to_string(),
    })
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
    /// Отправка в Kafka по исключению (контракт: docs/TELEMETRY_BY_EXCEPTION_CONTRACT.md).
    pub publish: PublishSettings,
    /// Какие точки попадают в локальную историю (если она включена).
    pub history: HistorySettings,
    /// Команды старше — не исполняются (REJECTED_EXPIRED).
    pub command_max_age: Duration,
    /// Проверка эффекта записи OPC UA: через столько мс узел читается снова; 0 — выключено.
    pub command_verify: Duration,
    pub scripts: ScriptSettings,
    pub ha: HaSettings,
    pub opcua_op_timeout: Duration,
    /// Каталог сертификатов OPC UA-клиента (`own/`, `private/`, `trusted/`, `rejected/`): свой
    /// самоподписанный сертификат создаётся здесь, сертификаты серверов, которым шлюз доверяет, лежат в `trusted/`.
    pub opcua_pki_dir: PathBuf,
    /// Доверять любому сертификату сервера (только для стенда: без проверки сервера защищённый канал
    /// не защищает от подмены). По умолчанию — нет: сертификат сервера кладут в `trusted/`.
    pub opcua_trust_server_certs: bool,
    pub modbus_op_timeout: Duration,
    pub pac_op_timeout: Duration,
    /// Нет удачных чтений дольше — связь считается мёртвой, сессия пересоздаётся.
    pub stale_after: Duration,
    /// Пауза между попытками подключения.
    pub reconnect_interval: Duration,
    pub health_log_interval: Duration,
    pub heartbeat_interval: Duration,
}

/// `gateway.publish.*`: значение уходит в Kafka при первом появлении, смене качества, изменении
/// за зону нечувствительности и раз в `full_resend` неизменившимся («полная отправка»).
#[derive(Debug, Clone)]
pub struct PublishSettings {
    /// false — прежнее поведение: каждый тег каждый цикл.
    pub enabled: bool,
    pub deadband: f64,
    pub deadband_percent: f64,
    pub min_interval: Duration,
    pub full_resend: Duration,
}

/// `gateway.history.*`: фильтр локальной истории; тег переопределяет поля блоком `history:` в YAML.
#[derive(Debug, Clone)]
pub struct HistorySettings {
    pub deadband: f64,
    pub deadband_percent: f64,
    pub min_interval: Duration,
    /// «Пульс»: точка раз в столько, даже если значение стоит. 0 — выключен.
    pub max_interval: Duration,
}

/// `gateway.scripts.*`: пользовательские Lua-скрипты обработки значений.
#[derive(Debug, Clone)]
pub struct ScriptSettings {
    pub dir: PathBuf,
    /// Лимит времени на один вызов скрипта.
    pub timeout: Duration,
    pub reload_interval: Duration,
}

/// `gateway.ha.*`: горячее резервирование (выборы активного экземпляра через группу Kafka).
#[derive(Debug, Clone)]
pub struct HaSettings {
    pub enabled: bool,
    pub instance_id: String,
    /// Служебный топик выборов (одна партиция).
    pub topic: String,
    pub group_id: String,
    pub session_timeout_ms: u32,
    pub heartbeat_interval_ms: u32,
    /// Активный экземпляр без связи ни с одним контроллером столько времени передаёт лидерство
    /// партнёру (если тот в группе). 0 — не передавать.
    pub yield_after: Duration,
}

impl Settings {
    /// Настройки из окружения; значения по умолчанию — как в application.yaml Java-шлюза.
    pub fn from_env() -> Result<Self> {
        let db = if env_bool(&["DB_ENABLED"], true) { Some(DbSettings::from_env()?) } else { None };
        let commands_topic = env_or(&["KAFKA_TOPICS_COMMANDS"], "scada-commands");
        let instance_id =
            env_any(&["GATEWAY_HA_INSTANCE_ID", "GATEWAY_HA_INSTANCEID"]).unwrap_or_else(default_instance_id);
        Ok(Settings {
            http_port: env_parse(&["SERVER_PORT"], 8888)?,
            http_bind: env_or(&["GATEWAY_HTTP_BIND", "SERVER_ADDRESS"], "0.0.0.0"),
            api_token: api_token()?,
            // CONTROLLERS_CONFIG — имя и форма (`file:/путь`) из Java-шлюза: его compose работает без правок.
            controllers_path: PathBuf::from(
                env_any(&["CONTROLLERS_YAML", "CONTROLLERS_CONFIG"])
                    .map(|p| p.strip_prefix("file:").map(str::to_string).unwrap_or(p))
                    .unwrap_or_else(|| "config/controllers.yaml".into()),
            ),
            db,
            kafka: KafkaSettings {
                enabled: env_bool(&["KAFKA_ENABLED"], true),
                bootstrap_servers: env_any(&["SPRING_KAFKA_BOOTSTRAP_SERVERS", "KAFKA_BOOTSTRAP_SERVERS"])
                    .unwrap_or_else(|| "localhost:9092".into()),
                publish_events: env_bool(&["KAFKA_PUBLISH_EVENTS"], true),
                publish_alarms: env_bool(&["KAFKA_PUBLISH_ALARMS"], true),
                replication: env_parse(&["KAFKA_TOPICS_REPLICATION"], 1)?,
                client: client_properties(std::env::vars()),
                topics: Topics {
                    telemetry: env_or(&["KAFKA_TOPICS_TELEMETRY"], "scada.tags"),
                    alarms: env_or(&["KAFKA_TOPICS_ALARMS"], "scada-alarms"),
                    events: env_or(&["KAFKA_TOPICS_EVENTS"], "scada-events"),
                    commands: commands_topic.clone(),
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
                publish: PublishSettings {
                    enabled: env_bool(&["GATEWAY_PUBLISH_ENABLED"], true),
                    deadband: env_parse(&["GATEWAY_PUBLISH_DEADBAND"], 0.0)?,
                    deadband_percent: env_parse(
                        &["GATEWAY_PUBLISH_DEADBAND_PERCENT", "GATEWAY_PUBLISH_DEADBANDPERCENT"],
                        0.0,
                    )?,
                    min_interval: env_ms(&["GATEWAY_PUBLISH_MIN_INTERVAL_MS", "GATEWAY_PUBLISH_MININTERVALMS"], 0)?,
                    full_resend: env_ms(&["GATEWAY_PUBLISH_FULL_RESEND_MS", "GATEWAY_PUBLISH_FULLRESENDMS"], 30_000)?,
                },
                history: HistorySettings {
                    deadband: env_parse(&["GATEWAY_HISTORY_DEADBAND"], 0.0)?,
                    deadband_percent: env_parse(
                        &["GATEWAY_HISTORY_DEADBAND_PERCENT", "GATEWAY_HISTORY_DEADBANDPERCENT"],
                        0.0,
                    )?,
                    min_interval: env_ms(&["GATEWAY_HISTORY_MIN_INTERVAL_MS", "GATEWAY_HISTORY_MININTERVALMS"], 0)?,
                    max_interval: env_ms(
                        &["GATEWAY_HISTORY_MAX_INTERVAL_MS", "GATEWAY_HISTORY_MAXINTERVALMS"],
                        600_000,
                    )?,
                },
                command_max_age: env_ms(&["GATEWAY_COMMANDS_MAX_AGE_MS", "GATEWAY_COMMANDS_MAXAGEMS"], 30_000)?,
                command_verify: env_ms(&["GATEWAY_COMMANDS_VERIFY_MS"], 0)?,
                scripts: ScriptSettings {
                    dir: PathBuf::from(env_or(&["GATEWAY_SCRIPTS_DIR"], "scripts")),
                    timeout: env_ms(&["GATEWAY_SCRIPTS_TIMEOUT_MS", "GATEWAY_SCRIPTS_TIMEOUTMS"], 50)?,
                    reload_interval: env_ms(
                        &["GATEWAY_SCRIPTS_RELOAD_INTERVAL_MS", "GATEWAY_SCRIPTS_RELOADINTERVALMS"],
                        5000,
                    )?,
                },
                ha: HaSettings {
                    enabled: env_bool(&["GATEWAY_HA_ENABLED"], false),
                    instance_id,
                    topic: env_or(&["GATEWAY_HA_TOPIC"], "scada-gateway-ha"),
                    group_id: env_any(&["GATEWAY_HA_GROUP_ID", "GATEWAY_HA_GROUPID"])
                        .unwrap_or_else(|| format!("scada-gateway-ha.{commands_topic}")),
                    session_timeout_ms: env_parse(
                        &["GATEWAY_HA_SESSION_TIMEOUT_MS", "GATEWAY_HA_SESSIONTIMEOUTMS"],
                        6000,
                    )?,
                    heartbeat_interval_ms: env_parse(
                        &["GATEWAY_HA_HEARTBEAT_INTERVAL_MS", "GATEWAY_HA_HEARTBEATINTERVALMS"],
                        1000,
                    )?,
                    yield_after: env_ms(&["GATEWAY_HA_YIELD_AFTER_MS", "GATEWAY_HA_YIELDAFTERMS"], 30_000)?,
                },
                opcua_op_timeout: env_ms(&["GATEWAY_OPCUA_OP_TIMEOUT_MS", "GATEWAY_OPCUAOPTIMEOUTMS"], 5000)?,
                opcua_pki_dir: env_any(&["GATEWAY_OPCUA_PKI_DIR"])
                    .map(PathBuf::from)
                    .unwrap_or_else(|| std::env::temp_dir().join("scada-gateway-pki")),
                opcua_trust_server_certs: env_bool(&["GATEWAY_OPCUA_TRUST_SERVER_CERTS"], false),
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

/// Токен REST: `GATEWAY_API_TOKEN` или содержимое файла из `GATEWAY_API_TOKEN_FILE` (секреты Docker).
fn api_token() -> Result<Option<String>> {
    if let Some(t) = env_any(&["GATEWAY_API_TOKEN"]) {
        return Ok(Some(t.trim().to_string()));
    }
    let Some(path) = env_any(&["GATEWAY_API_TOKEN_FILE"]) else { return Ok(None) };
    let token = std::fs::read_to_string(&path).with_context(|| format!("GATEWAY_API_TOKEN_FILE={path}"))?;
    let token = token.trim().to_string();
    if token.is_empty() {
        bail!("GATEWAY_API_TOKEN_FILE={path}: файл пуст");
    }
    Ok(Some(token))
}

/// Имя экземпляра по умолчанию — хост и pid (как у Java-шлюза).
fn default_instance_id() -> String {
    let host = std::env::var("HOSTNAME").ok().filter(|h| !h.is_empty()).unwrap_or_else(|| "gateway".into());
    format!("{host}-{}", std::process::id())
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
    #[allow(dead_code)] // id разбирается для совместимости формата с Java-шлюзом
    pub id: Option<String>,
    pub name: String,
    pub endpoint: String,
    /// Политика и режим канала OPC UA: `None` (по умолчанию), `Basic256Sha256`, `Aes256_Sha256_RsaPss_Sign`…
    pub security: Option<String>,
    /// Пользователь OPC UA (вместе с `password`; пароль удобно держать в `${PLC_PASSWORD}`).
    pub username: Option<String>,
    pub password: Option<String>,
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub tags: Vec<TagConfig>,
}

impl ServerConfig {
    /// Защита соединения с OPC UA-контроллером из `security`/`username`/`password`.
    pub fn opc_security(&self) -> Result<crate::model::OpcSecurity> {
        crate::model::OpcSecurity::parse(self.security.as_deref(), self.username.as_deref(), self.password.as_deref())
            .map_err(|e| anyhow::anyhow!("{e}"))
    }

    /// Проверка при старте: молча проигнорированные настройки защиты — дыра, поэтому ошибка в `security`
    /// или защита у протокола, который её не поддерживает (Modbus, PAC), останавливают запуск.
    pub fn validate(&self) -> Result<()> {
        let security = self.opc_security().with_context(|| format!("контроллер {:?}", self.name))?;
        let is_opcua = self.endpoint.starts_with("opc.tcp://");
        if !is_opcua && (security.is_secure() || security.username.is_some()) {
            bail!(
                "контроллер {:?}: security/username/password поддерживаются только для OPC UA (endpoint {}); \
                 для Modbus и PAC защиты на уровне протокола нет — не задавайте их, чтобы не думать, что канал защищён",
                self.name,
                self.endpoint
            );
        }
        Ok(())
    }
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
    /// Переопределение фильтра истории для тега; не заданные поля — умолчания `gateway.history.*`.
    pub history: Option<HistoryConfig>,
}

/// Блок `history:` тега: `{deadband: 0.5, deadbandPercent: 1, minIntervalMs: 60000, maxIntervalMs: 600000}`.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct HistoryConfig {
    pub deadband: Option<f64>,
    pub deadband_percent: Option<f64>,
    pub min_interval_ms: Option<u64>,
    pub max_interval_ms: Option<u64>,
}

/// Прочитать controllers.yaml с подстановкой `${VAR:default}` из окружения.
pub fn load_controllers(path: &std::path::Path) -> Result<Vec<ServerConfig>> {
    let raw = std::fs::read_to_string(path).with_context(|| format!("не прочитан {}", path.display()))?;
    let expanded = expand_placeholders(&raw, |k| std::env::var(k).ok())
        .with_context(|| format!("{}: не заданы обязательные переменные", path.display()))?;
    parse_controllers(&expanded)
}

pub fn parse_controllers(yaml: &str) -> Result<Vec<ServerConfig>> {
    let file: ControllersFile = serde_yaml_ng::from_str(yaml).context("controllers.yaml не разобран")?;
    for server in file.opcua.servers.iter().filter(|s| s.enabled) {
        server.validate()?;
    }
    validate_station(&file.opcua.servers)?;
    Ok(file.opcua.servers)
}

/// Проверка конфигурации в целом (включённые контроллеры): имена контроллеров и тегов уникальны, протокол
/// контроллера известен. Имя тега — ключ сообщения Kafka и адрес команды: два тега с одним именем делали бы
/// телеметрию и команды неоднозначными, а контроллер с неизвестным протоколом молча не опрашивался бы.
pub fn validate_station(servers: &[ServerConfig]) -> Result<()> {
    let mut problems: Vec<String> = Vec::new();
    let mut controller_names = std::collections::HashSet::new();
    let mut tag_owner: std::collections::HashMap<&str, &str> = std::collections::HashMap::new();
    let mut duplicates: Vec<String> = Vec::new();
    for server in servers.iter().filter(|s| s.enabled) {
        if !controller_names.insert(server.name.as_str()) {
            problems.push(format!("контроллер {:?} описан дважды", server.name));
        }
        if crate::model::ControllerKind::from_endpoint(&server.endpoint).is_none() {
            problems.push(format!(
                "контроллер {:?}: неизвестный протокол в endpoint {:?} (ожидается opc.tcp://, modbus:// или pac://)",
                server.name, server.endpoint
            ));
        }
        for tag in server.tags.iter().filter(|t| t.enabled) {
            if tag.name.trim().is_empty() {
                problems.push(format!("контроллер {:?}: у включённого тега пустое имя", server.name));
            } else if let Some(first) = tag_owner.insert(tag.name.as_str(), server.name.as_str()) {
                duplicates.push(format!("{} ({first} и {})", tag.name, server.name));
            }
        }
    }
    if !duplicates.is_empty() {
        problems.push(format!(
            "имена тегов должны быть уникальны (это ключ Kafka и адрес команды), повторяются {}: {}{}",
            duplicates.len(),
            duplicates.iter().take(5).cloned().collect::<Vec<_>>().join("; "),
            if duplicates.len() > 5 { "; …" } else { "" }
        ));
    }
    if problems.is_empty() {
        Ok(())
    } else {
        bail!("конфигурация контроллеров не принята:\n  - {}", problems.join("\n  - "))
    }
}

/// Подстановка в стиле Spring: `${NAME:default}` — со значением по умолчанию (пустое `${NAME:}`
/// тоже допустимо); `${NAME}` без умолчания **обязательна** — без неё ошибка, а не пустая строка:
/// адрес контроллера `opc.tcp://:4840` шлюз молча не обнаружил бы до первой попытки подключения.
pub fn expand_placeholders(text: &str, lookup: impl Fn(&str) -> Option<String>) -> Result<String> {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    let mut missing: Vec<String> = Vec::new();
    while let Some(start) = rest.find("${") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        match after.find('}') {
            Some(end) => {
                let expr = &after[..end];
                match expr.split_once(':') {
                    Some((name, default)) => out.push_str(&lookup(name).unwrap_or_else(|| default.to_string())),
                    None => match lookup(expr) {
                        Some(v) => out.push_str(&v),
                        None => missing.push(expr.to_string()),
                    },
                }
                rest = &after[end + 1..];
            }
            None => {
                out.push_str(&rest[start..]);
                rest = "";
            }
        }
    }
    out.push_str(rest);
    if !missing.is_empty() {
        missing.sort();
        missing.dedup();
        bail!("задайте переменные окружения: {}", missing.join(", "));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    #[test]
    fn kafka_client_properties_from_named_and_generic_vars() {
        let vars =
            |pairs: &[(&str, &str)]| pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect::<Vec<_>>();
        let p = super::client_properties(vars(&[
            ("KAFKA_SECURITY_PROTOCOL", "SASL_SSL"),
            ("KAFKA_SASL_MECHANISM", "SCRAM-SHA-512"),
            ("KAFKA_SASL_USERNAME", "gw"),
            ("KAFKA_SASL_PASSWORD", "p@ss"),
            ("KAFKA_SSL_CA_LOCATION", "/certs/ca.pem"),
            ("KAFKA_CLIENT_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM", "none"),
            ("KAFKA_CLIENT_SASL_USERNAME", "override"),
            ("KAFKA_TOPICS_TELEMETRY", "scada.tags"),
            ("KAFKA_SSL_KEY_PASSWORD", ""),
        ]));
        let get = |k: &str| p.0.iter().find(|(n, _)| n == k).map(|(_, v)| v.as_str());
        assert_eq!(get("security.protocol"), Some("SASL_SSL"));
        assert_eq!(get("sasl.mechanism"), Some("SCRAM-SHA-512"));
        assert_eq!(get("sasl.username"), Some("override"), "KAFKA_CLIENT_* перекрывает именованную");
        assert_eq!(get("sasl.password"), Some("p@ss"));
        assert_eq!(get("ssl.endpoint.identification.algorithm"), Some("none"));
        assert_eq!(get("ssl.key.password"), None, "пустое значение — не задано");
        assert_eq!(get("topics.telemetry"), None, "чужие KAFKA_* не попадают в клиент");
        let shown = format!("{p:?}");
        assert!(!shown.contains("p@ss"), "пароль не должен печататься: {shown}");
        assert!(!p.summary().contains("p@ss"));
        assert!(p.summary().contains("security.protocol=SASL_SSL"));
    }

    #[test]
    fn kafka_client_properties_from_spring_jaas() {
        let p = super::client_properties([
            ("SPRING_KAFKA_PROPERTIES_SECURITY_PROTOCOL".to_string(), "SASL_PLAINTEXT".to_string()),
            ("SPRING_KAFKA_PROPERTIES_SASL_MECHANISM".to_string(), "PLAIN".to_string()),
            (
                "SPRING_KAFKA_PROPERTIES_SASL_JAAS_CONFIG".to_string(),
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"alice\" password=\"s3\";"
                    .to_string(),
            ),
        ]);
        let get = |k: &str| p.0.iter().find(|(n, _)| n == k).map(|(_, v)| v.as_str());
        assert_eq!(get("security.protocol"), Some("SASL_PLAINTEXT"));
        assert_eq!(get("sasl.username"), Some("alice"));
        assert_eq!(get("sasl.password"), Some("s3"));
    }

    #[test]
    fn no_security_vars_means_no_properties() {
        assert!(super::client_properties([("KAFKA_ENABLED".to_string(), "true".to_string())]).0.is_empty());
    }

    use super::*;

    #[test]
    fn placeholders_use_env_or_default() {
        let env = |k: &str| (k == "SIM_HOST").then(|| "simulator".to_string());
        assert_eq!(
            expand_placeholders("opc.tcp://${SIM_HOST:127.0.0.1}:4840 ${NOPE:x} [${EMPTY:}]", env).unwrap(),
            "opc.tcp://simulator:4840 x []"
        );
    }

    #[test]
    fn required_placeholder_without_value_is_an_error() {
        let err = expand_placeholders("opc.tcp://${PLC_HOST}:${PLC_PORT}", |_| None).unwrap_err().to_string();
        assert!(err.contains("PLC_HOST") && err.contains("PLC_PORT"), "{err}");
        assert_eq!(expand_placeholders("${PLC_HOST}", |_| Some("10.0.0.1".into())).unwrap(), "10.0.0.1");
    }

    #[test]
    fn station_validation_rejects_duplicates_and_unknown_protocols() {
        let tag = |name: &str| format!("{{name: \"{name}\", nodeId: \"ns=2;s=1\", dataType: INT32, enabled: true}}");
        let server = |name: &str, endpoint: &str, tags: &[&str]| {
            format!(
                "    - {{name: \"{name}\", endpoint: \"{endpoint}\", enabled: true, tags: [{}]}}\n",
                tags.iter().map(|t| tag(t)).collect::<Vec<_>>().join(", ")
            )
        };
        let yaml = |servers: String| format!("opcua:\n  servers:\n{servers}");
        assert!(
            parse_controllers(&yaml(
                server("A", "opc.tcp://h:1", &["x", "y"]) + &server("B", "modbus://h:502", &["z"])
            ))
            .is_ok()
        );
        let dup_in = parse_controllers(&yaml(server("A", "opc.tcp://h:1", &["x", "x"]))).unwrap_err();
        assert!(format!("{dup_in:#}").contains("повторяются 1: x"), "{dup_in:#}");
        let dup_across =
            parse_controllers(&yaml(server("A", "opc.tcp://h:1", &["x"]) + &server("B", "pac://h:2", &["x"])))
                .unwrap_err();
        assert!(format!("{dup_across:#}").contains("(A и B)"), "{dup_across:#}");
        let same_name =
            parse_controllers(&yaml(server("A", "opc.tcp://h:1", &["x"]) + &server("A", "opc.tcp://h:2", &["y"])))
                .unwrap_err();
        assert!(format!("{same_name:#}").contains("описан дважды"), "{same_name:#}");
        let proto = parse_controllers(&yaml(server("A", "http://h:1", &["x"]))).unwrap_err();
        assert!(format!("{proto:#}").contains("неизвестный протокол"), "{proto:#}");
        // Выключенное в расчёт не идёт: дубль имени в выключенном контроллере и выключенный тег не мешают.
        let off = "    - {name: \"A\", endpoint: \"opc.tcp://h:1\", enabled: true, tags: [{name: \"x\", nodeId: \"n\", dataType: INT32, enabled: true}, {name: \"x\", nodeId: \"n\", dataType: INT32, enabled: false}]}\n    - {name: \"B\", endpoint: \"junk\", enabled: false, tags: [{name: \"x\", nodeId: \"n\", dataType: INT32, enabled: true}]}\n";
        assert!(parse_controllers(&yaml(off.to_string())).is_ok());
    }

    /// Конфигурации из репозитория проходят проверку целиком (имена уникальны, протоколы известны).
    #[test]
    fn repository_configs_pass_station_validation() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        for (file, tags) in [("config/controllers.yaml", 2517usize), ("config/stations/BN1_MCA1.yaml", 1834)] {
            let raw = std::fs::read_to_string(root.join(file)).unwrap();
            let yaml = expand_placeholders(&raw, |_| Some("host".to_string())).unwrap();
            let servers = parse_controllers(&yaml).unwrap_or_else(|e| panic!("{file}: {e:#}"));
            let enabled: usize =
                servers.iter().filter(|s| s.enabled).flat_map(|s| &s.tags).filter(|t| t.enabled).count();
            assert_eq!(enabled, tags, "{file}");
        }
    }

    #[test]
    fn opcua_security_and_credentials_are_parsed_and_validated() {
        let yaml = |extra: &str, endpoint: &str| {
            format!(
                "opcua:\n  servers:\n    - {{name: C, endpoint: \"{endpoint}\", enabled: true, {extra}, tags: []}}\n"
            )
        };
        let ok =
            parse_controllers(&yaml("security: Basic256Sha256_Sign, username: op, password: pw", "opc.tcp://h:4840"))
                .unwrap();
        let sec = ok[0].opc_security().unwrap();
        assert_eq!((sec.policy, sec.mode), (crate::model::OpcPolicy::Basic256Sha256, crate::model::OpcMode::Sign));
        assert_eq!(sec.username.as_deref(), Some("op"));

        // Молча проигнорированная защита — дыра: ошибка в security и логин без пароля останавливают запуск.
        let err = parse_controllers(&yaml("security: Basic999", "opc.tcp://h:4840")).unwrap_err();
        assert!(format!("{err:#}").contains("Basic999"), "{err:#}");
        assert!(parse_controllers(&yaml("username: op", "opc.tcp://h:4840")).is_err(), "логин без пароля");
        // Modbus и PAC защиты на уровне протокола не имеют: настройка не должна создавать иллюзию защиты.
        for endpoint in ["modbus://h:502", "pac://h:10000"] {
            let err = parse_controllers(&yaml("security: Basic256Sha256", endpoint)).unwrap_err();
            assert!(format!("{err:#}").contains("только для OPC UA"), "{err:#}");
            assert!(parse_controllers(&yaml("username: u, password: p", endpoint)).is_err());
        }
        // Выключенный контроллер с ошибкой не мешает запуску остальных.
        let disabled = "opcua:\n  servers:\n    - {name: D, endpoint: \"opc.tcp://h:4840\", enabled: false, security: Nonsense, tags: []}\n";
        assert!(parse_controllers(disabled).is_ok());
    }

    #[test]
    fn tag_history_override_is_parsed() {
        let yaml = r#"
opcua:
  servers:
    - name: c
      endpoint: "opc.tcp://h:4840"
      enabled: true
      tags:
        - {name: a, nodeId: "ns=2;s=1", dataType: FLOAT, pollingRate: 1000, enabled: true, history: {deadband: 0.5, maxIntervalMs: 60000}}
        - {name: b, nodeId: "ns=2;s=2", dataType: FLOAT, pollingRate: 1000, enabled: true}
"#;
        let servers = parse_controllers(yaml).unwrap();
        assert_eq!(
            servers[0].tags[0].history,
            Some(HistoryConfig { deadband: Some(0.5), max_interval_ms: Some(60000), ..Default::default() })
        );
        assert_eq!(servers[0].tags[1].history, None);
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
