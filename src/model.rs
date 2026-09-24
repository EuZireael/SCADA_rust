//! Доменная модель: контроллер, тег, значение, метка времени.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::{Serialize, Serializer};

use crate::config::{ServerConfig, TagConfig};

/// Протокол, по которому тег читается и пишется.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Protocol {
    OpcUa,
    Modbus,
    Pac,
}

/// Тип контроллера — по схеме endpoint (`opc.tcp://`, `modbus://`, `pac://`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControllerKind {
    OpcUa,
    Modbus,
    Pac,
}

impl ControllerKind {
    pub fn from_endpoint(endpoint: &str) -> Option<Self> {
        let e = endpoint.to_ascii_lowercase();
        if e.contains("opc.tcp") {
            Some(Self::OpcUa)
        } else if e.contains("modbus") {
            Some(Self::Modbus)
        } else if e.contains("pac://") {
            Some(Self::Pac)
        } else {
            None
        }
    }
}

/// Тег (канал) с адресацией и правами — неизменяемая часть конфигурации.
#[derive(Debug, Clone)]
pub struct Tag {
    /// PK тега в БД шлюза (0 — работа без БД).
    pub id: i64,
    /// Полный путь канала = Kafka-key телеметрии = адрес команды монитора.
    pub name: String,
    pub node_id: String,
    pub channel_id: Option<i64>,
    pub device_name: Option<String>,
    pub field_name: Option<String>,
    pub device_type: Option<String>,
    pub protocol: Protocol,
    pub protocol_raw: String,
    pub data_type: String,
    pub polling_rate_ms: u64,
    pub enabled: bool,
    pub writable: bool,
    pub unit: Option<String>,
    pub min_value: Option<f64>,
    pub max_value: Option<f64>,
    pub modbus_address: Option<i32>,
    pub modbus_type: Option<String>,
    pub modbus_unit_id: u8,
}

impl Tag {
    pub fn from_config(cfg: &TagConfig) -> Self {
        let protocol_raw = cfg.protocol.clone().unwrap_or_else(|| "opcua".into());
        Tag {
            id: 0,
            name: cfg.name.clone(),
            node_id: cfg.node_id.clone(),
            channel_id: cfg.channel_id,
            device_name: cfg.device_name.clone(),
            field_name: cfg.field_name.clone(),
            device_type: cfg.device_type.clone(),
            protocol: classify(&protocol_raw, &cfg.node_id, cfg.modbus_address),
            protocol_raw,
            data_type: cfg.data_type.clone(),
            polling_rate_ms: cfg.polling_rate,
            enabled: cfg.enabled,
            writable: cfg.writable,
            unit: cfg.unit.clone(),
            min_value: cfg.min_value,
            max_value: cfg.max_value,
            modbus_address: cfg.modbus_address,
            modbus_type: cfg.modbus_type.clone(),
            modbus_unit_id: cfg.modbus_unit_id.unwrap_or(1),
        }
    }
}

/// Протокол тега — как TagProtocols Java-шлюза: явный modbus/pac, иначе OPC UA при
/// protocol=opcua или непустом nodeId; фолбэк Modbus по положительному адресу.
pub fn classify(protocol: &str, node_id: &str, modbus_address: Option<i32>) -> Protocol {
    match protocol.to_ascii_lowercase().as_str() {
        "modbus" => Protocol::Modbus,
        "pac" => Protocol::Pac,
        "opcua" => Protocol::OpcUa,
        _ if modbus_address.is_some_and(|a| a > 0) => Protocol::Modbus,
        _ if !node_id.is_empty() => Protocol::OpcUa,
        _ => Protocol::OpcUa,
    }
}

/// Контроллер со своими тегами.
#[derive(Debug, Clone)]
pub struct Controller {
    /// PK контроллера в БД шлюза (0 — работа без БД).
    pub id: i64,
    pub name: String,
    pub endpoint: String,
    pub kind: ControllerKind,
    pub enabled: bool,
    /// Только включённые теги.
    pub tags: Vec<Arc<Tag>>,
}

impl Controller {
    pub fn from_config(cfg: &ServerConfig) -> Option<Self> {
        Some(Controller {
            id: 0,
            name: cfg.name.clone(),
            endpoint: cfg.endpoint.clone(),
            kind: ControllerKind::from_endpoint(&cfg.endpoint)?,
            enabled: cfg.enabled,
            tags: cfg.tags.iter().filter(|t| t.enabled).map(|t| Arc::new(Tag::from_config(t))).collect(),
        })
    }

    /// Период опроса: минимальный pollingRate тегов, не чаще 100 мс. (Java-шлюз брал
    /// min с 1000 мс и опрашивал раз в секунду при pollingRate 2000.)
    pub fn cycle_period_ms(&self) -> u64 {
        self.tags.iter().map(|t| t.polling_rate_ms).filter(|&r| r > 0).min().unwrap_or(1000).max(100)
    }
}

// ------------------------------------------------------------------- типы данных --

/// Имя типа тега начинается с префикса (INT покрывает INT16/INT32/INTEGER).
fn has_prefix(data_type: &str, prefix: &str) -> bool {
    data_type.trim().to_ascii_uppercase().starts_with(prefix)
}

pub fn is_bool(data_type: &str) -> bool {
    has_prefix(data_type, "BOOL")
}

pub fn is_int(data_type: &str) -> bool {
    has_prefix(data_type, "INT")
}

pub fn is_float(data_type: &str) -> bool {
    has_prefix(data_type, "FLOAT") || has_prefix(data_type, "REAL") || has_prefix(data_type, "DOUBLE")
}

pub fn is_string(data_type: &str) -> bool {
    has_prefix(data_type, "STRING")
}

/// Значение тега. На проводе — типизированный JSON (число/bool/строка): монитор строит
/// график по числу. F32 сериализуется кратчайшим представлением float (64.7, а не
/// 64.69999694824219), как Jackson у Java-шлюза.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(untagged)]
pub enum TagValue {
    Bool(bool),
    Int(i64),
    F32(f32),
    F64(f64),
    Text(String),
}

impl TagValue {
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            TagValue::Bool(b) => Some(if *b { 1.0 } else { 0.0 }),
            TagValue::Int(i) => Some(*i as f64),
            TagValue::F32(f) => Some(*f as f64),
            TagValue::F64(f) => Some(*f),
            TagValue::Text(_) => None,
        }
    }

    pub fn is_numeric(&self) -> bool {
        matches!(self, TagValue::Int(_) | TagValue::F32(_) | TagValue::F64(_))
    }
}

impl std::fmt::Display for TagValue {
    /// Как на проводе: число/bool — JSON-литерал, строка — без кавычек.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TagValue::Text(s) => f.write_str(s),
            other => f.write_str(&serde_json::to_string(other).unwrap_or_default()),
        }
    }
}

/// Качество отсчёта.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Quality {
    Good,
    Bad,
}

impl Quality {
    pub fn as_str(self) -> &'static str {
        match self {
            Quality::Good => "GOOD",
            Quality::Bad => "BAD",
        }
    }
}

impl Serialize for Quality {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(self.as_str())
    }
}

/// Момент времени на проводе — дробные epoch-секунды с наносекундами
/// (`1790233762.061208074`), байт-в-байт как Instant у Jackson в Java-шлюзе.
/// Монитор понимает и это, и ISO-8601.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Timestamp(pub DateTime<Utc>);

impl Timestamp {
    pub fn now() -> Self {
        Timestamp(Utc::now())
    }

    pub fn epoch_seconds_text(&self) -> String {
        format!("{}.{:09}", self.0.timestamp(), self.0.timestamp_subsec_nanos())
    }
}

impl Serialize for Timestamp {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let raw =
            serde_json::value::RawValue::from_string(self.epoch_seconds_text()).map_err(serde::ser::Error::custom)?;
        raw.serialize(s)
    }
}

/// Одно снятое значение тега.
#[derive(Debug, Clone)]
pub struct Reading {
    pub tag: Arc<Tag>,
    /// `None` — значение не снято (BAD).
    pub value: Option<TagValue>,
    pub quality: Quality,
    pub timestamp: Timestamp,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn value_serializes_typed() {
        assert_eq!(serde_json::to_string(&TagValue::F32(64.7)).unwrap(), "64.7");
        assert_eq!(serde_json::to_string(&TagValue::Int(1)).unwrap(), "1");
        assert_eq!(serde_json::to_string(&TagValue::Bool(true)).unwrap(), "true");
        assert_eq!(serde_json::to_string(&TagValue::Text("REC".into())).unwrap(), "\"REC\"");
    }

    #[test]
    fn timestamp_is_epoch_seconds_with_nanos() {
        let ts = Timestamp(Utc.timestamp_opt(1790233762, 61_208_074).unwrap());
        assert_eq!(serde_json::to_string(&ts).unwrap(), "1790233762.061208074");
    }

    #[test]
    fn data_types_by_prefix() {
        assert!(is_int("INT32") && is_int("int16") && is_int("INTEGER"));
        assert!(is_float("FLOAT") && is_float("REAL") && is_float("DOUBLE"));
        assert!(is_bool("BOOLEAN") && is_string("STRING"));
        assert!(!is_int("FLOAT"));
    }

    #[test]
    fn protocol_classification_matches_java() {
        assert_eq!(classify("pac", "pac:385", None), Protocol::Pac);
        assert_eq!(classify("modbus", "modbus:40001", Some(40001)), Protocol::Modbus);
        assert_eq!(classify("opcua", "ns=2;s=6", None), Protocol::OpcUa);
        assert_eq!(classify("", "", Some(40005)), Protocol::Modbus);
    }

    #[test]
    fn cycle_period_uses_polling_rate() {
        let mut t = TagConfig {
            name: "x".into(),
            node_id: "n".into(),
            channel_id: None,
            device_name: None,
            field_name: None,
            device_type: None,
            protocol: None,
            data_type: "FLOAT".into(),
            polling_rate: 2000,
            enabled: true,
            writable: false,
            unit: None,
            min_value: None,
            max_value: None,
            modbus_address: None,
            modbus_type: None,
            modbus_unit_id: None,
        };
        let mut s = ServerConfig {
            id: None,
            name: "c".into(),
            endpoint: "opc.tcp://h:4840".into(),
            security: None,
            username: None,
            password: None,
            enabled: true,
            tags: vec![t.clone()],
        };
        assert_eq!(Controller::from_config(&s).unwrap().cycle_period_ms(), 2000);
        t.polling_rate = 20;
        s.tags.push(t);
        assert_eq!(Controller::from_config(&s).unwrap().cycle_period_ms(), 100);
    }
}
