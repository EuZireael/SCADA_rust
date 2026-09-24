//! Сообщения Kafka — поля и формат как у Java-шлюза (контракт с монитором).

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::model::{Quality, TagValue, Timestamp};

/// Телеметрия → `scada.tags`, ключ = путь канала. Ровно три поля (аналог OPC UA DataValue):
/// всё статическое монитор берёт из своего реестра по ключу.
#[derive(Debug, Serialize)]
pub struct TelemetryMessage {
    /// Типизированное значение; `null` — кадр потери связи (quality=BAD).
    pub value: Option<TagValue>,
    pub quality: Quality,
    /// Момент снятия значения (sourceTimestamp OPC UA, момент чтения Modbus/PAC).
    pub timestamp: Timestamp,
}

/// Событие → `scada-events`, ключ = eventType.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EventMessage {
    pub message_id: String,
    #[serde(rename = "type")]
    pub kind: &'static str,
    pub event_type: String,
    pub source: String,
    pub severity: String,
    pub message: String,
    pub timestamp: Timestamp,
    pub details: Value,
}

/// Аларм → `scada-alarms`, ключ = путь канала. cleared=true — снятие.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AlarmMessage {
    pub message_id: String,
    #[serde(rename = "type")]
    pub kind: &'static str,
    pub alarm_id: String,
    pub tag_id: Option<i64>,
    pub tag_name: String,
    pub severity: String,
    pub message: String,
    pub threshold: f64,
    pub current_value: f64,
    pub timestamp: Timestamp,
    pub acknowledged: bool,
    pub cleared: bool,
    pub controller_id: Option<i64>,
    pub controller_name: Option<String>,
}

/// Команда из `scada-commands`. Монитор адресует тег по имени (tagName); tagId — для
/// старого Monitor Srv. Незнакомые поля игнорируются.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommandMessage {
    pub command_id: Option<String>,
    pub tag_id: Option<i64>,
    pub tag_name: Option<String>,
    /// Подсказка типа. Монитор её не шлёт: тип берётся из конфигурации тега.
    pub data_type: Option<String>,
    #[serde(default)]
    pub value: Value,
    pub requested_by: Option<String>,
}

/// Результат команды → `scada-command-results`, ключ = tagName (или commandId).
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CommandResultMessage {
    pub command_id: Option<String>,
    pub tag_id: Option<i64>,
    pub tag_name: Option<String>,
    pub status: &'static str,
    pub success: bool,
    pub message: String,
    pub applied_value: Option<TagValue>,
    pub timestamp: Timestamp,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn ts() -> Timestamp {
        Timestamp(chrono::Utc.timestamp_opt(1790233762, 61_208_074).unwrap())
    }

    #[test]
    fn telemetry_wire_is_exactly_three_fields() {
        let msg = TelemetryMessage { value: Some(TagValue::F32(1.07)), quality: Quality::Good, timestamp: ts() };
        assert_eq!(
            serde_json::to_string(&msg).unwrap(),
            r#"{"value":1.07,"quality":"GOOD","timestamp":1790233762.061208074}"#
        );
    }

    #[test]
    fn bad_frame_has_null_value() {
        let msg = TelemetryMessage { value: None, quality: Quality::Bad, timestamp: ts() };
        assert!(serde_json::to_string(&msg).unwrap().starts_with(r#"{"value":null,"quality":"BAD""#));
    }

    #[test]
    fn command_from_monitor_is_parsed() {
        let raw = r#"{"commandId":"c1","tagName":"A.B.LINE1V0.M","value":1,"requestedBy":"scada-runtime","timestamp":"2026-09-24T08:00:00Z","extra":true}"#;
        let cmd: CommandMessage = serde_json::from_str(raw).unwrap();
        assert_eq!(cmd.command_id.as_deref(), Some("c1"));
        assert_eq!(cmd.tag_name.as_deref(), Some("A.B.LINE1V0.M"));
        assert_eq!(cmd.value, serde_json::json!(1));
        assert!(cmd.tag_id.is_none() && cmd.data_type.is_none());
    }

    #[test]
    fn result_uses_java_field_names() {
        let r = CommandResultMessage {
            command_id: Some("c1".into()),
            tag_id: None,
            tag_name: Some("T".into()),
            status: "APPLIED",
            success: true,
            message: "ok".into(),
            applied_value: Some(TagValue::Int(1)),
            timestamp: ts(),
        };
        let json: Value = serde_json::to_value(&r).unwrap();
        for field in ["commandId", "tagId", "tagName", "status", "success", "message", "appliedValue", "timestamp"] {
            assert!(json.get(field).is_some(), "нет поля {field}");
        }
    }
}
