//! Обработка снятых значений: смена качества → событие, алармы (по флагу), Kafka, история.

use std::collections::HashMap;
use std::sync::Arc;

use serde_json::json;
use tracing::{error, info, warn};

use crate::app::{App, ControllerHandle};
use crate::db::TelemetryRow;
use crate::events::Event;
use crate::messages::{AlarmMessage, TelemetryMessage};
use crate::model::{Quality, Reading, Tag, Timestamp};

/// Обработчик одного контроллера (состояние качества и алармов — свои, без блокировок).
pub struct Processor {
    app: Arc<App>,
    controller: Arc<ControllerHandle>,
    last_quality: HashMap<String, Quality>,
    alarms: AlarmEvaluator,
}

impl Processor {
    pub fn new(app: Arc<App>, controller: Arc<ControllerHandle>) -> Self {
        Processor { app, controller, last_quality: HashMap::new(), alarms: AlarmEvaluator::default() }
    }

    /// Результат цикла опроса.
    pub fn process(&mut self, readings: &[Reading]) {
        let settings = &self.app.settings.gateway;
        let mut to_bad = Vec::new();
        let mut to_good = Vec::new();
        let mut history = Vec::new();

        for r in readings {
            if let Some(prev) = self.last_quality.insert(r.tag.name.clone(), r.quality)
                && prev != r.quality
            {
                if r.quality == Quality::Bad {
                    to_bad.push(r.tag.name.clone())
                } else {
                    to_good.push(r.tag.name.clone())
                }
            }

            if settings.alarms_enabled
                && let Some(v) = r.value.as_ref().filter(|v| v.is_numeric()).and_then(|v| v.as_f64())
            {
                self.alarms.evaluate(&self.app, &self.controller, &r.tag, v);
            }

            if (r.value.is_some() || settings.send_bad_frames)
                && let Some(kafka) = &self.app.kafka
            {
                kafka.send_telemetry(
                    &r.tag.name,
                    &TelemetryMessage { value: r.value.clone(), quality: r.quality, timestamp: r.timestamp },
                );
                self.app.metrics.telemetry_sent.inc();
            }

            if self.app.telemetry.is_some() && r.tag.id > 0 {
                history.push(TelemetryRow {
                    tag_id: r.tag.id,
                    time: r.timestamp.0,
                    quality: r.quality.as_str(),
                    value: r.value.clone(),
                });
            }
        }

        if let Some(sink) = &self.app.telemetry
            && !history.is_empty()
        {
            sink.push(history);
        }
        self.emit_quality_change(to_bad, to_good);
    }

    /// Смена качества — одно сводное событие на цикл. (Java-шлюз писал событие на КАЖДЫЙ
    /// тег: обрыв OPC UA-контроллера давал 2133 строки в журнале и столько же в Kafka.)
    fn emit_quality_change(&self, to_bad: Vec<String>, to_good: Vec<String>) {
        let total = to_bad.len() + to_good.len();
        if total == 0 {
            return;
        }
        let severity = if to_bad.is_empty() { "INFO" } else { "WARNING" };
        let sample: Vec<&String> = to_bad.iter().chain(to_good.iter()).take(20).collect();
        self.app.events.emit(
            Event::new(
                "QUALITY_CHANGE",
                "Gateway",
                severity,
                format!(
                    "{}: качество сменилось у {total} тегов (→BAD {}, →GOOD {})",
                    self.controller.ctrl.name,
                    to_bad.len(),
                    to_good.len()
                ),
            )
            .controller(self.controller.ctrl.id)
            .details(json!({"toBad": to_bad.len(), "toGood": to_good.len(), "tags": sample})),
        );
    }
}

/// Кадры BAD для всех тегов (обрыв на уровне запроса).
pub fn all_bad(tags: &[Arc<Tag>]) -> Vec<Reading> {
    let ts = Timestamp::now();
    tags.iter().map(|t| Reading { tag: t.clone(), value: None, quality: Quality::Bad, timestamp: ts }).collect()
}

// ---------------------------------------------------------------------- алармы --

struct ActiveAlarm {
    condition: &'static str,
    alarm_id: String,
    severity: &'static str,
    threshold: f64,
}

/// Edge-триггер по minValue/maxValue: аларм — один раз при выходе за предел, снятие — один
/// раз при возврате в норму с гистерезисом 2 % диапазона. Логика как у Java AlarmEvaluator.
#[derive(Default)]
pub struct AlarmEvaluator {
    active: HashMap<String, ActiveAlarm>,
}

impl AlarmEvaluator {
    pub fn evaluate(&mut self, app: &App, ctrl: &ControllerHandle, tag: &Tag, value: f64) {
        if let Some(action) = self.decide(tag, value) {
            match action {
                AlarmAction::Raise { alarm_id, severity, message, threshold, cleared_previous } => {
                    if let Some(prev) = cleared_previous {
                        publish_clear(app, ctrl, tag, &prev, value);
                    }
                    if severity == "CRITICAL" {
                        error!("🚨 ALARM [{severity}]: {message}")
                    } else {
                        warn!("🚨 ALARM [{severity}]: {message}")
                    }
                    app.events.emit(
                        Event::new("ALARM", "Gateway", severity, message.clone())
                            .tag(tag.id)
                            .controller(ctrl.ctrl.id)
                            .details(json!({"threshold": threshold, "currentValue": value, "tagName": tag.name, "unit": tag.unit})),
                    );
                    publish_alarm(app, ctrl, tag, &alarm_id, severity, &message, threshold, value, false);
                }
                AlarmAction::Clear(prev) => publish_clear(app, ctrl, tag, &prev, value),
            }
        }
    }

    fn decide(&mut self, tag: &Tag, value: f64) -> Option<AlarmAction> {
        let (min, max) = (tag.min_value, tag.max_value);
        if min.is_none() && max.is_none() {
            return None;
        }
        let unit = tag.unit.as_deref().unwrap_or("");
        let range = match (min, max) {
            (Some(lo), Some(hi)) => (hi - lo).max(0.0001),
            _ => value.abs().max(1.0),
        };
        let deadband = 0.02 * range;
        let violation = match (min, max) {
            (_, Some(hi)) if value > hi => Some((
                "HIGH",
                if value > hi + 0.3 * range { "CRITICAL" } else { "MAJOR" },
                hi,
                format!("High value: {value:.2} > {hi:.2} {unit}"),
            )),
            (Some(lo), _) if value < lo => Some((
                "LOW",
                if value < lo - 0.3 * range { "MAJOR" } else { "MINOR" },
                lo,
                format!("Low value: {value:.2} < {lo:.2} {unit}"),
            )),
            _ => None,
        };
        match violation {
            Some((condition, severity, threshold, message)) => {
                if self.active.get(&tag.name).is_some_and(|a| a.condition == condition) {
                    return None; // то же нарушение — молчим (анти-флуд)
                }
                let alarm_id = format!("ALARM_{}_{}_{}", tag.id, condition, chrono::Utc::now().timestamp_millis());
                let previous = self.active.insert(
                    tag.name.clone(),
                    ActiveAlarm { condition, alarm_id: alarm_id.clone(), severity, threshold },
                );
                Some(AlarmAction::Raise { alarm_id, severity, message, threshold, cleared_previous: previous })
            }
            None => {
                let back = min.is_none_or(|lo| value >= lo + deadband) && max.is_none_or(|hi| value <= hi - deadband);
                if back { self.active.remove(&tag.name).map(AlarmAction::Clear) } else { None }
            }
        }
    }
}

enum AlarmAction {
    Raise {
        alarm_id: String,
        severity: &'static str,
        message: String,
        threshold: f64,
        cleared_previous: Option<ActiveAlarm>,
    },
    Clear(ActiveAlarm),
}

fn publish_clear(app: &App, ctrl: &ControllerHandle, tag: &Tag, prev: &ActiveAlarm, value: f64) {
    info!("✅ Аларм снят: {} ({value:.2})", tag.name);
    app.events.emit(
        Event::new("ALARM_CLEARED", "Gateway", "INFO", format!("Alarm cleared for {} ({value:.2})", tag.name))
            .tag(tag.id)
            .controller(ctrl.ctrl.id)
            .details(json!({"tagName": tag.name, "alarmId": prev.alarm_id})),
    );
    publish_alarm(
        app,
        ctrl,
        tag,
        &prev.alarm_id,
        prev.severity,
        &format!("Cleared: {value:.2} back to normal"),
        prev.threshold,
        value,
        true,
    );
}

#[allow(clippy::too_many_arguments)]
fn publish_alarm(
    app: &App,
    ctrl: &ControllerHandle,
    tag: &Tag,
    alarm_id: &str,
    severity: &str,
    message: &str,
    threshold: f64,
    value: f64,
    cleared: bool,
) {
    if let Some(kafka) = &app.kafka {
        kafka.send_alarm(&AlarmMessage {
            message_id: uuid::Uuid::new_v4().to_string(),
            kind: "ALARM",
            alarm_id: alarm_id.to_string(),
            // tagId на проводе — id канала общей базы (как у Java-шлюза), иначе PK шлюза.
            tag_id: tag.channel_id.or((tag.id > 0).then_some(tag.id)),
            tag_name: tag.name.clone(),
            severity: severity.to_string(),
            message: message.to_string(),
            threshold,
            current_value: value,
            timestamp: Timestamp::now(),
            acknowledged: false,
            cleared,
            controller_id: (ctrl.ctrl.id > 0).then_some(ctrl.ctrl.id),
            controller_name: Some(ctrl.ctrl.name.clone()),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::TagConfig;

    fn tag(min: Option<f64>, max: Option<f64>) -> Tag {
        Tag::from_config(&TagConfig {
            name: "T".into(),
            node_id: "ns=2;s=1".into(),
            channel_id: Some(1),
            device_name: None,
            field_name: None,
            device_type: None,
            protocol: None,
            data_type: "FLOAT".into(),
            polling_rate: 1000,
            enabled: true,
            writable: false,
            unit: None,
            min_value: min,
            max_value: max,
            modbus_address: None,
            modbus_type: None,
            modbus_unit_id: None,
        })
    }

    fn kind(a: &Option<AlarmAction>) -> &'static str {
        match a {
            Some(AlarmAction::Raise { severity, .. }) => severity,
            Some(AlarmAction::Clear(_)) => "CLEAR",
            None => "-",
        }
    }

    #[test]
    fn edge_trigger_with_hysteresis() {
        let t = tag(Some(0.0), Some(100.0));
        let mut ev = AlarmEvaluator::default();
        assert_eq!(kind(&ev.decide(&t, 50.0)), "-");
        assert_eq!(kind(&ev.decide(&t, 110.0)), "MAJOR");
        assert_eq!(kind(&ev.decide(&t, 120.0)), "-", "то же нарушение — без повторов");
        assert_eq!(kind(&ev.decide(&t, 99.0)), "-", "в полосе гистерезиса — ещё не норма");
        assert_eq!(kind(&ev.decide(&t, 97.0)), "CLEAR");
        assert_eq!(kind(&ev.decide(&t, 97.0)), "-");
    }

    #[test]
    fn severity_by_distance_and_direction_change() {
        let t = tag(Some(0.0), Some(100.0));
        let mut ev = AlarmEvaluator::default();
        assert_eq!(kind(&ev.decide(&t, 140.0)), "CRITICAL");
        match ev.decide(&t, -40.0) {
            Some(AlarmAction::Raise { severity, cleared_previous, .. }) => {
                assert_eq!(severity, "MAJOR");
                assert!(cleared_previous.is_some(), "смена направления закрывает прежний аларм");
            }
            _ => panic!("ожидался новый аларм"),
        }
    }

    #[test]
    fn no_limits_no_alarms() {
        assert_eq!(kind(&AlarmEvaluator::default().decide(&tag(None, None), 1e9)), "-");
    }
}
