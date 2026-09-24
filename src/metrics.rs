//! Метрики Prometheus (`/actuator/prometheus`). Имена — как у Java-шлюза (Micrometer),
//! чтобы дашборд Grafana работал без правок: scada_telemetry_sent_total,
//! scada_commands_total{status}, scada_controllers_connected, scada_controllers_total.

use std::collections::HashMap;

use prometheus::{Encoder, IntCounter, IntCounterVec, IntGauge, Opts, Registry, TextEncoder};

pub struct Metrics {
    registry: Registry,
    pub telemetry_sent: IntCounter,
    pub commands: IntCounterVec,
    pub controllers_connected: IntGauge,
    pub controllers_total: IntGauge,
    pub kafka_send_errors: IntCounter,
    pub events_dropped: IntCounter,
    pub telemetry_rows_dropped: IntCounter,
}

impl Metrics {
    pub fn new() -> Self {
        let labels = HashMap::from([("application".to_string(), "scada-gateway".to_string())]);
        let registry = Registry::new_custom(None, Some(labels)).expect("реестр метрик");
        let counter = |name: &str, help: &str| {
            let c = IntCounter::new(name, help).expect("метрика");
            registry.register(Box::new(c.clone())).expect("регистрация метрики");
            c
        };
        let gauge = |name: &str, help: &str| {
            let g = IntGauge::new(name, help).expect("метрика");
            registry.register(Box::new(g.clone())).expect("регистрация метрики");
            g
        };
        let commands = IntCounterVec::new(Opts::new("scada_commands_total", "Команды записи по исходу"), &["status"])
            .expect("метрика");
        registry.register(Box::new(commands.clone())).expect("регистрация метрики");
        #[cfg(target_os = "linux")]
        registry
            .register(Box::new(prometheus::process_collector::ProcessCollector::for_self()))
            .expect("метрики процесса");

        Metrics {
            telemetry_sent: counter("scada_telemetry_sent_total", "Отправлено точек телеметрии в Kafka"),
            commands,
            controllers_connected: gauge("scada_controllers_connected", "Контроллеров на связи"),
            controllers_total: gauge("scada_controllers_total", "Всего сконфигурировано контроллеров"),
            kafka_send_errors: counter("scada_kafka_send_errors_total", "Ошибки постановки/доставки в Kafka"),
            events_dropped: counter("scada_events_dropped_total", "События, отброшенные при переполнении очереди"),
            telemetry_rows_dropped: counter(
                "scada_telemetry_rows_dropped_total",
                "Точки истории, отброшенные при переполнении очереди записи в БД",
            ),
            registry,
        }
    }

    /// Текстовый формат Prometheus.
    pub fn render(&self) -> String {
        let mut buf = Vec::new();
        TextEncoder::new().encode(&self.registry.gather(), &mut buf).ok();
        String::from_utf8(buf).unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metric_names_match_java_gateway() {
        let m = Metrics::new();
        m.telemetry_sent.inc();
        m.commands.with_label_values(&["APPLIED"]).inc();
        let text = m.render();
        for name in ["scada_telemetry_sent_total", "scada_commands_total", "scada_controllers_connected"] {
            assert!(text.contains(name), "нет метрики {name}");
        }
        assert!(text.contains(r#"application="scada-gateway""#));
    }
}
