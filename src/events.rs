//! События шлюза: журнал event_log в БД + публикация в `scada-events`.
//!
//! Источники (опрос, команды, heartbeat) кладут событие в очередь и идут дальше; писатель
//! в отдельной задаче пачкой вставляет строки в БД и отправляет события в Kafka. Переполнение
//! очереди — событие отбрасывается со счётчиком, но опрос не ждёт никогда.

use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};
use sqlx::PgPool;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::db::{self, EventRow, TelemetryRow};
use crate::kafka::KafkaOut;
use crate::leadership::Leadership;
use crate::messages::EventMessage;
use crate::metrics::Metrics;
use crate::model::Timestamp;

#[derive(Debug, Clone)]
pub struct Event {
    pub event_type: String,
    pub source: String,
    pub severity: String,
    pub message: String,
    pub tag_id: Option<i64>,
    pub controller_id: Option<i64>,
    pub details: Value,
    pub time: Timestamp,
}

impl Event {
    pub fn new(event_type: &str, source: &str, severity: &str, message: impl Into<String>) -> Self {
        Event {
            event_type: event_type.into(),
            source: source.into(),
            severity: severity.into(),
            message: message.into(),
            tag_id: None,
            controller_id: None,
            details: json!({}),
            time: Timestamp::now(),
        }
    }

    pub fn details(mut self, details: Value) -> Self {
        self.details = details;
        self
    }

    pub fn controller(mut self, id: i64) -> Self {
        self.controller_id = (id > 0).then_some(id);
        self
    }

    pub fn tag(mut self, id: i64) -> Self {
        self.tag_id = (id > 0).then_some(id);
        self
    }
}

/// Отправитель событий (дёшево клонируется).
#[derive(Clone)]
pub struct EventSink {
    tx: mpsc::Sender<Event>,
    metrics: Arc<Metrics>,
}

impl EventSink {
    pub fn emit(&self, event: Event) {
        if self.tx.try_send(event).is_err() {
            self.metrics.events_dropped.inc();
        }
    }
}

pub fn channel(metrics: Arc<Metrics>) -> (EventSink, mpsc::Receiver<Event>) {
    let (tx, rx) = mpsc::channel(10_000);
    (EventSink { tx, metrics }, rx)
}

/// Писатель событий: Kafka — сразу, БД — пачками до 500 строк.
pub async fn run_writer(
    mut rx: mpsc::Receiver<Event>,
    db: Option<PgPool>,
    kafka: Option<Arc<KafkaOut>>,
    leadership: Arc<Leadership>,
    cancel: CancellationToken,
) {
    let mut batch: Vec<Event> = Vec::with_capacity(500);
    loop {
        let closed = tokio::select! {
            n = rx.recv_many(&mut batch, 500) => n == 0,
            _ = cancel.cancelled() => { rx.close(); while let Ok(e) = rx.try_recv() { batch.push(e); } true }
        };
        // Пара горячего резерва пишет в общую БД: помечаем, чей экземпляр записал событие.
        if leadership.is_ha_enabled() {
            let role = if leadership.is_active() { "ACTIVE" } else { "STANDBY" };
            for e in &mut batch {
                if let Value::Object(map) = &mut e.details {
                    map.insert("instance".into(), json!(leadership.instance_id()));
                    map.insert("role".into(), json!(role));
                }
            }
        }
        if let Some(kafka) = &kafka {
            for e in &batch {
                kafka.send_event(&EventMessage {
                    message_id: uuid::Uuid::new_v4().to_string(),
                    kind: "EVENT",
                    event_type: e.event_type.clone(),
                    source: e.source.clone(),
                    severity: e.severity.clone(),
                    message: e.message.clone(),
                    timestamp: e.time,
                    details: e.details.clone(),
                });
            }
        }
        if let Some(pool) = &db {
            let rows: Vec<EventRow> = batch
                .iter()
                .map(|e| EventRow {
                    time: e.time.0,
                    event_type: e.event_type.clone(),
                    source: e.source.clone(),
                    severity: e.severity.clone(),
                    message: e.message.clone(),
                    tag_id: e.tag_id,
                    controller_id: e.controller_id,
                    details: (!e.details.as_object().is_some_and(|m| m.is_empty())).then(|| e.details.to_string()),
                })
                .collect();
            if let Err(err) = db::insert_events(pool, &rows).await {
                warn!("Журнал: {} событий не записано в БД: {err:#}", rows.len());
            }
        }
        batch.clear();
        if closed {
            return;
        }
    }
}

/// Очередь записи истории телеметрии (включается GATEWAY_PERSIST_TELEMETRY).
#[derive(Clone)]
pub struct TelemetrySink {
    tx: mpsc::Sender<Vec<TelemetryRow>>,
    metrics: Arc<Metrics>,
}

impl TelemetrySink {
    /// Пачка точек цикла опроса. Очередь полна (БД не успевает) — точки отбрасываются.
    pub fn push(&self, rows: Vec<TelemetryRow>) {
        let n = rows.len() as u64;
        if self.tx.try_send(rows).is_err() {
            self.metrics.telemetry_rows_dropped.inc_by(n);
        }
    }
}

pub fn telemetry_channel(metrics: Arc<Metrics>) -> (TelemetrySink, mpsc::Receiver<Vec<TelemetryRow>>) {
    let (tx, rx) = mpsc::channel(64);
    (TelemetrySink { tx, metrics }, rx)
}

/// Писатель истории: вставка пачками + удаление старше retention раз в 10 минут.
pub async fn run_telemetry_writer(
    mut rx: mpsc::Receiver<Vec<TelemetryRow>>,
    pool: PgPool,
    retention: Duration,
    cancel: CancellationToken,
) {
    let mut prune = tokio::time::interval(Duration::from_secs(600));
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = prune.tick() => match db::prune_telemetry(&pool, retention).await {
                Ok(n) if n > 0 => tracing::info!("История: удалено {n} точек старше {} ч", retention.as_secs() / 3600),
                Ok(_) => {}
                Err(e) => warn!("История: очистка не удалась: {e:#}"),
            },
            rows = rx.recv() => match rows {
                Some(rows) => if let Err(e) = db::insert_telemetry(&pool, &rows).await {
                    warn!("История: {} точек не записано: {e:#}", rows.len());
                },
                None => return,
            },
        }
    }
}
