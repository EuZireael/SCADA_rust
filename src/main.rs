//! SCADA Gateway на Rust: опрос контроллеров (OPC UA, Modbus TCP, PAC driver-master) →
//! Kafka для монитора; приём команд записи; журнал событий в PostgreSQL.
//!
//! Контракты — как у Java-шлюза (controllers.yaml, топики и формат Kafka, статусы команд,
//! env-переменные, /actuator/health и метрики), поэтому встаёт на его место без правок.

mod app;
mod command;
mod config;
mod db;
mod events;
mod http;
mod kafka;
mod messages;
mod metrics;
mod modbus;
mod model;
mod opcua;
mod pac;
mod poller;
mod telemetry;

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use serde_json::json;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

use crate::app::App;
use crate::command::Dedup;
use crate::config::Settings;
use crate::events::Event;
use crate::kafka::KafkaOut;
use crate::metrics::Metrics;
use crate::model::Controller;

fn main() -> Result<()> {
    // `scada-gateway healthcheck` — проверка для HEALTHCHECK контейнера без curl в образе.
    if std::env::args().nth(1).as_deref() == Some("healthcheck") {
        std::process::exit(if healthcheck() { 0 } else { 1 });
    }
    tokio::runtime::Builder::new_multi_thread().enable_all().build()?.block_on(run())
}

/// GET /actuator/health на своём порту: 200 и status UP.
fn healthcheck() -> bool {
    use std::io::{Read, Write};
    let port: u16 = std::env::var("SERVER_PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(8888);
    let Ok(mut s) = std::net::TcpStream::connect_timeout(&([127, 0, 0, 1], port).into(), Duration::from_secs(3)) else {
        return false;
    };
    let _ = s.set_read_timeout(Some(Duration::from_secs(3)));
    if s.write_all(b"GET /actuator/health HTTP/1.0\r\nHost: localhost\r\n\r\n").is_err() {
        return false;
    }
    let mut resp = String::new();
    let _ = s.read_to_string(&mut resp);
    resp.split_whitespace().nth(1) == Some("200") && resp.contains(r#""status":"UP""#)
}

async fn run() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| {
            EnvFilter::new("info,opcua_client=error,opcua_crypto=error,opcua_core=warn,rdkafka=warn,sqlx=warn")
        }))
        .with_target(false)
        .compact()
        .init();

    let settings = Settings::from_env()?;
    info!("SCADA Gateway (Rust) {} стартует", env!("CARGO_PKG_VERSION"));

    // --- Конфигурация контроллеров ---
    let servers = config::load_controllers(&settings.controllers_path)?;
    let yaml_names: Vec<String> = servers.iter().map(|s| s.name.clone()).collect();
    let mut controllers: Vec<Controller> = Vec::new();
    for s in &servers {
        match Controller::from_config(s) {
            Some(c) => controllers.push(c),
            None => warn!("Неизвестный протокол контроллера {}: {}", s.name, s.endpoint),
        }
    }

    // --- БД: схема, синхронизация тегов с YAML (id тегов для команд и истории) ---
    let db = match &settings.db {
        Some(db_settings) => {
            let pool = db::connect(db_settings).await?;
            db::sync_config(&pool, &mut controllers, &yaml_names).await.context("синхронизация с YAML")?;
            Some(pool)
        }
        None => {
            warn!("БД выключена (DB_ENABLED=false): журнал и история не пишутся");
            None
        }
    };
    let enabled: Vec<Controller> = controllers.into_iter().filter(|c| c.enabled).collect();
    let tag_count: usize = enabled.iter().map(|c| c.tags.len()).sum();
    info!("Загружено {} контроллеров, {tag_count} тегов", enabled.len());

    // --- Kafka, метрики, очереди событий и истории ---
    let metrics = Arc::new(Metrics::new());
    let kafka = if settings.kafka.enabled {
        kafka::ensure_topics(&settings.kafka).await;
        Some(Arc::new(KafkaOut::new(&settings.kafka, metrics.clone())?))
    } else {
        warn!("Kafka выключена (KAFKA_ENABLED=false)");
        None
    };
    let cancel = CancellationToken::new();
    let mut background = JoinSet::new();

    let (events, events_rx) = events::channel(metrics.clone());
    background.spawn(events::run_writer(events_rx, db.clone(), kafka.clone(), cancel.clone()));

    let telemetry_sink = match (&db, settings.gateway.persist_telemetry) {
        (Some(pool), true) => {
            let (sink, rx) = events::telemetry_channel(metrics.clone());
            background.spawn(events::run_telemetry_writer(
                rx,
                pool.clone(),
                settings.gateway.telemetry_retention,
                cancel.clone(),
            ));
            info!(
                "История телеметрии пишется в БД (retention {} ч)",
                settings.gateway.telemetry_retention.as_secs() / 3600
            );
            Some(sink)
        }
        _ => None,
    };

    info!(
        "⚙ alarms={} persist-telemetry={} send-bad-frames={}",
        settings.gateway.alarms_enabled,
        telemetry_sink.is_some(),
        settings.gateway.send_bad_frames
    );
    let http_port = settings.http_port;
    let app = Arc::new(App::new(settings, metrics, kafka.clone(), events.clone(), telemetry_sink, db, enabled));
    events.emit(
        Event::new("SYSTEM", "Gateway", "INFO", "SCADA Gateway starting up")
            .details(json!({"controllers": app.controllers.len(), "tags": app.tag_count(), "runtime": "rust"})),
    );

    // --- Опрос контроллеров ---
    let mut pollers = JoinSet::new();
    for handle in &app.controllers {
        let name = handle.ctrl.name.clone();
        let fut = poller::run(app.clone(), handle.clone(), cancel.clone());
        pollers.spawn(async move {
            fut.await;
            name
        });
    }
    background.spawn(poller::health_log(app.clone(), cancel.clone()));
    background.spawn(poller::heartbeat(app.clone(), cancel.clone()));

    // --- Команды из Kafka ---
    if app.kafka.is_some() {
        let app_cmd = app.clone();
        let dedup = Arc::new(Dedup::new(Duration::from_secs(60), 1000));
        background.spawn(kafka::consume_commands(app.settings.kafka.clone(), cancel.clone(), move |cmd| {
            let app = app_cmd.clone();
            let dedup = dedup.clone();
            async move { command::handle(&app, &dedup, cmd).await }
        }));
    }

    // --- HTTP ---
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", http_port)).await.context("HTTP-порт занят")?;
    info!("HTTP на :{http_port} (/actuator/health, /actuator/prometheus, /api/events)");
    let http_cancel = cancel.clone();
    background.spawn(async move {
        let server =
            axum::serve(listener, http::router(app.clone())).with_graceful_shutdown(http_cancel.cancelled_owned());
        if let Err(e) = server.await {
            warn!("HTTP-сервер остановлен с ошибкой: {e}");
        }
    });

    // --- Остановка по SIGTERM/SIGINT ---
    shutdown_signal().await;
    info!("Остановка...");
    events.emit(Event::new("SYSTEM", "Gateway", "INFO", "SCADA Gateway shutting down"));
    cancel.cancel();
    let stop = async {
        while let Some(res) = pollers.join_next().await {
            if let Ok(name) = &res {
                info!("Опрос {name} остановлен");
            }
        }
        while background.join_next().await.is_some() {}
    };
    if tokio::time::timeout(Duration::from_secs(8), stop).await.is_err() {
        warn!("Не все задачи остановились за 8 с");
    }
    if let Some(k) = &kafka {
        k.flush(Duration::from_secs(3));
    }
    info!("Остановлено");
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("SIGTERM");
        tokio::select! {
            _ = ctrl_c => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    ctrl_c.await.ok();
}
