//! SCADA Gateway на Rust: опрос контроллеров (OPC UA, Modbus TCP, PAC driver-master) →
//! Kafka для монитора; приём команд записи; журнал событий в PostgreSQL.
//!
//! Контракты — как у Java-шлюза (controllers.yaml, топики и формат Kafka, статусы команд,
//! env-переменные, /actuator/health и метрики), поэтому встаёт на его место без правок.

use scada_gateway::{command, config, db, events, ha, http, kafka, poller};

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use serde_json::json;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

use scada_gateway::app::{App, AppDeps};
use scada_gateway::command::Dedup;
use scada_gateway::config::Settings;
use scada_gateway::events::Event;
use scada_gateway::kafka::KafkaOut;
use scada_gateway::leadership::Leadership;
use scada_gateway::metrics::Metrics;
use scada_gateway::model::Controller;
use scada_gateway::script::Scripts;
use scada_gateway::supervisor::supervise;

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

    // --- Kafka, метрики, роль в паре, очереди событий и истории ---
    let metrics = Arc::new(Metrics::new());
    let ha_settings = settings.gateway.ha.clone();
    let leadership = if ha_settings.enabled {
        if !settings.kafka.enabled {
            anyhow::bail!("горячее резервирование (GATEWAY_HA_ENABLED) работает через Kafka: включите KAFKA_ENABLED");
        }
        Leadership::standby(
            ha_settings.instance_id.clone(),
            ha_settings.group_id.clone(),
            metrics.ha_gauge(&ha_settings.instance_id),
        )
    } else {
        Leadership::single(ha_settings.instance_id.clone(), metrics.ha_gauge(&ha_settings.instance_id))
    };
    let kafka = if settings.kafka.enabled {
        kafka::ensure_topics(&settings.kafka).await;
        if !settings.kafka.client.0.is_empty() {
            info!("🔐 Kafka: {}", settings.kafka.client.summary());
        }
        Some(Arc::new(KafkaOut::new(&settings.kafka, metrics.clone(), leadership.clone())?))
    } else {
        warn!("Kafka выключена (KAFKA_ENABLED=false)");
        None
    };
    let cancel = CancellationToken::new();
    let mut background = JoinSet::new();
    // Задачи под надзором: упавшая перезапускается (см. supervisor.rs).
    let mut supervised: Vec<tokio::task::JoinHandle<()>> = Vec::new();

    let (events, events_rx) = events::channel(metrics.clone());
    background.spawn(events::run_writer(
        events_rx,
        db.clone(),
        kafka.clone(),
        leadership.clone(),
        metrics.clone(),
        cancel.clone(),
    ));

    // --- Пользовательские скрипты: ошибка при старте — ошибка старта шлюза ---
    let tag_names: Vec<String> = enabled.iter().flat_map(|c| c.tags.iter().map(|t| t.name.clone())).collect();
    let scripts = Scripts::load(
        settings.gateway.scripts.clone(),
        tag_names,
        metrics.script_errors.clone(),
        metrics.scripts_bound_tags.clone(),
        events.clone(),
    )?;
    supervised.push(supervise("scripts-reload", cancel.clone(), metrics.clone(), events.clone(), {
        let (scripts, cancel) = (scripts.clone(), cancel.clone());
        move || scripts.clone().run_reload(cancel.clone())
    }));

    let telemetry_sink = match (&db, settings.gateway.persist_telemetry) {
        (Some(pool), true) => {
            let (sink, rx) = events::telemetry_channel(metrics.clone());
            background.spawn(events::run_telemetry_writer(
                rx,
                pool.clone(),
                settings.gateway.telemetry_retention,
                metrics.clone(),
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
    let http_bind = settings.http_bind.clone();
    let api_protected = settings.api_token.is_some();
    let kafka_settings = settings.kafka.clone();
    let app = Arc::new(App::new(
        settings,
        AppDeps {
            metrics,
            kafka: kafka.clone(),
            events: events.clone(),
            telemetry: telemetry_sink,
            db,
            leadership: leadership.clone(),
            scripts,
        },
        enabled,
    ));
    events.emit(
        Event::new("SYSTEM", "Gateway", "INFO", "SCADA Gateway starting up")
            .details(json!({"controllers": app.controllers.len(), "tags": app.tag_count(), "runtime": "rust"})),
    );

    // --- Горячее резервирование: выборы и журнал смены роли ---
    let blind_app = app.clone();
    let elector = ha::spawn(
        ha_settings,
        kafka_settings,
        leadership.clone(),
        Arc::new(move || blind_app.all_links_down()),
        cancel.clone(),
    );
    supervised.push(supervise("ha-events", cancel.clone(), app.metrics.clone(), events.clone(), {
        let (leadership, events, cancel) = (leadership.clone(), events.clone(), cancel.clone());
        move || ha::record_events(leadership.clone(), events.clone(), cancel.clone())
    }));

    // --- Опрос контроллеров: каждая задача под надзором (упала — перезапускается) ---
    for handle in &app.controllers {
        let (app, handle_, cancel_) = (app.clone(), handle.clone(), cancel.clone());
        supervised.push(supervise(
            &format!("опрос {}", handle.ctrl.name),
            cancel.clone(),
            app.metrics.clone(),
            events.clone(),
            move || poller::run(app.clone(), handle_.clone(), cancel_.clone()),
        ));
    }
    supervised.push(supervise("health-log", cancel.clone(), app.metrics.clone(), events.clone(), {
        let (app, cancel) = (app.clone(), cancel.clone());
        move || poller::health_log(app.clone(), cancel.clone())
    }));
    supervised.push(supervise("heartbeat", cancel.clone(), app.metrics.clone(), events.clone(), {
        let (app, cancel) = (app.clone(), cancel.clone());
        move || poller::heartbeat(app.clone(), cancel.clone())
    }));

    // --- Команды из Kafka ---
    if app.kafka.is_some() {
        let dedup = Arc::new(Dedup::new(Duration::from_secs(60), 1000));
        let (app_cmd, leadership_cmd, cancel_cmd) = (app.clone(), leadership.clone(), cancel.clone());
        supervised.push(supervise("команды", cancel.clone(), app.metrics.clone(), events.clone(), move || {
            let (app, dedup) = (app_cmd.clone(), dedup.clone());
            kafka::consume_commands(
                app.settings.kafka.clone(),
                leadership_cmd.clone(),
                cancel_cmd.clone(),
                move |cmd, record_ts| {
                    let (app, dedup) = (app.clone(), dedup.clone());
                    async move { command::handle(&app, &dedup, cmd, record_ts).await }
                },
            )
        }));
    }

    // --- HTTP ---
    let listener = tokio::net::TcpListener::bind((http_bind.as_str(), http_port)).await.context("HTTP-порт занят")?;
    info!(
        "HTTP на {}:{http_port} (/actuator/health, /actuator/prometheus, /api/*: {})",
        http_bind,
        if api_protected { "по токену" } else { "без токена — задайте GATEWAY_API_TOKEN" }
    );
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
        for task in supervised {
            let _ = task.await;
        }
        while background.join_next().await.is_some() {}
    };
    if tokio::time::timeout(Duration::from_secs(8), stop).await.is_err() {
        warn!("Не все задачи остановились за 8 с");
    }
    if let Some(handle) = elector {
        // Поток выборов сам переведёт роль в резерв и выйдет из группы — резерв подхватит сразу.
        let _ = tokio::task::spawn_blocking(move || handle.join()).await;
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
