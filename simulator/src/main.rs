//! PLC-симулятор для стенда шлюза: OPC UA, Modbus TCP и PAC (driver-master) в одном процессе;
//! значения тегов проигрываются из 5-суточного архива станции BN1_MCA1.
//!
//!   simulator [config/replay_config.yaml]     запуск
//!   simulator healthcheck                     проверка живости (порты Modbus и PAC) для контейнера
//!   simulator conformance [host] [port] [config]  проверка: PAC ведёт себя при записи как описано в конфигурации
//!   simulator probe-pac [host] [port]         снимок любого PAC (симулятор, прошивка ptusa): по умолчанию localhost:10000
//!
//! Окружение: OPCUA_ENDPOINT (напр. opc.tcp://simulator:4840 — адрес привязки и анонса вместо
//! конфигурации), MODBUS_PORT, PAC_PORT, RUST_LOG; SIM_OPCUA_USER и SIM_OPCUA_PASSWORD — включить
//! защищённые конечные точки (Basic256Sha256) для этого пользователя.

mod config;
mod conformance;
mod luatab;
mod modbus;
mod opcua;
mod pac;
mod pac_client;
mod plc;
mod replay;
mod tag;
mod value;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::net::TcpListener;
use tracing::{info, warn};

use crate::pac::{PacDevice, PacField, PacModel};
use crate::plc::Plc;
use crate::replay::{Archive, Replay};
use crate::tag::Protocol;

fn env_port(name: &str, default: u16) -> Result<u16> {
    match std::env::var(name) {
        Ok(v) if !v.is_empty() => v.trim().parse().with_context(|| format!("{name}={v}")),
        _ => Ok(default),
    }
}

/// Путь к архиву: относительный ищется от текущей папки, затем от папки симулятора (родитель config/).
fn resolve_data_path(data_path: &str, config_path: &Path) -> PathBuf {
    let p = PathBuf::from(data_path);
    if p.is_absolute() || p.exists() {
        return p;
    }
    config_path.parent().and_then(Path::parent).map(|base| base.join(&p)).unwrap_or(p)
}

/// Приборы PAC в порядке появления в конфигурации.
fn pac_devices(plc: &Plc) -> Vec<PacDevice> {
    let mut devices: Vec<PacDevice> = Vec::new();
    for tag in plc.tags().iter().filter(|t| t.protocol == Protocol::Pac) {
        let name = tag.device.clone().unwrap_or_else(|| tag.name.clone());
        let field =
            PacField { field: tag.field.clone().unwrap_or_else(|| tag.name.clone()), address: tag.address.clone() };
        match devices.iter_mut().find(|d| d.device == name) {
            Some(d) => d.fields.push(field),
            None => devices.push(PacDevice {
                device: name,
                dev_type: tag.dev_type.clone().filter(|t| !t.is_empty()).unwrap_or_else(|| "DEV".into()),
                fields: vec![field],
            }),
        }
    }
    devices
}

async fn healthcheck() -> Result<()> {
    let cfg_path = std::env::args().nth(2).unwrap_or_else(|| "config/replay_config.yaml".into());
    let cfg = config::load(Path::new(&cfg_path))?;
    for port in [env_port("MODBUS_PORT", cfg.modbus_port)?, env_port("PAC_PORT", cfg.pac_port)?] {
        tokio::time::timeout(std::time::Duration::from_secs(2), tokio::net::TcpStream::connect(("127.0.0.1", port)))
            .await
            .with_context(|| format!("порт {port}: таймаут"))?
            .with_context(|| format!("порт {port}"))?;
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info,opcua=warn".into()),
        )
        .init();
    match std::env::args().nth(1).as_deref() {
        Some("healthcheck") => return healthcheck().await,
        Some("conformance") => {
            let host = std::env::args().nth(2).unwrap_or_else(|| "127.0.0.1".into());
            let port = std::env::args().nth(3).map(|p| p.parse()).transpose().context("порт")?.unwrap_or(10000);
            let cfg = std::env::args().nth(4).unwrap_or_else(|| "config/replay_config.yaml".into());
            return conformance::run(&host, port, Path::new(&cfg)).await;
        }
        Some("probe-pac") => {
            let host = std::env::args().nth(2).unwrap_or_else(|| "localhost".into());
            let port = std::env::args().nth(3).map(|p| p.parse()).transpose().context("порт")?.unwrap_or(10000);
            return pac::probe(&host, port).await;
        }
        _ => {}
    }
    let config_path = PathBuf::from(std::env::args().nth(1).unwrap_or_else(|| "config/replay_config.yaml".into()));
    let cfg = config::load(&config_path)?;

    let replay = if cfg.replay.enabled {
        let archive = Archive::load(&resolve_data_path(&cfg.replay.data_path, &config_path))?;
        let replay = Replay::new(archive, cfg.replay.speed, cfg.replay.looped);
        info!(
            "Загружен архив replay: {} тегов, длительность {:.2} сут, speed={}, loop={}",
            replay.series_count(),
            replay.duration / 86400.0,
            replay.speed,
            replay.looped
        );
        Some(replay)
    } else {
        None
    };
    let plc = Arc::new(Plc::new(&cfg, replay)?);
    let st = plc.stats();
    info!(
        "PLC {} ({}): {} тегов — OPC UA {}, Modbus {}, PAC {}; шаг {:?}",
        plc.name, plc.id, st.total, st.opcua, st.modbus, st.pac, plc.update_rate
    );

    let regs = Arc::new(modbus::Registers::new());
    let modbus_listener =
        TcpListener::bind(("0.0.0.0", env_port("MODBUS_PORT", cfg.modbus_port)?)).await.context("порт Modbus")?;
    tokio::spawn(modbus::serve(modbus_listener, regs.clone()));

    let pac = if st.pac > 0 {
        let writer = plc.clone();
        let model = Arc::new(PacModel::new(
            &plc.name,
            pac_devices(&plc),
            Box::new(move |device, field, index, value| writer.pac_write(device, field, index, value)),
        ));
        let listener = TcpListener::bind(("0.0.0.0", env_port("PAC_PORT", cfg.pac_port)?)).await.context("порт PAC")?;
        tokio::spawn(pac::serve(listener, model.clone()));
        Some(model)
    } else {
        None
    };

    let endpoint =
        std::env::var("OPCUA_ENDPOINT").ok().filter(|v| !v.is_empty()).unwrap_or_else(|| cfg.plc.endpoint.clone());
    // Защищённые конечные точки для проверки шлюза: SIM_OPCUA_USER / SIM_OPCUA_PASSWORD.
    let user = (std::env::var("SIM_OPCUA_USER").ok().filter(|v| !v.is_empty()))
        .zip(std::env::var("SIM_OPCUA_PASSWORD").ok().filter(|v| !v.is_empty()));
    let (opcua_server, pusher) = opcua::build(&plc, &endpoint, user.as_ref().map(|(u, p)| (u.as_str(), p.as_str())))?;
    if user.is_some() {
        info!("OPC UA: включены защищённые конечные точки Basic256Sha256 (пользователь из SIM_OPCUA_USER)");
    }
    let server_task = tokio::spawn(async move {
        if let Err(e) = opcua_server.run().await {
            warn!("OPC UA-сервер остановлен: {e}");
        }
    });

    // Цикл обновления: значения из архива → регистры, узлы OPC UA, снимок PAC.
    let cycle_plc = plc.clone();
    let cycle_regs = regs.clone();
    let cycle = tokio::spawn(async move {
        let mut tick = tokio::time::interval(cycle_plc.update_rate);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = tick.tick() => {}
                _ = cycle_plc.wake.notified() => {}
            }
            let out = cycle_plc.cycle(&cycle_regs);
            pusher.push(&out.opcua);
            if let Some(pac) = &pac {
                let snapshot: HashMap<String, value::Value> = out.pac;
                pac.update_snapshot(snapshot);
            }
        }
    });

    info!("Симулятор запущен, остановка — Ctrl+C / SIGTERM");
    shutdown_signal().await;
    info!("Останавливаю симулятор");
    cycle.abort();
    server_task.abort();
    Ok(())
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut term = signal(SignalKind::terminate()).expect("SIGTERM");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
