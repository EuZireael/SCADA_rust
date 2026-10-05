//! OPC UA-фасад эмулятора мойки (см. документацию библиотеки).
//!
//!   ptusa-opcua              запуск
//!   ptusa-opcua healthcheck  проверка живости (порт OPC UA) для контейнера
//!
//! Окружение:
//!   PAC_HOST, PAC_PORT   прошивка ptusa (driver-master), по умолчанию ptusa:10000
//!   STATION_CONFIG       конфигурация станции, по умолчанию /config/station.yaml
//!   OPCUA_BIND           на чём слушать, по умолчанию 0.0.0.0:4840
//!   OPCUA_ENDPOINT       адрес, который сервер называет в FindServers (по умолчанию opc.tcp://localhost:4840);
//!                        конечные точки GetEndpoints строятся из OPCUA_BIND — клиент (шлюз) подключается
//!                        по адресу, который знает сам
//!   POLL_MS              период снимка, по умолчанию 500
//!   RUST_LOG             уровни журнала

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use ptusa_opcua::channel::load_channels;
use ptusa_opcua::pac::PacClient;
use ptusa_opcua::server::{Bridge, Stats, build, parse_bind};
use ptusa_opcua::snapshot::Snapshot;
use tracing::{info, warn};

fn env(name: &str, default: &str) -> String {
    std::env::var(name).ok().filter(|v| !v.is_empty()).unwrap_or_else(|| default.to_string())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info,opcua=warn".into()),
        )
        .init();
    let bind = parse_bind(&env("OPCUA_BIND", "0.0.0.0:4840"))?;
    if std::env::args().nth(1).as_deref() == Some("healthcheck") {
        let port = bind.1;
        tokio::time::timeout(Duration::from_secs(2), tokio::net::TcpStream::connect(("127.0.0.1", port)))
            .await
            .with_context(|| format!("порт {port}: таймаут"))?
            .with_context(|| format!("порт {port}"))?;
        return Ok(());
    }

    let channels = Arc::new(load_channels(&PathBuf::from(env("STATION_CONFIG", "/config/station.yaml")))?);
    let pac_port: u16 = env("PAC_PORT", "10000").parse().context("PAC_PORT")?;
    let poll_ms: u64 = env("POLL_MS", "500").parse().context("POLL_MS")?;
    let pac = Arc::new(PacClient::new(&env("PAC_HOST", "ptusa"), pac_port, Duration::from_secs(3)));
    let stats = Arc::new(Stats::default());
    let announce = env("OPCUA_ENDPOINT", "opc.tcp://localhost:4840");
    let (server, pusher) = build(&channels, pac.clone(), stats.clone(), &announce, bind.clone())?;
    info!("OPC UA на {}:{}, прошивка {}:{}, снимок раз в {poll_ms} мс", bind.0, bind.1, pac.host, pac.port);

    let bridge =
        Bridge { channels, pac, pusher, snapshot: Snapshot::new()?, poll: Duration::from_millis(poll_ms), stats };
    let server_task = tokio::spawn(async move {
        if let Err(e) = server.run().await {
            warn!("OPC UA-сервер остановлен: {e}");
        }
    });
    let bridge_task = tokio::spawn(bridge.run());
    shutdown_signal().await;
    info!("Останавливаю фасад");
    bridge_task.abort();
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
