//! OPC UA-сервер фасада и цикл опроса прошивки.
//!
//! Узлы получают значения из снимка раз в период опроса; запись клиента идёт командой прошивке
//! прямо из обработчика записи (он синхронный, поэтому ждёт ответа прошивки через
//! `block_in_place` — нужен многопоточный рантайм tokio).

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, ensure};
use opcua::crypto::SecurityPolicy;
use opcua::server::address_space::{ObjectBuilder, VariableBuilder};
use opcua::server::diagnostics::NamespaceMetadata;
use opcua::server::node_manager::memory::{SimpleNodeManager, simple_node_manager};
use opcua::server::{Server, ServerBuilder, ServerHandle};
use opcua::types::{DataTypeId, DataValue, DateTime, MessageSecurityMode, NodeId, ObjectId, StatusCode, Variant};
use tracing::{info, warn};

use crate::channel::{Channel, Converted, Kind, command_text, convert};
use crate::pac::PacClient;
use crate::snapshot::Snapshot;

pub const NAMESPACE_URI: &str = "urn:savushkin:ptusa";
/// Потолок ожидания ответа прошивки на команду записи.
const REPORT_EVERY: Duration = Duration::from_secs(60);

#[derive(Default)]
pub struct Stats {
    pub polls: AtomicU64,
    pub commands: AtomicU64,
    pub rejected: AtomicU64,
}

/// Кладёт значения и статусы в узлы запущенного сервера.
pub struct Pusher {
    handle: ServerHandle,
    manager: Arc<SimpleNodeManager>,
    ns: u16,
}

impl Pusher {
    fn node(&self, ch: &Channel) -> NodeId {
        NodeId::new(self.ns, ch.node_name.clone())
    }

    /// Значения каналов (None — нет значения: статус `bad`) в момент `ts`.
    pub fn push(&self, items: &[(&Channel, Option<Converted>)], bad: StatusCode, ts: DateTime) {
        let batch: Vec<(NodeId, DataValue)> = items
            .iter()
            .map(|(ch, value)| {
                let dv = match value {
                    Some(v) => DataValue {
                        value: Some(variant(v)),
                        status: Some(StatusCode::Good),
                        source_timestamp: Some(ts),
                        server_timestamp: Some(ts),
                        ..Default::default()
                    },
                    None => bad_value(bad, ts),
                };
                (self.node(ch), dv)
            })
            .collect();
        let _ =
            self.manager.set_values(self.handle.subscriptions(), batch.iter().map(|(id, dv)| (id, None, dv.clone())));
    }

    /// У всех каналов один и тот же плохой статус.
    pub fn push_all_bad(&self, channels: &[Channel], status: StatusCode) {
        let ts = DateTime::now();
        let items: Vec<(&Channel, Option<Converted>)> = channels.iter().map(|c| (c, None)).collect();
        self.push(&items, status, ts);
    }
}

/// Значение с плохим статусом. Пустое значение (`Empty`) нужно сервису чтения async-opcua: без значения
/// он статус в ответ не кладёт, и узел выглядел бы хорошим.
fn bad_value(status: StatusCode, ts: DateTime) -> DataValue {
    DataValue {
        value: Some(Variant::Empty),
        status: Some(status),
        source_timestamp: Some(ts),
        server_timestamp: Some(ts),
        ..Default::default()
    }
}

fn variant(v: &Converted) -> Variant {
    match v {
        Converted::Int(i) => Variant::Int32(*i),
        Converted::Float(f) => Variant::Float(*f),
        Converted::Text(s) => Variant::from(s.as_str()),
        Converted::Bool(b) => Variant::Boolean(*b),
    }
}

fn initial(kind: Kind) -> Variant {
    match kind {
        Kind::Int32 => Variant::Int32(0),
        Kind::Float => Variant::Float(0.0),
        Kind::String => Variant::from(""),
        Kind::Boolean => Variant::Boolean(false),
    }
}

fn data_type_id(kind: Kind) -> DataTypeId {
    match kind {
        Kind::Int32 => DataTypeId::Int32,
        Kind::Float => DataTypeId::Float,
        Kind::String => DataTypeId::String,
        Kind::Boolean => DataTypeId::Boolean,
    }
}

/// Значение записи клиента → скаляр для `set_cmd` (bool → 1/0, число — как есть). None — не число.
fn scalar(v: &Variant) -> Option<String> {
    Some(match v {
        Variant::Boolean(b) => u8::from(*b).to_string(),
        Variant::SByte(i) => i.to_string(),
        Variant::Byte(i) => i.to_string(),
        Variant::Int16(i) => i.to_string(),
        Variant::UInt16(i) => i.to_string(),
        Variant::Int32(i) => i.to_string(),
        Variant::UInt32(i) => i.to_string(),
        Variant::Int64(i) => i.to_string(),
        Variant::UInt64(i) => i.to_string(),
        Variant::Float(f) if f.is_finite() => f.to_string(),
        Variant::Double(f) if f.is_finite() => f.to_string(),
        _ => return None,
    })
}

/// Запись клиента → команда прошивке; её исход — статус записи.
async fn command(ch: &Channel, value: &Variant, pac: &PacClient, stats: &Stats) -> StatusCode {
    stats.commands.fetch_add(1, Ordering::Relaxed);
    // Строковый канал прошивке командой не пишется (set_cmd принимает только числа).
    let Some(scalar) = scalar(value).filter(|_| ch.kind != Kind::String) else {
        stats.rejected.fetch_add(1, Ordering::Relaxed);
        return StatusCode::BadTypeMismatch;
    };
    if !pac.connected() {
        return StatusCode::BadCommunicationError;
    }
    let text = command_text(ch, &scalar);
    match pac.command(&text).await {
        Ok(0) => {
            info!("✍ {text}");
            StatusCode::Good
        }
        Ok(code) => {
            stats.rejected.fetch_add(1, Ordering::Relaxed);
            warn!("команда {text}: прошивка вернула код {code}");
            StatusCode::BadInvalidState
        }
        Err(e) => {
            warn!("команда {text}: нет связи с прошивкой: {e:#}");
            pac.close().await;
            StatusCode::BadCommunicationError
        }
    }
}

/// Хост и порт из `host:port`.
pub fn parse_bind(bind: &str) -> Result<(String, u16)> {
    let (host, port) =
        bind.rsplit_once(':').with_context(|| format!("адрес привязки {bind:?}: ожидается host:port"))?;
    ensure!(!host.is_empty(), "адрес привязки {bind:?}: пустой хост");
    Ok((host.to_string(), port.parse().with_context(|| format!("адрес привязки {bind:?}: порт"))?))
}

/// Собирает сервер с адресным пространством из каналов. Сервер надо запустить (`run`).
///
/// `bind` — на чём слушать; `announce` — адрес, который сервер называет в FindServers (сами
/// конечные точки GetEndpoints строятся из адреса привязки: у async-opcua это один параметр).
pub fn build(
    channels: &[Channel],
    pac: Arc<PacClient>,
    stats: Arc<Stats>,
    announce: &str,
    bind: (String, u16),
) -> Result<(Server, Pusher)> {
    let pki = std::env::temp_dir().join(format!("ptusa-opcua-pki-{}-{}", std::process::id(), bind.1));
    let (server, handle) = ServerBuilder::new_anonymous("ptusa (эмулятор мойки) — OPC UA-фасад")
        .application_uri("urn:savushkin:ptusa-opcua")
        .product_uri("urn:savushkin:ptusa-opcua")
        .host(bind.0)
        .port(bind.1)
        .pki_dir(pki)
        .create_sample_keypair(true)
        .trust_client_certs(true)
        .add_endpoint("none", ("/", SecurityPolicy::None, MessageSecurityMode::None, &["ANONYMOUS"] as &[&str]))
        .discovery_urls(vec![announce.to_string()])
        .with_node_manager(simple_node_manager(
            NamespaceMetadata { namespace_uri: NAMESPACE_URI.into(), ..Default::default() },
            "ptusa",
        ))
        .build()
        .map_err(|e| anyhow::anyhow!("OPC UA-сервер: {e}"))?;
    let manager = handle.node_managers().get_of_type::<SimpleNodeManager>().context("менеджер узлов")?;
    let ns = handle.get_namespace_index(NAMESPACE_URI).context("namespace")?;
    ensure!(ns == 2, "namespace должен быть 2, а не {ns}: узлы станции — ns=2;s=<прибор>.<поле>");

    let mut devices: HashMap<&str, NodeId> = HashMap::new();
    {
        let mut space = manager.address_space().write();
        let root = NodeId::new(ns, "PAC");
        ObjectBuilder::new(&root, "PAC", "PAC").organized_by(NodeId::from(ObjectId::ObjectsFolder)).insert(&mut *space);
        for ch in channels {
            let parent = devices
                .entry(ch.device.as_str())
                .or_insert_with(|| {
                    let id = NodeId::new(ns, ch.device.clone());
                    ObjectBuilder::new(&id, ch.device.clone(), ch.device.clone())
                        .organized_by(root.clone())
                        .insert(&mut *space);
                    id
                })
                .clone();
            let id = NodeId::new(ns, ch.node_name.clone());
            // Имя переменной — поле внутри прибора (`ST`, `RT_PAR_F[12]`).
            let browse = ch.node_name.split_once('.').map_or(ch.node_name.as_str(), |(_, f)| f).to_string();
            let mut builder = VariableBuilder::new(&id, browse.clone(), browse)
                .data_type(data_type_id(ch.kind))
                .value(initial(ch.kind))
                .organized_by(parent);
            if ch.writable {
                builder = builder.writable();
            }
            builder.insert(&mut *space);
        }
    }
    // Запись клиента: сначала команда прошивке, статус записи — её исход. Значение узла не трогаем:
    // оно придёт из следующего снимка.
    let rt = tokio::runtime::Handle::current();
    for ch in channels.iter().filter(|c| c.writable) {
        let (ch_cb, pac, stats, rt) = (ch.clone(), pac.clone(), stats.clone(), rt.clone());
        manager.inner().add_write_callback(NodeId::new(ns, ch.node_name.clone()), move |dv: DataValue, _range| {
            let Some(value) = dv.value else { return StatusCode::BadTypeMismatch };
            tokio::task::block_in_place(|| rt.block_on(command(&ch_cb, &value, &pac, &stats)))
        });
    }
    let pusher = Pusher { handle, manager, ns };
    pusher.push_all_bad(channels, StatusCode::BadWaitingForInitialData);
    info!(
        "адресное пространство: {} каналов ({} на запись), {} объектов",
        channels.len(),
        channels.iter().filter(|c| c.writable).count(),
        devices.len()
    );
    Ok((server, pusher))
}

/// Цикл опроса: снимок прошивки → узлы; при обрыве связи все узлы `BadCommunicationError`, фасад
/// переподключается сам (пауза 1 → 10 с).
pub struct Bridge {
    pub channels: Arc<Vec<Channel>>,
    pub pac: Arc<PacClient>,
    pub pusher: Pusher,
    pub snapshot: Snapshot,
    pub poll: Duration,
    pub stats: Arc<Stats>,
}

impl Bridge {
    /// Один шаг: снимок → значения узлов. Возвращает, у скольких каналов в снимке нет значения.
    pub async fn poll_once(&self) -> Result<usize> {
        let text = self.pac.snapshot().await?;
        let ts = DateTime::now();
        self.snapshot.load(&text)?;
        let items: Vec<(&Channel, Option<Converted>)> = self
            .channels
            .iter()
            .map(|ch| (ch, self.snapshot.get(&ch.device, &ch.base, ch.index).and_then(|raw| convert(&raw, ch.kind))))
            .collect();
        let missing = items.iter().filter(|(_, v)| v.is_none()).count();
        self.pusher.push(&items, StatusCode::BadNoData, ts);
        self.stats.polls.fetch_add(1, Ordering::Relaxed);
        Ok(missing)
    }

    pub async fn run(self) {
        let (mut backoff, mut last_report) = (Duration::from_secs(1), Instant::now() - REPORT_EVERY);
        loop {
            let started = Instant::now();
            let step = async {
                if !self.pac.connected() {
                    self.pac.connect().await?;
                    info!("🟢 связь с прошивкой {}:{}", self.pac.host, self.pac.port);
                    backoff = Duration::from_secs(1);
                }
                self.poll_once().await
            };
            match step.await {
                Ok(missing) => {
                    if last_report.elapsed() >= REPORT_EVERY {
                        last_report = Instant::now();
                        info!(
                            "снимков {}, команд {} (отклонено {}), каналов без значения {missing}",
                            self.stats.polls.load(Ordering::Relaxed),
                            self.stats.commands.load(Ordering::Relaxed),
                            self.stats.rejected.load(Ordering::Relaxed)
                        );
                    }
                    tokio::time::sleep(self.poll.saturating_sub(started.elapsed())).await;
                }
                Err(e) => {
                    let was = self.pac.connected();
                    self.pac.close().await;
                    if was || backoff == Duration::from_secs(1) {
                        warn!("🔴 нет связи с прошивкой {}:{}: {e:#}", self.pac.host, self.pac.port);
                    }
                    self.pusher.push_all_bad(&self.channels, StatusCode::BadCommunicationError);
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(10));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bind_address_is_split() {
        assert_eq!(parse_bind("0.0.0.0:4840").unwrap(), ("0.0.0.0".into(), 4840));
        assert!(parse_bind("4840").is_err());
        assert!(parse_bind(":4840").is_err());
        assert!(parse_bind("host:x").is_err());
    }

    #[test]
    fn write_values_become_command_scalars() {
        assert_eq!(scalar(&Variant::Boolean(true)).as_deref(), Some("1"));
        assert_eq!(scalar(&Variant::Int32(-5)).as_deref(), Some("-5"));
        assert_eq!(scalar(&Variant::Float(7.5)).as_deref(), Some("7.5"));
        assert_eq!(scalar(&Variant::Float(0.1)).as_deref(), Some("0.1"), "f32 печатается как f32, без хвоста double");
        assert_eq!(scalar(&Variant::Double(3.0)).as_deref(), Some("3"));
        assert_eq!(scalar(&Variant::from("x")), None);
        assert_eq!(scalar(&Variant::Float(f32::NAN)), None);
    }
}
