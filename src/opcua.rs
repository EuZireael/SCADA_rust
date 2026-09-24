//! OPC UA-клиент (async-opcua): подключение, пакетное чтение, запись.
//!
//! Подключение напрямую к адресу из конфига, без discovery: у Milo Java-шлюза после
//! discovery клиент уходил на адрес, который анонсирует сервер, и с хоста не подключался
//! (`opc.tcp://simulator:4840`). Политика безопасности — None, как у стенда.
//!
//! Чтение — пачками по [`READ_CHUNK`] узлов: по умолчанию клиент декодирует массивы не
//! длиннее 1000 элементов, а у реальных серверов бывает MaxNodesPerRead.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use chrono::Utc;
use opcua::client::{Client, ClientBuilder, IdentityToken, Session};
use opcua::types::{
    AttributeId, DataValue, EndpointDescription, MessageSecurityMode, NodeId, NumericRange, ReadValueId, StatusCode,
    TimestampsToReturn, UAString, UserTokenPolicy, Variant, WriteValue,
};
use tokio::task::JoinHandle;
use tokio::time::timeout;

use crate::model::{self, Quality, TagValue, Timestamp};

/// Узлов в одном запросе чтения.
pub const READ_CHUNK: usize = 500;

pub struct OpcConnection {
    session: Arc<Session>,
    event_loop: JoinHandle<StatusCode>,
    _client: Client,
    op_timeout: Duration,
}

impl OpcConnection {
    pub async fn connect(url: &str, op_timeout: Duration) -> Result<Self> {
        let mut client = ClientBuilder::new()
            .application_name("SCADA Gateway")
            .application_uri("urn:scada:gateway")
            .product_uri("urn:scada:gateway")
            .pki_dir(std::env::temp_dir().join("scada-gateway-pki"))
            // Самоподписанный сертификат клиента (создаётся один раз в pki_dir): для политики
            // None он не нужен, но без него клиент при каждом подключении пишет ошибки в лог.
            .create_sample_keypair(true)
            .trust_server_certs(true)
            // Переподключением управляет опрос шлюза: оборвалась сессия — событие DISCONNECTED
            // и новая попытка по своему расписанию.
            .session_retry_limit(0)
            .request_timeout(op_timeout)
            .max_array_length(100_000)
            .client()
            .map_err(|e| anyhow!("конфигурация OPC UA-клиента: {e:?}"))?;
        let endpoint: EndpointDescription =
            (url, "None", MessageSecurityMode::None, UserTokenPolicy::anonymous()).into();
        let (session, event_loop) = client
            .connect_to_endpoint_directly(endpoint, IdentityToken::Anonymous)
            .map_err(|e| anyhow!("OPC UA {url}: {e}"))?;
        let mut event_loop = event_loop.spawn();

        tokio::select! {
            connected = session.wait_for_connection() => {
                if !connected {
                    event_loop.abort();
                    bail!("OPC UA {url}: сессия не установлена");
                }
            }
            status = &mut event_loop => bail!("OPC UA {url}: соединение не установлено ({status:?})"),
            _ = tokio::time::sleep(op_timeout) => {
                event_loop.abort();
                bail!("OPC UA {url}: таймаут {} мс (handshake завис)", op_timeout.as_millis());
            }
        }
        Ok(OpcConnection { session, event_loop, _client: client, op_timeout })
    }

    /// Сессия жива, пока крутится её event loop.
    pub fn is_alive(&self) -> bool {
        !self.event_loop.is_finished()
    }

    /// Прочитать значения узлов (порядок результатов = порядок `nodes`).
    pub async fn read(&self, nodes: &[ReadValueId]) -> Result<Vec<DataValue>> {
        let mut out = Vec::with_capacity(nodes.len());
        for chunk in nodes.chunks(READ_CHUNK) {
            let values = timeout(self.op_timeout, self.session.read(chunk, TimestampsToReturn::Both, 0.0))
                .await
                .with_context(|| format!("таймаут чтения {} мс (сервер завис)", self.op_timeout.as_millis()))?
                .map_err(|e| anyhow!("{e}"))?;
            if values.len() != chunk.len() {
                bail!("сервер вернул {} значений на {} узлов", values.len(), chunk.len());
            }
            out.extend(values);
        }
        Ok(out)
    }

    /// Записать значение в узел; StatusCode — ответ сервера.
    pub async fn write(&self, node_id: NodeId, value: Variant) -> Result<StatusCode> {
        let write = WriteValue {
            node_id,
            attribute_id: AttributeId::Value as u32,
            index_range: NumericRange::None,
            value: DataValue::value_only(value),
        };
        let statuses = timeout(self.op_timeout, self.session.write(&[write]))
            .await
            .context("таймаут записи")?
            .map_err(|e| anyhow!("{e}"))?;
        statuses.into_iter().next().context("сервер не вернул статус записи")
    }

    pub async fn close(&self) {
        let _ = timeout(self.op_timeout, self.session.disconnect()).await;
        self.event_loop.abort();
    }
}

/// Узел чтения значения по строке nodeId (`ns=2;s=6`).
pub fn read_value_id(node_id: &str) -> Result<ReadValueId> {
    Ok(ReadValueId::from(parse_node_id(node_id)?))
}

pub fn parse_node_id(node_id: &str) -> Result<NodeId> {
    node_id.parse::<NodeId>().map_err(|_| anyhow!("некорректный nodeId: {node_id}"))
}

/// Значение, качество и момент снятия из DataValue OPC UA.
pub fn reading(dv: &DataValue) -> (Option<TagValue>, Quality, Timestamp) {
    let good = dv.status.map(|s| s.is_good()).unwrap_or(true);
    let value = dv.value.as_ref().and_then(extract_value);
    // Момент снятия значения сервером: sourceTimestamp, иначе serverTimestamp, иначе сейчас.
    let ts = [dv.source_timestamp, dv.server_timestamp]
        .into_iter()
        .flatten()
        .find(|t| !t.is_null())
        .map(|t| Timestamp(t.as_chrono()))
        .unwrap_or_else(|| Timestamp(Utc::now()));
    (value, if good { Quality::Good } else { Quality::Bad }, ts)
}

pub fn extract_value(v: &Variant) -> Option<TagValue> {
    Some(match v {
        Variant::Boolean(b) => TagValue::Bool(*b),
        Variant::SByte(n) => TagValue::Int(*n as i64),
        Variant::Byte(n) => TagValue::Int(*n as i64),
        Variant::Int16(n) => TagValue::Int(*n as i64),
        Variant::UInt16(n) => TagValue::Int(*n as i64),
        Variant::Int32(n) => TagValue::Int(*n as i64),
        Variant::UInt32(n) => TagValue::Int(*n as i64),
        Variant::Int64(n) => TagValue::Int(*n),
        Variant::UInt64(n) => TagValue::Int(*n as i64),
        Variant::Float(f) => TagValue::F32(*f),
        Variant::Double(f) => TagValue::F64(*f),
        Variant::String(s) => TagValue::Text(s.as_ref().to_string()),
        _ => return None,
    })
}

/// Значение команды → Variant по типу тега (сервер проверяет тип строго: Float-узел не
/// примет Double).
pub fn to_variant(data_type: &str, value: &TagValue) -> Result<Variant> {
    let dt = data_type.trim().to_ascii_uppercase();
    Ok(if model::is_bool(&dt) {
        Variant::Boolean(value.as_f64().context("не число/bool")? != 0.0)
    } else if model::is_int(&dt) {
        let n = value.as_f64().context("не число")?;
        Variant::Int32(i32::try_from(n.trunc() as i64).map_err(|_| anyhow!("{n} вне диапазона Int32"))?)
    } else if dt.starts_with("FLOAT") || dt.starts_with("REAL") {
        Variant::Float(value.as_f64().context("не число")? as f32)
    } else if dt.starts_with("DOUBLE") {
        Variant::Double(value.as_f64().context("не число")?)
    } else if model::is_string(&dt) {
        Variant::String(UAString::from(match value {
            TagValue::Text(s) => s.clone(),
            other => serde_json::to_string(other).unwrap_or_default(),
        }))
    } else {
        match value {
            TagValue::Bool(b) => Variant::Boolean(*b),
            TagValue::Int(i) => Variant::Int64(*i),
            TagValue::F32(f) => Variant::Float(*f),
            TagValue::F64(f) => Variant::Double(*f),
            TagValue::Text(s) => Variant::String(UAString::from(s.clone())),
        }
    })
}

/// Исход неудачной записи по StatusCode сервера (коды — OPC UA Part 4/6).
pub fn classify_write_status(status: StatusCode) -> &'static str {
    match status.bits() & 0xFFFF_0000 {
        // Bad_NotWritable, Bad_WriteNotSupported, Bad_UserAccessDenied — последним на запись в
        // узел только для чтения отвечают многие серверы (asyncua симулятора в том числе).
        0x803B_0000 | 0x8073_0000 | 0x801F_0000 => "REJECTED_NOT_WRITABLE",
        0x8074_0000 | 0x803C_0000 => "REJECTED_TYPE_MISMATCH", // Bad_TypeMismatch, Bad_OutOfRange
        _ => "FAILED_WRITE",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn variant_by_tag_type() {
        assert_eq!(to_variant("INT32", &TagValue::F64(3.9)).unwrap(), Variant::Int32(3));
        assert_eq!(to_variant("FLOAT", &TagValue::Int(2)).unwrap(), Variant::Float(2.0));
        assert_eq!(to_variant("DOUBLE", &TagValue::Int(2)).unwrap(), Variant::Double(2.0));
        assert_eq!(to_variant("BOOLEAN", &TagValue::Int(1)).unwrap(), Variant::Boolean(true));
        assert!(to_variant("INT32", &TagValue::Text("x".into())).is_err());
        assert!(to_variant("INT32", &TagValue::F64(1e12)).is_err());
    }

    #[test]
    fn extract_like_milo() {
        assert_eq!(extract_value(&Variant::UInt32(7)), Some(TagValue::Int(7)));
        assert_eq!(extract_value(&Variant::Float(64.7)), Some(TagValue::F32(64.7)));
        assert_eq!(extract_value(&Variant::String(UAString::from("REC"))), Some(TagValue::Text("REC".into())));
    }

    #[test]
    fn write_status_classification() {
        assert_eq!(classify_write_status(StatusCode::from(0x803B_0000u32)), "REJECTED_NOT_WRITABLE");
        assert_eq!(classify_write_status(StatusCode::from(0x801F_0000u32)), "REJECTED_NOT_WRITABLE");
        assert_eq!(classify_write_status(StatusCode::from(0x8074_0000u32)), "REJECTED_TYPE_MISMATCH");
        assert_eq!(classify_write_status(StatusCode::from(0x8002_0000u32)), "FAILED_WRITE");
    }

    #[test]
    fn node_id_parsing() {
        assert!(parse_node_id("ns=2;s=6").is_ok());
        assert!(parse_node_id("pac:385").is_err());
    }
}
