//! Modbus TCP: батч-чтение holding-регистров (FC03). Контроллер только на чтение.
//!
//! Теги группируются в непрерывные блоки (≤120 регистров на запрос) и читаются одной FC03
//! на блок — для WAGO это ~десятки запросов за цикл вместо запроса на каждый тег.
//! Декодирование как у Java-шлюза и симулятора: FLOAT — 2 регистра little-endian по
//! словам (`struct.pack('<f')`), целые — регистр со знаком (int16), BOOL — регистр ≠ 0.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, Result, anyhow};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_modbus::client::Context;
use tokio_modbus::prelude::*;

use crate::model::{self, Tag, TagValue};

/// Holding-регистры адресуются с 40001; в протоколе — смещение с 0.
const ADDRESS_BASE: i32 = 40001;
/// Лимит FC03 — 125 регистров; берём с запасом.
const MAX_BLOCK_REGISTERS: i32 = 120;

/// `host:port` → (host, port) с портом по умолчанию.
pub fn split_host_port(s: &str, default_port: u16) -> (String, u16) {
    let s = s.trim().trim_end_matches('/');
    match s.rsplit_once(':') {
        Some((h, p)) => (h.to_string(), p.parse().unwrap_or(default_port)),
        None => (s.to_string(), default_port),
    }
}

/// Хост и порт из `modbus://host:port` (порт по умолчанию 502).
pub fn endpoint(endpoint: &str) -> (String, u16) {
    split_host_port(endpoint.trim_start_matches("modbus://"), 502)
}

/// Блок регистров одной FC03 и теги, которые из него читаются.
#[derive(Debug, Clone)]
pub struct Block {
    /// 0-based адрес первого регистра.
    pub start: u16,
    pub count: u16,
    pub tags: Vec<Arc<Tag>>,
}

fn reg0(tag: &Tag) -> i32 {
    tag.modbus_address.unwrap_or(ADDRESS_BASE) - ADDRESS_BASE
}

fn width(tag: &Tag) -> i32 {
    if model::is_float(&tag.data_type) { 2 } else { 1 }
}

/// План чтения: теги с адресом по возрастанию, набранные в блоки ≤120 регистров.
pub fn plan_blocks(tags: &[Arc<Tag>]) -> Vec<Block> {
    let mut sorted: Vec<Arc<Tag>> = tags.iter().filter(|t| reg0(t) >= 0).cloned().collect();
    sorted.sort_by_key(|t| reg0(t));
    let mut blocks: Vec<Block> = Vec::new();
    for tag in sorted {
        let (start, end) = (reg0(&tag), reg0(&tag) + width(&tag));
        match blocks.last_mut() {
            Some(b) if end - b.start as i32 <= MAX_BLOCK_REGISTERS => {
                b.count = b.count.max((end - b.start as i32) as u16);
                b.tags.push(tag);
            }
            _ => blocks.push(Block { start: start as u16, count: (end - start) as u16, tags: vec![tag] }),
        }
    }
    blocks
}

/// Значение тега из сырых регистров блока (`off` — смещение тега от начала блока).
pub fn decode(tag: &Tag, regs: &[u16], off: usize) -> Option<TagValue> {
    if model::is_float(&tag.data_type) {
        let (lo, hi) = (*regs.get(off)?, *regs.get(off + 1)?);
        return Some(TagValue::F32(f32::from_bits(((hi as u32) << 16) | lo as u32)));
    }
    let reg = *regs.get(off)?;
    if model::is_bool(&tag.data_type) {
        return Some(TagValue::Bool(reg != 0));
    }
    // Регистр беззнаковый, а целый тег знаковый: коды состояния ptusa бывают отрицательными.
    Some(TagValue::Int(reg as i16 as i64))
}

/// Клиент одного Modbus-контроллера: ленивое соединение, сброс при ошибке.
pub struct ModbusClient {
    host: String,
    port: u16,
    unit_id: u8,
    op_timeout: Duration,
    ctx: Option<Context>,
}

impl ModbusClient {
    pub fn new(host: String, port: u16, unit_id: u8, op_timeout: Duration) -> Self {
        ModbusClient { host, port, unit_id, op_timeout, ctx: None }
    }

    async fn context(&mut self) -> Result<&mut Context> {
        if self.ctx.is_none() {
            let stream = timeout(self.op_timeout, TcpStream::connect((self.host.as_str(), self.port)))
                .await
                .context("таймаут подключения")??;
            stream.set_nodelay(true).ok();
            self.ctx = Some(tokio_modbus::client::tcp::attach_slave(stream, Slave(self.unit_id)));
        }
        Ok(self.ctx.as_mut().expect("соединение только что создано"))
    }

    /// Прочитать все блоки плана. Ошибка — весь цикл BAD, соединение сбрасывается.
    pub async fn read(&mut self, blocks: &[Block]) -> Result<Vec<(Arc<Tag>, Option<TagValue>)>> {
        let result = self.read_inner(blocks).await;
        if result.is_err() {
            self.ctx = None;
        }
        result
    }

    async fn read_inner(&mut self, blocks: &[Block]) -> Result<Vec<(Arc<Tag>, Option<TagValue>)>> {
        let op_timeout = self.op_timeout;
        let ctx = self.context().await?;
        let mut out = Vec::with_capacity(blocks.iter().map(|b| b.tags.len()).sum());
        for block in blocks {
            let regs = timeout(op_timeout, ctx.read_holding_registers(block.start, block.count))
                .await
                .context("таймаут чтения")??
                .map_err(|e| anyhow!("исключение Modbus {e:?}"))?;
            for tag in &block.tags {
                let off = (reg0(tag) - block.start as i32) as usize;
                out.push((tag.clone(), decode(tag, &regs, off)));
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::TagConfig;

    fn tag(addr: i32, dt: &str) -> Arc<Tag> {
        Arc::new(Tag::from_config(&TagConfig {
            name: format!("t{addr}"),
            node_id: format!("modbus:{addr}"),
            channel_id: None,
            device_name: None,
            field_name: None,
            device_type: None,
            protocol: Some("modbus".into()),
            data_type: dt.into(),
            polling_rate: 2000,
            enabled: true,
            writable: false,
            unit: None,
            min_value: None,
            max_value: None,
            modbus_address: Some(addr),
            modbus_type: None,
            modbus_unit_id: None,
        }))
    }

    #[test]
    fn endpoint_parsing() {
        assert_eq!(endpoint("modbus://simulator:5020"), ("simulator".into(), 5020));
        assert_eq!(endpoint("modbus://10.0.0.5"), ("10.0.0.5".into(), 502));
    }

    #[test]
    fn contiguous_tags_share_a_block() {
        let tags = vec![tag(40003, "FLOAT"), tag(40001, "INT32"), tag(40002, "INT32")];
        let blocks = plan_blocks(&tags);
        assert_eq!(blocks.len(), 1);
        assert_eq!((blocks[0].start, blocks[0].count), (0, 4));
    }

    #[test]
    fn block_is_split_at_120_registers() {
        let tags: Vec<_> = (0..70).map(|i| tag(40001 + i * 2, "FLOAT")).collect();
        let blocks = plan_blocks(&tags);
        assert_eq!(blocks.len(), 2);
        assert!(blocks.iter().all(|b| b.count <= 120));
        assert_eq!(blocks.iter().map(|b| b.tags.len()).sum::<usize>(), 70);
    }

    #[test]
    fn decode_like_simulator() {
        let bits = 64.7f32.to_bits();
        let regs = [(bits & 0xFFFF) as u16, (bits >> 16) as u16, 0xFFFB, 1];
        assert_eq!(decode(&tag(40001, "FLOAT"), &regs, 0), Some(TagValue::F32(64.7)));
        assert_eq!(decode(&tag(40003, "INT32"), &regs, 2), Some(TagValue::Int(-5)));
        assert_eq!(decode(&tag(40004, "BOOLEAN"), &regs, 3), Some(TagValue::Bool(true)));
        assert_eq!(decode(&tag(40004, "FLOAT"), &regs, 3), None, "второго регистра нет");
    }
}
