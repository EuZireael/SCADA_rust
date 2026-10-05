//! Modbus TCP-сервер симулятора: holding-регистры 0..65535 (FC03/04 — чтение, FC06/16 — запись).
//!
//! Держит один набор регистров (как `ModbusSlaveContext` в pymodbus: все адреса читаются, пустые —
//! нулями; unit id любой). Цикл симулятора кладёт сюда значения тегов и читает обратно регистры
//! RW-тегов — так запись шлюза не затирается. FLOAT занимает два регистра, младшее слово первым.

use std::sync::{Arc, Mutex};

use anyhow::Result;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, info};

const REGISTERS: usize = 0x1_0000;
const FC_READ_HOLDING: u8 = 3;
const FC_READ_INPUT: u8 = 4;
const FC_WRITE_SINGLE: u8 = 6;
const FC_WRITE_MULTIPLE: u8 = 16;
const EX_ILLEGAL_FUNCTION: u8 = 1;
const EX_ILLEGAL_ADDRESS: u8 = 2;
const EX_ILLEGAL_VALUE: u8 = 3;
/// Предел спецификации: ответ FC03 — до 125 регистров, запрос FC16 — до 123.
const MAX_READ: u16 = 125;
const MAX_WRITE: u16 = 123;

/// Регистры сервера.
pub struct Registers {
    regs: Mutex<Vec<u16>>,
}

impl Default for Registers {
    fn default() -> Self {
        Self::new()
    }
}

impl Registers {
    pub fn new() -> Self {
        Registers { regs: Mutex::new(vec![0; REGISTERS]) }
    }

    /// `None` — диапазон выходит за 0..65535.
    pub fn read(&self, address: u16, count: u16) -> Option<Vec<u16>> {
        let start = usize::from(address);
        let end = start.checked_add(usize::from(count)).filter(|&e| e <= REGISTERS)?;
        Some(self.regs.lock().expect("регистры")[start..end].to_vec())
    }

    /// false — диапазон выходит за 0..65535 (ничего не записано).
    pub fn write(&self, address: u16, values: &[u16]) -> bool {
        let start = usize::from(address);
        let Some(end) = start.checked_add(values.len()).filter(|&e| e <= REGISTERS) else { return false };
        self.regs.lock().expect("регистры")[start..end].copy_from_slice(values);
        true
    }
}

fn exception(fc: u8, code: u8) -> Vec<u8> {
    vec![fc | 0x80, code]
}

/// Ответ на один PDU запроса (код функции + данные).
pub fn process(regs: &Registers, pdu: &[u8]) -> Vec<u8> {
    let Some(&fc) = pdu.first() else { return exception(0, EX_ILLEGAL_FUNCTION) };
    let word = |i: usize| pdu.get(i..i + 2).map(|b| u16::from_be_bytes([b[0], b[1]]));
    match fc {
        FC_READ_HOLDING | FC_READ_INPUT => {
            let (Some(address), Some(count)) = (word(1), word(3)) else { return exception(fc, EX_ILLEGAL_VALUE) };
            if !(1..=MAX_READ).contains(&count) {
                return exception(fc, EX_ILLEGAL_VALUE);
            }
            let Some(values) = regs.read(address, count) else { return exception(fc, EX_ILLEGAL_ADDRESS) };
            let mut out = vec![fc, (count * 2) as u8];
            values.iter().for_each(|v| out.extend(v.to_be_bytes()));
            out
        }
        FC_WRITE_SINGLE => {
            let (Some(address), Some(value)) = (word(1), word(3)) else { return exception(fc, EX_ILLEGAL_VALUE) };
            if !regs.write(address, &[value]) {
                return exception(fc, EX_ILLEGAL_ADDRESS);
            }
            pdu[..5].to_vec()
        }
        FC_WRITE_MULTIPLE => {
            let (Some(address), Some(count)) = (word(1), word(3)) else { return exception(fc, EX_ILLEGAL_VALUE) };
            let bytes = pdu.get(5).copied().map(usize::from);
            let data = pdu.get(6..);
            if !(1..=MAX_WRITE).contains(&count)
                || bytes != Some(usize::from(count) * 2)
                || data.map(<[u8]>::len) != bytes
            {
                return exception(fc, EX_ILLEGAL_VALUE);
            }
            let values: Vec<u16> =
                data.unwrap_or_default().as_chunks::<2>().0.iter().map(|b| u16::from_be_bytes(*b)).collect();
            if !regs.write(address, &values) {
                return exception(fc, EX_ILLEGAL_ADDRESS);
            }
            pdu[..5].to_vec()
        }
        _ => exception(fc, EX_ILLEGAL_FUNCTION),
    }
}

async fn handle(mut stream: TcpStream, regs: Arc<Registers>) -> Result<()> {
    loop {
        let mut mbap = [0u8; 7];
        if stream.read_exact(&mut mbap).await.is_err() {
            return Ok(()); // клиент закрыл соединение
        }
        let length = usize::from(u16::from_be_bytes([mbap[4], mbap[5]]));
        if mbap[2] != 0 || mbap[3] != 0 || !(2..=254).contains(&length) {
            return Ok(()); // не Modbus TCP — рвём связь
        }
        let mut pdu = vec![0u8; length - 1];
        stream.read_exact(&mut pdu).await?;
        let reply = process(&regs, &pdu);
        let mut frame = Vec::with_capacity(7 + reply.len());
        frame.extend_from_slice(&mbap[..4]);
        frame.extend(((reply.len() + 1) as u16).to_be_bytes());
        frame.push(mbap[6]);
        frame.extend(reply);
        stream.write_all(&frame).await?;
    }
}

/// Принимает соединения, пока жив процесс.
pub async fn serve(listener: TcpListener, regs: Arc<Registers>) {
    if let Ok(addr) = listener.local_addr() {
        info!("Modbus TCP: порт {}", addr.port());
    }
    loop {
        match listener.accept().await {
            Ok((stream, peer)) => {
                let regs = regs.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle(stream, regs).await {
                        debug!("Modbus {peer}: {e}");
                    }
                });
            }
            Err(e) => {
                tracing::warn!("Modbus: accept: {e}");
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_return_big_endian_registers_and_zeros_for_blank_ones() {
        let r = Registers::new();
        r.write(100, &[0x1234, 0xABCD]);
        assert_eq!(process(&r, &[3, 0, 100, 0, 3]), vec![3, 6, 0x12, 0x34, 0xAB, 0xCD, 0, 0]);
        assert_eq!(process(&r, &[4, 0, 100, 0, 1]), vec![4, 2, 0x12, 0x34], "FC04 читает те же регистры");
    }

    #[test]
    fn read_limits_and_address_range() {
        let r = Registers::new();
        assert_eq!(process(&r, &[3, 0, 0, 0, 0]), vec![0x83, EX_ILLEGAL_VALUE], "0 регистров");
        assert_eq!(process(&r, &[3, 0, 0, 0, 126]), vec![0x83, EX_ILLEGAL_VALUE], "больше 125");
        assert_eq!(process(&r, &[3, 0, 0, 0, 125]).len(), 2 + 250);
        assert_eq!(process(&r, &[3, 0xFF, 0xFF, 0, 1]).len(), 4, "последний регистр читается");
        assert_eq!(process(&r, &[3, 0xFF, 0xFF, 0, 2]), vec![0x83, EX_ILLEGAL_ADDRESS]);
        assert_eq!(process(&r, &[3, 0, 0]), vec![0x83, EX_ILLEGAL_VALUE], "обрезанный запрос");
    }

    #[test]
    fn writes_single_and_multiple() {
        let r = Registers::new();
        assert_eq!(process(&r, &[6, 0, 7, 0xFF, 0xFB]), vec![6, 0, 7, 0xFF, 0xFB]);
        assert_eq!(r.read(7, 1), Some(vec![0xFFFB]));
        assert_eq!(process(&r, &[16, 0, 20, 0, 2, 4, 0, 1, 0, 2]), vec![16, 0, 20, 0, 2]);
        assert_eq!(r.read(20, 2), Some(vec![1, 2]));
        // Число байт не сходится с количеством регистров.
        assert_eq!(process(&r, &[16, 0, 20, 0, 2, 2, 0, 1]), vec![0x90, EX_ILLEGAL_VALUE]);
        assert_eq!(process(&r, &[16, 0xFF, 0xFF, 0, 2, 4, 0, 1, 0, 2]), vec![0x90, EX_ILLEGAL_ADDRESS]);
        assert_eq!(r.read(0xFFFF, 1), Some(vec![0]), "неудачная запись ничего не меняет");
    }

    #[test]
    fn unsupported_function_is_an_exception() {
        assert_eq!(process(&Registers::new(), &[1, 0, 0, 0, 1]), vec![0x81, EX_ILLEGAL_FUNCTION]);
    }

    #[tokio::test]
    async fn serves_a_tcp_client_with_any_unit_id() {
        let regs = Arc::new(Registers::new());
        regs.write(5, &[42]);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(serve(listener, regs));
        let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        s.write_all(&[0, 9, 0, 0, 0, 6, 17, 3, 0, 5, 0, 1]).await.unwrap();
        let mut resp = [0u8; 11];
        tokio::time::timeout(std::time::Duration::from_secs(3), s.read_exact(&mut resp)).await.unwrap().unwrap();
        assert_eq!(resp, [0, 9, 0, 0, 0, 5, 17, 3, 2, 0, 42]);
    }
}
