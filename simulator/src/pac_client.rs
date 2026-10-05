//! Клиент driver-master для проверки любого PAC — симулятора или настоящей прошивки ptusa: одно
//! TCP-соединение, запрос → ответ (тот же проводной формат, что и у сервера в `pac.rs`).

use std::io::Read;
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::pac::{BANNER, CMD_EXEC_DEVICE_COMMAND, CMD_GET_DEVICES, CMD_GET_DEVICES_STATES, CMD_GET_INFO_ON_CONNECT};

const NET_ID: u8 = b's';
const STATUS_ERROR: u8 = 7;

pub struct PacClient {
    stream: TcpStream,
    pidx: u8,
}

impl PacClient {
    /// Подключиться, принять приветствие `PAC accept` и сразу сделать handshake (прошивка рвёт молчащее
    /// соединение через ~300 мс).
    pub async fn connect(host: &str, port: u16) -> Result<Self> {
        let mut stream = tokio::time::timeout(Duration::from_secs(3), TcpStream::connect((host, port)))
            .await
            .with_context(|| format!("PAC {host}:{port}: таймаут подключения"))?
            .with_context(|| format!("PAC {host}:{port}: нет подключения"))?;
        stream.set_nodelay(true).ok();
        let mut banner = vec![0u8; BANNER.len()];
        stream.read_exact(&mut banner).await?;
        ensure!(banner == BANNER, "нет приветствия PAC accept: {banner:?}");
        let mut client = PacClient { stream, pidx: 0 };
        client.request(CMD_GET_INFO_ON_CONNECT, &[]).await?;
        Ok(client)
    }

    /// Запрос → распакованное тело ответа.
    pub async fn request(&mut self, cmd: u8, extra: &[u8]) -> Result<Vec<u8>> {
        self.pidx = self.pidx.wrapping_add(1);
        let len = 1 + extra.len();
        let mut frame = vec![NET_ID, 1, 1, self.pidx, (len >> 8) as u8, len as u8, cmd];
        frame.extend_from_slice(extra);
        self.stream.write_all(&frame).await?;
        let mut hdr = [0u8; 5];
        tokio::time::timeout(Duration::from_secs(5), self.stream.read_exact(&mut hdr))
            .await
            .context("PAC: таймаут ответа")??;
        ensure!(hdr[0] == NET_ID && hdr[1] != STATUS_ERROR, "PAC: ответ {hdr:?} на команду {cmd}");
        let mut packed = vec![0u8; (usize::from(hdr[3]) << 8) | usize::from(hdr[4])];
        self.stream.read_exact(&mut packed).await?;
        if packed.is_empty() {
            return Ok(Vec::new());
        }
        let mut body = Vec::new();
        flate2::read::ZlibDecoder::new(&packed[..]).take(8 * 1024 * 1024).read_to_end(&mut body)?;
        Ok(body)
    }

    /// Lua-текст ответа на запрос devices/states (без 2 байт devices_request_id и хвоста после `\0`).
    pub async fn lua(&mut self, cmd: u8) -> Result<String> {
        let body = self.request(cmd, &[]).await?;
        let text = body.get(2..).unwrap_or_default();
        let end = text.iter().position(|&b| b == 0).unwrap_or(text.len());
        Ok(String::from_utf8_lossy(&text[..end]).into_owned())
    }

    pub async fn devices_lua(&mut self) -> Result<String> {
        self.lua(CMD_GET_DEVICES).await
    }

    pub async fn states_lua(&mut self) -> Result<String> {
        self.lua(CMD_GET_DEVICES_STATES).await
    }

    /// EXEC_DEVICE_COMMAND: код результата (0 — принято).
    pub async fn exec(&mut self, text: &str) -> Result<u16> {
        let body = self.request(CMD_EXEC_DEVICE_COMMAND, text.as_bytes()).await?;
        Ok(if body.len() >= 2 { u16::from_le_bytes([body[0], body[1]]) } else { 0 })
    }
}
