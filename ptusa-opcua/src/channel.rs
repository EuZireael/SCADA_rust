//! Каналы станции: что фасад отдаёт по OPC UA (из `config/stations/*.yaml`) и как значение
//! снимка превращается в значение узла, а запись клиента — в команду прошивке.

use std::collections::HashSet;
use std::path::Path;

use anyhow::{Context, Result};
use serde::Deserialize;
use tracing::warn;

use crate::snapshot::Raw;

/// Тип узла по `dataType` канала.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Int32,
    Float,
    String,
    Boolean,
}

impl Kind {
    /// INT32 → Int32, FLOAT → Float, STRING → String, BOOLEAN → Boolean; неизвестный — Float.
    pub fn parse(data_type: &str) -> Kind {
        match data_type.to_ascii_uppercase().as_str() {
            "INT32" => Kind::Int32,
            "STRING" => Kind::String,
            "BOOLEAN" => Kind::Boolean,
            _ => Kind::Float,
        }
    }
}

/// Канал станции: прибор, поле прибора и имя узла OPC UA.
#[derive(Debug, Clone, PartialEq)]
pub struct Channel {
    /// Прибор (`LINE1V0`).
    pub device: String,
    /// Поле как в базе каналов (`RT_PAR_F[12]`, `PAR_MAIN[1].P_CZAD_S`).
    pub field: String,
    /// Имя поля без индекса и подписи.
    pub base: String,
    /// Индекс массива (Lua-индекс с 1).
    pub index: Option<u32>,
    pub kind: Kind,
    pub writable: bool,
    /// `<прибор>.<поле>[индекс]` — NodeId (`ns=2;s=…`).
    pub node_name: String,
}

/// Корень конфигурации станции (`opcua:`).
#[derive(Deserialize)]
struct Root {
    opcua: OpcUa,
}

/// Секция `opcua`.
#[derive(Deserialize)]
struct OpcUa {
    #[serde(default)]
    servers: Vec<Server>,
}

/// Контроллер станции.
#[derive(Deserialize)]
struct Server {
    #[serde(default)]
    tags: Vec<Tag>,
}

/// Тег станции: нужны только прибор, поле, тип и права.
#[derive(Deserialize)]
struct Tag {
    #[serde(default, rename = "deviceName")]
    device_name: Option<String>,
    #[serde(default, rename = "fieldName")]
    field_name: Option<String>,
    #[serde(default, rename = "dataType")]
    data_type: Option<String>,
    #[serde(default)]
    enabled: bool,
    #[serde(default)]
    writable: bool,
}

/// Имя поля и необязательный индекс массива: `ST` → (ST, None), `ST_CH[ 3 ]` → (ST_CH, Some(3)),
/// `PAR_MAIN[2].P_CZAD_S` → (PAR_MAIN, Some(2)) — хвост после `]` подпись канала, в адрес не входит.
/// None — поле не начинается с имени.
pub fn parse_field(field: &str) -> Option<(String, Option<u32>)> {
    let mut chars = field.char_indices();
    let (_, first) = chars.next()?;
    if !(first.is_ascii_alphabetic() || first == '_') {
        return None;
    }
    let end = chars.find(|(_, c)| !(c.is_ascii_alphanumeric() || *c == '_')).map_or(field.len(), |(i, _)| i);
    let base = field[..end].to_string();
    let index = field[end..].strip_prefix('[').and_then(|rest| {
        let (inside, _) = rest.split_once(']')?;
        let digits = inside.trim();
        (!digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit())).then(|| digits.parse().ok())?
    });
    Some((base, index))
}

/// Включённые каналы конфигурации станции с адресом в ПЛК (deviceName/fieldName), без дублей.
pub fn load_channels(path: &Path) -> Result<Vec<Channel>> {
    let raw = std::fs::read_to_string(path).with_context(|| format!("конфигурация станции {}", path.display()))?;
    let root: Root =
        serde_yaml_ng::from_str(&raw).with_context(|| format!("конфигурация станции {}", path.display()))?;
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for tag in root.opcua.servers.iter().flat_map(|s| &s.tags) {
        let (Some(device), Some(field)) = (&tag.device_name, &tag.field_name) else { continue };
        if !tag.enabled || device.is_empty() || field.is_empty() {
            continue;
        }
        let Some((base, index)) = parse_field(field) else {
            warn!("поле {device}.{field} не разобрано — пропуск");
            continue;
        };
        let node_name = match index {
            Some(i) => format!("{device}.{base}[{i}]"),
            None => format!("{device}.{base}"),
        };
        if !seen.insert(node_name.clone()) {
            continue;
        }
        out.push(Channel {
            device: device.clone(),
            field: field.clone(),
            base,
            index,
            kind: Kind::parse(tag.data_type.as_deref().unwrap_or("FLOAT")),
            writable: tag.writable,
            node_name,
        });
    }
    Ok(out)
}

/// Значение узла, приведённое к типу канала.
#[derive(Debug, Clone, PartialEq)]
pub enum Converted {
    Int(i32),
    Float(f32),
    Text(String),
    Bool(bool),
}

/// Значение снимка → значение узла его типа; None — не приводится.
pub fn convert(raw: &Raw, kind: Kind) -> Option<Converted> {
    if kind == Kind::String {
        return Some(Converted::Text(match raw {
            Raw::Text(s) => s.clone(),
            Raw::Number(n) => number_text(*n),
        }));
    }
    let n = match raw {
        Raw::Number(n) => *n,
        Raw::Text(s) => s.trim().parse().ok()?,
    };
    Some(match kind {
        Kind::Int32 => Converted::Int(n.round() as i32),
        Kind::Boolean => Converted::Bool(n != 0.0),
        _ => Converted::Float(n as f32),
    })
}

/// Число как строка: целое — без `.0` (`5.0` → "5").
fn number_text(n: f64) -> String {
    if n.fract() == 0.0 && n.abs() < 1e15 { format!("{}", n as i64) } else { format!("{n}") }
}

/// Команда прошивке: `__<прибор>:set_cmd('<поле>', <индекс>, <значение>)`. Индекс массива — отдельным
/// аргументом (`set_cmd('RT_PAR_F[12]', 1, v)` ptusa принимает с кодом 0, но ничего не меняет).
pub fn command_text(ch: &Channel, scalar: &str) -> String {
    format!("__{}:set_cmd('{}', {}, {scalar})", ch.device, ch.base, ch.index.unwrap_or(1))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ch(device: &str, base: &str, index: Option<u32>) -> Channel {
        Channel {
            device: device.into(),
            field: base.into(),
            base: base.into(),
            index,
            kind: Kind::Float,
            writable: true,
            node_name: "n".into(),
        }
    }

    #[test]
    fn field_names_normalize_indexes() {
        assert_eq!(parse_field("ST_CH[ 3 ]"), Some(("ST_CH".into(), Some(3))));
        assert_eq!(parse_field("PAR_MAIN[2].P_CZAD_S"), Some(("PAR_MAIN".into(), Some(2))), "хвост — подпись канала");
        assert_eq!(parse_field("ST"), Some(("ST".into(), None)));
        assert_eq!(parse_field("RT_PAR_F[x]"), Some(("RT_PAR_F".into(), None)), "не индекс");
        assert_eq!(parse_field("1V1"), None);
        assert_eq!(parse_field(""), None);
    }

    #[test]
    fn convert_by_kind() {
        assert_eq!(convert(&Raw::Number(1.0), Kind::Int32), Some(Converted::Int(1)));
        assert_eq!(convert(&Raw::Number(2.6), Kind::Int32), Some(Converted::Int(3)));
        assert_eq!(convert(&Raw::Number(3.0), Kind::Float), Some(Converted::Float(3.0)));
        assert_eq!(convert(&Raw::Number(0.0), Kind::Boolean), Some(Converted::Bool(false)));
        assert_eq!(convert(&Raw::Text("0 дн.".into()), Kind::String), Some(Converted::Text("0 дн.".into())));
        assert_eq!(convert(&Raw::Number(5.0), Kind::String), Some(Converted::Text("5".into())));
        assert_eq!(convert(&Raw::Number(5.5), Kind::String), Some(Converted::Text("5.5".into())));
        assert_eq!(convert(&Raw::Text("7".into()), Kind::Int32), Some(Converted::Int(7)));
        assert_eq!(convert(&Raw::Text("abc".into()), Kind::Float), None);
    }

    #[test]
    fn command_text_puts_array_index_in_its_own_argument() {
        assert_eq!(command_text(&ch("OBJECT1", "RT_PAR_F", Some(12)), "7.5"), "__OBJECT1:set_cmd('RT_PAR_F', 12, 7.5)");
        assert_eq!(command_text(&ch("LINE1V0", "ST", None), "1"), "__LINE1V0:set_cmd('ST', 1, 1)");
    }

    #[test]
    fn load_channels_skips_disabled_and_dedups() {
        let dir = std::env::temp_dir().join(format!("ptusa-opcua-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("s.yaml");
        std::fs::write(
            &path,
            r#"
opcua:
  servers:
    - tags:
        - {deviceName: LINE1G1, fieldName: "ST_CH[ 1 ]", dataType: INT32, enabled: true, writable: false}
        - {deviceName: LINE1G1, fieldName: "ST_CH[1]", dataType: INT32, enabled: true}
        - {deviceName: LINE1V0, fieldName: ST, dataType: INT32, enabled: false}
        - {deviceName: LINE1V0, fieldName: M, dataType: INT32, enabled: true, writable: true}
        - {name: no-address, dataType: INT32, enabled: true}
"#,
        )
        .unwrap();
        let chans = load_channels(&path).unwrap();
        assert_eq!(chans.iter().map(|c| c.node_name.as_str()).collect::<Vec<_>>(), ["LINE1G1.ST_CH[1]", "LINE1V0.M"]);
        assert_eq!(chans.iter().map(|c| c.writable).collect::<Vec<_>>(), [false, true]);
        std::fs::remove_dir_all(dir).ok();
    }

    /// Рабочая конфигурация станции из репозитория: 1834 канала с адресом в ПЛК, 1816 записываемых.
    #[test]
    fn repository_station_config_loads() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../config/stations/BN1_MCA1.yaml");
        let chans = load_channels(&path).unwrap();
        assert_eq!(chans.len(), 1834);
        assert_eq!(chans.iter().filter(|c| c.writable).count(), 1816);
        assert!(chans.iter().all(|c| !c.node_name.is_empty()));
    }
}
