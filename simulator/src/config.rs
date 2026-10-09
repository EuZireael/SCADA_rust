//! Конфигурация симулятора (`config/replay_config.yaml`).

use std::path::Path;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Deserializer};

use crate::replay::{Mode, ReplaySpec};
use crate::value::{DataType, Value};

/// Корень конфигурации симулятора (`replay_config.yaml`): контроллер, порты Modbus и PAC, реплей архива.
#[derive(Debug, Deserialize)]
pub struct Config {
    pub plc: PlcCfg,
    #[serde(default = "default_modbus_port")]
    pub modbus_port: u16,
    #[serde(default = "default_pac_port")]
    pub pac_port: u16,
    #[serde(default)]
    pub replay: ReplayCfg,
}

/// Порт Modbus TCP по умолчанию.
fn default_modbus_port() -> u16 {
    5020
}

/// Порт PAC (driver-master) по умолчанию.
fn default_pac_port() -> u16 {
    10000
}

/// Контроллер: идентификатор, имя, адрес OPC UA, период цикла и блоки данных с тегами.
#[derive(Debug, Deserialize)]
pub struct PlcCfg {
    pub id: String,
    pub name: String,
    pub endpoint: String,
    #[serde(default = "default_update_rate")]
    pub update_rate: f64,
    pub data_blocks: Vec<DbCfg>,
}

/// Период цикла обновления по умолчанию, секунд.
fn default_update_rate() -> f64 {
    0.5
}

/// Блок данных контроллера: группа тегов.
#[derive(Debug, Deserialize)]
#[allow(dead_code)] // db_number и name описывают DB контроллера; адресация от них не зависит
pub struct DbCfg {
    pub db_number: u32,
    pub name: String,
    pub tags: Vec<TagCfg>,
}

/// Реплей архива: откуда брать значения и с какой скоростью.
#[derive(Debug, Default, Deserialize)]
pub struct ReplayCfg {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub data_path: String,
    #[serde(default = "default_speed")]
    pub speed: f64,
    #[serde(default = "default_true", rename = "loop")]
    pub looped: bool,
}

/// Скорость реплея по умолчанию: реальное время.
fn default_speed() -> f64 {
    1.0
}

/// Умолчание `true` для serde.
fn default_true() -> bool {
    true
}

/// Строка или число (в YAML имена и адреса пишут и так, и так).
fn text<'de, D: Deserializer<'de>>(d: D) -> Result<String, D::Error> {
    Ok(match serde_yaml_ng::Value::deserialize(d)? {
        serde_yaml_ng::Value::String(s) => s,
        serde_yaml_ng::Value::Number(n) => n.to_string(),
        serde_yaml_ng::Value::Bool(b) => b.to_string(),
        other => return Err(serde::de::Error::custom(format!("ожидалась строка, а не {other:?}"))),
    })
}

/// То же для необязательного поля.
fn opt_text<'de, D: Deserializer<'de>>(d: D) -> Result<Option<String>, D::Error> {
    Ok(match Option::<serde_yaml_ng::Value>::deserialize(d)? {
        None | Some(serde_yaml_ng::Value::Null) => None,
        Some(serde_yaml_ng::Value::String(s)) => Some(s),
        Some(serde_yaml_ng::Value::Number(n)) => Some(n.to_string()),
        Some(other) => return Err(serde::de::Error::custom(format!("ожидалась строка, а не {other:?}"))),
    })
}

/// Тег из YAML. Неизвестные ключи (`noise_enabled`, `drift_enabled`…) игнорируются.
#[derive(Debug, Deserialize)]
pub struct TagCfg {
    #[serde(deserialize_with = "text")]
    pub name: String,
    #[serde(default, deserialize_with = "opt_text")]
    pub address: Option<String>,
    #[serde(rename = "type")]
    pub data_type: String,
    #[serde(default)]
    pub protocol: Option<String>,
    #[serde(default, deserialize_with = "opt_text")]
    pub device: Option<String>,
    #[serde(default, deserialize_with = "opt_text")]
    pub field: Option<String>,
    #[serde(default, deserialize_with = "opt_text")]
    pub dev_type: Option<String>,
    #[serde(default)]
    pub access: Option<String>,
    #[serde(default)]
    pub unit: Option<String>,
    #[serde(default)]
    pub initial: Option<serde_yaml_ng::Value>,
    #[serde(default)]
    pub generator: Option<String>,
    #[serde(default)]
    pub modbus_address: Option<u16>,
    #[serde(default)]
    pub modbus_type: Option<String>,
    #[serde(default, deserialize_with = "opt_text")]
    pub replay_source: Option<String>,
    #[serde(default)]
    pub replay_offset: Option<f64>,
    #[serde(default)]
    pub replay_mode: Option<String>,
    #[serde(default)]
    pub replay_valid_min: Option<f64>,
    #[serde(default)]
    pub replay_valid_max: Option<f64>,
    #[serde(default)]
    pub replay_above: Option<f64>,
    #[serde(default)]
    pub replay_on: Option<f64>,
    #[serde(default)]
    pub replay_off: Option<f64>,
    #[serde(default)]
    pub replay_scale: Option<f64>,
    #[serde(default)]
    pub replay_bias: Option<f64>,
    #[serde(default)]
    pub replay_min: Option<f64>,
    #[serde(default)]
    pub replay_max: Option<f64>,
    #[serde(default)]
    pub replay_format: Option<String>,
    /// Что делает запись оператора с этим тегом (поведение настоящей прошивки ptusa, снятое с эмулятора мойки):
    /// `hold` (по умолчанию) — значение принимается и держится; `ignore` — команда принимается (код 0), но значение
    /// считает программа ПЛК (датчики, обратная связь, регистры команд и состояния техобъектов) — оно не меняется.
    #[serde(default)]
    pub write: Option<String>,
    /// Запись действует, только если другое поле прибора имеет значение (например, `ST` клапана `LINE1V0`
    /// принимается лишь в ручном режиме: `{field: M, equals: 1}`); иначе команда принимается, но значение
    /// остаётся за программой. Как только условие пропало — значение возвращается к тому, что считает программа.
    #[serde(default)]
    pub write_requires: Option<RequiresCfg>,
    /// Прошивка хранит поле целым: записанное `7.5` становится `7`.
    #[serde(default)]
    pub write_int: bool,
    /// Поле — состояние (клапан, выход): целая часть записанного, отличная от нуля, даёт 1.
    #[serde(default)]
    pub write_state: bool,
}

/// Условие действия записи: поле `field` прибора `device` (по умолчанию — того же прибора) равно `equals`.
#[derive(Debug, Clone, Deserialize)]
pub struct RequiresCfg {
    #[serde(default, deserialize_with = "opt_text")]
    pub device: Option<String>,
    #[serde(deserialize_with = "text")]
    pub field: String,
    pub equals: f64,
}

impl TagCfg {
    /// Адрес тега: `address` из YAML, а если не задан — имя.
    pub fn address(&self) -> String {
        self.address.clone().unwrap_or_else(|| self.name.clone())
    }

    /// Тип данных тега; неизвестное имя — ошибка с именем тега.
    pub fn parsed_type(&self) -> Result<DataType> {
        DataType::parse(&self.data_type)
            .with_context(|| format!("тег {}: неизвестный type={:?}", self.name, self.data_type))
    }

    /// Как канал берёт значение из серии архива (источник по умолчанию — address тега).
    pub fn replay_spec(&self) -> Result<ReplaySpec> {
        let mode = match &self.replay_mode {
            Some(m) => Mode::parse(m).with_context(|| format!("тег {}", self.name))?,
            None => Mode::Value,
        };
        Ok(ReplaySpec {
            source: self.replay_source.clone().unwrap_or_else(|| self.address()),
            offset: self.replay_offset.unwrap_or(0.0),
            mode,
            valid_min: self.replay_valid_min,
            valid_max: self.replay_valid_max,
            above: self.replay_above,
            on: self.replay_on.unwrap_or(1.0),
            off: self.replay_off.unwrap_or(0.0),
            scale: self.replay_scale.unwrap_or(1.0),
            bias: self.replay_bias.unwrap_or(0.0),
            clamp_min: self.replay_min,
            clamp_max: self.replay_max,
        })
    }

    /// Начальное значение (`initial:`) в типе тега.
    pub fn initial_value(&self, data_type: DataType) -> Value {
        use serde_yaml_ng::Value as Y;
        let raw = match &self.initial {
            None | Some(Y::Null) => Value::Int(0),
            Some(Y::Bool(b)) => Value::Bool(*b),
            Some(Y::Number(n)) => n.as_f64().map(Value::Float).unwrap_or(Value::Int(0)),
            Some(Y::String(s)) => Value::Text(s.clone()),
            Some(_) => Value::Int(0),
        };
        data_type.convert(&raw).unwrap_or_else(|| data_type.zero())
    }
}

/// Прочитать и разобрать конфигурацию; пустой список блоков данных — ошибка.
pub fn load(path: &Path) -> Result<Config> {
    let raw = std::fs::read_to_string(path).with_context(|| format!("конфигурация {}", path.display()))?;
    let cfg: Config = serde_yaml_ng::from_str(&raw).with_context(|| format!("конфигурация {}", path.display()))?;
    if cfg.plc.data_blocks.is_empty() {
        bail!("{}: нет data_blocks", path.display());
    }
    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tag(yaml: &str) -> TagCfg {
        serde_yaml_ng::from_str(yaml).unwrap()
    }

    #[test]
    fn replay_spec_reads_flat_keys() {
        let t = tag("{name: 385, type: float, replay_source: 2A1D0000, replay_mode: integral, replay_valid_min: 0, \
             replay_valid_max: 120, replay_above: 1, replay_on: 75, replay_scale: 0.5, replay_bias: 2, \
             replay_min: 0, replay_max: 100, replay_offset: 30}");
        let s = t.replay_spec().unwrap();
        assert_eq!(
            (s.source.as_str(), s.mode, s.valid_min, s.valid_max, s.above, s.on, s.off),
            ("2A1D0000", Mode::Integral, Some(0.0), Some(120.0), Some(1.0), 75.0, 0.0)
        );
        assert_eq!((s.scale, s.bias, s.clamp_min, s.clamp_max, s.offset), (0.5, 2.0, Some(0.0), Some(100.0), 30.0));
        // Источник по умолчанию — адрес тега, а числовое имя — строка.
        let t = tag("{name: 385, type: int}");
        assert_eq!(t.name, "385");
        assert_eq!(t.replay_spec().unwrap().source, "385");
    }

    #[test]
    fn spec_rejects_unknown_mode_and_type() {
        assert!(tag("{name: a, type: float, replay_mode: sum}").replay_spec().is_err());
        assert!(tag("{name: a, type: decimal}").parsed_type().is_err());
    }

    #[test]
    fn initial_value_is_converted_to_the_tag_type() {
        assert_eq!(tag("{name: a, type: float, initial: 1}").initial_value(DataType::Float), Value::Float(1.0));
        assert_eq!(tag("{name: a, type: int, initial: 7.0}").initial_value(DataType::Int), Value::Int(7));
        assert_eq!(
            tag("{name: a, type: string, initial: \"\"}").initial_value(DataType::String),
            Value::Text(String::new())
        );
        assert_eq!(tag("{name: a, type: bool}").initial_value(DataType::Bool), Value::Bool(false));
    }

    /// Рабочая конфигурация репозитория разбирается целиком.
    #[test]
    fn repository_config_parses() {
        let cfg = load(&Path::new(env!("CARGO_MANIFEST_DIR")).join("config/replay_config.yaml")).unwrap();
        let tags: Vec<&TagCfg> = cfg.plc.data_blocks.iter().flat_map(|d| &d.tags).collect();
        assert_eq!(tags.len(), 2517);
        for t in &tags {
            t.parsed_type().unwrap();
            t.replay_spec().unwrap();
        }
        assert_eq!((cfg.modbus_port, cfg.pac_port), (5020, 10000));
        assert!(cfg.replay.enabled && cfg.replay.looped);
    }
}
