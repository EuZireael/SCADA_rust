//! Модель тега симулятора: одна переменная контроллера (OPC UA, Modbus или PAC).
//!
//! Значение тега берётся из архива (`generator: replay`, значение подставляет [`crate::plc::Plc`])
//! или остаётся статическим (`initial`). У RW-тегов значение может записать оператор через шлюз;
//! после первой записи тег с источником данных «защёлкивается» на ручном значении и архив его
//! больше не трогает.

use anyhow::{Result, bail};

use crate::config::TagCfg;
use crate::replay::ReplaySpec;
use crate::value::{DataType, Value};

/// Протокол, по которому тег выставляется клиентам.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Protocol {
    OpcUa,
    Modbus,
    /// driver-master (PAC-контроллеры Savushkin/ptusa).
    Pac,
}

/// Как значение раскладывается по регистрам Modbus.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModbusType {
    /// Два регистра, младший первым (как `struct.pack('<f')`).
    Float32,
    /// Два регистра.
    Int32,
    /// Один регистр со знаком.
    Int16,
    /// Один регистр без знака.
    Uint16,
    /// Один регистр: 0 или 1.
    Bool,
}

impl ModbusType {
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "float32" => ModbusType::Float32,
            "int32" => ModbusType::Int32,
            "int16" => ModbusType::Int16,
            "uint16" => ModbusType::Uint16,
            "bool" => ModbusType::Bool,
            _ => return None,
        })
    }

    /// Сколько регистров занимает значение.
    pub fn width(self) -> u16 {
        match self {
            ModbusType::Float32 | ModbusType::Int32 => 2,
            _ => 1,
        }
    }

    /// Значение → регистры (float32 и int32 — два регистра, младшее слово первым).
    pub fn encode(self, value: &Value) -> Vec<u16> {
        let n = value.as_f64().unwrap_or(0.0);
        match self {
            ModbusType::Float32 => {
                let bits = (n as f32).to_bits();
                vec![(bits & 0xFFFF) as u16, (bits >> 16) as u16]
            }
            ModbusType::Int32 => {
                let raw = (n.trunc() as i64 & 0xFFFF_FFFF) as u32;
                vec![(raw & 0xFFFF) as u16, (raw >> 16) as u16]
            }
            ModbusType::Bool => vec![u16::from(n != 0.0)],
            ModbusType::Int16 | ModbusType::Uint16 => vec![(n.trunc() as i64 & 0xFFFF) as u16],
        }
    }

    /// Регистры → значение. int16 разворачиваем СО ЗНАКОМ, иначе запись отрицательного кода вернулась
    /// бы оператору как 65531.
    pub fn decode(self, regs: &[u16]) -> Option<Value> {
        if regs.len() < usize::from(self.width()) {
            return None;
        }
        Some(match self {
            ModbusType::Float32 => {
                Value::Float(f64::from(f32::from_bits(u32::from(regs[0]) | u32::from(regs[1]) << 16)))
            }
            ModbusType::Int32 => Value::Int(i64::from((u32::from(regs[0]) | u32::from(regs[1]) << 16) as i32)),
            ModbusType::Bool => Value::Bool(regs[0] != 0),
            ModbusType::Int16 => Value::Int(i64::from(regs[0] as i16)),
            ModbusType::Uint16 => Value::Int(i64::from(regs[0])),
        })
    }
}

/// Тег симулятора: адрес, тип, протокол, право записи, значение и правила прошивки (какие записи игнорировать).
#[derive(Debug, Clone)]
pub struct Tag {
    pub name: String,
    pub address: String,
    pub device: Option<String>,
    pub field: Option<String>,
    pub dev_type: Option<String>,
    pub unit: String,
    pub data_type: DataType,
    pub writable: bool,
    pub protocol: Protocol,
    pub modbus_address: Option<u16>,
    pub modbus_type: ModbusType,
    /// Значение подставляет архив.
    pub replay: bool,
    pub spec: ReplaySpec,
    pub replay_format: Option<String>,
    pub value: Value,
    /// Значение, которое считает программа ПЛК без участия оператора (`initial:` для тегов без архива).
    pub base: Value,
    /// Оператор записал значение — архив тег больше не трогает.
    pub operator_override: bool,
    /// Запись принимается, но не меняет значение (его считает программа ПЛК).
    pub write_ignored: bool,
    /// Запись действует, только если поле `(device, field)` равно значению (см. `write_requires` в конфигурации).
    pub write_requires: Option<Requires>,
    /// Прошивка хранит поле целым.
    pub write_int: bool,
    /// Состояние: записанное целое, отличное от нуля, — 1.
    pub write_state: bool,
}

/// Условие действия записи.
#[derive(Debug, Clone, PartialEq)]
pub struct Requires {
    pub device: String,
    pub field: String,
    pub equals: f64,
}

impl Tag {
    /// Тег из записи конфигурации; неизвестный тип или протокол — ошибка с именем тега.
    pub fn from_config(cfg: &TagCfg) -> Result<Self> {
        let data_type = cfg.parsed_type()?;
        let protocol = match cfg.protocol.as_deref().unwrap_or("opcua") {
            "opcua" => Protocol::OpcUa,
            "modbus" => Protocol::Modbus,
            "pac" => Protocol::Pac,
            other => bail!("тег {}: неизвестный protocol={other:?}", cfg.name),
        };
        let writable = match cfg.access.as_deref().unwrap_or("RO") {
            "RW" => true,
            "RO" => false,
            other => bail!("тег {}: неизвестный access={other:?}", cfg.name),
        };
        let modbus_type = match cfg.modbus_type.as_deref() {
            None => ModbusType::Float32,
            Some(s) => ModbusType::parse(s)
                .ok_or_else(|| anyhow::anyhow!("тег {}: неизвестный modbus_type={s:?}", cfg.name))?,
        };
        let generator = cfg.generator.as_deref();
        if let Some(g) = generator
            && g != "replay"
        {
            bail!("тег {}: generator={g:?} не поддерживается (только replay)", cfg.name);
        }
        let write_ignored = match cfg.write.as_deref() {
            None | Some("hold") => false,
            Some("ignore") => true,
            Some(other) => bail!("тег {}: write={other:?} — ожидается hold или ignore", cfg.name),
        };
        let write_requires = cfg.write_requires.as_ref().map(|r| Requires {
            device: r.device.clone().or_else(|| cfg.device.clone()).unwrap_or_default(),
            field: r.field.clone(),
            equals: r.equals,
        });
        if let Some(r) = &write_requires
            && r.device.is_empty()
        {
            bail!("тег {}: у write_requires нет прибора (задайте device у тега или у условия)", cfg.name);
        }
        Ok(Tag {
            name: cfg.name.clone(),
            address: cfg.address(),
            device: cfg.device.clone(),
            field: cfg.field.clone(),
            dev_type: cfg.dev_type.clone(),
            unit: cfg.unit.clone().unwrap_or_default(),
            data_type,
            writable,
            protocol,
            modbus_address: cfg.modbus_address,
            modbus_type,
            replay: generator == Some("replay"),
            spec: cfg.replay_spec()?,
            replay_format: cfg.replay_format.clone(),
            value: cfg.initial_value(data_type),
            base: cfg.initial_value(data_type),
            operator_override: false,
            write_ignored,
            write_requires,
            write_int: cfg.write_int,
            write_state: cfg.write_state,
        })
    }

    /// Записанное значение в виде, в котором его хранит прошивка: целое для `write_int`, 0/1 для `write_state`.
    /// None — значение не приводится к типу тега (в прошивке это ошибка Lua, команда получает код 1).
    pub fn shape(&self, value: &Value) -> Option<Value> {
        let converted = self.data_type.convert(value)?;
        let Some(n) = converted.as_f64().filter(|_| self.write_int || self.write_state) else { return Some(converted) };
        let n = n.trunc();
        let n = if self.write_state { f64::from(u8::from(n != 0.0)) } else { n };
        self.data_type.convert(&Value::Float(n))
    }

    /// Запись оператора без условий `write`/`write_requires` (их проверяет [`crate::plc::Plc`], у которого есть
    /// остальные поля прибора): только RW-теги. false — тег не RW или значение не приводится к типу.
    #[cfg(test)]
    pub fn set_operator(&mut self, value: &Value) -> bool {
        if !self.writable {
            return false;
        }
        let Some(shaped) = self.shape(value) else { return false };
        self.value = shaped;
        self.operator_override = true;
        true
    }

    /// Значение из архива, минуя проверку доступа RW.
    pub fn set_replay(&mut self, raw: f64) {
        self.value = match &self.replay_format {
            Some(fmt) => Value::Text(format_int(fmt, raw.trunc() as i64)),
            None => self.data_type.of_f64(raw),
        };
    }
}

/// `"Рецепт %d"` / `"REC-%02d"` с целым числом (единственная форма printf, нужная архиву меток).
pub fn format_int(fmt: &str, n: i64) -> String {
    let mut out = String::new();
    let mut chars = fmt.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '%' {
            out.push(c);
            continue;
        }
        let mut zero = false;
        let mut width = String::new();
        while let Some(&d) = chars.peek() {
            if d == '0' && width.is_empty() && !zero {
                zero = true;
            } else if d.is_ascii_digit() {
                width.push(d);
            } else {
                break;
            }
            chars.next();
        }
        match chars.next() {
            Some('d') => {
                let w: usize = width.parse().unwrap_or(0);
                if zero { out.push_str(&format!("{n:0w$}")) } else { out.push_str(&format!("{n:w$}")) }
            }
            Some('%') => out.push('%'),
            Some(other) => {
                out.push('%');
                out.push_str(&width);
                out.push(other);
            }
            None => out.push('%'),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tag(yaml: &str) -> Tag {
        Tag::from_config(&serde_yaml_ng::from_str::<TagCfg>(yaml).unwrap()).unwrap()
    }

    /// 1.0f == 0x3F800000; little-endian по словам: reg0 = младшее, reg1 = старшее.
    #[test]
    fn float32_is_two_registers_low_word_first() {
        assert_eq!(ModbusType::Float32.encode(&Value::Float(1.0)), vec![0x0000, 0x3F80]);
    }

    #[test]
    fn float32_roundtrip() {
        for v in [0.0, 3.25, -50.0, 42.5] {
            let regs = ModbusType::Float32.encode(&Value::Float(v));
            assert_eq!(ModbusType::Float32.decode(&regs), Some(Value::Float(f64::from(v as f32))));
        }
    }

    #[test]
    fn int16_wraps_to_unsigned_and_decodes_with_sign() {
        assert_eq!(ModbusType::Int16.encode(&Value::Int(5)), vec![5]);
        assert_eq!(ModbusType::Int16.encode(&Value::Int(-1)), vec![0xFFFF]);
        assert_eq!(ModbusType::Int16.decode(&[0xFFFB]), Some(Value::Int(-5)));
        assert_eq!(ModbusType::Uint16.decode(&[0xFFFB]), Some(Value::Int(65531)));
    }

    #[test]
    fn int32_low_high_words() {
        assert_eq!(ModbusType::Int32.encode(&Value::Int(0x0001_2345)), vec![0x2345, 0x0001]);
        assert_eq!(ModbusType::Int32.decode(&[0x2345, 0x0001]), Some(Value::Int(0x0001_2345)));
        assert_eq!(ModbusType::Int32.decode(&ModbusType::Int32.encode(&Value::Int(-7))), Some(Value::Int(-7)));
    }

    #[test]
    fn bool_true_false() {
        assert_eq!(ModbusType::Bool.encode(&Value::Bool(true)), vec![1]);
        assert_eq!(ModbusType::Bool.encode(&Value::Int(0)), vec![0]);
        assert_eq!(ModbusType::Bool.decode(&[7]), Some(Value::Bool(true)));
    }

    #[test]
    fn too_few_registers_do_not_decode() {
        assert_eq!(ModbusType::Float32.decode(&[1]), None);
    }

    #[test]
    fn operator_write_latches_only_writable_tags() {
        let mut rw = tag("{name: a, type: float, access: RW, generator: replay}");
        assert!(rw.set_operator(&Value::Int(3)));
        assert_eq!((rw.value.clone(), rw.operator_override), (Value::Float(3.0), true));
        let mut ro = tag("{name: b, type: float, access: RO}");
        assert!(!ro.set_operator(&Value::Float(1.0)));
        assert_eq!((ro.value.clone(), ro.operator_override), (Value::Float(0.0), false));
        assert!(!rw.set_operator(&Value::Text("abc".into())), "строка не приводится к float");
    }

    #[test]
    fn replay_value_uses_format_for_labels() {
        let mut t = tag("{name: a, type: string, replay_format: 'Рецепт %d', generator: replay}");
        t.set_replay(7.9);
        assert_eq!(t.value, Value::Text("Рецепт 7".into()));
        let mut f = tag("{name: f, type: float, generator: replay}");
        f.set_replay(2.5);
        assert_eq!(f.value, Value::Float(2.5));
    }

    #[test]
    fn printf_int_formats() {
        assert_eq!(format_int("REC-%02d", 7), "REC-07");
        assert_eq!(format_int("%d%%", 5), "5%");
        assert_eq!(format_int("№%3d", 4), "№  4");
    }

    #[test]
    fn only_replay_generator_is_supported() {
        let cfg: TagCfg = serde_yaml_ng::from_str("{name: a, type: float, generator: sine}").unwrap();
        assert!(Tag::from_config(&cfg).is_err());
    }
}
