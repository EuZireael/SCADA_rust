//! Ядро симулятора: теги всех трёх протоколов, воспроизведение архива и цикл обновления.
//!
//! Цикл ([`Plc::cycle`]) на каждом шаге подставляет значения из архива, кладёт свежие значения в
//! Modbus-регистры (RW-регистры читает обратно — запись шлюза не затирается), собирает изменившиеся
//! значения OPC UA-узлов и снимок для PAC. Запись оператора приходит из серверов протоколов:
//! OPC UA — [`Plc::opcua_write`], PAC — [`Plc::pac_write`], Modbus — через регистры.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use anyhow::{Result, ensure};
use tokio::sync::Notify;

use crate::config::Config;
use crate::modbus::Registers;
use crate::replay::Replay;
use crate::tag::{Protocol, Tag};
use crate::value::Value;

pub struct Plc {
    pub id: String,
    pub name: String,
    pub update_rate: Duration,
    state: Mutex<State>,
    replay: Option<Replay>,
    /// Запись оператора: цикл не ждёт конца периода, а обновляет узлы сразу.
    pub wake: Notify,
}

struct State {
    tags: Vec<Tag>,
    by_address: HashMap<String, usize>,
    /// Что мы сами положили в регистры: база для обратного чтения.
    modbus_pushed: HashMap<u16, Vec<u16>>,
    /// Что мы сами положили в OPC UA-узлы (по индексу тега): кладём только изменения.
    opcua_pushed: Vec<Option<Value>>,
}

/// Результат одного шага цикла.
#[derive(Default)]
pub struct Cycle {
    /// OPC UA-узлы, значение которых изменилось: (адрес = NodeId, значение).
    pub opcua: Vec<(String, Value)>,
    /// Снимок PAC: address → значение.
    pub pac: HashMap<String, Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteError {
    UnknownTag,
    NotWritable,
    TypeMismatch,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct Stats {
    pub total: usize,
    pub opcua: usize,
    pub modbus: usize,
    pub pac: usize,
    pub replay: usize,
}

impl Plc {
    pub fn new(cfg: &Config, replay: Option<Replay>) -> Result<Self> {
        let mut tags = Vec::new();
        for db in &cfg.plc.data_blocks {
            for tag_cfg in &db.tags {
                tags.push(Tag::from_config(tag_cfg)?);
            }
        }
        let mut by_address = HashMap::with_capacity(tags.len());
        for (i, t) in tags.iter().enumerate() {
            ensure!(by_address.insert(t.address.clone(), i).is_none(), "address {:?} повторяется", t.address);
        }
        let opcua_pushed = vec![None; tags.len()];
        Ok(Plc {
            id: cfg.plc.id.clone(),
            name: cfg.plc.name.clone(),
            update_rate: Duration::from_secs_f64(cfg.plc.update_rate),
            state: Mutex::new(State { tags, by_address, modbus_pushed: HashMap::new(), opcua_pushed }),
            replay,
            wake: Notify::new(),
        })
    }

    #[cfg(test)]
    pub fn replay(&self) -> Option<&Replay> {
        self.replay.as_ref()
    }

    pub fn stats(&self) -> Stats {
        let s = self.state.lock().expect("состояние");
        let mut st = Stats { total: s.tags.len(), ..Stats::default() };
        for t in &s.tags {
            match t.protocol {
                Protocol::OpcUa => st.opcua += 1,
                Protocol::Modbus => st.modbus += 1,
                Protocol::Pac => st.pac += 1,
            }
            st.replay += usize::from(t.replay);
        }
        st
    }

    /// Теги целиком (для сборки адресного пространства и устройств PAC при старте).
    pub fn tags(&self) -> Vec<Tag> {
        self.state.lock().expect("состояние").tags.clone()
    }

    /// Значение тега по адресу (для тестов).
    #[cfg(test)]
    pub fn value(&self, address: &str) -> Option<Value> {
        let s = self.state.lock().expect("состояние");
        s.by_address.get(address).map(|&i| s.tags[i].value.clone())
    }

    /// Подставить в теги текущие значения из архива.
    fn apply_replay(&self, s: &mut State) {
        let Some(replay) = &self.replay else { return };
        let elapsed = replay.elapsed();
        for tag in s.tags.iter_mut().filter(|t| t.replay && !t.operator_override) {
            if let Some(raw) = replay.evaluate(&tag.spec, elapsed) {
                tag.set_replay(raw);
            }
        }
    }

    /// Шаг цикла. `modbus` — хранилище регистров сервера Modbus.
    pub fn cycle(&self, modbus: &Registers) -> Cycle {
        let mut guard = self.state.lock().expect("состояние");
        let s = &mut *guard;
        self.apply_replay(s);

        let mut out = Cycle::default();
        for i in 0..s.tags.len() {
            match s.tags[i].protocol {
                Protocol::OpcUa => {
                    let tag = &s.tags[i];
                    if s.opcua_pushed[i].as_ref() != Some(&tag.value) {
                        out.opcua.push((tag.address.clone(), tag.value.clone()));
                        s.opcua_pushed[i] = Some(tag.value.clone());
                    }
                }
                Protocol::Modbus => Self::update_modbus(s, i, modbus),
                Protocol::Pac => {
                    let tag = &s.tags[i];
                    out.pac.insert(tag.address.clone(), tag.value.clone());
                }
            }
        }
        out
    }

    /// Сначала обратное чтение (шлюз пишет команду прямо в регистры, и без этой проверки мы затёрли
    /// бы её своим значением), потом — запись текущего значения. Регистр RO-тега чужую запись не
    /// принимает: на следующем цикле в нём снова значение прибора.
    fn update_modbus(s: &mut State, i: usize, modbus: &Registers) {
        let Some(address) = s.tags[i].modbus_address else { return };
        let mtype = s.tags[i].modbus_type;
        if s.tags[i].writable
            && let Some(regs) = modbus.read(address, mtype.width())
            // Пока мы ничего не клали в регистр, его пустое содержимое — не запись оператора.
            && let Some(pushed) = s.modbus_pushed.get(&address)
            && &regs != pushed
            && let Some(written) = mtype.decode(&regs)
        {
            s.tags[i].set_operator(&written);
        }
        let regs = mtype.encode(&s.tags[i].value);
        modbus.write(address, &regs);
        s.modbus_pushed.insert(address, regs);
    }

    /// Запись по OPC UA (адрес узла = address тега).
    pub fn opcua_write(&self, address: &str, value: &Value) -> Result<(), WriteError> {
        let mut s = self.state.lock().expect("состояние");
        let &i = s.by_address.get(address).ok_or(WriteError::UnknownTag)?;
        let tag = &mut s.tags[i];
        if tag.protocol != Protocol::OpcUa {
            return Err(WriteError::UnknownTag);
        }
        if !tag.writable {
            return Err(WriteError::NotWritable);
        }
        if !tag.set_operator(value) {
            return Err(WriteError::TypeMismatch);
        }
        drop(s);
        self.wake.notify_one();
        Ok(())
    }

    /// Команда драйвера (EXEC_DEVICE_COMMAND): установить RW-тег `device.field` (поле-массив —
    /// `field[idx]`). false — такого RW-тега нет.
    pub fn pac_write(&self, device: &str, field: &str, index: Option<u32>, value: &Value) -> bool {
        let mut s = self.state.lock().expect("состояние");
        let wanted = index.map(|i| format!("{field}[{i}]"));
        let found = s.tags.iter_mut().find(|t| {
            t.protocol == Protocol::Pac
                && t.writable
                && t.device.as_deref() == Some(device)
                && t.field.as_deref().is_some_and(|f| match &wanted {
                    // set_cmd('RT_PAR_F', 12, v): поле канала — RT_PAR_F[12] (хвост после ] — подпись).
                    Some(w) if f.starts_with(w.as_str()) => f.len() == w.len() || f[w.len()..].starts_with('.'),
                    _ => f == field,
                })
        });
        let ok = found.is_some_and(|t| t.set_operator(value));
        drop(s);
        if ok {
            self.wake.notify_one();
        }
        ok
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replay::{Archive, Series};

    fn config(tags: &str) -> Config {
        serde_yaml_ng::from_str(&format!(
            "plc: {{id: T, name: Test, endpoint: 'opc.tcp://0.0.0.0:4840', data_blocks: [{{db_number: 1, name: B, tags: [{tags}]}}]}}"
        ))
        .unwrap()
    }

    fn replay(duration: f64, series: &[(&str, &[f64], &[f64])]) -> Replay {
        let map = series.iter().map(|(id, t, v)| (id.to_string(), Series { t: t.to_vec(), v: v.to_vec() })).collect();
        Replay::new(Archive { start_epoch: 0.0, duration, series: map }, 1.0, true)
    }

    #[test]
    fn replay_drives_tags_until_the_operator_overrides() {
        let cfg = config(
            "{name: a, type: float, access: RW, generator: replay, replay_source: S}, \
             {name: b, type: float, access: RO, generator: replay, replay_source: S}",
        );
        let plc = Plc::new(&cfg, Some(replay(100.0, &[("S", &[0.0], &[5.0])]))).unwrap();
        let regs = Registers::new();
        let first = plc.cycle(&regs);
        assert_eq!(first.opcua.len(), 2);
        assert_eq!(plc.value("a"), Some(Value::Float(5.0)));
        // Повторный шаг без изменений ничего не отдаёт узлам.
        assert!(plc.cycle(&regs).opcua.is_empty());
        // Оператор перебил RW-тег: архив его больше не трогает, RO-тег ведёт архив.
        plc.opcua_write("a", &Value::Float(9.0)).unwrap();
        let after = plc.cycle(&regs);
        assert_eq!(after.opcua, vec![("a".to_string(), Value::Float(9.0))]);
        assert_eq!(plc.value("a"), Some(Value::Float(9.0)));
        assert_eq!(plc.opcua_write("b", &Value::Float(1.0)), Err(WriteError::NotWritable));
        assert_eq!(plc.opcua_write("nope", &Value::Float(1.0)), Err(WriteError::UnknownTag));
        assert_eq!(plc.opcua_write("a", &Value::Text("x".into())), Err(WriteError::TypeMismatch));
    }

    #[test]
    fn modbus_registers_follow_values_and_keep_gateway_writes_on_rw_tags() {
        let cfg = config(
            "{name: ro, type: float, protocol: modbus, modbus_address: 0, modbus_type: float32, access: RO, initial: 2.5}, \
             {name: rw, type: int, protocol: modbus, modbus_address: 10, modbus_type: int16, access: RW, initial: 4}",
        );
        let plc = Plc::new(&cfg, None).unwrap();
        let regs = Registers::new();
        plc.cycle(&regs);
        assert_eq!(regs.read(0, 2), Some(vec![0x0000, 0x4020]), "2.5f = 0x40200000");
        assert_eq!(regs.read(10, 1), Some(vec![4]));
        // Шлюз пишет в оба регистра: RW-тег принимает запись, RO-регистр на следующем шаге восстанавливается.
        regs.write(10, &[(-5i16) as u16]);
        regs.write(0, &[0, 0]);
        plc.cycle(&regs);
        assert_eq!(plc.value("rw"), Some(Value::Int(-5)));
        assert_eq!(regs.read(10, 1), Some(vec![0xFFFB]));
        assert_eq!(regs.read(0, 2), Some(vec![0x0000, 0x4020]));
    }

    /// Регрессия: пустой регистр на первом шаге не считается записью оператора.
    #[test]
    fn blank_register_on_first_cycle_is_not_an_operator_write() {
        let cfg = config(
            "{name: rw, type: float, protocol: modbus, modbus_address: 5, modbus_type: float32, access: RW, generator: replay, replay_source: S}",
        );
        let plc = Plc::new(&cfg, Some(replay(100.0, &[("S", &[0.0], &[7.0])]))).unwrap();
        let regs = Registers::new();
        plc.cycle(&regs);
        assert_eq!(plc.value("rw"), Some(Value::Float(7.0)), "значение ведёт архив, а не нули регистра");
    }

    #[test]
    fn pac_snapshot_and_commands() {
        let cfg = config(
            "{name: v, type: int, protocol: pac, device: LINE1V0, field: M, access: RW, initial: 0}, \
             {name: p, type: float, protocol: pac, device: OBJECT1, field: 'RT_PAR_F[12]', access: RW, initial: 0.5}, \
             {name: q, type: float, protocol: pac, device: OBJECT1, field: 'PAR_MAIN[1].P_CZAD_S', access: RW, initial: 1}, \
             {name: s, type: int, protocol: pac, device: LINE1V0, field: ST, access: RO, initial: 1}",
        );
        let plc = Plc::new(&cfg, None).unwrap();
        let regs = Registers::new();
        assert_eq!(plc.cycle(&regs).pac.len(), 4);
        assert!(plc.pac_write("LINE1V0", "M", None, &Value::Int(1)));
        assert!(plc.pac_write("OBJECT1", "RT_PAR_F", Some(12), &Value::Float(7.5)), "поле-массив по индексу");
        assert!(plc.pac_write("OBJECT1", "PAR_MAIN", Some(1), &Value::Float(2.0)), "хвост после ] — подпись канала");
        assert!(!plc.pac_write("LINE1V0", "ST", None, &Value::Int(5)), "RO-поле");
        assert!(!plc.pac_write("NO_SUCH", "M", None, &Value::Int(1)));
        let snap = plc.cycle(&regs).pac;
        assert_eq!(snap["v"], Value::Int(1));
        assert_eq!(snap["p"], Value::Float(7.5));
        assert_eq!(snap["q"], Value::Float(2.0));
        assert_eq!(snap["s"], Value::Int(1));
    }

    #[test]
    fn duplicate_addresses_are_rejected() {
        assert!(Plc::new(&config("{name: a, type: int}, {name: b, address: a, type: int}"), None).is_err());
    }

    /// Вся рабочая конфигурация вместе с настоящим архивом: тег каждой серии получает значение.
    #[test]
    fn repository_config_runs_against_the_real_archive() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        let cfg = crate::config::load(&root.join("config/replay_config.yaml")).unwrap();
        let archive = Archive::load(&root.join("data/archive_replay.bin.gz")).unwrap();
        let replay = Replay::new(archive, 1.0, true);
        let plc = Plc::new(&cfg, Some(replay)).unwrap();
        let st = plc.stats();
        assert_eq!((st.total, st.opcua, st.modbus, st.pac), (2517, 2133, 212, 172));
        let missing: Vec<String> = plc
            .tags()
            .iter()
            .filter(|t| t.replay && !plc.replay().unwrap().has(&t.spec.source))
            .map(|t| format!("{} → {}", t.name, t.spec.source))
            .collect();
        assert!(missing.is_empty(), "нет серий архива для тегов: {missing:?}");
        let regs = Registers::new();
        let out = plc.cycle(&regs);
        assert_eq!(out.opcua.len(), 2133);
        assert_eq!(out.pac.len(), 172);
    }
}
