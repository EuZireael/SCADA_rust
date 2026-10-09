//! Lua-стейт снимка PAC: ответ GET_DEVICES_STATES исполняется настоящим Lua, значения канала —
//! `t[прибор][поле]` или `t[прибор][массив][индекс]`.
//!
//! ```lua
//! t={ LINE1V0={M=0, ST=1}, LINE1M1={FRQ=12.5, RPM=750} }
//! t.OBJECT1={CMD=0, CUR_REC='Танк №1', RT_PAR_F={0, 0, 1, 0.98}}
//! ```

use std::time::Duration;

use anyhow::Result;
use mlua::{Lua, Table, Value};

use crate::sandbox;

/// Потолок памяти стейта. Снимок станции — единицы МБ вместе с мусором между сборками.
pub const MEMORY_LIMIT: usize = 64 * 1024 * 1024;
/// Время на разбор одного снимка (в норме — миллисекунды).
pub const TIME_BUDGET: Duration = Duration::from_secs(1);

/// Значение поля снимка, как оно пришло: число или строка (булевы — числом 1/0).
#[derive(Debug, Clone, PartialEq)]
pub enum Raw {
    Number(f64),
    Text(String),
}

/// Lua-стейт снимка прошивки в песочнице: `load` исполняет текст ответа, из таблицы `t` читаются значения каналов.
pub struct Snapshot {
    lua: Lua,
    budget: Duration,
}

impl Snapshot {
    /// Стейт с потолками памяти и времени по умолчанию.
    pub fn new() -> Result<Self> {
        Self::with_limits(MEMORY_LIMIT, TIME_BUDGET)
    }

    /// Стейт с заданными потолками памяти и времени (для тестов).
    pub fn with_limits(memory: usize, budget: Duration) -> Result<Self> {
        Ok(Snapshot { lua: sandbox::new_lua(memory)?, budget })
    }

    /// Исполнить Lua-текст снимка (наполняет глобальную таблицу `t`). Ошибка — в том числе исчерпание
    /// памяти или времени; после неё стейт надо считать негодным.
    pub fn load(&self, text: &str) -> Result<()> {
        sandbox::run_limited(&self.lua, self.budget, || self.lua.load(text).exec())?;
        Ok(())
    }

    /// Значение поля: `t[device][base]` или, при индексе, `t[device][base][index]` (Lua-индекс с 1).
    /// None — нет снимка, прибора, поля, элемента массива либо там таблица/функция (не значение канала).
    pub fn get(&self, device: &str, base: &str, index: Option<u32>) -> Option<Raw> {
        let t: Table = self.lua.globals().get("t").ok()?;
        let dev: Table = t.get(device).ok()?;
        let mut value: Value = dev.get(base).ok()?;
        if let Some(i) = index {
            let Value::Table(array) = value else { return None };
            value = array.get(i64::from(i)).ok()?;
        }
        match value {
            Value::Integer(i) => Some(Raw::Number(i as f64)),
            Value::Number(n) => Some(Raw::Number(n)),
            Value::Boolean(b) => Some(Raw::Number(f64::from(u8::from(b)))),
            Value::String(s) => Some(Raw::Text(s.to_string_lossy())),
            _ => None,
        }
    }

    #[cfg(test)]
    fn global_is_nil(&self, name: &str) -> bool {
        matches!(self.lua.globals().get::<Value>(name), Ok(Value::Nil))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use super::*;

    const SNAPSHOT: &str = "t=\n\t{\n\tLINE1V0={M=0, ST=1},\n\tLINE1TE1={V=21.5, P_CZ=0.0},\n\t}\n\
        t.OBJECT1 = t.OBJECT1 or {}\nt.OBJECT1=\n\t{\n\tCMD=0,\n\tCUR_REC='Танк сырого молока №1',\n\
        \tRT_PAR_F=\n\t\t{\n\t\t0, 0.5, 7.5,\n\t\t},\n\t}\nt.SYSTEM =\n\t{\n\tUP_TIME=\"0 дн. 00:00:20\",\n\t}\n";

    fn loaded() -> Snapshot {
        let s = Snapshot::new().unwrap();
        s.load(SNAPSHOT).unwrap();
        s
    }

    #[test]
    fn reads_fields_arrays_and_strings() {
        let s = loaded();
        assert_eq!(s.get("LINE1V0", "ST", None), Some(Raw::Number(1.0)));
        assert_eq!(s.get("LINE1TE1", "V", None), Some(Raw::Number(21.5)));
        assert_eq!(s.get("OBJECT1", "RT_PAR_F", Some(2)), Some(Raw::Number(0.5)));
        assert_eq!(s.get("OBJECT1", "RT_PAR_F", Some(3)), Some(Raw::Number(7.5)));
        assert_eq!(s.get("SYSTEM", "UP_TIME", None), Some(Raw::Text("0 дн. 00:00:20".into())));
        assert_eq!(s.get("OBJECT1", "CUR_REC", None), Some(Raw::Text("Танк сырого молока №1".into())));
    }

    #[test]
    fn missing_or_not_a_value_is_none() {
        let s = loaded();
        assert_eq!(s.get("LINE9V9", "ST", None), None, "нет прибора");
        assert_eq!(s.get("LINE1V0", "NOPE", None), None, "нет поля");
        assert_eq!(s.get("OBJECT1", "RT_PAR_F", Some(9)), None, "нет элемента массива");
        assert_eq!(s.get("OBJECT1", "RT_PAR_F", None), None, "таблица — не значение канала");
        assert_eq!(s.get("LINE1V0", "ST", Some(1)), None, "скаляр — не массив");
        assert_eq!(Snapshot::new().unwrap().get("LINE1V0", "ST", None), None, "снимка ещё нет");
    }

    #[test]
    fn snapshot_text_is_replaced_by_the_next_one() {
        let s = loaded();
        s.load("t={LINE1V0={ST=5}}").unwrap();
        assert_eq!(s.get("LINE1V0", "ST", None), Some(Raw::Number(5.0)));
        assert_eq!(s.get("LINE1V0", "M", None), None);
    }

    #[test]
    fn state_has_no_host_access() {
        let s = Snapshot::new().unwrap();
        for name in ["os", "io", "package", "debug"].into_iter().chain(sandbox::REMOVED_GLOBALS.iter().copied()) {
            assert!(s.global_is_nil(name), "{name} не должен быть доступен снимку");
        }
        assert!(s.load("f = loadstring(string.dump(function() return 42 end))").is_err());
        assert!(s.load("x = os.execute('true')").is_err());
    }

    #[test]
    fn endless_script_is_stopped_by_time_budget() {
        sandbox::within(20, || {
            let s = Snapshot::with_limits(MEMORY_LIMIT, Duration::from_millis(200)).unwrap();
            let started = Instant::now();
            assert!(s.load("while true do end").is_err());
            assert!(started.elapsed() < Duration::from_secs(2), "{:?}", started.elapsed());
            s.load(SNAPSHOT).unwrap(); // лимит — на каждый разбор, а не на жизнь стейта
            assert_eq!(s.get("LINE1V0", "ST", None), Some(Raw::Number(1.0)));
        });
    }

    #[test]
    fn coroutine_cannot_run_outside_the_time_budget() {
        sandbox::within(20, || {
            let s = Snapshot::with_limits(MEMORY_LIMIT, Duration::from_millis(200)).unwrap();
            assert!(s.load("co = coroutine.create(function() while true do end end); coroutine.resume(co)").is_err());
        });
    }

    #[test]
    fn memory_bomb_is_stopped_by_memory_limit() {
        let s = Snapshot::with_limits(8 * 1024 * 1024, TIME_BUDGET).unwrap();
        let err = s.load("s = string.rep('x', 64 * 1024 * 1024)").unwrap_err();
        assert!(err.to_string().contains("memory"), "{err}");
    }
}
