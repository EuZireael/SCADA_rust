//! Один привязанный скрипт: Lua-состояние, вызов `process`/`write`.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use anyhow::{Result, anyhow, bail};
use mlua::{Function, Lua, Table, Value};
use serde_json::Value as Json;

use super::MEMORY_LIMIT;
use super::convert::*;
use super::glob::TagGlob;
use crate::model::{Quality, Tag, TagValue, Timestamp};
use crate::sandbox;

/// Столько превышений времени подряд отключают скрипт…
const TRIP_AFTER: u32 = 3;
/// …на столько. Зависший скрипт, привязанный к тысячам каналов, иначе тратил бы `timeout` на каждый канал каждого
/// цикла и останавливал обработку всего контроллера; пока скрипт отключён, его каналы идут как BAD, остальные
/// работают. Потом одна попытка: удалась — скрипт снова в строю, нет — снова пауза.
const TRIP_PAUSE: Duration = Duration::from_secs(30);

/// Предохранитель скрипта: считает превышения времени подряд.
#[derive(Default)]
struct Breaker {
    timeouts: u32,
    paused_until: Option<Instant>,
}

/// Итог обработки значения цепочкой скриптов.
#[derive(Debug, Clone, PartialEq)]
pub struct Processed {
    /// Значение после скрипта; `None` — BAD.
    pub value: Option<TagValue>,
    /// Качество после скрипта.
    pub quality: Quality,
}

/// Изменяемая часть скрипта под мьютексом: Lua-стейт и найденные в нём функции.
struct Inner {
    /// Песочница Lua.
    lua: Lua,
    /// Функция `process` из скрипта.
    process: Function,
    /// Функция `write` (необязательная).
    write: Option<Function>,
    /// Параметры привязки (`ctx.params`).
    params: Table,
    /// `ctx` по имени канала: у каждого свой `state`.
    ctx: HashMap<String, Table>,
}

/// Скрипт, привязанный к маскам тегов: свой Lua-стейт, предохранитель и состояние каналов.
pub struct BoundScript {
    /// Имя файла скрипта.
    pub file: String,
    /// Маски имён тегов, к которым скрипт привязан.
    pub globs: Vec<TagGlob>,
    /// Лимит времени одного вызова.
    timeout: Duration,
    /// Есть ли функция `write`.
    pub(super) has_write: bool,
    /// Lua-стейт: вызовы идут по одному.
    inner: Mutex<Inner>,
    /// Предохранитель от зависшего скрипта.
    breaker: Mutex<Breaker>,
    /// Длительность паузы предохранителя.
    trip_pause: Duration,
}

impl BoundScript {
    /// Загрузить скрипт: создать песочницу, выполнить тело файла (оно определяет `process` и необязательную `write`) и убедиться, что `process` есть.
    pub(super) fn new(
        file: &str,
        source: &str,
        globs: Vec<TagGlob>,
        params: &serde_yaml_ng::Value,
        timeout: Duration,
    ) -> Result<Self> {
        let lua = sandbox::new_lua(MEMORY_LIMIT)?;
        let params = yaml_to_lua(&lua, params)?;
        let chunk = lua.load(source).set_name(format!("@{file}"));
        sandbox::run_limited(&lua, timeout.max(Duration::from_millis(500)), || chunk.exec())
            .map_err(|e| anyhow!("{file}: {}", short(&e)))?;
        let process = match lua.globals().get::<Value>("process")? {
            Value::Function(f) => f,
            _ => bail!("{file}: нет функции process(value, quality, ctx)"),
        };
        let write = match lua.globals().get::<Value>("write")? {
            Value::Function(f) => Some(f),
            _ => None,
        };
        Ok(BoundScript {
            file: file.to_string(),
            globs,
            timeout,
            has_write: write.is_some(),
            inner: Mutex::new(Inner { lua, process, write, params, ctx: HashMap::new() }),
            breaker: Mutex::new(Breaker::default()),
            trip_pause: TRIP_PAUSE,
        })
    }

    /// Подходит ли имя тега под одну из масок привязки.
    pub(super) fn matches(&self, name: &str) -> bool {
        self.globs.iter().any(|g| g.matches(name))
    }

    /// process(value, quality, ctx) → (значение с типом по dataType канала, качество).
    pub(super) fn process(
        &self,
        tag: &Tag,
        value: Option<&TagValue>,
        quality: Quality,
        ts: Timestamp,
    ) -> Result<Processed, String> {
        self.check_breaker()?;
        let result = self.call_process(tag, value, quality, ts);
        self.record(&result);
        let (out, q) = result?;
        let out = from_lua(&out, value, Some(&tag.data_type))?;
        let quality = match q {
            Value::Nil => quality,
            other => match lua_text(&other).to_ascii_uppercase().as_str() {
                "GOOD" => Quality::Good,
                "BAD" => Quality::Bad,
                _ => return Err(format!("качество должно быть GOOD или BAD, а не {}", lua_text(&other))),
            },
        };
        let quality = if out.is_none() { Quality::Bad } else { quality };
        Ok(Processed { value: out, quality })
    }

    /// Сам вызов `process` в песочнице.
    fn call_process(
        &self,
        tag: &Tag,
        value: Option<&TagValue>,
        quality: Quality,
        ts: Timestamp,
    ) -> Result<(Value, Value), String> {
        let mut inner = self.inner.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let Inner { lua, process, ctx, params, .. } = &mut *inner;
        let ctx = ctx_for(lua, ctx, params, tag).map_err(|e| short(&e))?;
        ctx.set("timestamp", ts.0.timestamp_millis() as f64).map_err(|e| short(&e))?;
        let args = (to_lua(lua, value).map_err(|e| short(&e))?, quality.as_str(), ctx.clone());
        sandbox::run_limited(lua, self.timeout, || process.call(args)).map_err(|e| short(&e))
    }

    /// Отключён ли скрипт предохранителем (тогда вызов не делаем вообще).
    fn check_breaker(&self) -> Result<(), String> {
        let mut breaker = self.breaker.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        match breaker.paused_until {
            Some(until) if Instant::now() < until => Err(format!(
                "скрипт приостановлен после {TRIP_AFTER} превышений времени подряд (ещё {} с)",
                until.saturating_duration_since(Instant::now()).as_secs() + 1
            )),
            Some(_) => {
                breaker.paused_until = None; // пауза вышла: пробный вызов
                Ok(())
            }
            None => Ok(()),
        }
    }

    /// Учесть исход вызова: превышение времени приближает отключение, любой иной исход обнуляет счёт.
    fn record<T>(&self, result: &Result<T, String>) {
        let mut breaker = self.breaker.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        match result {
            Err(message) if message.contains(sandbox::TIME_LIMIT_MARK) => {
                breaker.timeouts += 1;
                if breaker.timeouts >= TRIP_AFTER {
                    breaker.paused_until = Some(Instant::now() + self.trip_pause);
                }
            }
            _ => breaker.timeouts = 0,
        }
    }

    /// Только для тестов: короткая пауза предохранителя.
    #[cfg(test)]
    pub(super) fn with_trip_pause(mut self, pause: Duration) -> Self {
        self.trip_pause = pause;
        self
    }

    /// write(value, ctx) → значение для ПЛК (без `write` — как есть).
    pub(super) fn to_plc(&self, tag: &Tag, value: &Json) -> Result<Json, String> {
        if !self.has_write {
            return Ok(value.clone());
        }
        let mut inner = self.inner.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let Inner { lua, write, ctx, params, .. } = &mut *inner;
        let ctx = ctx_for(lua, ctx, params, tag).map_err(|e| short(&e))?;
        let arg = json_to_lua(value);
        let out: Value =
            sandbox::run_limited(lua, self.timeout, || write.as_ref().expect("has_write").call((arg, ctx.clone())))
                .map_err(|e| short(&e))?;
        match from_lua(&out, None, None)? {
            None => Err("write вернул nil".into()),
            Some(v) => Ok(tag_value_to_json(&v)),
        }
    }
}

/// Таблица `ctx` канала: создаётся при первом обращении, дальше возвращается та же — в ней живёт `state`.
fn ctx_for<'a>(lua: &Lua, cache: &'a mut HashMap<String, Table>, params: &Table, tag: &Tag) -> mlua::Result<&'a Table> {
    if !cache.contains_key(&tag.name) {
        let ctx = lua.create_table()?;
        ctx.set("tag", tag.name.as_str())?;
        ctx.set("device", tag.device_name.as_deref())?;
        ctx.set("field", tag.field_name.as_deref())?;
        ctx.set("device_type", tag.device_type.as_deref())?;
        ctx.set("data_type", tag.data_type.as_str())?;
        ctx.set("unit", tag.unit.as_deref())?;
        ctx.set("params", params.clone())?;
        ctx.set("state", lua.create_table()?)?;
        cache.insert(tag.name.clone(), ctx);
    }
    Ok(cache.get(&tag.name).expect("только что добавлен"))
}
