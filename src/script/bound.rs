//! Один привязанный скрипт: Lua-состояние, вызов `process`/`write`.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use anyhow::{Result, anyhow, bail};
use mlua::{Function, Lua, Table, Value};
use serde_json::Value as Json;

use super::MEMORY_LIMIT;
use super::convert::*;
use super::glob::TagGlob;
use crate::model::{Quality, Tag, TagValue, Timestamp};
use crate::sandbox;

/// Итог обработки значения цепочкой скриптов.
#[derive(Debug, Clone, PartialEq)]
pub struct Processed {
    pub value: Option<TagValue>,
    pub quality: Quality,
}

struct Inner {
    lua: Lua,
    process: Function,
    write: Option<Function>,
    params: Table,
    /// `ctx` по имени канала: у каждого свой `state`.
    ctx: HashMap<String, Table>,
}

pub struct BoundScript {
    pub file: String,
    pub globs: Vec<TagGlob>,
    timeout: Duration,
    pub(super) has_write: bool,
    inner: Mutex<Inner>,
}

impl BoundScript {
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
        })
    }

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
        let mut inner = self.inner.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let Inner { lua, process, ctx, params, .. } = &mut *inner;
        let ctx = ctx_for(lua, ctx, params, tag).map_err(|e| short(&e))?;
        ctx.set("timestamp", ts.0.timestamp_millis() as f64).map_err(|e| short(&e))?;
        let args = (to_lua(lua, value).map_err(|e| short(&e))?, quality.as_str(), ctx.clone());
        let (out, q): (Value, Value) =
            sandbox::run_limited(lua, self.timeout, || process.call(args)).map_err(|e| short(&e))?;
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
