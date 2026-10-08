//! Преобразования значений: Rust ⇄ Lua ⇄ JSON/YAML.

use anyhow::Result;
use mlua::{Lua, Table, Value};
use serde_json::{Value as Json, json};

use crate::model::{self, TagValue};

/// Значение канала → Lua. Float отдаётся десятичным (64.7f32 → 64.7, а не 64.69999694824219):
/// скрипт сравнивает с уставками так, как их видит оператор.
pub(super) fn to_lua(lua: &Lua, v: Option<&TagValue>) -> mlua::Result<Value> {
    Ok(match v {
        None => Value::Nil,
        Some(TagValue::Bool(b)) => Value::Boolean(*b),
        Some(TagValue::Int(i)) => Value::Number(*i as f64),
        Some(TagValue::F32(f)) => Value::Number(f.to_string().parse().unwrap_or(f64::from(*f))),
        Some(TagValue::F64(f)) => Value::Number(*f),
        Some(TagValue::Text(s)) => Value::String(lua.create_string(s)?),
    })
}

pub(super) fn lua_text(v: &Value) -> String {
    match v {
        Value::String(s) => s.to_string_lossy(),
        Value::Number(n) => number_text(*n),
        Value::Integer(i) => i.to_string(),
        Value::Boolean(b) => b.to_string(),
        other => other.type_name().to_string(),
    }
}

/// Число как его печатает Lua (`tostring`): целое без `.0`.
pub(super) fn number_text(n: f64) -> String {
    if n.fract() == 0.0 && n.abs() < 1e15 { format!("{}", n as i64) } else { format!("{n}") }
}

/// Результат скрипта → значение канала с типом по `data_type` (`None` — тип неизвестен, как у
/// `write`): на проводе значение остаётся типизированным (INT — целым, FLOAT — числом, BOOLEAN —
/// bool); FLOAT, снятый как f32, остаётся f32 (кратчайшая запись в JSON, как без скрипта).
pub(super) fn from_lua(
    v: &Value,
    original: Option<&TagValue>,
    data_type: Option<&str>,
) -> Result<Option<TagValue>, String> {
    let is = |f: fn(&str) -> bool| data_type.is_some_and(f);
    match v {
        Value::Nil => Ok(None),
        Value::Boolean(b) => Ok(Some(if data_type.is_some() && !is(model::is_bool) && !is(model::is_string) {
            if is(model::is_int) { TagValue::Int(i64::from(*b)) } else { TagValue::F64(f64::from(u8::from(*b))) }
        } else {
            TagValue::Bool(*b)
        })),
        Value::Number(_) | Value::Integer(_) => {
            let d = match v {
                Value::Number(n) => *n,
                Value::Integer(i) => *i as f64,
                _ => unreachable!(),
            };
            if !d.is_finite() {
                return Err(format!("значение не число: {d}"));
            }
            Ok(Some(match data_type {
                None => TagValue::F64(d),
                Some(_) if is(model::is_bool) => TagValue::Bool(d != 0.0),
                Some(_) if is(model::is_int) => TagValue::Int(d.round() as i64),
                Some(_) if is(model::is_string) => TagValue::Text(number_text(d)),
                Some(_) if matches!(original, Some(TagValue::F32(_))) => TagValue::F32(d as f32),
                Some(_) => TagValue::F64(d),
            }))
        }
        Value::String(s) => {
            let s = s.to_string_lossy();
            if data_type.is_some() && !is(model::is_string) {
                return Err(format!("строка '{s}' в канале типа {}", data_type.unwrap_or("")));
            }
            Ok(Some(TagValue::Text(s)))
        }
        other => Err(format!("скрипт вернул {} вместо значения", other.type_name())),
    }
}

pub(super) fn json_to_lua(v: &Json) -> Value {
    match v {
        Json::Bool(b) => Value::Boolean(*b),
        Json::Number(n) => Value::Number(n.as_f64().unwrap_or(f64::NAN)),
        _ => Value::Nil,
    }
}

pub(super) fn tag_value_to_json(v: &TagValue) -> Json {
    match v {
        TagValue::Bool(b) => json!(b),
        TagValue::Int(i) => json!(i),
        TagValue::F32(f) => json!(f.to_string().parse::<f64>().unwrap_or(f64::from(*f))),
        TagValue::F64(f) if f.is_finite() && f.fract() == 0.0 && f.abs() < 9e15 => json!(*f as i64),
        TagValue::F64(f) => json!(f),
        TagValue::Text(s) => json!(s),
    }
}

pub(super) fn yaml_to_lua(lua: &Lua, v: &serde_yaml_ng::Value) -> Result<Table> {
    fn value(lua: &Lua, v: &serde_yaml_ng::Value) -> Result<Value> {
        use serde_yaml_ng::Value as Y;
        Ok(match v {
            Y::Null => Value::Nil,
            Y::Bool(b) => Value::Boolean(*b),
            Y::Number(n) => Value::Number(n.as_f64().unwrap_or(f64::NAN)),
            Y::String(s) => Value::String(lua.create_string(s)?),
            Y::Sequence(items) => {
                let t = lua.create_table()?;
                for (i, item) in items.iter().enumerate() {
                    t.set(i + 1, value(lua, item)?)?;
                }
                Value::Table(t)
            }
            Y::Mapping(map) => {
                let t = lua.create_table()?;
                for (k, item) in map {
                    let key = match k {
                        Y::String(s) => s.clone(),
                        other => serde_yaml_ng::to_string(other)?.trim().to_string(),
                    };
                    t.set(key, value(lua, item)?)?;
                }
                Value::Table(t)
            }
            Y::Tagged(t) => value(lua, &t.value)?,
        })
    }
    match value(lua, v)? {
        Value::Table(t) => Ok(t),
        _ => Ok(lua.create_table()?),
    }
}

/// Краткий текст ошибки Lua: первая строка без трассировки.
pub(super) fn short(e: &mlua::Error) -> String {
    let text = e.to_string();
    let first = text.lines().next().unwrap_or("").trim();
    first.strip_prefix("runtime error: ").unwrap_or(first).to_string()
}
