//! Чтение переменных окружения: имена-синонимы (Spring relaxed binding), типы, секреты из файлов.

use std::time::Duration;

use anyhow::{Context, Result, bail};

/// Токен REST: `GATEWAY_API_TOKEN` или содержимое файла из `GATEWAY_API_TOKEN_FILE` (секреты Docker).
pub(super) fn api_token() -> Result<Option<String>> {
    if let Some(t) = env_any(&["GATEWAY_API_TOKEN"]) {
        return Ok(Some(t.trim().to_string()));
    }
    let Some(path) = env_any(&["GATEWAY_API_TOKEN_FILE"]) else { return Ok(None) };
    let token = std::fs::read_to_string(&path).with_context(|| format!("GATEWAY_API_TOKEN_FILE={path}"))?;
    let token = token.trim().to_string();
    if token.is_empty() {
        bail!("GATEWAY_API_TOKEN_FILE={path}: файл пуст");
    }
    Ok(Some(token))
}

/// Имя экземпляра по умолчанию — хост и pid (как у Java-шлюза).
pub(super) fn default_instance_id() -> String {
    let host = std::env::var("HOSTNAME").ok().filter(|h| !h.is_empty()).unwrap_or_else(|| "gateway".into());
    format!("{host}-{}", std::process::id())
}

pub(super) fn env_any(names: &[&str]) -> Option<String> {
    names.iter().find_map(|n| std::env::var(n).ok().filter(|v| !v.is_empty()))
}

pub(super) fn env_or(names: &[&str], default: &str) -> String {
    env_any(names).unwrap_or_else(|| default.to_string())
}

pub(super) fn env_bool(names: &[&str], default: bool) -> bool {
    match env_any(names) {
        Some(v) => matches!(v.trim().to_ascii_lowercase().as_str(), "true" | "1" | "yes" | "on"),
        None => default,
    }
}

pub(super) fn env_parse<T: std::str::FromStr>(names: &[&str], default: T) -> Result<T>
where
    T::Err: std::fmt::Display,
{
    match env_any(names) {
        Some(v) => v.trim().parse().map_err(|e| anyhow::anyhow!("{}={v}: {e}", names[0])),
        None => Ok(default),
    }
}

pub(super) fn env_ms(names: &[&str], default: u64) -> Result<Duration> {
    Ok(Duration::from_millis(env_parse(names, default)?))
}
