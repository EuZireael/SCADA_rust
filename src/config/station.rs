//! `controllers.yaml`: контроллеры и теги, подстановка `${VAR:default}`, проверка станции.

use anyhow::{Context, Result, bail};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct ControllersFile {
    pub opcua: ControllersSection,
}

#[derive(Debug, Deserialize)]
pub struct ControllersSection {
    #[serde(default)]
    pub servers: Vec<ServerConfig>,
}

/// Один контроллер из YAML (OPC UA / Modbus / PAC — по схеме endpoint).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServerConfig {
    #[allow(dead_code)] // id разбирается для совместимости формата с Java-шлюзом
    pub id: Option<String>,
    pub name: String,
    pub endpoint: String,
    /// Политика и режим канала OPC UA: `None` (по умолчанию), `Basic256Sha256`, `Aes256_Sha256_RsaPss_Sign`…
    pub security: Option<String>,
    /// Пользователь OPC UA (вместе с `password`; пароль удобно держать в `${PLC_PASSWORD}`).
    pub username: Option<String>,
    pub password: Option<String>,
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub tags: Vec<TagConfig>,
}

impl ServerConfig {
    /// Защита соединения с OPC UA-контроллером из `security`/`username`/`password`.
    pub fn opc_security(&self) -> Result<crate::model::OpcSecurity> {
        crate::model::OpcSecurity::parse(self.security.as_deref(), self.username.as_deref(), self.password.as_deref())
            .map_err(|e| anyhow::anyhow!("{e}"))
    }

    /// Проверка при старте: молча проигнорированные настройки защиты — дыра, поэтому ошибка в `security`
    /// или защита у протокола, который её не поддерживает (Modbus, PAC), останавливают запуск.
    pub fn validate(&self) -> Result<()> {
        let security = self.opc_security().with_context(|| format!("контроллер {:?}", self.name))?;
        let is_opcua = self.endpoint.starts_with("opc.tcp://");
        if !is_opcua && (security.is_secure() || security.username.is_some()) {
            bail!(
                "контроллер {:?}: security/username/password поддерживаются только для OPC UA (endpoint {}); \
                 для Modbus и PAC защиты на уровне протокола нет — не задавайте их, чтобы не думать, что канал защищён",
                self.name,
                self.endpoint
            );
        }
        Ok(())
    }
}

/// Один тег/канал из YAML — поля как у TagConfig Java-шлюза.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TagConfig {
    pub name: String,
    pub node_id: String,
    pub channel_id: Option<i64>,
    pub device_name: Option<String>,
    pub field_name: Option<String>,
    pub device_type: Option<String>,
    pub protocol: Option<String>,
    pub data_type: String,
    #[serde(default)]
    pub polling_rate: u64,
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub writable: bool,
    pub unit: Option<String>,
    pub min_value: Option<f64>,
    pub max_value: Option<f64>,
    pub modbus_address: Option<i32>,
    pub modbus_type: Option<String>,
    pub modbus_unit_id: Option<u8>,
    /// Переопределение фильтра истории для тега; не заданные поля — умолчания `gateway.history.*`.
    pub history: Option<HistoryConfig>,
}

/// Блок `history:` тега: `{deadband: 0.5, deadbandPercent: 1, minIntervalMs: 60000, maxIntervalMs: 600000}`.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct HistoryConfig {
    pub deadband: Option<f64>,
    pub deadband_percent: Option<f64>,
    pub min_interval_ms: Option<u64>,
    pub max_interval_ms: Option<u64>,
}

/// Прочитать controllers.yaml с подстановкой `${VAR:default}` из окружения.
pub fn load_controllers(path: &std::path::Path) -> Result<Vec<ServerConfig>> {
    let raw = std::fs::read_to_string(path).with_context(|| format!("не прочитан {}", path.display()))?;
    let expanded = expand_placeholders(&raw, |k| std::env::var(k).ok())
        .with_context(|| format!("{}: не заданы обязательные переменные", path.display()))?;
    parse_controllers(&expanded)
}

pub fn parse_controllers(yaml: &str) -> Result<Vec<ServerConfig>> {
    let file: ControllersFile = serde_yaml_ng::from_str(yaml).context("controllers.yaml не разобран")?;
    for server in file.opcua.servers.iter().filter(|s| s.enabled) {
        server.validate()?;
    }
    validate_station(&file.opcua.servers)?;
    Ok(file.opcua.servers)
}

/// Проверка конфигурации в целом (включённые контроллеры): имена контроллеров и тегов уникальны, протокол
/// контроллера известен. Имя тега — ключ сообщения Kafka и адрес команды: два тега с одним именем делали бы
/// телеметрию и команды неоднозначными, а контроллер с неизвестным протоколом молча не опрашивался бы.
pub fn validate_station(servers: &[ServerConfig]) -> Result<()> {
    let mut problems: Vec<String> = Vec::new();
    let mut controller_names = std::collections::HashSet::new();
    let mut tag_owner: std::collections::HashMap<&str, &str> = std::collections::HashMap::new();
    let mut duplicates: Vec<String> = Vec::new();
    for server in servers.iter().filter(|s| s.enabled) {
        if !controller_names.insert(server.name.as_str()) {
            problems.push(format!("контроллер {:?} описан дважды", server.name));
        }
        if crate::model::ControllerKind::from_endpoint(&server.endpoint).is_none() {
            problems.push(format!(
                "контроллер {:?}: неизвестный протокол в endpoint {:?} (ожидается opc.tcp://, modbus:// или pac://)",
                server.name, server.endpoint
            ));
        }
        for tag in server.tags.iter().filter(|t| t.enabled) {
            if tag.name.trim().is_empty() {
                problems.push(format!("контроллер {:?}: у включённого тега пустое имя", server.name));
            } else if let Some(first) = tag_owner.insert(tag.name.as_str(), server.name.as_str()) {
                duplicates.push(format!("{} ({first} и {})", tag.name, server.name));
            }
        }
        check_tags(server, &mut problems);
    }
    if !duplicates.is_empty() {
        problems.push(format!(
            "имена тегов должны быть уникальны (это ключ Kafka и адрес команды), повторяются {}: {}{}",
            duplicates.len(),
            duplicates.iter().take(5).cloned().collect::<Vec<_>>().join("; "),
            if duplicates.len() > 5 { "; …" } else { "" }
        ));
    }
    if problems.is_empty() {
        Ok(())
    } else {
        bail!("конфигурация контроллеров не принята:\n  - {}", problems.join("\n  - "))
    }
}

/// Допустимый адрес поля PAC: имя прибора или поля, элемент массива `RT_PAR_F[12]`, вложенность `PAR_MAIN[1].P`.
/// Адрес подставляется в Lua-текст команды `set_cmd`, поэтому кавычки, пробелы и управляющие символы запрещены.
fn is_pac_identifier(text: &str) -> bool {
    !text.is_empty() && text.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '[' | ']' | '.'))
}

/// Проверки тегов одного включённого контроллера: молча не работающий тег хуже отказа запуска.
fn check_tags(server: &ServerConfig, problems: &mut Vec<String>) {
    use crate::model::{ControllerKind, Protocol, classify};
    let Some(kind) = ControllerKind::from_endpoint(&server.endpoint) else { return };
    let (mut node_ids, mut modbus_units) = (std::collections::HashSet::new(), std::collections::BTreeSet::new());
    for tag in server.tags.iter().filter(|t| t.enabled && !t.name.trim().is_empty()) {
        let who = format!("контроллер {:?}, тег {:?}", server.name, tag.name);
        let protocol = classify(tag.protocol.as_deref().unwrap_or("opcua"), &tag.node_id, tag.modbus_address);
        // Контроллер опрашивает только теги своего протокола; остальные никогда не читались бы.
        if protocol != kind.protocol() {
            problems.push(format!(
                "{who}: протокол тега {protocol:?} не совпадает с протоколом контроллера {:?} — тег не опрашивался бы",
                kind.protocol()
            ));
            continue;
        }
        // Адрес узла — ключ синхронизации с БД: два тега с одним адресом получили бы один id и общую историю.
        if !node_ids.insert(tag.node_id.as_str()) {
            problems.push(format!("{who}: nodeId {:?} уже занят другим тегом контроллера", tag.node_id));
        }
        match protocol {
            Protocol::Pac => {
                for (what, value) in [("deviceName", &tag.device_name), ("fieldName", &tag.field_name)] {
                    match value.as_deref() {
                        Some(v) if is_pac_identifier(v) => {}
                        Some(v) => problems.push(format!(
                            "{who}: {what} {v:?} содержит недопустимые символы (допустимы буквы, цифры, _ [ ] .)"
                        )),
                        None => problems.push(format!("{who}: для PAC-тега не задан {what}")),
                    }
                }
            }
            Protocol::Modbus => {
                // Holding-регистры 40001…105536 (адрес в протоколе — 16 бит); за границей адрес молча обрезался бы.
                let wide = crate::model::is_float(&tag.data_type);
                match tag.modbus_address {
                    Some(a) if (40001..=105536 - i32::from(wide)).contains(&a) => {}
                    Some(a) => problems.push(format!("{who}: modbusAddress {a} вне диапазона 40001…105536")),
                    None => problems.push(format!("{who}: для Modbus-тега не задан modbusAddress")),
                }
                modbus_units.insert(tag.modbus_unit_id.unwrap_or(1));
            }
            Protocol::OpcUa => {}
        }
    }
    if modbus_units.len() > 1 {
        problems.push(format!(
            "контроллер {:?}: у тегов разные modbusUnitId {modbus_units:?} — шлюз читает контроллер с одним",
            server.name
        ));
    }
}

/// Подстановка в стиле Spring: `${NAME:default}` — со значением по умолчанию (пустое `${NAME:}`
/// тоже допустимо); `${NAME}` без умолчания **обязательна** — без неё ошибка, а не пустая строка:
/// адрес контроллера `opc.tcp://:4840` шлюз молча не обнаружил бы до первой попытки подключения.
pub fn expand_placeholders(text: &str, lookup: impl Fn(&str) -> Option<String>) -> Result<String> {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    let mut missing: Vec<String> = Vec::new();
    while let Some(start) = rest.find("${") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        match after.find('}') {
            Some(end) => {
                let expr = &after[..end];
                match expr.split_once(':') {
                    Some((name, default)) => out.push_str(&lookup(name).unwrap_or_else(|| default.to_string())),
                    None => match lookup(expr) {
                        Some(v) => out.push_str(&v),
                        None => missing.push(expr.to_string()),
                    },
                }
                rest = &after[end + 1..];
            }
            None => {
                out.push_str(&rest[start..]);
                rest = "";
            }
        }
    }
    out.push_str(rest);
    if !missing.is_empty() {
        missing.sort();
        missing.dedup();
        bail!("задайте переменные окружения: {}", missing.join(", "));
    }
    Ok(out)
}
