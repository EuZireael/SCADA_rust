//! Проверка соответствия: ведёт ли себя PAC при записи так, как описано в конфигурации (`write:`,
//! `write_requires:`, `write_int:`, `write_state:`). Идёт по driver-master, поэтому проверяет что угодно:
//! симулятор (в CI) и настоящую прошивку ptusa (эмулятор мойки) — правила в конфигурации сняты с неё, и
//! эта проверка показывает, что они до сих пор верны.
//!
//!   simulator conformance \[host\] \[port\] \[config\]      по умолчанию 127.0.0.1 10000 config/replay_config.yaml
//!
//! По каждому записываемому PAC-тегу: пишет значение, ждёт, читает снимок и сверяет с ожиданием; потом
//! возвращает прежнее. Теги, которых у проверяемого PAC нет (у настоящего проекта — меньше каналов), пропускаются.

use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result, bail};

use crate::config;
use crate::luatab::{self, Snapshot};
use crate::pac_client::PacClient;
use crate::tag::{Protocol, Tag};
use crate::value::{DataType, Value};

/// Сколько ждать после записи: программа ПЛК успевает пересчитать поле (сканирование — десятки мс).
const SETTLE: Duration = Duration::from_millis(450);

/// Исход проверки одного тега.
#[derive(Debug, PartialEq)]
enum Outcome {
    /// Прошивка ведёт себя как описано в конфигурации.
    Pass,
    /// Расхождение (пояснение внутри).
    Fail(String),
    /// Проверка невозможна (причина внутри).
    Skip(&'static str),
}

/// `device`, имя поля и индекс массива из поля тега (`RT_PAR_F[12]`, `PAR_MAIN[1].P_CZAD_S`).
fn split_field(field: &str) -> (String, Option<i64>) {
    let base_end = field.find(|c: char| !(c.is_alphanumeric() || c == '_')).unwrap_or(field.len());
    let base = field[..base_end].to_string();
    let index = field[base_end..]
        .strip_prefix('[')
        .and_then(|r| r.split_once(']'))
        .and_then(|(inside, _)| inside.trim().parse().ok());
    (base, index)
}

/// Lua-текст команды записи, как его шлёт драйвер: `__<прибор>:set_cmd('<поле>', <индекс>, <значение>)`.
fn command(device: &str, base: &str, index: Option<i64>, value: f64) -> String {
    format!("__{device}:set_cmd('{base}', {}, {value})", index.unwrap_or(1))
}

/// Проверяемый PAC: клиент driver-master с короткими операциями «снимок», «запись», «чтение».
struct Probe<'a> {
    client: &'a mut PacClient,
}

impl Probe<'_> {
    /// Свежий снимок состояния всех приборов.
    async fn snapshot(&mut self) -> Result<Snapshot> {
        luatab::parse_snapshot(&self.client.states_lua().await?)
    }

    /// Записать значение командой; код результата (0 — принято).
    async fn write(&mut self, device: &str, base: &str, index: Option<i64>, value: f64) -> Result<u16> {
        self.client.exec(&command(device, base, index, value)).await
    }

    /// Прочитать число поля из свежего снимка.
    async fn read(&mut self, device: &str, base: &str, index: Option<i64>) -> Result<Option<f64>> {
        Ok(luatab::number(&self.snapshot().await?, device, base, index))
    }

    /// Записать, подождать, прочитать.
    async fn write_and_read(
        &mut self,
        device: &str,
        base: &str,
        index: Option<i64>,
        value: f64,
    ) -> Result<Option<f64>> {
        self.write(device, base, index, value).await?;
        tokio::time::sleep(SETTLE).await;
        self.read(device, base, index).await
    }
}

/// Равны ли значения с допуском 1e-3 (после округления в прошивке).
fn close(a: Option<f64>, b: f64) -> bool {
    a.is_some_and(|a| (a - b).abs() < 1e-3)
}

/// Значение, которое прошивка должна сохранить после записи `test` (целое, состояние 0/1).
fn expected_after_write(tag: &Tag, test: f64) -> f64 {
    let shaped = tag.shape(&Value::Float(test)).and_then(|v| v.as_f64());
    shaped.unwrap_or(test)
}

/// Проверить один записываемый тег: записать, подождать, перечитать и сравнить с тем, что должна оставить прошивка по правилам из конфигурации; значение тега после проверки возвращается.
async fn check(probe: &mut Probe<'_>, tag: &Tag) -> Result<Outcome> {
    let (Some(device), Some(field)) = (&tag.device, &tag.field) else {
        return Ok(Outcome::Skip("нет device/field"));
    };
    let (base, index) = split_field(field);
    let Some(before) = probe.read(device, &base, index).await? else {
        return Ok(Outcome::Skip("у проверяемого PAC нет поля"));
    };
    // Проверочное значение: заведомо другое; для состояния — «странное» ненулевое или 0.
    let test = match (tag.write_state, tag.data_type) {
        (true, _) => {
            if before == 0.0 {
                7.0
            } else {
                0.0
            }
        }
        (false, DataType::Int | DataType::Byte | DataType::Bool) => before + 1.0,
        (false, _) => before + 1.5,
    };
    let want_held = expected_after_write(tag, test);

    let fail = |msg: String| Ok(Outcome::Fail(format!("{device}.{field}: {msg}")));
    let outcome = if tag.write_ignored {
        let after = probe.write_and_read(device, &base, index, test).await?;
        if close(after, before) {
            Outcome::Pass
        } else {
            return fail(format!("write: ignore — записали {test}, стало {after:?} (было {before})"));
        }
    } else if let Some(req) = &tag.write_requires {
        let (mbase, mindex) = split_field(&req.field);
        // В автоматическом режиме команда принимается, но значение остаётся за программой.
        let auto = probe.write_and_read(device, &base, index, test).await?;
        if !close(auto, before) {
            return fail(format!("в автоматическом режиме записали {test}, стало {auto:?} (должно остаться {before})"));
        }
        // Условие выполнено — запись действует.
        let original_mode = probe.read(&req.device, &mbase, mindex).await?;
        probe.write(&req.device, &mbase, mindex, req.equals).await?;
        tokio::time::sleep(SETTLE).await;
        let held = probe.write_and_read(device, &base, index, test).await?;
        if !close(held, want_held) {
            probe.write(&req.device, &mbase, mindex, original_mode.unwrap_or(0.0)).await?;
            return fail(format!(
                "при {}={} записали {test}, стало {held:?} (ждали {want_held})",
                req.field, req.equals
            ));
        }
        // Условие пропало — значение возвращается к тому, что считает программа.
        probe.write(&req.device, &mbase, mindex, original_mode.unwrap_or(0.0)).await?;
        tokio::time::sleep(SETTLE).await;
        let reverted = probe.read(device, &base, index).await?;
        if close(reverted, before) {
            Outcome::Pass
        } else {
            return fail(format!("после снятия условия стало {reverted:?}, ждали возврата к {before}"));
        }
    } else {
        let after = probe.write_and_read(device, &base, index, test).await?;
        if close(after, want_held) {
            Outcome::Pass
        } else {
            return fail(format!("записали {test}, стало {after:?} (ждали {want_held})"));
        }
    };
    probe.write(device, &base, index, before).await?; // вернуть как было (ignore/gated его не меняют)
    Ok(outcome)
}

/// Ответы на команды, не относящиеся к одному полю: разбор как у прошивки.
async fn check_protocol_codes(client: &mut PacClient, device: &str, base: &str) -> Result<Vec<String>> {
    let mut problems = Vec::new();
    for (text, expected, what) in [
        ("__NO_SUCH_DEVICE:set_cmd('ST', 1, 1)".to_string(), 1u16, "неизвестный прибор"),
        (format!("__{device}:set_cmd('NO_SUCH_FIELD', 1, 1)"), 0, "неизвестное поле известного прибора"),
        (format!("__{device}:set_cmd('{base}', 1, 'abc')"), 1, "значение не число"),
        ("garbage".to_string(), 1, "не команда"),
    ] {
        let code = client.exec(&text).await?;
        if code != expected {
            problems.push(format!("{what}: код {code}, ждали {expected}"));
        }
    }
    Ok(problems)
}

/// `simulator conformance`: проверить все записываемые PAC-теги конфигурации на PAC по адресу `host:port`; расхождение — ненулевой код выхода.
pub async fn run(host: &str, port: u16, config_path: &Path) -> Result<()> {
    let cfg = config::load(config_path)?;
    let mut tags = Vec::new();
    for db in &cfg.plc.data_blocks {
        for t in &db.tags {
            let tag = Tag::from_config(t)?;
            if tag.protocol == Protocol::Pac && tag.writable {
                tags.push(tag);
            }
        }
    }
    let mut client = PacClient::connect(host, port).await?;
    let mut probe = Probe { client: &mut client };
    let (mut passed, mut skipped, mut failures) = (0usize, 0usize, Vec::<String>::new());
    for (n, tag) in tags.iter().enumerate() {
        match check(&mut probe, tag).await.with_context(|| format!("тег {}", tag.name))? {
            Outcome::Pass => passed += 1,
            Outcome::Skip(_) => skipped += 1,
            Outcome::Fail(m) => failures.push(m),
        }
        if n % 25 == 24 {
            eprintln!("проверено {} из {}", n + 1, tags.len());
        }
    }
    // Коды ответов на странные команды — на первом прибореполе, которое есть у PAC.
    let mut protocol_problems = Vec::new();
    if let Some(t) = tags.iter().find(|t| t.write_requires.is_none() && !t.write_ignored) {
        let (device, field) = (t.device.clone().unwrap_or_default(), t.field.clone().unwrap_or_default());
        if probe.read(&device, &split_field(&field).0, split_field(&field).1).await?.is_some() {
            protocol_problems = check_protocol_codes(probe.client, &device, &split_field(&field).0).await?;
        }
    }
    println!(
        "PAC {host}:{port}: проверено тегов {}, соответствуют {passed}, пропущено {skipped}, расходятся {}",
        tags.len(),
        failures.len()
    );
    for f in failures.iter().take(30) {
        println!("  ✗ {f}");
    }
    for p in &protocol_problems {
        println!("  ✗ протокол: {p}");
    }
    if passed == 0 {
        bail!("ни один тег не проверен (нет общих приборов с конфигурацией?)");
    }
    if !failures.is_empty() || !protocol_problems.is_empty() {
        bail!("поведение PAC расходится с конфигурацией");
    }
    println!("✓ поведение записи соответствует конфигурации");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fields_are_split_into_name_and_index() {
        assert_eq!(split_field("ST"), ("ST".into(), None));
        assert_eq!(split_field("RT_PAR_F[12]"), ("RT_PAR_F".into(), Some(12)));
        assert_eq!(split_field("PAR_MAIN[ 3 ].P_CZAD_S"), ("PAR_MAIN".into(), Some(3)));
    }

    #[test]
    fn commands_put_the_index_in_their_own_argument() {
        assert_eq!(command("OBJECT1", "RT_PAR_F", Some(12), 7.5), "__OBJECT1:set_cmd('RT_PAR_F', 12, 7.5)");
        assert_eq!(command("LINE1V0", "M", None, 1.0), "__LINE1V0:set_cmd('M', 1, 1)");
    }
}
