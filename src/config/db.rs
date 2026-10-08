//! Подключение к PostgreSQL: адрес из JDBC-URL (`jdbc:postgresql://host:port/db?sslmode=…`), учётные данные
//! из окружения, защита канала.

use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use tracing::warn;

use super::env::{Secret, env_any, env_or};

/// Режим TLS к PostgreSQL — как `sslmode` в libpq. Без проверки сертификата (`Require`) канал защищён от
/// прослушивания, но не от подмены сервера; от неё защищают `VerifyCa` и `VerifyFull`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DbSsl {
    /// Без TLS.
    Disable,
    /// Сначала без TLS, при отказе — с TLS.
    Allow,
    /// TLS, если сервер его предлагает, без проверки сертификата (по умолчанию, как в libpq).
    #[default]
    Prefer,
    /// Только TLS, сертификат сервера не проверяется: защита от прослушивания, но не от подмены сервера.
    Require,
    /// Только TLS, сертификат проверяется по корневому сертификату (имя сервера не сверяется).
    VerifyCa,
    /// Только TLS, проверяются и сертификат, и имя сервера.
    VerifyFull,
}

impl DbSsl {
    /// Разобрать значение `sslmode`; незнакомое — ошибка с перечнем допустимых.
    pub fn parse(text: &str) -> Result<Self> {
        Ok(match text.trim().to_ascii_lowercase().as_str() {
            "disable" => DbSsl::Disable,
            "allow" => DbSsl::Allow,
            "prefer" => DbSsl::Prefer,
            "require" => DbSsl::Require,
            "verify-ca" => DbSsl::VerifyCa,
            "verify-full" => DbSsl::VerifyFull,
            other => bail!("sslmode={other:?}: допустимо disable, allow, prefer, require, verify-ca, verify-full"),
        })
    }

    /// Канал шифруется всегда (без отката на открытый).
    pub fn is_encrypted(self) -> bool {
        matches!(self, DbSsl::Require | DbSsl::VerifyCa | DbSsl::VerifyFull)
    }
}

/// Подключение к PostgreSQL.
#[derive(Debug, Clone)]
pub struct DbSettings {
    /// Адрес сервера.
    pub host: String,
    /// Порт (по умолчанию 5432).
    pub port: u16,
    /// Имя базы.
    pub database: String,
    /// Пользователь (`SPRING_DATASOURCE_USERNAME` или `DB_USERNAME`).
    pub username: String,
    /// Пароль; не показывается в `Debug` и журнале.
    pub password: Secret,
    /// Режим TLS.
    pub ssl: DbSsl,
    /// Корневой сертификат УЦ сервера (`sslrootcert`); без него — системные корневые сертификаты.
    pub ssl_root_cert: Option<PathBuf>,
}

impl DbSettings {
    /// Подключение из окружения: адрес — из URL, логин и пароль — из своих переменных; `DB_SSLMODE` и `DB_SSLROOTCERT` сильнее адреса.
    pub(super) fn from_env() -> Result<Self> {
        let url = env_any(&["SPRING_DATASOURCE_URL", "DB_URL"])
            .unwrap_or_else(|| "jdbc:postgresql://localhost:5433/scada_db".into());
        let parsed = parse_jdbc_url(&url)?;
        // Переменные окружения сильнее URL: TLS включается, не трогая адрес.
        let ssl = match env_any(&["DB_SSLMODE"]) {
            Some(mode) => DbSsl::parse(&mode).context("DB_SSLMODE")?,
            None => parsed.ssl,
        };
        let ssl_root_cert = env_any(&["DB_SSLROOTCERT"]).map(PathBuf::from).or(parsed.ssl_root_cert);
        Ok(DbSettings {
            host: parsed.host,
            port: parsed.port,
            database: parsed.database,
            username: env_or(&["SPRING_DATASOURCE_USERNAME", "DB_USERNAME"], "scada_user"),
            password: Secret::new(env_or(&["SPRING_DATASOURCE_PASSWORD", "DB_PASSWORD"], "scada_password")),
            ssl,
            ssl_root_cert,
        })
    }
}

/// Разобранный адрес БД.
#[derive(Debug, PartialEq, Eq)]
pub struct JdbcUrl {
    /// Адрес сервера.
    pub host: String,
    /// Порт.
    pub port: u16,
    /// Имя базы.
    pub database: String,
    /// Режим TLS из `sslmode` или `ssl`; по умолчанию `prefer`.
    pub ssl: DbSsl,
    /// Корневой сертификат из `sslrootcert`.
    pub ssl_root_cert: Option<PathBuf>,
}

/// `jdbc:postgresql://host:port/db?params` (или `postgres://…`). Из параметров понимаются `sslmode`,
/// `sslrootcert` и `ssl` (`true` — как `verify-full`, `false` — `disable`, как в драйвере JDBC); остальные
/// игнорируются с предупреждением, а неверное значение — ошибка: молча отброшенная настройка защиты хуже
/// отказа запуска.
pub fn parse_jdbc_url(url: &str) -> Result<JdbcUrl> {
    let rest = url
        .strip_prefix("jdbc:postgresql://")
        .or_else(|| url.strip_prefix("postgresql://"))
        .or_else(|| url.strip_prefix("postgres://"))
        .with_context(|| format!("неподдерживаемый адрес БД: {url}"))?;
    let (rest, query) = rest.split_once('?').unwrap_or((rest, ""));
    let rest = rest.rsplit('@').next().unwrap_or(rest); // user:pass@ в URL не используем
    let (authority, database) = rest.split_once('/').unwrap_or((rest, "scada_db"));
    let (host, port) = match authority.rsplit_once(':') {
        Some((h, p)) => (h.to_string(), p.parse().with_context(|| format!("порт БД: {p}"))?),
        None => (authority.to_string(), 5432),
    };
    if host.is_empty() || database.is_empty() {
        bail!("неполный адрес БД: {url}");
    }

    let (mut ssl, mut ssl_flag, mut ssl_root_cert) = (None, None, None);
    let mut ignored: Vec<&str> = Vec::new();
    for pair in query.split('&').filter(|p| !p.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        match key {
            "sslmode" => ssl = Some(DbSsl::parse(value).context("адрес БД")?),
            "sslrootcert" if !value.is_empty() => ssl_root_cert = Some(PathBuf::from(value)),
            "ssl" => {
                ssl_flag = Some(match value.to_ascii_lowercase().as_str() {
                    "true" | "" => DbSsl::VerifyFull,
                    "false" => DbSsl::Disable,
                    other => bail!("адрес БД: ssl={other:?}: допустимо true или false"),
                })
            }
            _ => ignored.push(key),
        }
    }
    if !ignored.is_empty() {
        warn!("Параметры адреса БД не поддерживаются и игнорируются: {}", ignored.join(", "));
    }
    Ok(JdbcUrl { host, port, database: database.to_string(), ssl: ssl.or(ssl_flag).unwrap_or_default(), ssl_root_cert })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn url(text: &str) -> JdbcUrl {
        parse_jdbc_url(text).unwrap()
    }

    #[test]
    fn jdbc_url_is_parsed() {
        let u = url("jdbc:postgresql://postgres:5432/scada_db");
        assert_eq!((u.host.as_str(), u.port, u.database.as_str()), ("postgres", 5432, "scada_db"));
        assert_eq!(u.ssl, DbSsl::Prefer, "по умолчанию — как libpq: TLS, если сервер его предлагает");
        let u = url("jdbc:postgresql://localhost/scada_db?ssl=false");
        assert_eq!((u.host.as_str(), u.port, u.database.as_str()), ("localhost", 5432, "scada_db"));
        assert_eq!(u.ssl, DbSsl::Disable);
        assert!(parse_jdbc_url("mysql://x/y").is_err());
    }

    #[test]
    fn sslmode_and_root_cert_are_honoured() {
        let u = url("jdbc:postgresql://db:5432/scada?sslmode=verify-full&sslrootcert=/etc/ssl/db-ca.pem");
        assert_eq!(u.ssl, DbSsl::VerifyFull);
        assert_eq!(u.ssl_root_cert, Some(PathBuf::from("/etc/ssl/db-ca.pem")));
        assert_eq!(url("postgres://db/scada?sslmode=REQUIRE").ssl, DbSsl::Require);
        assert!(url("postgres://db/scada?sslmode=require").ssl.is_encrypted());
        assert!(!url("postgres://db/scada").ssl.is_encrypted());
    }

    #[test]
    fn jdbc_ssl_flag_follows_the_driver_and_sslmode_wins() {
        assert_eq!(url("jdbc:postgresql://db/scada?ssl=true").ssl, DbSsl::VerifyFull);
        assert_eq!(url("jdbc:postgresql://db/scada?ssl=true&sslmode=require").ssl, DbSsl::Require);
        assert_eq!(url("jdbc:postgresql://db/scada?currentSchema=x&ApplicationName=y").ssl, DbSsl::Prefer);
    }

    #[test]
    fn bad_security_values_stop_the_start_instead_of_being_ignored() {
        for bad in ["sslmode=encrypt", "sslmode=", "ssl=maybe"] {
            let err = parse_jdbc_url(&format!("jdbc:postgresql://db/scada?{bad}")).unwrap_err();
            assert!(format!("{err:#}").contains("допустимо"), "{bad}: {err:#}");
        }
    }

    #[test]
    fn password_stays_out_of_debug_output() {
        let s = DbSettings {
            host: "db".into(),
            port: 5432,
            database: "scada".into(),
            username: "u".into(),
            password: Secret::new("hunter2".into()),
            ssl: DbSsl::Require,
            ssl_root_cert: None,
        };
        assert!(!format!("{s:?}").contains("hunter2"));
    }
}
