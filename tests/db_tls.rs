//! TLS к PostgreSQL: `sslmode` из адреса БД действует, а не молча отбрасывается.
//!
//! Нужен PostgreSQL с `ssl=on` и самоподписанным сертификатом (CI поднимает его сам):
//!   IT_DATABASE_TLS_URL  — jdbc:postgresql://localhost:5434/scada_tls
//!   IT_DATABASE_TLS_CA   — путь к сертификату сервера (он же корневой)
//!   IT_DATABASE_TLS_USER / IT_DATABASE_TLS_PASSWORD — по умолчанию postgres / pw
//! Запуск: `cargo test --test db_tls -- --ignored`

use std::path::PathBuf;

use scada_gateway::config::{DbSettings, DbSsl, Secret, parse_jdbc_url};
use scada_gateway::db;

/// Настройки подключения к PostgreSQL с TLS из окружения; `None` — тест не настроен.
fn settings(ssl: DbSsl, root_cert: bool) -> Option<DbSettings> {
    let url = std::env::var("IT_DATABASE_TLS_URL").ok()?;
    let ca = std::env::var("IT_DATABASE_TLS_CA").ok()?;
    let parsed = parse_jdbc_url(&url).expect("IT_DATABASE_TLS_URL");
    Some(DbSettings {
        host: parsed.host,
        port: parsed.port,
        database: parsed.database,
        username: std::env::var("IT_DATABASE_TLS_USER").unwrap_or_else(|_| "postgres".into()),
        password: Secret::new(std::env::var("IT_DATABASE_TLS_PASSWORD").unwrap_or_else(|_| "pw".into())),
        ssl,
        ssl_root_cert: root_cert.then(|| PathBuf::from(ca)),
    })
}

/// Шифруется ли соединение, по данным самого сервера.
async fn is_encrypted(s: &DbSettings) -> bool {
    let pool = db::connect(s).await.expect("подключение");
    sqlx::query_scalar("SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()")
        .fetch_one(&pool)
        .await
        .expect("pg_stat_ssl")
}

/// `require` и `prefer` дают зашифрованное соединение (сервер сам это подтверждает в `pg_stat_ssl`), `disable` — открытое.
#[tokio::test]
#[ignore = "нужен PostgreSQL с TLS (IT_DATABASE_TLS_URL)"]
async fn require_encrypts_and_disable_does_not() {
    let Some(on) = settings(DbSsl::Require, false) else {
        panic!("IT_DATABASE_TLS_URL/IT_DATABASE_TLS_CA не заданы")
    };
    assert!(is_encrypted(&on).await, "sslmode=require: канал должен шифроваться");
    assert!(
        is_encrypted(&settings(DbSsl::Prefer, false).unwrap()).await,
        "prefer берёт TLS, если сервер его предлагает"
    );
    assert!(!is_encrypted(&settings(DbSsl::Disable, false).unwrap()).await, "sslmode=disable: открытый канал");
}

/// `verify-full` принимает сертификат своего УЦ и отвергает чужой сразу, без минуты повторов, не раскрывая пароль в ошибке.
#[tokio::test]
#[ignore = "нужен PostgreSQL с TLS (IT_DATABASE_TLS_URL)"]
async fn verify_full_checks_the_server_certificate() {
    // С корневым сертификатом сервера — подключается, канал зашифрован.
    let trusted = settings(DbSsl::VerifyFull, true).expect("IT_DATABASE_TLS_URL/IT_DATABASE_TLS_CA не заданы");
    assert!(is_encrypted(&trusted).await);
    // Без него самоподписанный сертификат не проверяется — отказ сразу, без минуты повторов.
    let untrusted = settings(DbSsl::VerifyFull, false).unwrap();
    let started = std::time::Instant::now();
    let err = db::connect(&untrusted).await.expect_err("чужой сертификат должен быть отвергнут");
    assert!(started.elapsed().as_secs() < 10, "ошибка сертификата не повторяется: {:?}", started.elapsed());
    assert!(!format!("{err:#}").contains("pw"), "пароль не попадает в ошибку: {err:#}");
}

/// Неверный пароль — ошибка за секунды, а не после повторов подключения.
#[tokio::test]
#[ignore = "нужен PostgreSQL с TLS (IT_DATABASE_TLS_URL)"]
async fn wrong_password_fails_fast() {
    let mut s = settings(DbSsl::Require, false).expect("IT_DATABASE_TLS_URL/IT_DATABASE_TLS_CA не заданы");
    s.password = Secret::new("не-тот-пароль".into());
    let started = std::time::Instant::now();
    assert!(db::connect(&s).await.is_err());
    assert!(started.elapsed().as_secs() < 10, "неверный пароль не лечится повторами: {:?}", started.elapsed());
}
