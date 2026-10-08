//! Шлюз целиком с защищённым OPC UA: политика Basic256Sha256 и пользователь из controllers.yaml.
//!
//! Симулятор запускают с SIM_OPCUA_USER / SIM_OPCUA_PASSWORD (CI делает это сам); те же значения для
//! тестов — IT_OPCUA_USER / IT_OPCUA_PASSWORD (по умолчанию operator / operator-pass).

mod common;

use std::time::Duration;

use common::gateway::Gateway;
use common::{controller, sim_host};
use scada_gateway::model::{ControllerKind, Protocol};

/// Станция из одного OPC UA-контроллера симулятора с заданной защитой и учётными данными.
fn controllers_yaml(dir: &std::path::Path, security: &str, user: &str, password: &str) -> std::path::PathBuf {
    let ctrl = controller(ControllerKind::OpcUa);
    let tags: Vec<String> = ctrl
        .tags
        .iter()
        .filter(|t| t.protocol == Protocol::OpcUa)
        .take(5)
        .map(|t| {
            format!(
                "        - {{name: \"{}\", nodeId: \"{}\", dataType: {}, pollingRate: 1000, enabled: true, writable: false}}",
                t.name, t.node_id, t.data_type
            )
        })
        .collect();
    let path = dir.join("controllers.yaml");
    std::fs::write(
        &path,
        format!(
            "opcua:\n  servers:\n    - name: Secure\n      endpoint: \"{}\"\n      security: {security}\n      username: \"{user}\"\n      password: \"{password}\"\n      enabled: true\n      tags:\n{}\n",
            ctrl.endpoint,
            tags.join("\n")
        ),
    )
    .unwrap();
    path
}

/// Пустая временная папка для хранилища сертификатов.
fn temp_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("scada-it-secure-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Логин и пароль защищённой точки симулятора (`SIM_OPCUA_USER`/`SIM_OPCUA_PASSWORD`).
fn creds() -> (String, String) {
    (
        std::env::var("IT_OPCUA_USER").unwrap_or_else(|_| "operator".into()),
        std::env::var("IT_OPCUA_PASSWORD").unwrap_or_else(|_| "operator-pass".into()),
    )
}

/// Шлюз с `security: Basic256Sha256` и пользователем из `controllers.yaml` подключается; в журнале есть режим канала, но нет
/// пароля, а доверие любому сертификату (стенд) отмечено предупреждением.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "нужен симулятор с SIM_OPCUA_USER/SIM_OPCUA_PASSWORD"]
async fn gateway_connects_with_policy_and_user_from_the_config() {
    if !matches!(sim_host().as_str(), "127.0.0.1" | "localhost") {
        eprintln!("симулятор не на этой машине — пропущено");
        return;
    }
    let dir = temp_dir("ok");
    let (user, pass) = creds();
    let yaml = controllers_yaml(&dir, "Basic256Sha256", &user, &pass);
    let pki = dir.join("pki");
    let gw = Gateway::start_with(
        &yaml,
        &[("GATEWAY_OPCUA_PKI_DIR", pki.to_str().unwrap()), ("GATEWAY_OPCUA_TRUST_SERVER_CERTS", "true")],
    );
    gw.wait_links("UP", Duration::from_secs(40)).await;
    let log = gw.log_tail();
    assert!(log.contains("Basic256Sha256") && !log.contains(&pass), "режим канала в журнале есть, пароля нет:\n{log}");
    assert!(log.contains("не проверяется"), "предупреждение о доверии любому сертификату:\n{log}");
    gw.delete_topics().await;
}

/// Неверный пароль — связь остаётся потерянной, а самого пароля нет в журнале.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "нужен симулятор с SIM_OPCUA_USER/SIM_OPCUA_PASSWORD"]
async fn wrong_password_keeps_the_link_down_and_the_secret_out_of_the_log() {
    if !matches!(sim_host().as_str(), "127.0.0.1" | "localhost") {
        return;
    }
    let dir = temp_dir("bad");
    let (user, _) = creds();
    let yaml = controllers_yaml(&dir, "Basic256Sha256", &user, "совсем-не-тот-пароль");
    let gw = Gateway::start_with(
        &yaml,
        &[("GATEWAY_OPCUA_PKI_DIR", dir.join("pki").to_str().unwrap()), ("GATEWAY_OPCUA_TRUST_SERVER_CERTS", "true")],
    );
    tokio::time::sleep(Duration::from_secs(8)).await;
    assert!(gw.links().values().all(|s| s == "DOWN"), "связь не должна подняться с неверным паролем: {:?}", gw.links());
    assert!(!gw.log_tail().contains("совсем-не-тот-пароль"), "пароль попал в журнал");
    gw.delete_topics().await;
}

/// Ошибка в `security:` — шлюз не стартует и говорит почему (а не молча работает без защиты).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "нужен Kafka для общего каркаса тестов"]
async fn unknown_security_value_stops_the_gateway_with_a_clear_message() {
    let dir = temp_dir("typo");
    let yaml = controllers_yaml(&dir, "Basic256Sha256x", "u", "p");
    let mut gw = Gateway::start_with(&yaml, &[]);
    let status = loop {
        if let Some(s) = gw.child.try_wait().unwrap() {
            break s;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    assert!(!status.success());
    assert!(gw.log_tail().contains("Basic256Sha256x"), "{}", gw.log_tail());
}
