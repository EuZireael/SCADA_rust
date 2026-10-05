//! REST под токеном: `/api/*` закрыт `GATEWAY_API_TOKEN`, `/actuator/*` открыт (проверка контейнера, метрики).

mod common;

use std::time::Duration;

use common::gateway::Gateway;

#[tokio::test(flavor = "multi_thread")]
#[ignore = "нужны симулятор и Kafka: cargo test -- --ignored"]
async fn api_requires_token_but_actuator_stays_open() {
    let gw = Gateway::start_with(&common::controllers_path(), &[("GATEWAY_API_TOKEN", "it-token-123")]);
    gw.wait_links("UP", Duration::from_secs(30)).await;

    // Без токена и с чужим — 401; здоровье и метрики открыты.
    for path in ["/api/status", "/api/ha", "/api/scripts", "/api/health"] {
        assert_eq!(gw.get(path).map(|r| r.0), Some(401), "{path} без токена");
        assert_eq!(gw.get_with(path, Some("wrong")).map(|r| r.0), Some(401), "{path} с чужим токеном");
        assert_eq!(gw.get_with(path, Some("it-token-123")).map(|r| r.0), Some(200), "{path} с токеном");
    }
    assert_eq!(gw.get("/actuator/health").map(|r| r.0), Some(200));
    assert_eq!(gw.get("/actuator/prometheus").map(|r| r.0), Some(200));
    gw.delete_topics().await;
}

/// Без токена поведение прежнее: `/api/*` открыт.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "нужны симулятор и Kafka: cargo test -- --ignored"]
async fn api_is_open_without_token() {
    let gw = Gateway::start();
    gw.wait_links("UP", Duration::from_secs(30)).await;
    assert_eq!(gw.get("/api/status").map(|r| r.0), Some(200));
    gw.delete_topics().await;
}
