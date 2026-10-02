//! HTTP: здоровье, метрики Prometheus и REST журнала — пути как у Java-шлюза.

use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::app::App;
use crate::db::{self, EventFilter};

pub fn router(app: Arc<App>) -> Router {
    Router::new()
        .route("/actuator/health", get(health))
        .route("/actuator/prometheus", get(prometheus))
        .route("/api/health", get(api_health))
        .route("/api/status", get(status))
        .route("/api/ha", get(ha))
        .route("/api/scripts", get(scripts))
        .route("/api/events", get(events))
        .route("/api/events/type/{event_type}", get(events_by_type))
        .route("/api/events/severity/{severity}", get(events_by_severity))
        .route("/api/events/alarms", get(alarms))
        .route("/api/events/alarms/unacknowledged", get(unacknowledged))
        .route("/api/events/{id}/acknowledge", post(acknowledge))
        .route("/api/events/stats", get(stats))
        .with_state(app)
}

/// Живость процесса + состояние БД и связи с контроллерами (для глаз; статус UP, пока
/// процесс жив — как health Java-шлюза без индикаторов).
async fn health(State(app): State<Arc<App>>) -> Json<Value> {
    let db = match &app.db {
        Some(pool) => {
            if db::ping(pool).await {
                "UP"
            } else {
                "DOWN"
            }
        }
        None => "DISABLED",
    };
    let controllers: serde_json::Map<String, Value> = app
        .controllers
        .iter()
        .map(|c| (c.ctrl.name.clone(), json!(if c.is_connected() { "UP" } else { "DOWN" })))
        .collect();
    Json(json!({"status": "UP", "components": {"db": {"status": db}, "controllers": controllers}}))
}

async fn prometheus(State(app): State<Arc<App>>) -> Response {
    ([(header::CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")], app.metrics.render()).into_response()
}

async fn api_health() -> Json<Value> {
    Json(json!({"status": "UP", "service": "SCADA Gateway"}))
}

async fn status(State(app): State<Arc<App>>) -> Json<Value> {
    let controllers: Vec<Value> = app
        .controllers
        .iter()
        .map(|c| {
            json!({"id": c.ctrl.id, "name": c.ctrl.name, "endpoint": c.ctrl.endpoint,
                        "protocol": format!("{:?}", c.ctrl.kind), "connected": c.is_connected(),
                        "tags": c.ctrl.tags.len()})
        })
        .collect();
    Json(json!({
        "server": "SCADA Gateway",
        "status": "RUNNING",
        "time": chrono::Utc::now().to_rfc3339(),
        "uptimeSeconds": app.started.elapsed().as_secs(),
        "controllers": controllers,
    }))
}

/// Роль экземпляра в паре горячего резерва.
async fn ha(State(app): State<Arc<App>>) -> Json<Value> {
    let l = &app.leadership;
    Json(json!({
        "enabled": l.is_ha_enabled(),
        "instance": l.instance_id(),
        "role": if l.is_active() { "ACTIVE" } else { "STANDBY" },
        "since": l.since().to_rfc3339(),
        "group": l.group_id(),
    }))
}

/// Пользовательские скрипты обработки значений: привязки, ошибки, состояние перезагрузки.
async fn scripts(State(app): State<Arc<App>>) -> Json<Value> {
    Json(app.scripts.info())
}

#[derive(Deserialize)]
struct LimitQuery {
    limit: Option<i64>,
}

fn no_db() -> Response {
    (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"error": "БД шлюза не подключена"}))).into_response()
}

fn db_error(e: anyhow::Error) -> Response {
    (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": format!("{e:#}")}))).into_response()
}

async fn query_events(app: &App, filter: EventFilter<'_>, limit: Option<i64>) -> Response {
    let Some(pool) = &app.db else { return no_db() };
    match db::events(pool, filter, limit.unwrap_or(100)).await {
        Ok(rows) => Json(rows).into_response(),
        Err(e) => db_error(e),
    }
}

async fn events(State(app): State<Arc<App>>, Query(q): Query<LimitQuery>) -> Response {
    query_events(&app, EventFilter::All, q.limit).await
}

async fn events_by_type(State(app): State<Arc<App>>, Path(t): Path<String>, Query(q): Query<LimitQuery>) -> Response {
    query_events(&app, EventFilter::Type(&t), q.limit).await
}

async fn events_by_severity(
    State(app): State<Arc<App>>,
    Path(s): Path<String>,
    Query(q): Query<LimitQuery>,
) -> Response {
    query_events(&app, EventFilter::Severity(&s), q.limit).await
}

async fn alarms(State(app): State<Arc<App>>, Query(q): Query<LimitQuery>) -> Response {
    query_events(&app, EventFilter::Type("ALARM"), q.limit).await
}

async fn unacknowledged(State(app): State<Arc<App>>, Query(q): Query<LimitQuery>) -> Response {
    query_events(&app, EventFilter::UnacknowledgedCritical, q.limit).await
}

#[derive(Deserialize)]
struct AckQuery {
    #[serde(rename = "userId")]
    user_id: String,
}

async fn acknowledge(State(app): State<Arc<App>>, Path(id): Path<i64>, Query(q): Query<AckQuery>) -> Response {
    let Some(pool) = &app.db else { return no_db() };
    match db::acknowledge_alarm(pool, id, &q.user_id).await {
        Ok(_) => {
            Json(json!({"status": "ACKNOWLEDGED", "message": format!("Alarm {id} acknowledged by {}", q.user_id)}))
                .into_response()
        }
        Err(e) => db_error(e),
    }
}

async fn stats(State(app): State<Arc<App>>) -> Response {
    let Some(pool) = &app.db else { return no_db() };
    match db::event_stats(pool).await {
        Ok((total, errors, warnings, unacked)) => Json(json!({
            "total_events": total,
            "errors": errors,
            "warnings": warnings,
            "unacknowledged_alarms": unacked,
            "timestamp": chrono::Utc::now().timestamp_millis(),
        }))
        .into_response(),
        Err(e) => db_error(e),
    }
}
