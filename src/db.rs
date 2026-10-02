//! PostgreSQL: схема, синхронизация конфигурации, журнал событий, история телеметрии.
//!
//! Всё, что пишет в БД, идёт через очереди и отдельные задачи — опрос ПЛК никогда не ждёт
//! базу. (В Java-шлюзе потоки опроса писали в БД сами, и зависшая база останавливала
//! телеметрию целиком.)

use std::collections::HashMap;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions};
use sqlx::{FromRow, Row};
use tracing::{info, warn};

use crate::config::DbSettings;
use crate::model::{Controller, TagValue};

pub async fn connect(settings: &DbSettings) -> Result<PgPool> {
    let options = PgConnectOptions::new()
        .host(&settings.host)
        .port(settings.port)
        .database(&settings.database)
        .username(&settings.username)
        .password(&settings.password);
    let mut attempt = 0;
    loop {
        attempt += 1;
        match PgPoolOptions::new()
            .max_connections(8)
            .acquire_timeout(Duration::from_secs(10))
            .connect_with(options.clone())
            .await
        {
            Ok(pool) => {
                sqlx::migrate!("./migrations").run(&pool).await.context("миграция схемы")?;
                info!("БД {}:{}/{} подключена", settings.host, settings.port, settings.database);
                return Ok(pool);
            }
            Err(e) if attempt < 30 => {
                warn!("БД недоступна (попытка {attempt}/30): {e}");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            Err(e) => return Err(e).context("БД недоступна"),
        }
    }
}

/// Синхронизировать controllers/tags с controllers.yaml (YAML — источник истины) и проставить
/// id из БД. Ключи upsert как у Java-шлюза: контроллер — по имени, тег — по nodeId в пределах
/// контроллера; исчезнувшие из YAML удаляются. Одной транзакцией.
pub async fn sync_config(pool: &PgPool, controllers: &mut [Controller], all_yaml_names: &[String]) -> Result<()> {
    let mut tx = pool.begin().await?;
    // Пара горячего резерва на общей БД стартует одновременно: без блокировки оба экземпляра вставляли
    // бы одни и те же контроллеры (нарушение UNIQUE → падение старта). Второй ждёт первого и видит
    // готовые строки. Блокировка снимается с концом транзакции.
    sqlx::query("SELECT pg_advisory_xact_lock(hashtext('scada-gateway:config-sync'))").execute(&mut *tx).await?;
    let (mut created, mut updated, mut deleted) = (0, 0, 0);
    for ctrl in controllers.iter_mut() {
        let row = sqlx::query(
            "INSERT INTO controllers (name, endpoint, enabled, created_at, updated_at)
             VALUES ($1, $2, $3, now(), now())
             ON CONFLICT (name) DO UPDATE SET endpoint = EXCLUDED.endpoint, enabled = EXCLUDED.enabled, updated_at = now()
             RETURNING id",
        )
        .bind(&ctrl.name)
        .bind(&ctrl.endpoint)
        .bind(ctrl.enabled)
        .fetch_one(&mut *tx)
        .await
        .with_context(|| format!("контроллер {}", ctrl.name))?;
        ctrl.id = row.get("id");

        let existing: HashMap<String, i64> = sqlx::query("SELECT id, node_id FROM tags WHERE controller_id = $1")
            .bind(ctrl.id)
            .fetch_all(&mut *tx)
            .await?
            .into_iter()
            .map(|r| (r.get::<String, _>("node_id"), r.get::<i64, _>("id")))
            .collect();
        let mut seen = Vec::with_capacity(ctrl.tags.len());
        for tag in ctrl.tags.iter_mut() {
            let t = std::sync::Arc::make_mut(tag);
            let id = match existing.get(&t.node_id) {
                Some(&id) => {
                    updated += 1;
                    sqlx::query(
                        "UPDATE tags SET name=$2, data_type=$3, polling_rate=$4, unit=$5, enabled=$6, min_value=$7,
                         max_value=$8, channel_id=$9, device_name=$10, field_name=$11, device_type=$12, protocol=$13,
                         modbus_address=$14, modbus_type=$15, modbus_unit_id=$16, writable=$17,
                         history_deadband=$18, history_deadband_percent=$19, history_min_interval_ms=$20,
                         history_max_interval_ms=$21, updated_at=now()
                         WHERE id=$1",
                    )
                    .bind(id)
                    .bind(&t.name)
                    .bind(&t.data_type)
                    .bind(t.polling_rate_ms as i64)
                    .bind(&t.unit)
                    .bind(t.enabled)
                    .bind(t.min_value)
                    .bind(t.max_value)
                    .bind(t.channel_id)
                    .bind(&t.device_name)
                    .bind(&t.field_name)
                    .bind(&t.device_type)
                    .bind(&t.protocol_raw)
                    .bind(t.modbus_address)
                    .bind(&t.modbus_type)
                    .bind(t.modbus_unit_id as i32)
                    .bind(t.writable)
                    .bind(t.history.deadband)
                    .bind(t.history.deadband_percent)
                    .bind(t.history.min_interval_ms.map(|v| v as i64))
                    .bind(t.history.max_interval_ms.map(|v| v as i64))
                    .execute(&mut *tx)
                    .await?;
                    id
                }
                None => {
                    created += 1;
                    sqlx::query(
                        "INSERT INTO tags (controller_id, node_id, name, data_type, polling_rate, unit, enabled, min_value,
                         max_value, channel_id, device_name, field_name, device_type, protocol, modbus_address,
                         modbus_type, modbus_unit_id, writable, record_device, history_deadband,
                         history_deadband_percent, history_min_interval_ms, history_max_interval_ms,
                         created_at, updated_at)
                         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,false,$19,$20,$21,$22,now(),now())
                         RETURNING id",
                    )
                    .bind(ctrl.id).bind(&t.node_id)
                    .bind(&t.name).bind(&t.data_type).bind(t.polling_rate_ms as i64).bind(&t.unit).bind(t.enabled)
                    .bind(t.min_value).bind(t.max_value).bind(t.channel_id).bind(&t.device_name)
                    .bind(&t.field_name).bind(&t.device_type).bind(&t.protocol_raw).bind(t.modbus_address)
                    .bind(&t.modbus_type).bind(t.modbus_unit_id as i32).bind(t.writable)
                    .bind(t.history.deadband).bind(t.history.deadband_percent)
                    .bind(t.history.min_interval_ms.map(|v| v as i64))
                    .bind(t.history.max_interval_ms.map(|v| v as i64))
                    .fetch_one(&mut *tx)
                    .await?
                    .get("id")
                }
            };
            t.id = id;
            seen.push(t.node_id.clone());
        }
        let stale = sqlx::query("DELETE FROM tags WHERE controller_id = $1 AND NOT (node_id = ANY($2))")
            .bind(ctrl.id)
            .bind(&seen)
            .execute(&mut *tx)
            .await?;
        deleted += stale.rows_affected();
    }
    // Контроллеры, которых больше нет в YAML (выключенные в YAML остаются), — вместе с тегами.
    let gone =
        sqlx::query("DELETE FROM tags WHERE controller_id IN (SELECT id FROM controllers WHERE NOT (name = ANY($1)))")
            .bind(all_yaml_names)
            .execute(&mut *tx)
            .await?;
    sqlx::query("DELETE FROM controllers WHERE NOT (name = ANY($1))").bind(all_yaml_names).execute(&mut *tx).await?;
    tx.commit().await?;
    info!(
        "Синхронизация с YAML: тегов создано {created}, обновлено {updated}, удалено {}",
        deleted + gone.rows_affected()
    );
    Ok(())
}

// ------------------------------------------------------------------- журнал событий --

/// Строка журнала для вставки.
pub struct EventRow {
    pub time: DateTime<Utc>,
    pub event_type: String,
    pub source: String,
    pub severity: String,
    pub message: String,
    pub tag_id: Option<i64>,
    pub controller_id: Option<i64>,
    pub details: Option<String>,
}

/// Пакетная вставка в event_log одним запросом (UNNEST).
pub async fn insert_events(pool: &PgPool, rows: &[EventRow]) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let col = |f: fn(&EventRow) -> String| rows.iter().map(f).collect::<Vec<_>>();
    sqlx::query(
        "INSERT INTO event_log (event_time, event_type, source, severity, message, tag_id, controller_id, details, acknowledged)
         SELECT t.*, false FROM UNNEST($1::timestamptz[], $2::varchar[], $3::varchar[], $4::varchar[], $5::varchar[],
                                      $6::bigint[], $7::bigint[], $8::text[]) AS t",
    )
    .bind(rows.iter().map(|r| r.time).collect::<Vec<_>>())
    .bind(col(|r| r.event_type.clone()))
    .bind(col(|r| r.source.clone()))
    .bind(col(|r| r.severity.clone()))
    .bind(col(|r| r.message.chars().take(500).collect()))
    .bind(rows.iter().map(|r| r.tag_id).collect::<Vec<_>>())
    .bind(rows.iter().map(|r| r.controller_id).collect::<Vec<_>>())
    .bind(rows.iter().map(|r| r.details.clone()).collect::<Vec<_>>())
    .execute(pool)
    .await?;
    Ok(())
}

/// Событие журнала для REST — поля как у EventLogEntity Java-шлюза.
#[derive(Debug, Serialize, FromRow)]
#[serde(rename_all = "camelCase")]
pub struct EventLogEntry {
    pub id: i64,
    pub event_time: DateTime<Utc>,
    pub event_type: String,
    pub source: Option<String>,
    pub severity: Option<String>,
    pub message: Option<String>,
    pub details: Option<String>,
    pub tag_id: Option<i64>,
    pub controller_id: Option<i64>,
    pub user_id: Option<String>,
    pub acknowledged: Option<bool>,
}

/// Выборка журнала для REST. Всегда с LIMIT (Java-шлюз читал таблицу целиком).
pub async fn events(pool: &PgPool, filter: EventFilter<'_>, limit: i64) -> Result<Vec<EventLogEntry>> {
    const COLS: &str = "SELECT id, event_time, event_type, source, severity, message, details, tag_id, controller_id, user_id, acknowledged FROM event_log";
    let limit = limit.clamp(1, 1000);
    let rows = match filter {
        EventFilter::All => {
            sqlx::query_as(const_format::concatcp!(COLS, " ORDER BY event_time DESC LIMIT $1"))
                .bind(limit)
                .fetch_all(pool)
                .await?
        }
        EventFilter::Type(t) => {
            sqlx::query_as(const_format::concatcp!(COLS, " WHERE event_type = $2 ORDER BY event_time DESC LIMIT $1"))
                .bind(limit)
                .bind(t)
                .fetch_all(pool)
                .await?
        }
        EventFilter::Severity(sev) => {
            sqlx::query_as(const_format::concatcp!(COLS, " WHERE severity = $2 ORDER BY event_time DESC LIMIT $1"))
                .bind(limit)
                .bind(sev)
                .fetch_all(pool)
                .await?
        }
        EventFilter::UnacknowledgedCritical => {
            sqlx::query_as(const_format::concatcp!(
                COLS,
                " WHERE severity = 'CRITICAL' AND acknowledged = false ORDER BY event_time DESC LIMIT $1"
            ))
            .bind(limit)
            .fetch_all(pool)
            .await?
        }
    };
    Ok(rows)
}

pub enum EventFilter<'a> {
    All,
    Type(&'a str),
    Severity(&'a str),
    UnacknowledgedCritical,
}

/// Подтвердить аларм (только события типа ALARM).
pub async fn acknowledge_alarm(pool: &PgPool, id: i64, user_id: &str) -> Result<bool> {
    let r =
        sqlx::query("UPDATE event_log SET acknowledged = true, user_id = $2 WHERE id = $1 AND event_type = 'ALARM'")
            .bind(id)
            .bind(user_id)
            .execute(pool)
            .await?;
    Ok(r.rows_affected() > 0)
}

/// Сводка по последним 10000 событиям — как /api/events/stats Java-шлюза, но на стороне БД.
pub async fn event_stats(pool: &PgPool) -> Result<(i64, i64, i64, i64)> {
    let row = sqlx::query(
        "SELECT count(*) AS total,
                count(*) FILTER (WHERE severity IN ('ERROR','CRITICAL')) AS errors,
                count(*) FILTER (WHERE severity = 'WARNING') AS warnings
         FROM (SELECT severity FROM event_log ORDER BY event_time DESC LIMIT 10000) recent",
    )
    .fetch_one(pool)
    .await?;
    let unacked: i64 =
        sqlx::query_scalar("SELECT count(*) FROM event_log WHERE severity = 'CRITICAL' AND acknowledged = false")
            .fetch_one(pool)
            .await?;
    Ok((row.get("total"), row.get("errors"), row.get("warnings"), unacked))
}

pub async fn ping(pool: &PgPool) -> bool {
    tokio::time::timeout(Duration::from_secs(2), sqlx::query("SELECT 1").execute(pool))
        .await
        .map(|r| r.is_ok())
        .unwrap_or(false)
}

// -------------------------------------------------------------- история телеметрии --

/// Точка истории.
pub struct TelemetryRow {
    pub tag_id: i64,
    pub time: DateTime<Utc>,
    pub quality: &'static str,
    pub value: Option<TagValue>,
}

/// Пакетная вставка точек одним запросом (UNNEST): число → value, строка → value_str.
pub async fn insert_telemetry(pool: &PgPool, rows: &[TelemetryRow]) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    sqlx::query(
        "INSERT INTO telemetry (tag_id, \"time\", quality, value, value_str)
         SELECT * FROM UNNEST($1::bigint[], $2::timestamptz[], $3::varchar[], $4::float8[], $5::varchar[])",
    )
    .bind(rows.iter().map(|r| r.tag_id).collect::<Vec<_>>())
    .bind(rows.iter().map(|r| r.time).collect::<Vec<_>>())
    .bind(rows.iter().map(|r| r.quality).collect::<Vec<_>>())
    .bind(rows.iter().map(|r| r.value.as_ref().and_then(TagValue::as_f64)).collect::<Vec<_>>())
    .bind(
        rows.iter()
            .map(|r| match &r.value {
                Some(TagValue::Text(s)) => Some(s.chars().take(255).collect::<String>()),
                _ => None,
            })
            .collect::<Vec<_>>(),
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// Удалить историю старше retention.
pub async fn prune_telemetry(pool: &PgPool, retention: Duration) -> Result<u64> {
    let r = sqlx::query("DELETE FROM telemetry WHERE \"time\" < now() - make_interval(secs => $1)")
        .bind(retention.as_secs_f64())
        .execute(pool)
        .await?;
    Ok(r.rows_affected())
}
