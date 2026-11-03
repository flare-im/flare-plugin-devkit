//! Domain events for audit consumers, committed with the business change and
//! delivered at least once: one leased event at a time, a fenced ACK, and a
//! stable event id (`<source>:<tenant>:<eventKey>`) consumers deduplicate on.
//!
//! The outbox can be switched off ([`Outbox::disabled`]): nothing is written
//! and nothing is delivered. Switching it on later does not recreate events of
//! the time it was off.
use crate::KitError;
use serde_json::Value;
use sqlx::{PgPool, Postgres, Row, Transaction};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Outbox {
    enabled: bool,
}
impl Outbox {
    pub fn enabled() -> Self {
        Self { enabled: true }
    }
    pub fn disabled() -> Self {
        Self { enabled: false }
    }
    pub fn is_enabled(self) -> bool {
        self.enabled
    }
    /// Append `kind` with a key unique per tenant; a repeated key is ignored.
    pub async fn emit(
        self,
        tx: &mut Transaction<'_, Postgres>,
        tenant: &str,
        key: &str,
        kind: &str,
        payload: Value,
        now_ms: i64,
    ) -> Result<(), KitError> {
        if !self.enabled {
            return Ok(());
        }
        if !payload.is_object() {
            return Err(KitError::Invalid);
        }
        sqlx::query("INSERT INTO plugin_outbox(app_id,event_key,event_type,payload,created_at_ms) VALUES($1,$2,$3,$4,$5) ON CONFLICT (app_id,event_key) DO NOTHING")
            .bind(tenant)
            .bind(key)
            .bind(kind)
            .bind(payload)
            .bind(now_ms)
            .execute(&mut **tx)
            .await?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct OutboxEvent {
    /// Stable across redeliveries.
    pub id: String,
    pub tenant_id: String,
    pub kind: String,
    pub payload: Value,
    pub fence: i64,
    pub sequence: u64,
    pub created_at_ms: u64,
}
#[tonic::async_trait]
pub trait EventSink: Send + Sync {
    async fn deliver(&self, event: &OutboxEvent) -> Result<(), ()>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Backlog {
    pub pending: i64,
    pub oldest_age_ms: i64,
}

/// Aggregate health only; never tenant payloads.
pub async fn backlog(pool: &PgPool) -> Result<Backlog, KitError> {
    let row = sqlx::query("SELECT count(*) AS pending,COALESCE(GREATEST((extract(epoch FROM clock_timestamp())*1000)::bigint-min(created_at_ms),0),0)::bigint AS age FROM plugin_outbox WHERE delivered_at_ms IS NULL")
        .fetch_one(pool)
        .await?;
    Ok(Backlog {
        pending: row.get("pending"),
        oldest_age_ms: row.get("age"),
    })
}

/// Deliver up to `limit` events. Each is claimed with a 30 second lease right
/// before delivery and acknowledged only by the lease holder at that fence.
pub async fn dispatch(
    pool: &PgPool,
    source: &str,
    worker: &str,
    sink: &dyn EventSink,
    limit: u32,
) -> Result<u32, KitError> {
    if worker.is_empty() || worker.len() > 128 || limit > 100 {
        return Err(KitError::Invalid);
    }
    let mut delivered = 0;
    for _ in 0..limit {
        let mut tx = pool.begin().await?;
        let Some(row) = sqlx::query("WITH selected AS (SELECT id FROM plugin_outbox WHERE delivered_at_ms IS NULL AND lease_until_ms<(extract(epoch FROM clock_timestamp())*1000)::bigint ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED) UPDATE plugin_outbox o SET lease_owner=$1,lease_until_ms=(extract(epoch FROM clock_timestamp())*1000)::bigint+30000,fence=fence+1 FROM selected WHERE o.id=selected.id RETURNING o.*")
            .bind(worker)
            .fetch_optional(&mut *tx)
            .await?
        else {
            break;
        };
        tx.commit().await?;
        let id: i64 = row.get("id");
        let tenant: String = row.get("app_id");
        let event = OutboxEvent {
            id: format!("{source}:{tenant}:{}", row.get::<String, _>("event_key")),
            tenant_id: tenant,
            kind: row.get("event_type"),
            payload: row.get("payload"),
            fence: row.get("fence"),
            sequence: id as u64,
            created_at_ms: row.get::<i64, _>("created_at_ms") as u64,
        };
        if tokio::time::timeout(std::time::Duration::from_secs(2), sink.deliver(&event))
            .await
            .is_ok_and(|r| r.is_ok())
        {
            let acknowledged = sqlx::query("UPDATE plugin_outbox SET delivered_at_ms=(extract(epoch FROM clock_timestamp())*1000)::bigint WHERE id=$1 AND lease_owner=$2 AND fence=$3 AND delivered_at_ms IS NULL")
                .bind(id)
                .bind(worker)
                .bind(event.fence)
                .execute(pool)
                .await?;
            delivered += acknowledged.rows_affected() as u32;
        }
    }
    Ok(delivered)
}

/// Retention: drop up to `limit` delivered events older than `before_ms`.
/// Undelivered events are kept.
pub async fn prune_delivered(pool: &PgPool, before_ms: i64, limit: i64) -> Result<u64, KitError> {
    Ok(sqlx::query("DELETE FROM plugin_outbox WHERE id IN (SELECT id FROM plugin_outbox WHERE delivered_at_ms IS NOT NULL AND delivered_at_ms<$1 AND created_at_ms<$1 ORDER BY id LIMIT $2)")
        .bind(before_ms)
        .bind(limit.clamp(1, 10_000))
        .execute(pool)
        .await?
        .rows_affected())
}
