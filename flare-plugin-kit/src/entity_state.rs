//! Entity shared state: what every member of a conversation sees of one
//! plugin entity (a poll's tally, a solitaire's lines).
//!
//! Two paths reach Core, and both carry the same versions:
//! - **in the call result**: a user call returns `data.sharedState` (and the
//!   caller's own `data.messageState`), which Core publishes at once;
//! - **by the plugin**: every change is also staged here in the business
//!   transaction ([`stage`]) and [`publish_due`] sends what Core has not
//!   acknowledged yet with the deployment's runtime credential. That is the
//!   only path for changes no user call carries (a deadline, a console
//!   change), and the safety net for the first: Core keeps the highest
//!   version, so a repeat is a no-op.
//!
//! A state of an ended entity is not retried: Core refuses it with
//! `FAILED_PRECONDITION` naming why ([`CARD_RECALLED`], [`CARD_UNBOUND`]),
//! and that version is left unpublished at once (see [`publish_due`]).
use crate::KitError;
use flare_plugin_host::{
    PluginRuntimeHost,
    protocol::{resource::EntityStatePublishView, state::SharedState, ui::PluginMessageState},
    resource::HostResourceClient,
};
use serde_json::{Map, Value};
use sqlx::{PgPool, Postgres, Row, Transaction};
use std::sync::Arc;

/// Staging delay of a state that the call result already carries: the
/// plugin's own publish then only catches what Core missed.
pub const CARRIED_DELAY_MS: i64 = 3_000;
/// Staging delay of a state no result carries.
pub const IMMEDIATE: i64 = 0;
/// Attempts before a state is left unpublished (until a newer version).
pub const MAX_ATTEMPTS: i32 = 12;
/// Core's refusal of an entity whose every card was recalled. A recalled card
/// never comes back: the state is not retried.
pub const CARD_RECALLED: &str = "PLUGIN_MESSAGE_RECALLED";
/// Core's refusal of an entity with no card of the plugin: the platform gave
/// the post up (the plugin is told, `entityLifecycle.unbound` reason
/// `post_failed`), or the card was stored a moment ago and Storage Writer has
/// not bound it yet. Retried for [`UNBOUND_GRACE_MS`] after the version was
/// staged, then left unpublished.
pub const CARD_UNBOUND: &str = "PLUGIN_CARD_UNBOUND";
/// How long a state of an entity without a card is still retried.
pub const UNBOUND_GRACE_MS: i64 = 120_000;
/// How long one publish attempt owns a staged state.
const LEASE_MS: i64 = 15_000;

/// `data` with the shared state and the caller's message state beside it,
/// both validated first.
pub fn with_states(
    data: Value,
    shared: Option<&SharedState>,
    message: Option<&PluginMessageState>,
) -> Result<Value, KitError> {
    let Value::Object(mut object) = data else {
        return Err(KitError::Invalid);
    };
    if let Some(shared) = shared {
        shared.validate().map_err(|_| KitError::Invalid)?;
        object.insert(
            "sharedState".into(),
            serde_json::to_value(shared).map_err(|_| KitError::Invalid)?,
        );
    }
    if let Some(message) = message {
        message.validate().map_err(|_| KitError::Invalid)?;
        object.insert(
            "messageState".into(),
            serde_json::to_value(message).map_err(|_| KitError::Invalid)?,
        );
    }
    Ok(Value::Object(object))
}

/// Stage `state` of `tenant`'s entity in the business transaction. An older
/// or equal version than the staged one is ignored. `delay_ms` defers the
/// plugin's own publish ([`CARRIED_DELAY_MS`] when the call result carries
/// the state); while an earlier change still waits, the earlier schedule
/// holds, so a stream of changes is published at least once per delay.
pub async fn stage(
    tx: &mut Transaction<'_, Postgres>,
    tenant: &str,
    grant_id: &str,
    state: &SharedState,
    delay_ms: i64,
    now_ms: i64,
) -> Result<(), KitError> {
    state.validate().map_err(|_| KitError::Invalid)?;
    let version = i64::try_from(state.version).map_err(|_| KitError::Invalid)?;
    sqlx::query("INSERT INTO plugin_entity_states AS old(app_id,entity,grant_id,version,values_json,published_version,attempts,next_attempt_at_ms,updated_at_ms) VALUES($1,$2,$3,$4,$5,0,0,$6,$7) ON CONFLICT (app_id,entity) DO UPDATE SET grant_id=excluded.grant_id,version=excluded.version,values_json=excluded.values_json,attempts=0,updated_at_ms=excluded.updated_at_ms,next_attempt_at_ms=CASE WHEN old.published_version<old.version AND old.attempts=0 THEN LEAST(old.next_attempt_at_ms,excluded.next_attempt_at_ms) ELSE excluded.next_attempt_at_ms END WHERE old.version<excluded.version")
        .bind(tenant)
        .bind(&state.entity)
        .bind(grant_id)
        .bind(version)
        .bind(Value::Object(state.values.clone()))
        .bind(now_ms.saturating_add(delay_ms.max(0)))
        .bind(now_ms)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

/// Sends a shared state to Core; implemented by [`HostStatePublisher`] and by
/// test doubles.
#[tonic::async_trait]
pub trait StatePublisher: Send + Sync {
    async fn publish(
        &self,
        tenant: &str,
        grant_id: &str,
        state: SharedState,
    ) -> Result<EntityStatePublishView, tonic::Status>;
}

/// Core's bridge under this deployment's runtime credential.
pub struct HostStatePublisher {
    pub client: HostResourceClient,
    pub runtime: PluginRuntimeHost,
}
#[tonic::async_trait]
impl StatePublisher for HostStatePublisher {
    async fn publish(
        &self,
        tenant: &str,
        grant_id: &str,
        state: SharedState,
    ) -> Result<EntityStatePublishView, tonic::Status> {
        let grant = self
            .runtime
            .current_grant_id(tenant, grant_id, false)
            .await?;
        self.client
            .publish_entity_state(&self.runtime, tenant, &grant, state)
            .await
    }
}

/// A publisher when both the bridge and the runtime credential are
/// configured; `None` otherwise (results still carry their states).
pub fn host_publisher(
    client: Option<HostResourceClient>,
    runtime: &PluginRuntimeHost,
) -> Option<Arc<dyn StatePublisher>> {
    match client {
        Some(client) if runtime.signs_runtime_writes() => Some(Arc::new(HostStatePublisher {
            client,
            runtime: runtime.clone(),
        })),
        _ => None,
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PublishReport {
    pub published: u32,
    /// Failed and scheduled again.
    pub failed: u32,
    /// Refused for an ended entity and left unpublished.
    pub ended: u32,
}

/// Whether Core refused a state because its entity has ended, `staged_ms`
/// after the version was staged: the card was recalled, or the entity still
/// has no card once the grace is over.
pub fn entity_ended(status: &tonic::Status, staged_ms: i64) -> bool {
    status.code() == tonic::Code::FailedPrecondition
        && match status.message() {
            CARD_RECALLED => true,
            CARD_UNBOUND => staged_ms >= UNBOUND_GRACE_MS,
            _ => false,
        }
}

/// Publish up to `limit` staged states that are due. Each is leased before
/// the network call, so replicas never send the same state at once and a
/// business transaction staging a newer version never waits on it. A failure
/// backs off and is retried up to [`MAX_ATTEMPTS`] times; a refusal for an
/// ended entity ([`entity_ended`]) is not retried. A newer version staged
/// later gets its own attempts.
pub async fn publish_due(
    pool: &PgPool,
    publisher: &dyn StatePublisher,
    limit: u32,
) -> Result<PublishReport, KitError> {
    let mut report = PublishReport::default();
    for _ in 0..limit.min(500) {
        let Some(row) = sqlx::query("WITH due AS (SELECT app_id,entity FROM plugin_entity_states WHERE published_version<version AND attempts<$1 AND next_attempt_at_ms<=(extract(epoch FROM clock_timestamp())*1000)::bigint ORDER BY next_attempt_at_ms LIMIT 1 FOR UPDATE SKIP LOCKED) UPDATE plugin_entity_states s SET next_attempt_at_ms=(extract(epoch FROM clock_timestamp())*1000)::bigint+$2 FROM due WHERE s.app_id=due.app_id AND s.entity=due.entity RETURNING s.app_id,s.entity,s.grant_id,s.version,s.values_json,s.attempts,(extract(epoch FROM clock_timestamp())*1000)::bigint-s.updated_at_ms AS staged_ms")
            .bind(MAX_ATTEMPTS)
            .bind(LEASE_MS)
            .fetch_optional(pool)
            .await?
        else {
            break;
        };
        let tenant: String = row.get("app_id");
        let entity: String = row.get("entity");
        let version: i64 = row.get("version");
        let attempts: i32 = row.get("attempts");
        let staged_ms: i64 = row.get("staged_ms");
        let values: Map<String, Value> = match row.get::<Value, _>("values_json") {
            Value::Object(values) => values,
            _ => Map::new(),
        };
        let state = SharedState {
            entity: entity.clone(),
            version: version as u64,
            values,
        };
        let sent = tokio::time::timeout(
            std::time::Duration::from_secs(3),
            publisher.publish(&tenant, &row.get::<String, _>("grant_id"), state),
        )
        .await;
        if matches!(sent, Ok(Ok(_))) {
            sqlx::query("UPDATE plugin_entity_states SET published_version=GREATEST(published_version,$3),attempts=CASE WHEN version=$3 THEN 0 ELSE attempts END WHERE app_id=$1 AND entity=$2")
                .bind(&tenant)
                .bind(&entity)
                .bind(version)
                .execute(pool)
                .await?;
            report.published += 1;
        } else if matches!(&sent, Ok(Err(status)) if entity_ended(status, staged_ms)) {
            sqlx::query("UPDATE plugin_entity_states SET attempts=$4 WHERE app_id=$1 AND entity=$2 AND version=$3")
                .bind(&tenant)
                .bind(&entity)
                .bind(version)
                .bind(MAX_ATTEMPTS)
                .execute(pool)
                .await?;
            report.ended += 1;
        } else {
            match &sent {
                Ok(Err(status)) => {
                    tracing::warn!(tenant_id=%tenant, %entity, version, code=?status.code(), error=%status.message(), "plugin shared state publish deferred")
                }
                Err(_) => {
                    tracing::warn!(tenant_id=%tenant, %entity, version, "plugin shared state publish timed out")
                }
                _ => {}
            }
            // Exponential backoff from 2 s to 5 min; a newer version resets it.
            let backoff = (2_000i64 << attempts.clamp(0, 8)).min(300_000);
            sqlx::query("UPDATE plugin_entity_states SET attempts=attempts+1,next_attempt_at_ms=(extract(epoch FROM clock_timestamp())*1000)::bigint+$4 WHERE app_id=$1 AND entity=$2 AND version=$3")
                .bind(&tenant)
                .bind(&entity)
                .bind(version)
                .bind(backoff)
                .execute(pool)
                .await?;
            report.failed += 1;
        }
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_ended_entity_is_told_from_a_passing_failure() {
        let refused = |code: &str| tonic::Status::failed_precondition(code);
        // A recalled card: final at once.
        assert!(entity_ended(&refused(CARD_RECALLED), 0));
        // No card: retried through the grace, final after it.
        assert!(!entity_ended(&refused(CARD_UNBOUND), 0));
        assert!(!entity_ended(&refused(CARD_UNBOUND), UNBOUND_GRACE_MS - 1));
        assert!(entity_ended(&refused(CARD_UNBOUND), UNBOUND_GRACE_MS));
        // Anything else is retried: other codes, other statuses with the same text, outages.
        assert!(!entity_ended(&refused("PLUGIN_DISABLED"), i64::MAX));
        for status in [
            tonic::Status::permission_denied(CARD_RECALLED),
            tonic::Status::permission_denied("plugin resource access denied"),
            tonic::Status::unavailable("resource bridge unavailable"),
            tonic::Status::deadline_exceeded("resource deadline exceeded"),
        ] {
            assert!(!entity_ended(&status, i64::MAX), "{status:?}");
        }
    }
}
