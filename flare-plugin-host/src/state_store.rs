//! Deployment-owned persistence, shared by every replica. Reads are authoritative
//! (no positive authorization cache); storage failure denies new admissions.
use flare_grpc_proto::capability::PluginTenantState;
use sqlx::{PgPool, Row, postgres::PgPoolOptions};
use tonic::Status;

/// DDL of the shared tenant state table, for the deployment's schema owner.
pub const RUNTIME_SQL: &str = include_str!("../initial/runtime.sql");
/// Least privileges of the runtime role on that table; psql variable
/// `runtime_role` (`:"runtime_role"`) names the role.
pub const RUNTIME_GRANTS_SQL: &str = include_str!("../initial/grants.sql");

#[tonic::async_trait]
pub trait TenantStateStore: Send + Sync {
    async fn get(
        &self,
        deployment: &str,
        app: &str,
        grant: &str,
    ) -> Result<Option<PluginTenantState>, Status>;
    /// Resolve this deployment's operator-projected tenant installation after
    /// an upgrade. Ambiguous installations fail closed.
    async fn current(
        &self,
        deployment: &str,
        app: &str,
        now_ms: i64,
        include_disabled: bool,
    ) -> Result<Option<PluginTenantState>, Status>;
    async fn apply(&self, state: PluginTenantState) -> Result<(), Status>;
    async fn healthy(&self) -> Result<(), Status>;
}

pub struct PostgresTenantStateStore(PgPool);
fn unavailable(_: sqlx::Error) -> Status {
    Status::unavailable("runtime persistence unavailable")
}
fn validate_workload_projection(state: &PluginTenantState) -> Result<(), Status> {
    for values in [
        &state.approved_operations,
        &state.permissions,
        &state.data_scopes,
    ] {
        if values.len() > 512
            || values.iter().any(|id| {
                id.is_empty()
                    || id.len() > 256
                    || !id
                        .bytes()
                        .all(|c| c.is_ascii_alphanumeric() || b"._:-".contains(&c))
            })
            || values
                .iter()
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                != values.len()
        {
            return Err(Status::invalid_argument(
                "invalid workload authorization projection",
            ));
        }
    }
    Ok(())
}
impl PostgresTenantStateStore {
    pub fn new(pool: PgPool) -> Self {
        Self(pool)
    }
    pub async fn connect(url: &str) -> Result<Self, sqlx::Error> {
        let pool = PgPoolOptions::new()
            .max_connections(16)
            .acquire_timeout(std::time::Duration::from_secs(2))
            .after_connect(|connection, _| {
                Box::pin(async move {
                    sqlx::query("SET statement_timeout = '2s'")
                        .execute(&mut *connection)
                        .await?;
                    sqlx::query("SET lock_timeout = '1s'")
                        .execute(connection)
                        .await?;
                    Ok(())
                })
            })
            .connect(url)
            .await?;
        let result = Self(pool);
        // Fail startup if explicit provisioning was omitted. Never execute DDL here.
        check_schema(&result.0).await?;
        Ok(result)
    }
}
async fn check_schema(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query("SELECT deployment_id, app_id, organization_id, grant_id, revision, manifest_digest, configuration_json, configuration_digest, enabled, valid_until_ms, approved_operations, permissions, data_scopes FROM plugin_runtime_tenants LIMIT 0")
        .execute(pool).await.map(|_|())
}
#[tonic::async_trait]
impl TenantStateStore for PostgresTenantStateStore {
    async fn get(
        &self,
        deployment: &str,
        app: &str,
        grant: &str,
    ) -> Result<Option<PluginTenantState>, Status> {
        let row = sqlx::query("SELECT * FROM plugin_runtime_tenants WHERE deployment_id=$1 AND app_id=$2 AND grant_id=$3")
            .bind(deployment).bind(app).bind(grant).fetch_optional(&self.0).await.map_err(unavailable)?;
        row.map(|row| {
            Ok(PluginTenantState {
                organization_id: row.try_get("organization_id").map_err(unavailable)?,
                deployment_id: row.try_get("deployment_id").map_err(unavailable)?,
                app_id: row.try_get("app_id").map_err(unavailable)?,
                grant_id: row.try_get("grant_id").map_err(unavailable)?,
                revision: row.try_get::<i64, _>("revision").map_err(unavailable)? as u64,
                manifest_digest: row.try_get("manifest_digest").map_err(unavailable)?,
                configuration_json: row.try_get("configuration_json").map_err(unavailable)?,
                configuration_digest: row.try_get("configuration_digest").map_err(unavailable)?,
                enabled: row.try_get("enabled").map_err(unavailable)?,
                valid_until_ms: row.try_get("valid_until_ms").map_err(unavailable)?,
                validate_configuration: false,
                approved_operations: row.try_get("approved_operations").map_err(unavailable)?,
                permissions: row.try_get("permissions").map_err(unavailable)?,
                data_scopes: row.try_get("data_scopes").map_err(unavailable)?,
            })
        })
        .transpose()
    }
    async fn current(
        &self,
        deployment: &str,
        app: &str,
        now_ms: i64,
        include_disabled: bool,
    ) -> Result<Option<PluginTenantState>, Status> {
        let grants: Vec<(String, bool)> = sqlx::query_as("SELECT grant_id,enabled FROM plugin_runtime_tenants WHERE deployment_id=$1 AND app_id=$2 AND valid_until_ms>$3 AND (enabled OR $4) ORDER BY enabled DESC,grant_id LIMIT 2")
            .bind(deployment).bind(app).bind(now_ms).bind(include_disabled).fetch_all(&self.0).await.map_err(unavailable)?;
        let Some((grant, enabled)) = grants.first() else {
            return Ok(None);
        };
        if grants.get(1).is_some_and(|(_, other)| other == enabled) {
            return Err(Status::failed_precondition(
                "ambiguous tenant runtime installation",
            ));
        }
        // `get` re-reads the authoritative row; the signer rechecks expiry and
        // enabled state if control changed between these two reads.
        self.get(deployment, app, grant).await
    }
    async fn apply(&self, s: PluginTenantState) -> Result<(), Status> {
        validate_workload_projection(&s)?;
        let mut tx = self.0.begin().await.map_err(unavailable)?;
        let scope = &s.app_id;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 51973))")
            .bind(scope)
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?;
        let conflicting: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM plugin_runtime_tenants WHERE app_id=$1 AND organization_id<>$2)")
            .bind(&s.app_id).bind(&s.organization_id)
            .fetch_one(&mut *tx).await.map_err(unavailable)?;
        if conflicting {
            return Err(Status::permission_denied("application owner cannot change"));
        }
        let updated = sqlx::query("INSERT INTO plugin_runtime_tenants AS old (deployment_id,app_id,grant_id,revision,manifest_digest,configuration_json,configuration_digest,enabled,valid_until_ms,approved_operations,permissions,data_scopes,organization_id) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13) ON CONFLICT (deployment_id,app_id,grant_id) DO UPDATE SET revision=excluded.revision,manifest_digest=excluded.manifest_digest,configuration_json=excluded.configuration_json,configuration_digest=excluded.configuration_digest,enabled=excluded.enabled,valid_until_ms=CASE WHEN old.revision=excluded.revision THEN GREATEST(old.valid_until_ms,excluded.valid_until_ms) ELSE excluded.valid_until_ms END,approved_operations=excluded.approved_operations,permissions=excluded.permissions,data_scopes=excluded.data_scopes WHERE old.organization_id=excluded.organization_id AND (old.revision<excluded.revision OR (old.revision=excluded.revision AND old.manifest_digest=excluded.manifest_digest AND old.configuration_digest=excluded.configuration_digest AND old.enabled=excluded.enabled AND old.approved_operations=excluded.approved_operations AND old.permissions=excluded.permissions AND old.data_scopes=excluded.data_scopes))")
            .bind(&s.deployment_id).bind(&s.app_id).bind(&s.grant_id).bind(s.revision as i64)
            .bind(&s.manifest_digest).bind(&s.configuration_json).bind(&s.configuration_digest)
            .bind(s.enabled).bind(s.valid_until_ms)
            .bind(&s.approved_operations).bind(&s.permissions).bind(&s.data_scopes).bind(&s.organization_id)
            .execute(&mut *tx).await.map_err(unavailable)?;
        if updated.rows_affected() != 1 {
            return Err(Status::failed_precondition(
                "stale or conflicting runtime revision",
            ));
        }
        tx.commit().await.map_err(unavailable)?;
        Ok(())
    }
    async fn healthy(&self) -> Result<(), Status> {
        check_schema(&self.0).await.map_err(unavailable)
    }
}

/// Deterministic fixture only. Production composition requires Postgres.
#[cfg(test)]
#[derive(Default)]
pub(crate) struct MemoryStore(
    pub std::sync::RwLock<std::collections::BTreeMap<(String, String, String), PluginTenantState>>,
);
#[cfg(test)]
#[tonic::async_trait]
impl TenantStateStore for MemoryStore {
    async fn get(&self, d: &str, a: &str, g: &str) -> Result<Option<PluginTenantState>, Status> {
        Ok(self
            .0
            .read()
            .unwrap()
            .get(&(d.into(), a.into(), g.into()))
            .cloned())
    }
    async fn current(
        &self,
        d: &str,
        a: &str,
        now_ms: i64,
        include_disabled: bool,
    ) -> Result<Option<PluginTenantState>, Status> {
        let rows = self.0.read().unwrap();
        let candidates = rows
            .values()
            .filter(|s| {
                s.deployment_id == d
                    && s.app_id == a
                    && s.valid_until_ms > now_ms
                    && (s.enabled || include_disabled)
            })
            .collect::<Vec<_>>();
        let enabled = candidates
            .iter()
            .copied()
            .filter(|s| s.enabled)
            .collect::<Vec<_>>();
        let chosen = if enabled.is_empty() {
            &candidates
        } else {
            &enabled
        };
        if chosen.len() > 1 {
            return Err(Status::failed_precondition(
                "ambiguous tenant runtime installation",
            ));
        }
        Ok(chosen.first().map(|s| (*s).clone()))
    }
    async fn apply(&self, s: PluginTenantState) -> Result<(), Status> {
        validate_workload_projection(&s)?;
        let mut rows = self.0.write().unwrap();
        if rows
            .values()
            .any(|old| old.app_id == s.app_id && old.organization_id != s.organization_id)
        {
            return Err(Status::permission_denied("application owner cannot change"));
        }
        let key = (
            s.deployment_id.clone(),
            s.app_id.clone(),
            s.grant_id.clone(),
        );
        if let Some(old) = rows.get(&key)
            && (old.organization_id != s.organization_id
                || old.revision > s.revision
                || (old.revision == s.revision
                    && (old.configuration_digest != s.configuration_digest
                        || old.enabled != s.enabled
                        || old.manifest_digest != s.manifest_digest
                        || old.approved_operations != s.approved_operations
                        || old.permissions != s.permissions
                        || old.data_scopes != s.data_scopes)))
        {
            return Err(Status::failed_precondition(
                "stale or conflicting runtime revision",
            ));
        }
        rows.insert(key, s);
        Ok(())
    }
    async fn healthy(&self) -> Result<(), Status> {
        Ok(())
    }
}

#[cfg(test)]
mod workload_projection_tests {
    use super::*;
    use crate::runtime::status_fixture::{host_with_store, tenant};

    #[tokio::test]
    async fn application_owner_cannot_change_even_for_a_new_grant() {
        let (host, store) = host_with_store();
        let state = tenant(&host, 1, true);
        store.apply(state.clone()).await.unwrap();
        let mut forged = state.clone();
        forged.organization_id = "other-org".into();
        forged.revision += 1;
        assert_eq!(
            store.apply(forged.clone()).await.unwrap_err().code(),
            tonic::Code::PermissionDenied
        );
        forged.grant_id = "new-grant".into();
        assert_eq!(
            store.apply(forged.clone()).await.unwrap_err().code(),
            tonic::Code::PermissionDenied
        );
        forged.deployment_id = "new-deployment".into();
        assert_eq!(
            store.apply(forged).await.unwrap_err().code(),
            tonic::Code::PermissionDenied
        );
        assert_eq!(
            store
                .get(&state.deployment_id, &state.app_id, &state.grant_id)
                .await
                .unwrap()
                .unwrap()
                .organization_id,
            state.organization_id
        );
    }

    #[tokio::test]
    async fn workload_authorization_changes_require_a_new_revision() {
        let (host, store) = host_with_store();
        let mut state = tenant(&host, 1, true);
        state.approved_operations = vec!["example.echo.run".into()];
        state.permissions = vec!["example.echo.read".into()];
        state.data_scopes = vec!["example.self".into()];
        store.apply(state.clone()).await.unwrap();
        let mut changed = state.clone();
        changed.permissions.push("example.echo.write".into());
        assert_eq!(
            store.apply(changed.clone()).await.unwrap_err().code(),
            tonic::Code::FailedPrecondition
        );
        changed.revision += 1;
        store.apply(changed.clone()).await.unwrap();
        assert_eq!(
            store
                .get("echo-1", "201", "grant-1")
                .await
                .unwrap()
                .unwrap()
                .permissions,
            changed.permissions
        );
    }

    #[tokio::test]
    async fn malformed_and_duplicate_workload_scopes_are_rejected() {
        let (host, store) = host_with_store();
        let mut state = tenant(&host, 1, true);
        state.permissions = vec!["scope".into(), "scope".into()];
        assert_eq!(
            store.apply(state).await.unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
        let mut state = tenant(&host, 1, true);
        state.data_scopes = vec!["../other-tenant".into()];
        assert_eq!(
            store.apply(state).await.unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
    }
}
