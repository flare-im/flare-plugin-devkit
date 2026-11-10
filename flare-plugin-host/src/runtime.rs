//! Shared multi-tenant runtime on the plugin's existing gRPC listener. Core's
//! reconciler delivers tenant states, and Core forwards the platform's data jobs, under Core
//! system tickets (`FLARE_PLUGIN_CALL_PUBLIC_KEYS`, the keys of every other ticket).
use crate::{VerifiedPluginStaff, VerifiedPluginSystem, VerifiedPluginUser};
use flare_grpc_proto::capability::{
    GenericRequest, PluginRuntimeInspectRequest, PluginRuntimeInspectResponse, PluginTenantState,
    PluginTenantStateAck, plugin_runtime_server::PluginRuntime,
};
use flare_plugin_protocol::{
    invocation::{
        MAX_RUNTIME_CALL_TTL_MS, PLUGIN_CALL_HEADER, PLUGIN_RUNTIME_HEADER, PluginCallSigner,
        PluginCallVerifier, PluginRuntimeClaims, PluginRuntimeSigner, PluginTenantStateClaims,
        ticket_key_id,
    },
    resource::{HostCapability, MAX_RESOURCE_REQUEST_BYTES, ResourceWriteRequest},
    runtime::{MAX_PROJECTION_LEASE_MS, PluginDeployment, configuration_digest, digest_bytes},
    signature::VerifiedManifest,
};
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tonic::{Request, Response, Status};
#[path = "runtime_event.rs"]
mod event;
#[path = "runtime_status.rs"]
mod status;
pub use status::TenantStatus;
#[cfg(test)]
pub(crate) use status::fixture as status_fixture;
#[cfg(test)]
#[path = "runtime_staff_tests.rs"]
mod staff_tests;
#[cfg(test)]
#[path = "runtime_system_tests.rs"]
mod system_tests;
#[cfg(test)]
#[path = "runtime_tests.rs"]
mod tests;

pub type ConfigurationValidator = dyn Fn(&Value) -> Result<(), Status> + Send + Sync;
/// Key id of this deployment's runtime credential key, as registered with Core.
pub const RUNTIME_KEY_ID_ENV: &str = "FLARE_PLUGIN_RUNTIME_KEY_ID";
/// Base64 of the raw 32-byte Ed25519 seed of the runtime credential key. Core
/// registers its public key for this deployment only
/// (`FLARE_PLUGIN_RUNTIME_PUBLIC_KEYS`). Independent of publisher, Cell and
/// call-signing keys.
pub const RUNTIME_SIGNING_KEY_ENV: &str = "FLARE_PLUGIN_RUNTIME_SIGNING_KEY";
/// Lifetime of one runtime credential; well under [`MAX_RUNTIME_CALL_TTL_MS`].
const RUNTIME_CREDENTIAL_TTL_MS: i64 = 30_000;
/// `state` as Core delivers it: a request carrying the ticket for exactly this state. For plugin
/// test harnesses, which stand in for Core.
pub fn tenant_state_request(
    signer: &PluginCallSigner,
    state: PluginTenantState,
) -> Result<Request<PluginTenantState>, Box<dyn std::error::Error>> {
    let now = now_ms();
    let ticket = signer.sign_tenant_state(
        &PluginTenantStateClaims {
            tenant_id: state.app_id.clone(),
            organization_id: state.organization_id.clone(),
            deployment_id: state.deployment_id.clone(),
            manifest_digest: state.manifest_digest.clone(),
            grant_id: state.grant_id.clone(),
            revision: state.revision,
            configuration_digest: state.configuration_digest.clone(),
            enabled: state.enabled,
            issued_at_ms: now,
            expires_at_ms: now + RUNTIME_CREDENTIAL_TTL_MS,
        },
        now,
    )?;
    let mut request = Request::new(state);
    request
        .metadata_mut()
        .insert(PLUGIN_CALL_HEADER, ticket.parse()?);
    Ok(request)
}
#[derive(Clone)]
pub struct PluginRuntimeHost(Arc<Inner>);
struct Inner {
    manifest: VerifiedManifest,
    deployment: PluginDeployment,
    calls: PluginCallVerifier,
    validate: Arc<ConfigurationValidator>,
    states: Arc<dyn super::state_store::TenantStateStore>,
    lifecycle: Arc<super::lifecycle::Lifecycle>,
    validation_slots: Arc<tokio::sync::Semaphore>,
    /// Operations only a staff ticket may invoke.
    admin_operations: BTreeSet<String>,
    /// The staff permission code each admin operation needs
    /// ([`PluginManifest::admin_permission_for`](flare_plugin_protocol::PluginManifest::admin_permission_for)).
    admin_permissions: BTreeMap<String, String>,
    organization_operations: BTreeSet<String>,
    /// Admin operations declaring `requiresReason`: their staff ticket must
    /// carry the operator's stated reason.
    reason_required: BTreeSet<String>,
    /// Operations only a platform system ticket may invoke: the `entityLifecycle.unbound`
    /// operations of message-content contributions.
    lifecycle_operations: BTreeSet<String>,
    runtime_signer: Option<PluginRuntimeSigner>,
    /// Platform protocols the binary serves besides its operations, reported by `Inspect`.
    protocols: Vec<String>,
}

/// A read admitted against the existing current tenant installation. Retain this
/// guard until the authoritative read has finished so shutdown waits for it.
pub struct WorkloadReadAdmission {
    pub app_id: String,
    pub organization_id: Option<String>,
    pub grant_id: String,
    pub grant_revision: u64,
    pub configuration: Value,
    pub valid_until_ms: i64,
    _work: super::RuntimeWork,
}

/// One host write, signed with this deployment's runtime credential for the
/// exact request bytes. Send it once, through
/// [`HostResourceClient`](crate::resource::HostResourceClient).
pub struct RuntimeWrite {
    pub(crate) capability: HostCapability,
    pub(crate) request_id: String,
    pub(crate) body: Vec<u8>,
    pub(crate) credential: String,
}
impl RuntimeWrite {
    pub fn capability(&self) -> HostCapability {
        self.capability
    }
    pub fn request_id(&self) -> &str {
        &self.request_id
    }
    pub fn body(&self) -> &[u8] {
        &self.body
    }
}
impl std::fmt::Debug for RuntimeWrite {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RuntimeWrite")
            .field("capability", &self.capability)
            .field("request_id", &self.request_id)
            .finish_non_exhaustive()
    }
}

/// The runtime credential key from [`RUNTIME_KEY_ID_ENV`] and
/// [`RUNTIME_SIGNING_KEY_ENV`]: both or neither. Neither: `Ok(None)`, and the
/// deployment cannot publish on its own (deadlines, console changes).
pub fn runtime_signer_from_env() -> Result<Option<PluginRuntimeSigner>, Box<dyn std::error::Error>>
{
    runtime_signer_from(
        std::env::var(RUNTIME_KEY_ID_ENV).ok(),
        std::env::var(RUNTIME_SIGNING_KEY_ENV).ok(),
    )
}
fn runtime_signer_from(
    key_id: Option<String>,
    seed: Option<String>,
) -> Result<Option<PluginRuntimeSigner>, Box<dyn std::error::Error>> {
    use base64::{Engine, engine::general_purpose::STANDARD};
    let present = |v: &Option<String>| v.as_deref().is_some_and(|v| !v.trim().is_empty());
    match (present(&key_id), present(&seed)) {
        (false, false) => Ok(None),
        (true, true) => {
            let seed: [u8; 32] = STANDARD
                .decode(seed.unwrap_or_default().trim())
                .ok()
                .and_then(|v| v.try_into().ok())
                .ok_or_else(|| format!("{RUNTIME_SIGNING_KEY_ENV} is invalid"))?;
            Ok(Some(
                PluginRuntimeSigner::new(key_id.unwrap_or_default().trim().to_owned(), seed)
                    .map_err(|_| format!("{RUNTIME_KEY_ID_ENV} is invalid"))?,
            ))
        }
        _ => Err(format!("{RUNTIME_KEY_ID_ENV} and {RUNTIME_SIGNING_KEY_ENV} go together").into()),
    }
}

/// Unique across calls and replicas: time, a per-process random value and a
/// sequence number.
fn runtime_request_id() -> String {
    use std::hash::{BuildHasher, Hasher};
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQUENCE: AtomicU64 = AtomicU64::new(0);
    static PROCESS: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    let process = *PROCESS.get_or_init(|| {
        let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
        hasher.write_u32(std::process::id());
        hasher.finish()
    });
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default();
    format!(
        "runtime-{nanos:x}-{process:016x}-{:x}",
        SEQUENCE.fetch_add(1, Ordering::Relaxed)
    )
}
pub(crate) fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|t| t.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}
fn denied() -> Status {
    Status::permission_denied("plugin runtime authorization mismatch")
}
const PROJECTION_STALE_MESSAGE: &str = "plugin projection not current";
/// A ticket (user, staff or platform system) this deployment cannot check against the projection
/// Core signed it under: another grant revision or configuration digest, no tenant state yet, a
/// lapsed lease or a disabled tenant here, the other side of a version switch (plugin, version,
/// manifest digest or deployment differ), or a signing key not given to this deployment yet.
/// `Unavailable`: the projections converge, so Core treats the call as not run and retryable (a
/// callback is delivered again, a user or staff call answers "outcome unknown"). A plugin's own
/// rejection is never `Unavailable`, and a ticket that does not match its call stays
/// `PermissionDenied`.
fn projection_stale() -> Status {
    Status::unavailable(PROJECTION_STALE_MESSAGE)
}
impl PluginRuntimeHost {
    /// Standalone plugin composition. Public keys and signed release artifacts
    /// are operator-provisioned; no Cell or platform private key is loaded.
    pub async fn from_env(
        validate: Arc<ConfigurationValidator>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::from_env_with(|_| Ok((validate, ())))
            .await
            .map(|(host, ())| host)
    }
    /// Build optional execution machinery from the same verified manifest bytes
    /// used by the host. Avoids unsigned re-reads and partially initialized hosts.
    pub async fn from_env_with<T>(
        build: impl FnOnce(
            &VerifiedManifest,
        )
            -> Result<(Arc<ConfigurationValidator>, T), Box<dyn std::error::Error>>,
    ) -> Result<(Self, T), Box<dyn std::error::Error>> {
        use base64::{Engine, engine::general_purpose::STANDARD};
        let path = std::env::var("FLARE_PLUGIN_MANIFEST_PATH")?;
        let limit = flare_plugin_protocol::manifest::MAX_MANIFEST_BYTES as u64;
        if std::fs::metadata(&path)?.len() > limit {
            return Err("manifest exceeds limit".into());
        }
        use std::io::Read;
        let mut raw = Vec::new();
        std::fs::File::open(path)?
            .take(limit + 1)
            .read_to_end(&mut raw)?;
        let mut trust = flare_plugin_protocol::signature::PublisherTrustStore::default();
        let key_id = std::env::var("FLARE_PLUGIN_PUBLISHER_KEY_ID")?;
        let publisher = std::env::var("FLARE_PLUGIN_PUBLISHER_ID")?;
        let public: [u8; 32] = STANDARD
            .decode(std::env::var("FLARE_PLUGIN_PUBLISHER_PUBLIC_KEY")?)?
            .try_into()
            .map_err(|_| "invalid publisher public key")?;
        trust.insert(key_id.clone(), publisher, public)?;
        let manifest = trust.verify(
            &raw,
            &key_id,
            &std::env::var("FLARE_PLUGIN_MANIFEST_SIGNATURE")?,
        )?;
        let (validate, attachment) = build(&manifest)?;
        let artifact = manifest
            .manifest()
            .artifact
            .as_ref()
            .ok_or("artifact required")?
            .digest
            .clone();
        let deployment = PluginDeployment {
            deployment_id: std::env::var("FLARE_PLUGIN_DEPLOYMENT_ID")?,
            plugin_id: manifest.manifest().plugin_id.clone(),
            plugin_version: manifest.manifest().version.clone(),
            manifest_digest: manifest.digest().into(),
            artifact_digest: artifact,
            grpc_authority: std::env::var("FLARE_PLUGIN_GRPC_AUTHORITY")?,
        };
        let raw_keys = std::env::var("FLARE_PLUGIN_CALL_PUBLIC_KEYS")?;
        let calls = PluginCallVerifier::from_public_keys_json(&raw_keys)?;
        let runtime_signer = runtime_signer_from_env()?;
        let host = Self::new(
            manifest,
            deployment,
            calls,
            validate,
            Arc::new(
                super::state_store::PostgresTenantStateStore::connect(&std::env::var(
                    "FLARE_PLUGIN_RUNTIME_DATABASE_URL",
                )?)
                .await?,
            ),
        )?;
        let host = match runtime_signer {
            Some(signer) => host.with_runtime_signer(signer)?,
            None => host,
        };
        Ok((host, attachment))
    }

    pub fn new(
        manifest: VerifiedManifest,
        deployment: PluginDeployment,
        calls: PluginCallVerifier,
        validate: Arc<ConfigurationValidator>,
        states: Arc<dyn super::state_store::TenantStateStore>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        deployment.validate(manifest.manifest(), manifest.digest())?;
        let admin_operations = manifest.manifest().admin_operations();
        let admin_permissions = admin_operations
            .iter()
            .filter_map(|operation| {
                manifest
                    .manifest()
                    .admin_permission_for(operation)
                    .map(|code| (operation.clone(), code))
            })
            .collect();
        let organization_operations = manifest
            .manifest()
            .admin
            .as_ref()
            .map(|a| {
                a.operations
                    .iter()
                    .filter(|op| {
                        op.scope == flare_plugin_protocol::admin::AdminOperationScope::Organization
                    })
                    .map(|op| op.id.clone())
                    .collect()
            })
            .unwrap_or_default();
        let reason_required = manifest
            .manifest()
            .admin
            .as_ref()
            .map(|admin| {
                admin
                    .operations
                    .iter()
                    .filter(|op| op.requires_reason)
                    .map(|op| op.id.clone())
                    .collect()
            })
            .unwrap_or_default();
        let lifecycle_operations = manifest
            .manifest()
            .client_extensions
            .contributions
            .iter()
            .filter_map(|c| c.entity_lifecycle.as_ref().map(|l| l.unbound.clone()))
            .collect();
        Ok(Self(Arc::new(Inner {
            admin_operations,
            admin_permissions,
            organization_operations,
            reason_required,
            lifecycle_operations,
            runtime_signer: None,
            protocols: Vec::new(),
            manifest,
            deployment,
            calls,
            validate,
            states,
            lifecycle: Arc::new(super::lifecycle::Lifecycle::default()),
            validation_slots: Arc::new(tokio::sync::Semaphore::new(4)),
        })))
    }
    /// Declare that the binary serves the data export protocol
    /// ([`crate::protocol::data::OPERATION`], verified with [`Self::verify_data_request`]): `Inspect`
    /// reports it, and the platform enables a version whose manifest promises data export
    /// (`admin.lifecycle.uninstall.export`) only on a deployment that does. Call before the host
    /// is shared.
    pub fn with_data_protocol(mut self) -> Result<Self, Box<dyn std::error::Error>> {
        let inner = Arc::get_mut(&mut self.0).ok_or("runtime host is already shared")?;
        let protocol = crate::protocol::data::OPERATION.to_owned();
        if !inner.protocols.contains(&protocol) {
            inner.protocols.push(protocol);
        }
        Ok(self)
    }
    /// Attach this deployment's runtime credential key (see
    /// [`runtime_signer_from_env`]) before the host is shared.
    pub fn with_runtime_signer(
        mut self,
        signer: PluginRuntimeSigner,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Arc::get_mut(&mut self.0)
            .ok_or("runtime host is already shared")?
            .runtime_signer = Some(signer);
        Ok(self)
    }
    /// Whether host writes without a call in flight are possible.
    pub fn signs_runtime_writes(&self) -> bool {
        self.0.runtime_signer.is_some()
    }
    pub fn manifest(&self) -> &flare_plugin_protocol::PluginManifest {
        self.0.manifest.manifest()
    }
    pub fn deployment_id(&self) -> &str {
        &self.0.deployment.deployment_id
    }
    /// Resolve a background job's stored grant in this deployment. An upgraded
    /// deployment may settle an old business obligation under its own current
    /// operator-projected grant. This does not change user-call admission or
    /// mint an installation: Core verifies the selected grant and the wallet
    /// restricts recovery to the original business reference.
    pub async fn current_grant_id(
        &self,
        tenant: &str,
        preferred: &str,
        recovery: bool,
    ) -> Result<String, Status> {
        let deployment = self.deployment_id();
        let now = now_ms();
        let usable = |s: &PluginTenantState| s.valid_until_ms > now && (s.enabled || recovery);
        if let Some(state) = self
            .0
            .states
            .get(deployment, tenant, preferred)
            .await?
            .filter(usable)
        {
            return Ok(state.grant_id);
        }
        self.0
            .states
            .current(deployment, tenant, now, recovery)
            .await?
            .filter(usable)
            .map(|s| s.grant_id)
            .ok_or_else(denied)
    }
    /// Sign one host write of `tenant_id` under its current grant, for no user
    /// or staff call in flight (a deadline passing, a console change). The
    /// grant must be enabled and unexpired here; Core checks it again against
    /// its own projection and the key registered for this deployment.
    pub async fn sign_runtime_write(
        &self,
        tenant_id: &str,
        grant_id: &str,
        request: &ResourceWriteRequest,
    ) -> Result<RuntimeWrite, Status> {
        let signer = self
            .0
            .runtime_signer
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("runtime credential not configured"))?;
        request
            .validate()
            .map_err(|_| Status::invalid_argument("invalid host write request"))?;
        let body = serde_json::to_vec(request)
            .map_err(|_| Status::invalid_argument("invalid host write request"))?;
        let limit = if matches!(request, ResourceWriteRequest::ServiceDispatch(_)) {
            flare_plugin_protocol::service::MAX_SERVICE_REQUEST_BYTES
        } else {
            MAX_RESOURCE_REQUEST_BYTES
        };
        if body.len() > limit {
            return Err(Status::resource_exhausted("host write request limit"));
        }
        let d = &self.0.deployment;
        let now = now_ms();
        let state = self
            .0
            .states
            .get(&d.deployment_id, tenant_id, grant_id)
            .await?
            .filter(|s| (s.enabled || matches!(request,ResourceWriteRequest::ServiceDispatch(r) if r.mode==flare_plugin_protocol::service::ServiceMode::Recovery)) && s.valid_until_ms > now)
            .ok_or_else(denied)?;
        let claims = PluginRuntimeClaims {
            tenant_id: tenant_id.into(),
            plugin_id: d.plugin_id.clone(),
            plugin_version: d.plugin_version.clone(),
            manifest_digest: d.manifest_digest.clone(),
            deployment_id: d.deployment_id.clone(),
            grant_id: grant_id.into(),
            grant_revision: state.revision,
            capability: request.capability(),
            request_id: runtime_request_id(),
            request_digest: digest_bytes(&body),
            issued_at_ms: now,
            expires_at_ms: (now + RUNTIME_CREDENTIAL_TTL_MS.min(MAX_RUNTIME_CALL_TTL_MS))
                .min(state.valid_until_ms),
        };
        let credential = signer.sign(&claims, now).map_err(|_| denied())?;
        Ok(RuntimeWrite {
            capability: claims.capability,
            request_id: claims.request_id,
            body,
            credential,
        })
    }
    pub fn manifest_digest(&self) -> &str {
        self.0.manifest.digest()
    }

    /// Draining is local to this process. Shared tenant revisions fence every replica.
    pub fn set_draining(&self, value: bool) {
        self.0.lifecycle.set_draining(value);
    }
    pub fn in_flight(&self) -> usize {
        self.0.lifecycle.in_flight()
    }
    pub(crate) fn lifecycle(&self) -> Arc<super::lifecycle::Lifecycle> {
        self.0.lifecycle.clone()
    }
    /// Only process admission; the caller must independently authenticate and
    /// authorize provider/background work before any business side effect.
    pub fn admit_work(&self) -> Result<super::RuntimeWork, Status> {
        Ok(super::RuntimeWork {
            _admission: self.0.lifecycle.admit()?,
        })
    }
    pub async fn drain(&self, deadline: std::time::Duration) -> Result<(), Status> {
        self.0.lifecycle.drain(deadline).await
    }
    /// Core's data ticket, bound to the exact request. User, staff, system and
    /// ApplyTenant tickets cannot authorize a data export or download.
    pub async fn verify_data_request(
        &self,
        request: &Request<GenericRequest>,
    ) -> Result<crate::protocol::data::DataRequest, Status> {
        use crate::protocol::data::{DataRequest, OPERATION};
        if request.get_ref().operation != OPERATION {
            return Err(Status::permission_denied("data purpose required"));
        }
        let payload = request
            .get_ref()
            .payload
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("data request required"))?;
        if payload.value.len() > crate::protocol::data::MAX_REQUEST_BYTES {
            return Err(Status::resource_exhausted("data request limit"));
        }
        let data: DataRequest = serde_json::from_slice(&payload.value)
            .map_err(|_| Status::invalid_argument("invalid data request"))?;
        let digest = data
            .digest()
            .map_err(|_| Status::invalid_argument("invalid data request"))?;
        let mut headers = request.metadata().get_all(PLUGIN_CALL_HEADER).iter();
        let token = headers
            .next()
            .and_then(|h| h.to_str().ok())
            .ok_or_else(|| Status::unauthenticated("data ticket required"))?;
        if headers.next().is_some() {
            return Err(Status::unauthenticated("one data ticket required"));
        }
        if ticket_key_id(token).is_some_and(|id| !self.0.calls.has_key(&id)) {
            return Err(projection_stale());
        }
        let now = now_ms();
        let claims = self
            .0
            .calls
            .verify_data(token, now)
            .map_err(|_| Status::unauthenticated("invalid data ticket"))?;
        if claims.request_digest != digest
            || claims.tenant_id != data.app_id
            || claims.deployment_id != data.deployment_id
            || claims.manifest_digest != data.manifest_digest
            || claims.grant_id != data.grant_id
            || claims.grant_revision != data.grant_revision
            || data.deployment_id != self.0.deployment.deployment_id
            || data.manifest_digest != self.0.deployment.manifest_digest
            || request.get_ref().request_id != data.job_id
        {
            return Err(Status::permission_denied("data authority mismatch"));
        }
        let state = self
            .0
            .states
            .get(&data.deployment_id, &data.app_id, &data.grant_id)
            .await?
            .ok_or_else(|| Status::permission_denied("data grant missing"))?;
        // Disabled tenants may export; revocation/configuration changes advance
        // the revision and invalidate any previously issued download request.
        if state.enabled
            && !matches!(
                data.action,
                crate::protocol::data::DataAction::Export
                    | crate::protocol::data::DataAction::Read { .. }
            )
        {
            return Err(Status::failed_precondition(
                "disable tenant plugin before maintenance",
            ));
        }
        if state.revision != data.grant_revision || state.valid_until_ms <= now {
            return Err(Status::permission_denied("data grant expired or changed"));
        }
        Ok(data)
    }

    /// Admit an independently authenticated workload read against the same
    /// current installation used by plugin invocation. The workload cannot pick
    /// a historical grant or supply its own revision/configuration. Callers must
    /// authenticate their source, request scope and read operation first.
    pub async fn admit_workload_read(
        &self,
        app: &str,
        operation: &str,
    ) -> Result<WorkloadReadAdmission, Status> {
        let work = self.admit_work()?;
        let declared = self
            .0
            .manifest
            .manifest()
            .operation(operation)
            .ok_or_else(denied)?;
        let now = now_ms();
        let state = self
            .0
            .states
            .current(&self.0.deployment.deployment_id, app, now, false)
            .await?
            .ok_or_else(denied)?;
        if state.app_id != app
            || state.deployment_id != self.0.deployment.deployment_id
            || state.manifest_digest != self.0.deployment.manifest_digest
            || !state.enabled
            || state.revision == 0
            || state.valid_until_ms <= now
            || !state
                .approved_operations
                .iter()
                .any(|approved| approved == operation)
            || !declared
                .permission_ids
                .iter()
                .all(|permission| state.permissions.contains(permission))
            || self
                .0
                .manifest
                .manifest()
                .permissions
                .iter()
                .filter(|permission| declared.permission_ids.contains(&permission.id))
                .any(|permission| {
                    permission
                        .data_scopes
                        .iter()
                        .any(|scope| !state.data_scopes.contains(scope))
                })
        {
            return Err(denied());
        }
        let configuration: Value =
            serde_json::from_slice(&state.configuration_json).map_err(|_| denied())?;
        if configuration_digest(&configuration).map_err(|_| denied())? != state.configuration_digest
        {
            return Err(denied());
        }
        Ok(WorkloadReadAdmission {
            organization_id: (!state.organization_id.is_empty())
                .then(|| state.organization_id.clone()),
            app_id: state.app_id,
            grant_id: state.grant_id,
            grant_revision: state.revision,
            configuration,
            valid_until_ms: state.valid_until_ms,
            _work: work,
        })
    }

    pub async fn configuration(&self, tenant: &str, grant: &str) -> Result<Value, Status> {
        let state = self
            .0
            .states
            .get(&self.0.deployment.deployment_id, tenant, grant)
            .await?
            .filter(|s| s.valid_until_ms > now_ms() && s.enabled)
            .ok_or_else(denied)?;
        serde_json::from_slice(&state.configuration_json).map_err(|_| denied())
    }
    pub(crate) async fn verify_call(
        &self,
        request: &Request<GenericRequest>,
    ) -> Result<VerifiedPluginUser, Status> {
        if self.0.lifecycle.is_draining() {
            return Err(Status::unavailable("plugin draining"));
        }
        let mut values = request.metadata().get_all(PLUGIN_CALL_HEADER).iter();
        let token = values
            .next()
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| Status::unauthenticated("plugin invocation ticket required"))?;
        if values.next().is_some() {
            return Err(Status::unauthenticated(
                "one plugin invocation ticket required",
            ));
        }
        let now = now_ms();
        let claims = match self.0.calls.verify(token, now) {
            Ok(claims) => claims,
            // Signed under a key this deployment has not been given yet: Core rotated first.
            Err(_) if ticket_key_id(token).is_some_and(|id| !self.0.calls.has_key(&id)) => {
                return Err(projection_stale());
            }
            Err(_) => return Err(Status::unauthenticated("invalid plugin invocation ticket")),
        };
        let d = &self.0.deployment;
        let body = request.get_ref();
        // Core's projection names another deployment (a version switch in flight).
        if claims.plugin_id != d.plugin_id
            || claims.plugin_version != d.plugin_version
            || claims.manifest_digest != d.manifest_digest
            || claims.deployment_id != d.deployment_id
        {
            return Err(projection_stale());
        }
        if claims.operation != body.operation
            || claims.request_id != body.request_id
            || self
                .0
                .manifest
                .manifest()
                .operation(&claims.operation)
                .is_none()
            || self
                .0
                .manifest
                .manifest()
                .service_operation_ids()
                .contains(&claims.operation)
            || self.0.admin_operations.contains(&claims.operation)
            || self.0.lifecycle_operations.contains(&claims.operation)
            || self
                .manifest()
                .event_subscriptions
                .iter()
                .any(|s| s.operation == claims.operation)
        {
            return Err(denied());
        }
        if body
            .metadata
            .get("tenant_id")
            .is_some_and(|v| v != &claims.tenant_id)
            || body
                .metadata
                .get("user_id")
                .is_some_and(|v| v != &claims.actor_id)
        {
            return Err(denied());
        }
        // The tenant state this deployment holds is not the one Core signed under (yet).
        let state = self
            .0
            .states
            .get(&d.deployment_id, &claims.tenant_id, &claims.grant_id)
            .await?
            .ok_or_else(projection_stale)?;
        if !state.enabled
            || state.revision != claims.grant_revision
            || state.configuration_digest != claims.configuration_digest
            || state.valid_until_ms <= now
            || claims.expires_at_ms > state.valid_until_ms
        {
            return Err(projection_stale());
        }
        Ok(VerifiedPluginUser {
            organization_id: (!state.organization_id.is_empty())
                .then(|| state.organization_id.clone()),
            tenant_id: claims.tenant_id,
            user_id: claims.actor_id,
            request_id: claims.request_id,
            grant_id: claims.grant_id,
            grant_revision: claims.grant_revision,
            conversation_id: claims.conversation_id,
            expires_at_ms: claims.expires_at_ms,
            delegation: token.into(),
            configuration: serde_json::from_slice(&state.configuration_json)
                .map_err(|_| denied())?,
            _admission: self.0.lifecycle.admit()?,
        })
    }

    pub(crate) async fn verify_service(
        &self,
        request: &Request<GenericRequest>,
    ) -> Result<crate::service::VerifiedPluginService, Status> {
        use flare_plugin_protocol::service::*;
        let work = self.admit_work()?;
        if request.metadata().contains_key(PLUGIN_CALL_HEADER)
            || request.metadata().contains_key(PLUGIN_RUNTIME_HEADER)
            || request.metadata().contains_key("authorization")
            || !request.get_ref().metadata.is_empty()
        {
            return Err(denied());
        }
        let mut values = request.metadata().get_all(SERVICE_HEADER).iter();
        let token = values
            .next()
            .and_then(|s| s.to_str().ok())
            .ok_or_else(denied)?;
        if values.next().is_some() {
            return Err(denied());
        }
        let now = now_ms();
        let claims = self
            .0
            .calls
            .verify_service(token, now)
            .map_err(|_| denied())?;
        let d = &self.0.deployment;
        let c = &claims.target;
        let body = request.get_ref();
        if c.plugin_id != d.plugin_id
            || c.version != d.plugin_version
            || c.deployment_id != d.deployment_id
            || c.manifest_digest != d.manifest_digest
            || claims.operation != body.operation
            || claims.request_id != body.request_id
        {
            return Err(denied());
        }
        let input: Value = serde_json::from_slice(
            body.payload
                .as_ref()
                .map(|p| p.value.as_slice())
                .unwrap_or(b"{}"),
        )
        .map_err(|_| denied())?;
        claims.matches_input(&input).map_err(|_| denied())?;
        if !self
            .manifest()
            .service_operations
            .iter()
            .any(|s| s.operation == claims.operation && s.mode == claims.mode)
        {
            return Err(denied());
        }
        let state = self
            .0
            .states
            .get(&d.deployment_id, &claims.tenant_id, &c.grant_id)
            .await?
            .ok_or_else(projection_stale)?;
        if (!state.enabled && claims.mode != ServiceMode::Recovery)
            || state.revision != c.grant_revision
            || state.configuration_digest != c.configuration_digest
            || state.valid_until_ms <= now
            || claims.expires_at_ms > state.valid_until_ms
        {
            return Err(projection_stale());
        }
        Ok(crate::service::VerifiedPluginService {
            organization_id: (!state.organization_id.is_empty())
                .then(|| state.organization_id.clone()),
            claims,
            _admission: work,
        })
    }
    pub(crate) async fn verify_staff(
        &self,
        request: &Request<GenericRequest>,
    ) -> Result<VerifiedPluginStaff, Status> {
        if self.0.lifecycle.is_draining() {
            return Err(Status::unavailable("plugin draining"));
        }
        let mut values = request.metadata().get_all(PLUGIN_CALL_HEADER).iter();
        let token = values
            .next()
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| Status::unauthenticated("plugin staff ticket required"))?;
        if values.next().is_some() {
            return Err(Status::unauthenticated("one plugin staff ticket required"));
        }
        let now = now_ms();
        let claims = match self.0.calls.verify_staff(token, now) {
            Ok(claims) => claims,
            Err(_) if ticket_key_id(token).is_some_and(|id| !self.0.calls.has_key(&id)) => {
                return Err(projection_stale());
            }
            Err(_) => return Err(Status::unauthenticated("invalid plugin staff ticket")),
        };
        let d = &self.0.deployment;
        let body = request.get_ref();
        if claims.plugin_id != d.plugin_id
            || claims.plugin_version != d.plugin_version
            || claims.manifest_digest != d.manifest_digest
            || claims.deployment_id != d.deployment_id
        {
            return Err(projection_stale());
        }
        if claims.operation != body.operation
            || claims.request_id != body.request_id
            || !self.0.admin_operations.contains(&claims.operation)
            // The staff member holds the permission this operation declares, not another one
            // of the plugin's (a read permission never reaches a manage operation).
            || self.0.admin_permissions.get(&claims.operation) != Some(&claims.permission)
            // A change the manifest asks a reason for comes with the one the staff member stated.
            || (self.0.reason_required.contains(&claims.operation) && claims.reason.is_none())
            || self.0.lifecycle_operations.contains(&claims.operation)
            || body
                .metadata
                .get("tenant_id")
                .is_some_and(|v| v != &claims.tenant_id)
        {
            return Err(denied());
        }
        let state = self
            .0
            .states
            .get(&d.deployment_id, &claims.tenant_id, &claims.grant_id)
            .await?
            .ok_or_else(projection_stale)?;
        if !state.enabled
            || state.revision != claims.grant_revision
            || state.configuration_digest != claims.configuration_digest
            || state.valid_until_ms <= now
            || claims.expires_at_ms > state.valid_until_ms
        {
            return Err(projection_stale());
        }
        let organization_call = claims.scope.organization_id();
        if self.0.organization_operations.contains(&claims.operation) != organization_call.is_some()
            || organization_call.is_some_and(|org| state.organization_id != org)
            || claims.org_id.as_deref().is_some_and(|org| {
                !state.organization_id.is_empty() && org != state.organization_id
            })
        {
            return Err(denied());
        }
        Ok(VerifiedPluginStaff {
            organization_id: (!state.organization_id.is_empty())
                .then(|| state.organization_id.clone()),
            scope: claims.scope,
            tenant_id: claims.tenant_id,
            staff_id: claims.staff_id,
            org_id: claims.org_id,
            permission: claims.permission,
            operation: claims.operation,
            request_id: claims.request_id,
            reason: claims.reason,
            grant_id: claims.grant_id,
            grant_revision: claims.grant_revision,
            expires_at_ms: claims.expires_at_ms,
            configuration: serde_json::from_slice(&state.configuration_json)
                .map_err(|_| denied())?,
            _admission: self.0.lifecycle.admit()?,
        })
    }
    /// The platform system ticket of a lifecycle callback (Core telling the plugin that the card
    /// of one of its entities was recalled or never posted). Same deployment, grant revision,
    /// configuration and enablement checks as a user ticket; the operation must be one of the
    /// manifest's `entityLifecycle.unbound` operations, and the input's `entity` and `reason`
    /// must be the ticket's. There is no user and no conversation.
    ///
    /// Refusals Core tells apart: a projection that does not match the ticket (see
    /// [`projection_stale`]) is `Unavailable`, so Core delivers the callback again; a ticket that
    /// does not verify under a key this deployment holds is `Unauthenticated`, and one that does
    /// not match its call is `PermissionDenied`.
    pub(crate) async fn verify_system(
        &self,
        request: &Request<GenericRequest>,
    ) -> Result<VerifiedPluginSystem, Status> {
        if self.0.lifecycle.is_draining() {
            return Err(Status::unavailable("plugin draining"));
        }
        let mut values = request.metadata().get_all(PLUGIN_CALL_HEADER).iter();
        let token = values
            .next()
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| Status::unauthenticated("plugin system ticket required"))?;
        if values.next().is_some() {
            return Err(Status::unauthenticated("one plugin system ticket required"));
        }
        let now = now_ms();
        let claims = match self.0.calls.verify_system(token, now) {
            Ok(claims) => claims,
            // Signed under a key this deployment has not been given yet: Core rotated first.
            Err(_) if ticket_key_id(token).is_some_and(|id| !self.0.calls.has_key(&id)) => {
                return Err(projection_stale());
            }
            Err(_) => return Err(Status::unauthenticated("invalid plugin system ticket")),
        };
        let d = &self.0.deployment;
        let body = request.get_ref();
        // Core's projection names another deployment (a version switch in flight).
        if claims.plugin_id != d.plugin_id
            || claims.plugin_version != d.plugin_version
            || claims.manifest_digest != d.manifest_digest
            || claims.deployment_id != d.deployment_id
        {
            return Err(projection_stale());
        }
        let input: Option<Value> = body
            .payload
            .as_ref()
            .and_then(|payload| serde_json::from_slice(&payload.value).ok());
        let names_ticket_entity = input.as_ref().is_some_and(|input| {
            input.get("entity").and_then(Value::as_str) == Some(claims.entity.as_str())
                && input.get("reason").and_then(Value::as_str) == Some(claims.reason.as_str())
        });
        if claims.operation != body.operation
            || claims.request_id != body.request_id
            || !self.0.lifecycle_operations.contains(&claims.operation)
            || !names_ticket_entity
            || body
                .metadata
                .get("tenant_id")
                .is_some_and(|v| v != &claims.tenant_id)
            || body.metadata.contains_key("user_id")
        {
            return Err(denied());
        }
        // The tenant state this deployment holds is not the one Core signed under (yet).
        let state = self
            .0
            .states
            .get(&d.deployment_id, &claims.tenant_id, &claims.grant_id)
            .await?
            .ok_or_else(projection_stale)?;
        if !state.enabled
            || state.revision != claims.grant_revision
            || state.configuration_digest != claims.configuration_digest
            || state.valid_until_ms <= now
            || claims.expires_at_ms > state.valid_until_ms
        {
            return Err(projection_stale());
        }
        Ok(VerifiedPluginSystem {
            tenant_id: claims.tenant_id,
            operation: claims.operation,
            entity: claims.entity,
            reason: claims.reason,
            request_id: claims.request_id,
            grant_id: claims.grant_id,
            grant_revision: claims.grant_revision,
            expires_at_ms: claims.expires_at_ms,
            configuration: serde_json::from_slice(&state.configuration_json)
                .map_err(|_| denied())?,
            _admission: self.0.lifecycle.admit()?,
        })
    }
    /// The state must be exactly the one Core's ticket names. A ticket under a key this deployment
    /// has not been given yet is not a refusal: Core rotated first, and retries.
    fn authenticate(&self, request: &Request<PluginTenantState>) -> Result<(), Status> {
        let mut values = request.metadata().get_all(PLUGIN_CALL_HEADER).iter();
        let token = values
            .next()
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| Status::unauthenticated("tenant state ticket required"))?;
        if values.next().is_some() {
            return Err(Status::unauthenticated("one tenant state ticket required"));
        }
        if ticket_key_id(token).is_some_and(|id| !self.0.calls.has_key(&id)) {
            return Err(projection_stale());
        }
        let claims = self
            .0
            .calls
            .verify_tenant_state(token, now_ms())
            .map_err(|_| Status::unauthenticated("invalid tenant state ticket"))?;
        let state = request.get_ref();
        if claims.tenant_id != state.app_id
            || claims.organization_id != state.organization_id
            || claims.deployment_id != state.deployment_id
            || claims.deployment_id != self.0.deployment.deployment_id
            || claims.manifest_digest != state.manifest_digest
            || claims.grant_id != state.grant_id
            || claims.revision != state.revision
            || claims.configuration_digest != state.configuration_digest
            || claims.enabled != state.enabled
        {
            return Err(denied());
        }
        Ok(())
    }
}
#[tonic::async_trait]
impl PluginRuntime for PluginRuntimeHost {
    async fn inspect(
        &self,
        _: Request<PluginRuntimeInspectRequest>,
    ) -> Result<Response<PluginRuntimeInspectResponse>, Status> {
        let d = &self.0.deployment;
        Ok(Response::new(PluginRuntimeInspectResponse {
            deployment_id: d.deployment_id.clone(),
            plugin_id: d.plugin_id.clone(),
            plugin_version: d.plugin_version.clone(),
            manifest_digest: d.manifest_digest.clone(),
            artifact_digest: d.artifact_digest.clone(),
            ready: self.0.states.healthy().await.is_ok(),
            draining: self.0.lifecycle.is_draining(),
            protocols: self.0.protocols.clone(),
        }))
    }
    async fn apply_tenant(
        &self,
        request: Request<PluginTenantState>,
    ) -> Result<Response<PluginTenantStateAck>, Status> {
        self.authenticate(&request)?;
        let state = request.into_inner();
        let now = now_ms();
        if state.organization_id.is_empty()
            || state.organization_id.len() > 128
            || !state
                .organization_id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b))
            || state.app_id.is_empty()
            || state.app_id.len() > 32
            || !state
                .app_id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
            || state.deployment_id != self.0.deployment.deployment_id
            || state.manifest_digest != self.0.deployment.manifest_digest
            || state.grant_id.is_empty()
            || state.grant_id.len() > 128
            || state.revision == 0
            || state.revision > i64::MAX as u64
            || state.valid_until_ms <= now
            || state.valid_until_ms > now + MAX_PROJECTION_LEASE_MS
            || state.configuration_json.len() > 16384
        {
            return Err(denied());
        }
        let config: Value = serde_json::from_slice(&state.configuration_json)
            .map_err(|_| Status::invalid_argument("invalid plugin configuration"))?;
        if configuration_digest(&config).map_err(|_| denied())? != state.configuration_digest {
            return Err(denied());
        }
        // Normal preparation validates before installation; emergency deny can bypass.
        if state.enabled || state.validate_configuration {
            let permit = self
                .0
                .validation_slots
                .clone()
                .try_acquire_owned()
                .map_err(|_| {
                    Status::resource_exhausted("configuration validation capacity exhausted")
                })?;
            let admission = self.admit_work()?;
            let validate = self.0.validate.clone();
            tokio::time::timeout(
                std::time::Duration::from_secs(2),
                tokio::task::spawn_blocking(move || {
                    let _permit = permit;
                    let _admission = admission;
                    validate(&config)
                }),
            )
            .await
            .map_err(|_| Status::deadline_exceeded("configuration validation deadline"))?
            .map_err(|_| Status::internal("configuration validator failed"))??;
        }
        let ack = PluginTenantStateAck {
            revision: state.revision,
            configuration_digest: state.configuration_digest.clone(),
            enabled: state.enabled,
        };
        self.0.states.apply(state).await?;
        Ok(Response::new(ack))
    }
}
