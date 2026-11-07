//! A plugin accepts only its dedicated invocation tickets: a user ticket for
//! the operations of its client contributions, a staff ticket for the
//! operations of its manifest's `admin` section, a platform system ticket for
//! its `entityLifecycle.unbound` operations.
use crate::runtime::PluginRuntimeHost;
use flare_grpc_proto::capability::GenericRequest;
use tonic::{Request, Status};
#[derive(Clone)]
pub struct VerifiedPluginUser {
    pub tenant_id: String,
    pub organization_id: Option<String>,
    pub user_id: String,
    pub request_id: String,
    pub grant_id: String,
    pub grant_revision: u64,
    pub conversation_id: Option<String>,
    pub expires_at_ms: i64,
    pub(crate) delegation: std::sync::Arc<str>,
    pub configuration: serde_json::Value,
    pub(crate) _admission: std::sync::Arc<crate::lifecycle::Admission>,
}
pub async fn verify_user_call(
    request: &Request<GenericRequest>,
    runtime: &PluginRuntimeHost,
) -> Result<VerifiedPluginUser, Status> {
    runtime.verify_call(request).await
}

impl std::fmt::Debug for VerifiedPluginUser {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VerifiedPluginUser")
            .field("tenant_id", &self.tenant_id)
            .field("user_id", &self.user_id)
            .field("request_id", &self.request_id)
            .field("grant_id", &self.grant_id)
            .field("grant_revision", &self.grant_revision)
            .finish_non_exhaustive()
    }
}

/// A tenant staff member invoking an admin operation (`admin.operations`) from
/// the console, through Core `dispatch_admin`. Admin work is tenant-scoped: there is no
/// conversation, and the entity is named in the input. The plugin still
/// applies its own rules to every operation.
#[derive(Clone)]
pub struct VerifiedPluginStaff {
    pub tenant_id: String,
    /// Immutable owner of the application, independently projected by Catalog.
    pub organization_id: Option<String>,
    pub scope: crate::protocol::invocation::PluginAdminScope,
    pub staff_id: String,
    /// The staff member's organization; `None` for platform staff.
    pub org_id: Option<String>,
    /// The permission code the manifest declares for `operation`
    /// ([`admin_permission_for`](crate::protocol::PluginManifest::admin_permission_for)):
    /// `plugin:<pluginId>:<permission>` of its `admin.operations` entry.
    pub permission: String,
    pub operation: String,
    pub request_id: String,
    /// The reason the staff member stated, for the plugin's own audit. Always
    /// present for an operation declaring `requiresReason`.
    pub reason: Option<String>,
    pub grant_id: String,
    pub grant_revision: u64,
    pub expires_at_ms: i64,
    pub configuration: serde_json::Value,
    pub(crate) _admission: std::sync::Arc<crate::lifecycle::Admission>,
}

/// Verify the `x-flare-plugin-call` staff ticket of an admin operation.
///
/// Deployment, plugin version, manifest digest, request id, grant revision,
/// configuration digest and enablement are checked exactly as for a user
/// ticket, the operation must be one of the manifest's
/// [`admin_operations`](crate::protocol::PluginManifest::admin_operations), and
/// the ticket's permission code must be the one the manifest declares for it
/// ([`admin_permission_for`](crate::protocol::PluginManifest::admin_permission_for)),
/// and an operation declaring `requiresReason` needs the ticket's `reason`.
/// A user ticket never verifies here, and [`verify_user_call`] refuses staff
/// tickets and admin operations.
pub async fn verify_staff_call(
    request: &Request<GenericRequest>,
    runtime: &PluginRuntimeHost,
) -> Result<VerifiedPluginStaff, Status> {
    runtime.verify_staff(request).await
}

impl std::fmt::Debug for VerifiedPluginStaff {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VerifiedPluginStaff")
            .field("tenant_id", &self.tenant_id)
            .field("staff_id", &self.staff_id)
            .field("operation", &self.operation)
            .field("request_id", &self.request_id)
            .field("grant_id", &self.grant_id)
            .field("grant_revision", &self.grant_revision)
            .finish_non_exhaustive()
    }
}

/// Core telling the plugin that the card of one of its entities went away
/// (`entityLifecycle.unbound`): `reason` is `recalled` (the bound card was
/// recalled) or `post_failed` (the platform could not post the card the plugin
/// returned). There is no user or conversation. Delivery is at least once with
/// a stable `request_id`: handle it idempotently per entity (cancel the poll,
/// refund the red packet).
#[derive(Clone)]
pub struct VerifiedPluginSystem {
    pub tenant_id: String,
    pub operation: String,
    pub entity: String,
    pub reason: String,
    pub request_id: String,
    pub grant_id: String,
    pub grant_revision: u64,
    pub expires_at_ms: i64,
    pub configuration: serde_json::Value,
    pub(crate) _admission: std::sync::Arc<crate::lifecycle::Admission>,
}

/// Verify the `x-flare-plugin-call` platform system ticket of a lifecycle
/// callback. Deployment, plugin version, manifest digest, request id, grant
/// revision, configuration digest and enablement are checked exactly as for a
/// user ticket; the operation must be one the manifest declares as
/// `entityLifecycle.unbound`, and the input's `entity` and `reason` must be the
/// ticket's. User and staff tickets never verify here, and [`verify_user_call`]
/// and [`verify_staff_call`] refuse lifecycle operations.
///
/// When this deployment's projection does not match the ticket (another grant
/// revision or configuration, no or a lapsed tenant state, a disabled tenant,
/// a version switch, a signing key it has not been given yet) the refusal is
/// `Unavailable`, and Core delivers the callback again later; return it as it
/// is. A ticket that does not verify is `Unauthenticated`, one that does not
/// match its call `PermissionDenied`.
pub async fn verify_system_call(
    request: &Request<GenericRequest>,
    runtime: &PluginRuntimeHost,
) -> Result<VerifiedPluginSystem, Status> {
    runtime.verify_system(request).await
}

impl std::fmt::Debug for VerifiedPluginSystem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VerifiedPluginSystem")
            .field("tenant_id", &self.tenant_id)
            .field("operation", &self.operation)
            .field("entity", &self.entity)
            .field("reason", &self.reason)
            .field("request_id", &self.request_id)
            .field("grant_revision", &self.grant_revision)
            .finish_non_exhaustive()
    }
}
