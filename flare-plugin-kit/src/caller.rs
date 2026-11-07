//! Verified callers: a conversation member (user ticket) or tenant staff
//! (staff ticket). Each retains its process admission until dropped, and a
//! user caller is also the delegation for host resource reads.
use crate::KitError;
pub use flare_plugin_host::protocol::invocation::PluginAdminScope;
use flare_plugin_host::{VerifiedPluginStaff, VerifiedPluginUser};
use serde_json::Value;

fn valid_actor(id: &str) -> bool {
    flare_plugin_host::protocol::resource::valid_resource_id(id)
}

/// A conversation member's admitted call.
pub struct UserCaller {
    tenant: String,
    organization_id: Option<String>,
    user: String,
    request_id: String,
    grant_id: String,
    grant_revision: u64,
    /// The conversation the Core ticket is bound to, never a payload value.
    conversation: Option<String>,
    configuration: Value,
    pub(crate) admission: Option<VerifiedPluginUser>,
}
impl UserCaller {
    pub fn admitted(identity: VerifiedPluginUser) -> Result<Self, KitError> {
        if !valid_actor(&identity.user_id) {
            return Err(KitError::Denied);
        }
        Ok(Self {
            tenant: identity.tenant_id.clone(),
            organization_id: identity.organization_id.clone(),
            user: identity.user_id.clone(),
            request_id: identity.request_id.clone(),
            grant_id: identity.grant_id.clone(),
            grant_revision: identity.grant_revision,
            conversation: identity.conversation_id.clone(),
            configuration: identity.configuration.clone(),
            admission: Some(identity),
        })
    }
    /// A caller without a ticket, for tests: no host reads go out for it.
    #[cfg(any(test, feature = "testing"))]
    pub fn unverified(
        tenant: &str,
        user: &str,
        conversation: Option<&str>,
        request_id: &str,
        configuration: Value,
    ) -> Self {
        Self {
            tenant: tenant.into(),
            organization_id: None,
            user: user.into(),
            request_id: request_id.into(),
            grant_id: "grant-1".into(),
            grant_revision: 1,
            conversation: conversation.map(str::to_owned),
            configuration,
            admission: None,
        }
    }
    #[cfg(any(test, feature = "testing"))]
    pub fn with_test_organization(mut self, organization: &str) -> Self {
        self.organization_id = Some(organization.into());
        self
    }
    /// Borrow the immutable Host admission for trusted service delegation.
    pub fn invocation(&self) -> Result<&VerifiedPluginUser, KitError> {
        self.admission.as_ref().ok_or(KitError::ResourceUnavailable)
    }
    /// Catalog-projected owner of the application, never the employee membership.
    pub fn organization_id(&self) -> Option<&str> {
        self.organization_id.as_deref()
    }
    pub fn tenant(&self) -> &str {
        &self.tenant
    }
    pub fn user(&self) -> &str {
        &self.user
    }
    pub fn request_id(&self) -> &str {
        &self.request_id
    }
    pub fn grant_id(&self) -> &str {
        &self.grant_id
    }
    pub fn grant_revision(&self) -> u64 {
        self.grant_revision
    }
    pub fn conversation(&self) -> Option<&str> {
        self.conversation.as_deref()
    }
    /// The tenant configuration snapshot the call was admitted with.
    pub fn configuration(&self) -> &Value {
        &self.configuration
    }
}

/// A tenant staff member's admitted admin call. Tenant-scoped: no
/// conversation, and no host reads on the staff member's behalf.
pub struct StaffCaller {
    tenant: String,
    organization_id: Option<String>,
    staff: String,
    org: Option<String>,
    scope: PluginAdminScope,
    request_id: String,
    reason: Option<String>,
    grant_id: String,
    grant_revision: u64,
    configuration: Value,
    _admission: Option<VerifiedPluginStaff>,
}
impl StaffCaller {
    pub fn admitted(identity: VerifiedPluginStaff) -> Result<Self, KitError> {
        if identity.staff_id.is_empty() || identity.staff_id.len() > 128 {
            return Err(KitError::Denied);
        }
        Ok(Self {
            tenant: identity.tenant_id.clone(),
            organization_id: identity.organization_id.clone(),
            staff: identity.staff_id.clone(),
            org: identity.org_id.clone(),
            scope: identity.scope.clone(),
            request_id: identity.request_id.clone(),
            reason: identity.reason.clone(),
            grant_id: identity.grant_id.clone(),
            grant_revision: identity.grant_revision,
            configuration: identity.configuration.clone(),
            _admission: Some(identity),
        })
    }
    #[cfg(any(test, feature = "testing"))]
    pub fn unverified(tenant: &str, staff: &str, request_id: &str) -> Self {
        Self {
            tenant: tenant.into(),
            organization_id: None,
            staff: staff.into(),
            org: None,
            scope: PluginAdminScope::Application,
            request_id: request_id.into(),
            reason: None,
            grant_id: "grant-1".into(),
            grant_revision: 1,
            configuration: Value::Object(Default::default()),
            _admission: None,
        }
    }
    #[cfg(any(test, feature = "testing"))]
    pub fn with_test_scope(mut self, organization: &str, scope: PluginAdminScope) -> Self {
        self.organization_id = Some(organization.into());
        self.scope = scope;
        self
    }
    /// The same caller under another tenant configuration, for tests.
    #[cfg(any(test, feature = "testing"))]
    pub fn with_configuration(mut self, configuration: Value) -> Self {
        self.configuration = configuration;
        self
    }
    /// Catalog-projected owner of the application, never the employee membership.
    pub fn organization_id(&self) -> Option<&str> {
        self.organization_id.as_deref()
    }
    pub fn tenant(&self) -> &str {
        &self.tenant
    }
    pub fn staff(&self) -> &str {
        &self.staff
    }
    /// The actor id the plugin's records name (`closed_by`, audit events):
    /// `$staff:<id>` ([`crate::commands::staff_actor`]), the same name the
    /// command keys use, never one a member id can take.
    pub fn actor(&self) -> String {
        crate::commands::staff_actor(&self.staff)
    }
    pub fn admin_scope(&self) -> &PluginAdminScope {
        &self.scope
    }
    pub fn is_organization_scope(&self) -> bool {
        matches!(self.scope, PluginAdminScope::Organization { .. })
    }
    pub fn org(&self) -> Option<&str> {
        self.org.as_deref()
    }
    pub fn request_id(&self) -> &str {
        &self.request_id
    }
    /// The reason the staff member stated; always present for an operation
    /// declaring `requiresReason`.
    pub fn reason(&self) -> Option<&str> {
        self.reason.as_deref()
    }
    pub fn grant_id(&self) -> &str {
        &self.grant_id
    }
    pub fn grant_revision(&self) -> u64 {
        self.grant_revision
    }
    pub fn configuration(&self) -> &Value {
        &self.configuration
    }
}
