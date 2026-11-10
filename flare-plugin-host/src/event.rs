//! A Core-authenticated asynchronous event. Construction is private to the verifier.
use crate::{PluginRuntimeHost, protocol::event::PluginMessageEvent};
use flare_grpc_proto::capability::GenericRequest;
use tonic::{Request, Status};

#[derive(Clone)]
pub struct VerifiedPluginEvent {
    pub(crate) event: PluginMessageEvent,
    pub(crate) grant_id: String,
    pub(crate) grant_revision: u64,
    pub(crate) expires_at_ms: i64,
    pub(crate) active_since_ms: i64,
    pub(crate) delegation: std::sync::Arc<str>,
    pub(crate) configuration: serde_json::Value,
    pub(crate) allowed_operations: std::collections::BTreeSet<String>,
    pub(crate) _admission: std::sync::Arc<crate::lifecycle::Admission>,
}
impl VerifiedPluginEvent {
    pub fn event(&self) -> &PluginMessageEvent {
        &self.event
    }
    pub fn tenant_id(&self) -> &str {
        &self.event.app
    }
    pub fn actor_id(&self) -> &str {
        &self.event.data.actor_id
    }
    pub fn conversation_id(&self) -> &str {
        &self.event.data.conversation_id
    }
    pub fn request_id(&self) -> &str {
        &self.event.id
    }
    pub fn grant_id(&self) -> &str {
        &self.grant_id
    }
    pub fn grant_revision(&self) -> u64 {
        self.grant_revision
    }
    pub fn expires_at_ms(&self) -> i64 {
        self.expires_at_ms
    }
    pub fn active_since_ms(&self) -> i64 {
        self.active_since_ms
    }
    pub fn configuration(&self) -> &serde_json::Value {
        &self.configuration
    }
    /// Whether the authenticated application projection permits this user operation.
    /// Event-driven business composition must still apply the same feature gates as
    /// the equivalent user operation. This grants no user ticket or write authority.
    pub fn operation_enabled(&self, operation: &str) -> bool {
        crate::runtime::now_ms() < self.expires_at_ms && self.allowed_operations.contains(operation)
    }
}
pub async fn verify_event_call(
    request: &Request<GenericRequest>,
    runtime: &PluginRuntimeHost,
) -> Result<VerifiedPluginEvent, Status> {
    runtime.verify_event_call(request).await
}
