//! The tenant state this deployment holds, for work that outlives one call (a
//! media session, a background sweep). It tells an installation that is gone
//! or disabled (disabled, revoked and uninstalled look the same to a plugin)
//! from one whose revision moved (a configuration change) and from a lease
//! that lapsed while the platform reconciles.
use super::*;

/// One tenant grant as this deployment holds it now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TenantStatus {
    pub revision: u64,
    pub enabled: bool,
    pub valid_until_ms: i64,
}
impl TenantStatus {
    /// Enabled under an unexpired lease: calls verify and writes sign.
    pub fn is_current(&self, now_ms: i64) -> bool {
        self.enabled && self.valid_until_ms > now_ms
    }
}

impl PluginRuntimeHost {
    /// The state of `tenant_id`'s grant `grant_id`, or `None` when this
    /// deployment holds none. Read from the shared store on every call, so
    /// every replica sees a disable or a new revision at once.
    pub async fn tenant_status(
        &self,
        tenant_id: &str,
        grant_id: &str,
    ) -> Result<Option<TenantStatus>, Status> {
        Ok(self
            .0
            .states
            .get(&self.0.deployment.deployment_id, tenant_id, grant_id)
            .await?
            .map(|state| TenantStatus {
                revision: state.revision,
                enabled: state.enabled,
                valid_until_ms: state.valid_until_ms,
            }))
    }
}

/// A host of one deployment (`echo-1`) over a memory store, for tests of
/// host writes and statuses.
#[cfg(test)]
pub(crate) mod fixture {
    use super::*;
    use crate::state_store::MemoryStore;
    use base64::{Engine, engine::general_purpose::STANDARD};
    use ed25519_dalek::{Signer, SigningKey};
    use flare_plugin_protocol::signature::PublisherTrustStore;
    use serde_json::json;

    /// The host and its store.
    pub(crate) fn host_with_store() -> (PluginRuntimeHost, Arc<MemoryStore>) {
        let raw = serde_json::to_vec(&json!({"manifestVersion":1,"pluginId":"example.echo","version":"1.0.0","apiVersion":"1.0","displayName":"Echo","publisher":{"publisherId":"test","displayName":"Test"},"runtime":{"kind":"grpc","service":"example.echo.v1.PluginService","protocol":"flare.capability.v1"},"capabilities":[{"id":"example.echo","operations":[{"id":"run","inputSchema":{"type":"object"},"outputSchema":{"type":"object"},"permissionIds":[],"idempotent":true,"timeoutMs":1000}]}],"permissions":[],"features":[{"id":"run","label":{"en-US":"Run"},"gates":{"operations":["example.echo.run"]}}],"artifact":{"digest":format!("sha256:{}","a".repeat(64)),"signatureRef":"test"}})).unwrap();
        let publisher = SigningKey::from_bytes(&[91; 32]);
        let mut trust = PublisherTrustStore::default();
        trust
            .insert(
                "test".into(),
                "test".into(),
                publisher.verifying_key().to_bytes(),
            )
            .unwrap();
        let manifest = trust
            .verify(
                &raw,
                "test",
                &STANDARD.encode(publisher.sign(&raw).to_bytes()),
            )
            .unwrap();
        let deployment = PluginDeployment {
            deployment_id: "echo-1".into(),
            plugin_id: "example.echo".into(),
            plugin_version: "1.0.0".into(),
            manifest_digest: manifest.digest().into(),
            artifact_digest: format!("sha256:{}", "a".repeat(64)),
            grpc_authority: "http://127.0.0.1:7901".into(),
        };
        let store = Arc::new(MemoryStore::default());
        let host = PluginRuntimeHost::new(
            manifest,
            deployment,
            flare_plugin_protocol::invocation::PluginCallVerifier::default(),
            Arc::new(|_| Ok(())),
            store.clone(),
        )
        .unwrap();
        (host, store)
    }

    pub(crate) fn tenant(
        host: &PluginRuntimeHost,
        revision: u64,
        enabled: bool,
    ) -> PluginTenantState {
        PluginTenantState {
            organization_id: "org-1".into(),
            deployment_id: "echo-1".into(),
            app_id: "201".into(),
            grant_id: "grant-1".into(),
            revision,
            manifest_digest: host.manifest_digest().into(),
            configuration_json: b"{}".to_vec(),
            configuration_digest: configuration_digest(&json!({})).unwrap(),
            enabled,
            valid_until_ms: now_ms() + 60_000,
            validate_configuration: false,
            approved_operations: Vec::new(),
            permissions: Vec::new(),
            data_scopes: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::fixture::{host_with_store, tenant};
    use super::*;
    use crate::state_store::TenantStateStore;

    #[tokio::test]
    async fn status_tells_a_moved_revision_from_a_disabled_installation() {
        let (host, store) = host_with_store();
        assert_eq!(host.tenant_status("201", "grant-1").await.unwrap(), None);
        store.apply(tenant(&host, 3, true)).await.unwrap();
        let status = host.tenant_status("201", "grant-1").await.unwrap().unwrap();
        assert_eq!((status.revision, status.enabled), (3, true));
        assert!(status.is_current(now_ms()));
        store.apply(tenant(&host, 4, true)).await.unwrap();
        assert_eq!(
            host.tenant_status("201", "grant-1")
                .await
                .unwrap()
                .unwrap()
                .revision,
            4
        );
        store.apply(tenant(&host, 5, false)).await.unwrap();
        let disabled = host.tenant_status("201", "grant-1").await.unwrap().unwrap();
        assert!(!disabled.enabled && !disabled.is_current(now_ms()));
        // Another grant or tenant is not this one.
        assert_eq!(host.tenant_status("201", "grant-2").await.unwrap(), None);
        assert_eq!(host.tenant_status("202", "grant-1").await.unwrap(), None);
        let lapsed = TenantStatus {
            revision: 5,
            enabled: true,
            valid_until_ms: now_ms() - 1,
        };
        assert!(!lapsed.is_current(now_ms()));
    }
}
