use super::*;
use crate::protocol::{
    authorization::TenantPluginGrant,
    resource::{HostCapability, ResourceWriteRequest},
    runtime::{PluginRuntimeSnapshot, ResolvedPlugin},
    service::*,
};
use crate::service::{PluginServiceRouter, ServiceDirectory};
use std::sync::Mutex;

struct Directory(Mutex<PluginRuntimeSnapshot>);
#[tonic::async_trait]
impl ServiceDirectory for Directory {
    async fn snapshot(&self, _: &str) -> Result<Arc<PluginRuntimeSnapshot>, Status> {
        Ok(Arc::new(self.0.lock().unwrap().clone()))
    }
    async fn check_policy(&self, _: &str, _: &str) -> Result<(), Status> {
        Ok(())
    }
    async fn reserve_service(
        &self,
        claims: &PluginServiceClaims,
        origin: &str,
    ) -> Result<(), Status> {
        assert_eq!(origin, "message-1");
        assert_eq!(claims.actor_id.as_deref(), Some("member"));
        assert_eq!(claims.conversation_id.as_deref(), Some("group"));
        Ok(())
    }
}
struct Target(PluginCallVerifier);
#[tonic::async_trait]
impl flare_grpc_proto::capability::extension_plugin_server::ExtensionPlugin for Target {
    async fn call(
        &self,
        request: Request<GenericRequest>,
    ) -> Result<Response<flare_grpc_proto::capability::GenericResponse>, Status> {
        let token = request
            .metadata()
            .get(SERVICE_HEADER)
            .unwrap()
            .to_str()
            .unwrap();
        let claims = self.0.verify_service(token, now_ms()).unwrap();
        assert_eq!(claims.actor_id.as_deref(), Some("member"));
        assert_eq!(claims.conversation_id.as_deref(), Some("group"));
        let input: Value =
            serde_json::from_slice(&request.get_ref().payload.as_ref().unwrap().value).unwrap();
        claims.matches_input(&input).unwrap();
        Ok(Response::new(
            flare_grpc_proto::capability::GenericResponse {
                ok: true,
                request_id: request.get_ref().request_id.clone(),
                payload: Some(prost_types::Any {
                    type_url: String::new(),
                    value: serde_json::to_vec(&json!({"ok":true,"data":{},"errorCode":null}))
                        .unwrap(),
                }),
                ..Default::default()
            },
        ))
    }
}
#[tokio::test]
async fn event_origin_delegates_only_signed_actor_conversation_and_current_approved_subscription() {
    let (host, calls, event, event_claims) = fixture().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let authority = format!("http://{}", listener.local_addr().unwrap());
    let mut verifier = PluginCallVerifier::default();
    verifier.insert("core".into(), calls.public_key()).unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(
                flare_grpc_proto::capability::extension_plugin_server::ExtensionPluginServer::new(
                    Target(verifier.clone()),
                ),
            )
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async {
                    let _ = stopped.await;
                },
            ),
    );
    let now = now_ms();
    let target_manifest=crate::protocol::PluginManifest::from_json(&serde_json::to_vec(&json!({
        "manifestVersion":1,"pluginId":"tasks","version":"1.0.0","apiVersion":"1.0","displayName":"Tasks",
        "publisher":{"publisherId":"test","displayName":"Test"},"runtime":{"kind":"grpc","service":"test.Plugin","protocol":"flare.capability.v1"},
        "capabilities":[{"id":"tasks","operations":[{"id":"prepare","inputSchema":{"type":"object"},"outputSchema":{"type":"object"},"permissionIds":["prepare"],"idempotent":true,"timeoutMs":1000}]}],
        "permissions":[{"id":"prepare","description":"Prepare task","dataScopes":[],"riskLevel":"low"}],
        "serviceOperations":[{"operation":"tasks.prepare","mode":"user"}],
        "features":[{"id":"prepare","label":{"en-US":"Prepare"},"gates":{"serviceOperations":["tasks.prepare"]}}],
        "artifact":{"digest":format!("sha256:{}","a".repeat(64)),"signatureRef":"test"}})).unwrap()).unwrap();
    let source_manifest = host.manifest().clone();
    assert!(
        source_manifest
            .dependencies
            .iter()
            .any(|dependency| { dependency.plugin_id == "tasks" && dependency.optional })
    );
    let principal = |manifest: &crate::protocol::PluginManifest,
                     digest: String,
                     grant: &str,
                     deployment: PluginDeployment| {
        ResolvedPlugin::new(
            TenantPluginGrant {
                grant_id: grant.into(),
                revision: 1,
                tenant_id: "app".into(),
                plugin_id: manifest.plugin_id.clone(),
                plugin_version: manifest.version.clone(),
                manifest_digest: digest,
                approved_operations: manifest.operation_ids(),
                approved_permissions: manifest.permissions.iter().map(|p| p.id.clone()).collect(),
                approved_data_scopes: manifest
                    .permissions
                    .iter()
                    .flat_map(|p| p.data_scopes.clone())
                    .collect(),
                enabled: true,
                revoked: false,
                suspended: false,
                valid_from_ms: now,
                valid_until_ms: now + 60000,
            },
            Arc::new(manifest.clone()),
            deployment,
            configuration_digest(&json!({})).unwrap(),
            Default::default(),
            manifest
                .features
                .iter()
                .map(|f| (f.id.clone(), true))
                .collect(),
        )
        .unwrap()
    };
    let target_digest = digest_bytes(&serde_json::to_vec(&target_manifest).unwrap());
    let target = principal(
        &target_manifest,
        target_digest.clone(),
        "target-grant",
        PluginDeployment {
            deployment_id: "task-deployment".into(),
            plugin_id: "tasks".into(),
            plugin_version: "1.0.0".into(),
            manifest_digest: target_digest,
            artifact_digest: format!("sha256:{}", "a".repeat(64)),
            grpc_authority: authority,
        },
    );
    let source = principal(
        &source_manifest,
        event_claims.manifest_digest.clone(),
        "grant",
        host.0.deployment.clone(),
    );
    let directory = Arc::new(Directory(Mutex::new(PluginRuntimeSnapshot {
        tenant_id: "app".into(),
        organization_id: Some("org".into()),
        version: 1,
        issued_at_ms: now,
        valid_until_ms: now + 60000,
        plugins: vec![source, target],
    })));
    let runtime = PluginRuntimeSigner::new("runtime".into(), [92; 32]).unwrap();
    let mut runtimes = crate::protocol::invocation::PluginRuntimeVerifier::default();
    runtimes
        .insert("runtime".into(), "deployment".into(), runtime.public_key())
        .unwrap();
    let router = PluginServiceRouter::new(directory.clone(), verifier, runtimes, Arc::new(calls));
    let origin = host
        .verify_event_call(&request(
            &event,
            &event_claims,
            &PluginCallSigner::new("core".into(), [91; 32]).unwrap(),
        ))
        .await
        .unwrap();
    let send = |actor_ticket: Option<String>| {
        let body = serde_json::to_vec(&ResourceWriteRequest::ServiceDispatch(
            PluginServiceRequest {
                target_plugin_id: "tasks".into(),
                operation: "tasks.prepare".into(),
                business_ref: "checkin-1".into(),
                mode: ServiceMode::User,
                actor_ticket,
                input: json!({"title":"Task"}),
            },
        ))
        .unwrap();
        let token = runtime
            .sign(
                &PluginRuntimeClaims {
                    tenant_id: "app".into(),
                    plugin_id: "observer".into(),
                    plugin_version: "1.0.0".into(),
                    manifest_digest: event_claims.manifest_digest.clone(),
                    deployment_id: "deployment".into(),
                    grant_id: "grant".into(),
                    grant_revision: 1,
                    capability: HostCapability::ServiceDispatch,
                    request_id: "service-1".into(),
                    request_digest: digest_bytes(&body),
                    issued_at_ms: now,
                    expires_at_ms: now + 30000,
                },
                now,
            )
            .unwrap();
        (token, body)
    };
    let (token, body) = send(Some(origin.delegation.to_string()));
    assert!(
        router
            .dispatch(&token, "service-1", &body)
            .await
            .unwrap()
            .ok
    );
    for actor_ticket in [None, Some("{\"actorId\":\"member\"}".into())] {
        let (token, body) = send(actor_ticket);
        assert!(router.dispatch(&token, "service-1", &body).await.is_err());
    }
    directory.0.lock().unwrap().plugins[0]
        .grant
        .approved_data_scopes
        .remove(MESSAGE_BODY_SCOPE);
    assert!(router.dispatch(&token, "service-1", &body).await.is_err());
    stop.send(()).unwrap();
    server.await.unwrap().unwrap();
}
