//! Staff tickets for admin operations and runtime-signed host writes.
use super::*;
use base64::{Engine, engine::general_purpose::STANDARD};
use ed25519_dalek::{
    Signer, SigningKey,
    pkcs8::{EncodePrivateKey, EncodePublicKey},
};
use flare_grpc_proto::capability::{
    GenericResponse,
    plugin_host_bridge_server::{PluginHostBridge, PluginHostBridgeServer},
};
use flare_plugin_protocol::{
    invocation::{
        PLUGIN_RUNTIME_HEADER, PluginCallClaims, PluginCallSigner, PluginRuntimeVerifier,
        PluginStaffCallClaims, plugin_permission,
    },
    resource::{EntityStatePublishOutcome, EntityStatePublishView},
    signature::PublisherTrustStore,
    state::SharedState,
};
use serde_json::json;

const PLUGIN: &str = "example.admin";

#[tokio::test]
async fn organization_calls_need_explicit_signed_scope_and_trusted_owner() {
    use flare_plugin_protocol::invocation::PluginAdminScope;
    let mut raw: serde_json::Value = serde_json::from_slice(&manifest()).unwrap();
    raw["admin"]["operations"][0]["scope"] = json!("organization");
    let f = fixture_with(serde_json::to_vec(&raw).unwrap(), None);
    f.enable(1, true).await;
    // An application permission and the staff member's membership do not grant org work.
    let mut request = f.staff("example.admin.list", 1);
    assert!(f.host.verify_staff(&request).await.is_err());
    request
        .get_mut()
        .metadata
        .insert("organization_id".into(), "org-1".into());
    assert!(f.host.verify_staff(&request).await.is_err());
    let ticket = request
        .metadata()
        .get(PLUGIN_CALL_HEADER)
        .unwrap()
        .to_str()
        .unwrap();
    let mut claims = f.host.0.calls.verify_staff(ticket, now_ms()).unwrap();
    claims.org_id = None; // Platform staff still needs an explicit target.
    request.metadata_mut().insert(
        PLUGIN_CALL_HEADER,
        f.calls
            .sign_staff(&claims, now_ms())
            .unwrap()
            .parse()
            .unwrap(),
    );
    assert!(f.host.verify_staff(&request).await.is_err());
    claims.scope = PluginAdminScope::Organization {
        organization_id: "org-1".into(),
    };
    request.metadata_mut().insert(
        PLUGIN_CALL_HEADER,
        f.calls
            .sign_staff(&claims, now_ms())
            .unwrap()
            .parse()
            .unwrap(),
    );
    let verified = f.host.verify_staff(&request).await.unwrap();
    assert_eq!(verified.organization_id.as_deref(), Some("org-1"));
    assert_eq!(verified.scope, claims.scope);
    claims.scope = PluginAdminScope::Organization {
        organization_id: "org-2".into(),
    };
    request.metadata_mut().insert(
        PLUGIN_CALL_HEADER,
        f.calls
            .sign_staff(&claims, now_ms())
            .unwrap()
            .parse()
            .unwrap(),
    );
    assert!(f.host.verify_staff(&request).await.is_err());
}

fn manifest() -> Vec<u8> {
    serde_json::to_vec(&json!({
        "manifestVersion": 1, "pluginId": PLUGIN, "version": "1.0.0", "apiVersion": "1.0",
        "displayName": "Admin", "publisher": {"publisherId": "test", "displayName": "Test"},
        "runtime": {"kind": "grpc", "service": "example.admin.v1.PluginService", "protocol": "flare.capability.v1"},
        "capabilities": [{"id": PLUGIN, "operations": [
            {"id": "run", "inputSchema": {"type": "object"}, "outputSchema": {"type": "object"},
             "permissionIds": ["example.admin.use"], "idempotent": true, "timeoutMs": 1000,
             "hostCapabilities": ["im.plugin.entity_state.publish"]},
            {"id": "list", "inputSchema": {"type": "object"}, "outputSchema": {"type": "object"},
             "permissionIds": ["example.admin.manage"], "idempotent": true, "timeoutMs": 1000,
             "mode": "query"},
            {"id": "close", "inputSchema": {"type": "object"}, "outputSchema": {"type": "object"},
             "permissionIds": ["example.admin.manage"], "idempotent": true, "timeoutMs": 1000}
        ]}],
        "permissions": [
            {"id": "example.admin.use", "description": "Use", "dataScopes": ["im.plugin.entity_state.publish"], "riskLevel": "low"},
            {"id": "example.admin.manage", "description": "Manage", "dataScopes": ["admin"], "riskLevel": "high"}
        ],
        "clientExtensions": {"platforms": ["web"], "nativeCapabilities": [], "contributions": []},
        // `list` needs `read`; `close` needs `manage` and the operator's reason.
        "admin": {
            "contractVersion": 1,
            "permissions": [
                {"id": "read", "label": {"en-US": "View"}, "risk": "low", "defaultRoles": ["org_owner"]},
                {"id": "manage", "label": {"en-US": "Manage"}, "risk": "high", "defaultRoles": ["org_owner"]}
            ],
            "operations": [
                {"id": "example.admin.list", "mode": "query", "permission": "read"},
                {"id": "example.admin.close", "mode": "command", "permission": "manage",
                 "requiresReason": true, "auditTarget": "/id"}
            ],
            "pages": [{"id": "items", "kind": "list", "permission": "read", "document": {
                "title": "Manage",
                "blocks": [{"kind": "rows", "source": "example.admin.list", "path": "/items",
                            "keyPath": "/id", "columns": [{"label": "Id", "path": "/id"}],
                            "actions": ["close"]}],
                "actions": [{"id": "close", "label": "Close",
                             "target": {"kind": "operation", "operation": "example.admin.close"},
                             "input": {"id": "/id"}}]
            }}]
        },
        "artifact": {"digest": format!("sha256:{}", "a".repeat(64)), "signatureRef": "test"}
    }))
    .unwrap()
}

struct Fixture {
    host: PluginRuntimeHost,
    issuer: TokenService,
    calls: PluginCallSigner,
}

fn fixture(runtime_key: Option<[u8; 32]>) -> Fixture {
    fixture_with(manifest(), runtime_key)
}

fn fixture_with(raw: Vec<u8>, runtime_key: Option<[u8; 32]>) -> Fixture {
    let publisher = SigningKey::from_bytes(&[71; 32]);
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
        deployment_id: "admin-1".into(),
        plugin_id: PLUGIN.into(),
        plugin_version: "1.0.0".into(),
        manifest_digest: manifest.digest().into(),
        artifact_digest: format!("sha256:{}", "a".repeat(64)),
        grpc_authority: "http://127.0.0.1:7901".into(),
    };
    let key = SigningKey::from_bytes(&[72; 32]);
    let issuer = TokenService::from_ed25519_private_key(
        &key.to_pkcs8_pem(Default::default()).unwrap(),
        "flare-platform",
        60,
    )
    .unwrap();
    let calls = PluginCallSigner::new("call-key".into(), [73; 32]).unwrap();
    let mut verifier = PluginCallVerifier::default();
    verifier
        .insert("call-key".into(), calls.public_key())
        .unwrap();
    let host = PluginRuntimeHost::new(
        manifest,
        deployment,
        &key.verifying_key()
            .to_public_key_pem(Default::default())
            .unwrap(),
        verifier,
        Arc::new(|_| Ok(())),
        Arc::new(crate::state_store::MemoryStore::default()),
    )
    .unwrap();
    let host = match runtime_key {
        Some(seed) => host
            .with_runtime_signer(PluginRuntimeSigner::new("runtime-key".into(), seed).unwrap())
            .unwrap(),
        None => host,
    };
    Fixture {
        host,
        issuer,
        calls,
    }
}

impl Fixture {
    async fn enable(&self, revision: u64, enabled: bool) {
        let state = PluginTenantState {
            organization_id: "org-1".into(),
            deployment_id: "admin-1".into(),
            app_id: "201".into(),
            grant_id: "grant-1".into(),
            revision,
            manifest_digest: self.host.0.deployment.manifest_digest.clone(),
            configuration_json: b"{}".to_vec(),
            configuration_digest: configuration_digest(&json!({})).unwrap(),
            enabled,
            valid_until_ms: now_ms() + 300000,
            validate_configuration: true,
            approved_operations: Vec::new(),
            permissions: Vec::new(),
            data_scopes: Vec::new(),
        };
        let token = self
            .issuer
            .generate_with_ed25519(
                "flare-platform-worker",
                Some("201"),
                "control",
                "flare-plugin-runtime:admin-1:org-1",
            )
            .unwrap();
        let mut request = Request::new(state);
        request
            .metadata_mut()
            .insert("authorization", format!("Bearer {token}").parse().unwrap());
        self.host.apply_tenant(request).await.unwrap();
    }
    /// A staff ticket holding the permission `operation` declares, with a reason.
    fn staff(&self, operation: &str, revision: u64) -> Request<GenericRequest> {
        let permission = if operation == "example.admin.list" {
            "read"
        } else {
            "manage"
        };
        self.staff_holding(
            operation,
            revision,
            &plugin_permission(PLUGIN, permission).unwrap(),
            Some("ticket-42"),
        )
    }
    fn staff_holding(
        &self,
        operation: &str,
        revision: u64,
        permission: &str,
        reason: Option<&str>,
    ) -> Request<GenericRequest> {
        let now = now_ms();
        let claims = PluginStaffCallClaims {
            scope: Default::default(),
            tenant_id: "201".into(),
            plugin_id: PLUGIN.into(),
            plugin_version: "1.0.0".into(),
            manifest_digest: self.host.0.deployment.manifest_digest.clone(),
            deployment_id: "admin-1".into(),
            grant_id: "grant-1".into(),
            grant_revision: revision,
            configuration_digest: configuration_digest(&json!({})).unwrap(),
            staff_id: "staff-9".into(),
            org_id: Some("org-1".into()),
            permission: permission.into(),
            operation: operation.into(),
            request_id: "r1".into(),
            reason: reason.map(str::to_owned),
            issued_at_ms: now,
            expires_at_ms: now + 1000,
        };
        let mut request = Request::new(GenericRequest {
            operation: operation.into(),
            request_id: "r1".into(),
            ..Default::default()
        });
        request.metadata_mut().insert(
            PLUGIN_CALL_HEADER,
            self.calls
                .sign_staff(&claims, now)
                .unwrap()
                .parse()
                .unwrap(),
        );
        request
    }
    fn user(&self, operation: &str, revision: u64) -> Request<GenericRequest> {
        let now = now_ms();
        let claims = PluginCallClaims {
            tenant_id: "201".into(),
            plugin_id: PLUGIN.into(),
            plugin_version: "1.0.0".into(),
            manifest_digest: self.host.0.deployment.manifest_digest.clone(),
            deployment_id: "admin-1".into(),
            grant_id: "grant-1".into(),
            grant_revision: revision,
            configuration_digest: configuration_digest(&json!({})).unwrap(),
            actor_id: "u1".into(),
            operation: operation.into(),
            request_id: "r1".into(),
            conversation_id: None,
            issued_at_ms: now,
            expires_at_ms: now + 1000,
        };
        let mut request = Request::new(GenericRequest {
            operation: operation.into(),
            request_id: "r1".into(),
            ..Default::default()
        });
        request.metadata_mut().insert(
            PLUGIN_CALL_HEADER,
            self.calls.sign(&claims, now).unwrap().parse().unwrap(),
        );
        request
    }
}

#[tokio::test]
async fn staff_tickets_reach_only_admin_operations_and_user_tickets_never_do() {
    let f = fixture(None);
    f.enable(1, true).await;
    let staff = f
        .host
        .verify_staff(&f.staff("example.admin.list", 1))
        .await
        .unwrap();
    assert_eq!(
        (staff.tenant_id.as_str(), staff.staff_id.as_str()),
        ("201", "staff-9")
    );
    assert_eq!(staff.org_id.as_deref(), Some("org-1"));
    assert_eq!(staff.grant_revision, 1);
    assert_eq!(staff.configuration, json!({}));
    assert!(
        f.host
            .verify_staff(&f.staff("example.admin.close", 1))
            .await
            .is_ok()
    );
    // Not an admin operation: never for staff.
    assert!(
        f.host
            .verify_staff(&f.staff("example.admin.run", 1))
            .await
            .is_err()
    );
    // User tickets: client operations only; the domains never cross.
    assert!(
        f.host
            .verify_call(&f.user("example.admin.run", 1))
            .await
            .is_ok()
    );
    assert!(
        f.host
            .verify_call(&f.user("example.admin.list", 1))
            .await
            .is_err()
    );
    assert!(
        f.host
            .verify_staff(&f.user("example.admin.list", 1))
            .await
            .is_err()
    );
    assert!(
        f.host
            .verify_call(&f.staff("example.admin.list", 1))
            .await
            .is_err()
    );
    // Body bindings.
    let mut request = f.staff("example.admin.list", 1);
    request.get_mut().operation = "example.admin.close".into();
    assert!(f.host.verify_staff(&request).await.is_err());
    let mut request = f.staff("example.admin.list", 1);
    request.get_mut().request_id = "r2".into();
    assert!(f.host.verify_staff(&request).await.is_err());
    let mut request = f.staff("example.admin.list", 1);
    request
        .get_mut()
        .metadata
        .insert("tenant_id".into(), "202".into());
    assert!(f.host.verify_staff(&request).await.is_err());
    // Grant revision and enablement, exactly as for users: a projection that has not reached
    // this deployment yet is `Unavailable`, so Core answers "outcome unknown".
    f.enable(2, true).await;
    assert_eq!(
        f.host
            .verify_staff(&f.staff("example.admin.list", 1))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unavailable
    );
    assert!(
        f.host
            .verify_staff(&f.staff("example.admin.list", 2))
            .await
            .is_ok()
    );
    f.enable(3, false).await;
    assert_eq!(
        f.host
            .verify_staff(&f.staff("example.admin.list", 3))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unavailable
    );
}

#[tokio::test]
async fn a_staff_ticket_must_hold_the_permission_its_operation_declares() {
    let code = |permission: &str| plugin_permission(PLUGIN, permission).unwrap();
    // Exactly the code of the operation's declared permission.
    let f = fixture(None);
    f.enable(1, true).await;
    for (operation, permission, allowed) in [
        ("example.admin.list", "read", true),
        ("example.admin.list", "manage", false),
        ("example.admin.list", "admin", false),
        ("example.admin.close", "manage", true),
        ("example.admin.close", "read", false),
        ("example.admin.close", "admin", false),
    ] {
        let verified = f
            .host
            .verify_staff(&f.staff_holding(operation, 1, &code(permission), Some("r")))
            .await;
        assert_eq!(verified.is_ok(), allowed, "{operation} {permission}");
        if let Ok(staff) = verified {
            assert_eq!(staff.permission, code(permission));
        } else {
            assert_eq!(verified.unwrap_err().code(), tonic::Code::PermissionDenied);
        }
    }
}

#[tokio::test]
async fn an_operation_declaring_requires_reason_needs_the_stated_reason() {
    let code = |permission: &str| plugin_permission(PLUGIN, permission).unwrap();
    let f = fixture(None);
    f.enable(1, true).await;
    let refused = f
        .host
        .verify_staff(&f.staff_holding("example.admin.close", 1, &code("manage"), None))
        .await
        .unwrap_err();
    assert_eq!(refused.code(), tonic::Code::PermissionDenied);
    let staff = f
        .host
        .verify_staff(&f.staff_holding(
            "example.admin.close",
            1,
            &code("manage"),
            Some("清理违规内容"),
        ))
        .await
        .unwrap();
    assert_eq!(staff.reason.as_deref(), Some("清理违规内容"));
    // A read asks for none, and a reason it carries reaches the plugin.
    let staff = f
        .host
        .verify_staff(&f.staff_holding("example.admin.list", 1, &code("read"), None))
        .await
        .unwrap();
    assert_eq!(staff.reason, None);
    let staff = f
        .host
        .verify_staff(&f.staff_holding("example.admin.list", 1, &code("read"), Some("audit")))
        .await
        .unwrap();
    assert_eq!(staff.reason.as_deref(), Some("audit"));
}

#[tokio::test]
async fn runtime_writes_are_signed_for_the_exact_body_under_the_current_grant() {
    let unsigned = fixture(None);
    unsigned.enable(1, true).await;
    let request = ResourceWriteRequest::EntityStatePublish(SharedState {
        entity: "poll-1".into(),
        version: 3,
        values: json!({"closed": true}).as_object().unwrap().clone(),
    });
    assert_eq!(
        unsigned
            .host
            .sign_runtime_write("201", "grant-1", &request)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    let f = fixture(Some([74; 32]));
    assert!(f.host.signs_runtime_writes());
    // No grant yet, then a disabled grant: nothing is signed.
    assert!(
        f.host
            .sign_runtime_write("201", "grant-1", &request)
            .await
            .is_err()
    );
    f.enable(1, false).await;
    assert!(
        f.host
            .sign_runtime_write("201", "grant-1", &request)
            .await
            .is_err()
    );
    f.enable(4, true).await;
    let write = f
        .host
        .sign_runtime_write("201", "grant-1", &request)
        .await
        .unwrap();
    assert_eq!(write.capability(), HostCapability::EntityStatePublish);
    let key = PluginRuntimeSigner::new("runtime-key".into(), [74; 32])
        .unwrap()
        .public_key();
    let verifier = PluginRuntimeVerifier::from_public_keys_json(
        &json!([{"key_id": "runtime-key", "deployment_id": "admin-1",
                 "public_key_base64": STANDARD.encode(key)}])
        .to_string(),
    )
    .unwrap();
    let claims = verifier
        .verify(&write.credential, write.body(), now_ms())
        .unwrap();
    assert_eq!(
        (
            claims.tenant_id.as_str(),
            claims.plugin_id.as_str(),
            claims.deployment_id.as_str(),
            claims.grant_id.as_str(),
            claims.grant_revision,
            claims.request_id.as_str()
        ),
        ("201", PLUGIN, "admin-1", "grant-1", 4, write.request_id())
    );
    assert!(claims.expires_at_ms - claims.issued_at_ms <= MAX_RUNTIME_CALL_TTL_MS);
    // The credential covers these bytes only.
    let mut other = write.body().to_vec();
    other.push(b' ');
    assert!(
        verifier
            .verify(&write.credential, &other, now_ms())
            .is_err()
    );
    // Registered for another deployment: refused.
    let elsewhere = PluginRuntimeVerifier::from_public_keys_json(
        &json!([{"key_id": "runtime-key", "deployment_id": "admin-2",
                 "public_key_base64": STANDARD.encode(key)}])
        .to_string(),
    )
    .unwrap();
    assert!(
        elsewhere
            .verify(&write.credential, write.body(), now_ms())
            .is_err()
    );
    let second = f
        .host
        .sign_runtime_write("201", "grant-1", &request)
        .await
        .unwrap();
    assert_ne!(second.request_id(), write.request_id());
}

#[test]
fn runtime_signer_environment_values() {
    assert!(runtime_signer_from(None, None).unwrap().is_none());
    assert!(
        runtime_signer_from(Some(" ".into()), Some("".into()))
            .unwrap()
            .is_none()
    );
    assert!(runtime_signer_from(Some("k1".into()), None).is_err());
    assert!(runtime_signer_from(None, Some(STANDARD.encode([1; 32]))).is_err());
    assert!(runtime_signer_from(Some("k1".into()), Some("not base64".into())).is_err());
    assert!(runtime_signer_from(Some("k1".into()), Some(STANDARD.encode([1; 31]))).is_err());
    assert!(runtime_signer_from(Some("bad id!".into()), Some(STANDARD.encode([1; 32]))).is_err());
    let signer = runtime_signer_from(Some(" k1 ".into()), Some(STANDARD.encode([1; 32])))
        .unwrap()
        .unwrap();
    assert_eq!(signer.key_id(), "k1");
}

/// Core's bridge as the plugin sees it: verifies the runtime credential over the
/// received bytes and answers with the publish view.
struct Bridge(PluginRuntimeVerifier);
#[tonic::async_trait]
impl PluginHostBridge for Bridge {
    async fn call(
        &self,
        request: Request<GenericRequest>,
    ) -> Result<tonic::Response<GenericResponse>, Status> {
        if request.metadata().get(PLUGIN_CALL_HEADER).is_some() {
            return Err(Status::unauthenticated("unexpected call ticket"));
        }
        let token = request
            .metadata()
            .get(PLUGIN_RUNTIME_HEADER)
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| Status::unauthenticated("runtime credential required"))?
            .to_owned();
        let body = request.into_inner();
        let bytes = body.payload.map(|p| p.value).unwrap_or_default();
        let claims = self
            .0
            .verify(&token, &bytes, now_ms())
            .map_err(|_| Status::permission_denied("invalid runtime credential"))?;
        if claims.request_id != body.request_id
            || body.operation != HostCapability::EntityStatePublish.id()
        {
            return Err(Status::permission_denied("runtime write mismatch"));
        }
        let ResourceWriteRequest::EntityStatePublish(state) =
            serde_json::from_slice(&bytes).map_err(|_| Status::invalid_argument("body"))?
        else {
            return Err(Status::invalid_argument("not an entity state publish"));
        };
        Ok(tonic::Response::new(GenericResponse {
            ok: true,
            request_id: body.request_id,
            payload: Some(prost_types::Any {
                type_url: String::new(),
                value: serde_json::to_vec(&EntityStatePublishView {
                    entity: state.entity,
                    version: state.version,
                    outcome: EntityStatePublishOutcome::Published,
                })
                .unwrap(),
            }),
            ..Default::default()
        }))
    }
}

#[tokio::test]
async fn host_resource_client_publishes_entity_state_under_the_runtime_credential() {
    let f = fixture(Some([75; 32]));
    f.enable(1, true).await;
    let key = PluginRuntimeSigner::new("runtime-key".into(), [75; 32])
        .unwrap()
        .public_key();
    let verifier = PluginRuntimeVerifier::from_public_keys_json(
        &json!([{"key_id": "runtime-key", "deployment_id": "admin-1",
                 "public_key_base64": STANDARD.encode(key)}])
        .to_string(),
    )
    .unwrap();
    let address = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(PluginHostBridgeServer::new(Bridge(verifier)))
            .serve(address),
    );
    let authority = format!("http://{address}");
    let client = {
        let mut attempts = 0;
        loop {
            match crate::resource::HostResourceClient::connect(&authority).await {
                Ok(client) => break client,
                Err(_) if attempts < 50 => {
                    attempts += 1;
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                }
                Err(error) => panic!("bridge unavailable: {error}"),
            }
        }
    };
    let state = SharedState {
        entity: "poll-1".into(),
        version: 7,
        values: json!({"counts": [1, 2], "voterCount": 3, "closed": true})
            .as_object()
            .unwrap()
            .clone(),
    };
    let view = client
        .publish_entity_state(&f.host, "201", "grant-1", state.clone())
        .await
        .unwrap();
    assert_eq!(
        (view.entity.as_str(), view.version, view.outcome),
        ("poll-1", 7, EntityStatePublishOutcome::Published)
    );
    // Another tenant has no grant here: nothing is signed or sent.
    assert!(
        client
            .publish_entity_state(&f.host, "202", "grant-1", state)
            .await
            .is_err()
    );
    server.abort();
}
