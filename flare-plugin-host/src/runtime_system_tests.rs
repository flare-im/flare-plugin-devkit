//! Platform system tickets for `entityLifecycle.unbound` operations.
use super::*;
use base64::{Engine, engine::general_purpose::STANDARD};
use ed25519_dalek::{
    Signer, SigningKey,
    pkcs8::{EncodePrivateKey, EncodePublicKey},
};
use flare_plugin_protocol::{
    invocation::{
        PluginCallClaims, PluginCallSigner, PluginStaffCallClaims, PluginSystemCallClaims,
        plugin_permission,
    },
    signature::PublisherTrustStore,
};
use serde_json::json;

const PLUGIN: &str = "example.life";
const UNBOUND: &str = "example.life.unbound";

fn operation(id: &str) -> Value {
    json!({"id": id, "inputSchema": {"type": "object"}, "outputSchema": {"type": "object"},
           "permissionIds": ["example.life.use"], "idempotent": true, "timeoutMs": 1000})
}

fn query(id: &str) -> Value {
    let mut operation = operation(id);
    operation["mode"] = json!("query");
    operation
}

fn manifest() -> Vec<u8> {
    serde_json::to_vec(&json!({
        "manifestVersion": 1, "pluginId": PLUGIN, "version": "1.0.0", "apiVersion": "1.0",
        "displayName": "Life", "publisher": {"publisherId": "test", "displayName": "Test"},
        "runtime": {"kind": "grpc", "service": "example.life.v1.PluginService", "protocol": "flare.capability.v1"},
        "capabilities": [{"id": PLUGIN, "operations": [operation("vote"), operation("unbound"), query("list")]}],
        "permissions": [{"id": "example.life.use", "description": "Use", "dataScopes": [], "riskLevel": "low"}],
        "clientExtensions": {"platforms": ["web"], "nativeCapabilities": [], "contributions": [
            {"id": "example.life.card", "kind": "message-content", "schemaVersion": 1,
             "requiredPermissions": ["example.life.use"], "entityPath": "/pollId",
             "entityLifecycle": {"unbound": UNBOUND},
             "schema": {"title": "Poll", "fields": [], "actions": [
                 {"id": "vote", "label": "Vote",
                  "target": {"kind": "operation", "operation": "example.life.vote"},
                  "input": {"pollId": "/pollId"}}]}}
        ]},
        "admin": {
            "contractVersion": 1,
            "permissions": [{"id": "read", "label": {"en-US": "View"}, "risk": "low"}],
            "operations": [{"id": "example.life.list", "mode": "query", "permission": "read"}]
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

fn fixture() -> Fixture {
    let raw = manifest();
    let publisher = SigningKey::from_bytes(&[81; 32]);
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
        deployment_id: "life-1".into(),
        plugin_id: PLUGIN.into(),
        plugin_version: "1.0.0".into(),
        manifest_digest: manifest.digest().into(),
        artifact_digest: format!("sha256:{}", "a".repeat(64)),
        grpc_authority: "http://127.0.0.1:7902".into(),
    };
    let key = SigningKey::from_bytes(&[82; 32]);
    let issuer = TokenService::from_ed25519_private_key(
        &key.to_pkcs8_pem(Default::default()).unwrap(),
        "flare-platform",
        60,
    )
    .unwrap();
    let calls = PluginCallSigner::new("call-key".into(), [83; 32]).unwrap();
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
            deployment_id: "life-1".into(),
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
                "flare-plugin-runtime:life-1:org-1",
            )
            .unwrap();
        let mut request = Request::new(state);
        request
            .metadata_mut()
            .insert("authorization", format!("Bearer {token}").parse().unwrap());
        self.host.apply_tenant(request).await.unwrap();
    }

    fn request(operation: &str, input: &Value, ticket: String) -> Request<GenericRequest> {
        let mut request = Request::new(GenericRequest {
            operation: operation.into(),
            request_id: "lifecycle-7".into(),
            payload: Some(prost_types::Any {
                type_url: "type.googleapis.com/flare.capability.v1.PayloadJson".into(),
                value: serde_json::to_vec(input).unwrap(),
            }),
            ..Default::default()
        });
        request
            .metadata_mut()
            .insert(PLUGIN_CALL_HEADER, ticket.parse().unwrap());
        request
    }

    fn system_claims(
        &self,
        operation: &str,
        entity: &str,
        revision: u64,
    ) -> PluginSystemCallClaims {
        let now = now_ms();
        PluginSystemCallClaims {
            tenant_id: "201".into(),
            plugin_id: PLUGIN.into(),
            plugin_version: "1.0.0".into(),
            manifest_digest: self.host.0.deployment.manifest_digest.clone(),
            deployment_id: "life-1".into(),
            grant_id: "grant-1".into(),
            grant_revision: revision,
            configuration_digest: configuration_digest(&json!({})).unwrap(),
            operation: operation.into(),
            entity: entity.into(),
            reason: "recalled".into(),
            request_id: "lifecycle-7".into(),
            issued_at_ms: now,
            expires_at_ms: now + 1000,
        }
    }

    fn system(&self, operation: &str, input: &Value, revision: u64) -> Request<GenericRequest> {
        let claims = self.system_claims(operation, "poll-1", revision);
        Self::request(
            operation,
            input,
            self.calls.sign_system(&claims, now_ms()).unwrap(),
        )
    }

    fn user(&self, operation: &str, input: &Value) -> Request<GenericRequest> {
        let now = now_ms();
        let claims = PluginCallClaims {
            tenant_id: "201".into(),
            plugin_id: PLUGIN.into(),
            plugin_version: "1.0.0".into(),
            manifest_digest: self.host.0.deployment.manifest_digest.clone(),
            deployment_id: "life-1".into(),
            grant_id: "grant-1".into(),
            grant_revision: 1,
            configuration_digest: configuration_digest(&json!({})).unwrap(),
            actor_id: "u1".into(),
            operation: operation.into(),
            request_id: "lifecycle-7".into(),
            conversation_id: None,
            issued_at_ms: now,
            expires_at_ms: now + 1000,
        };
        Self::request(operation, input, self.calls.sign(&claims, now).unwrap())
    }

    fn staff(&self, operation: &str, input: &Value) -> Request<GenericRequest> {
        let now = now_ms();
        let claims = PluginStaffCallClaims {
            scope: Default::default(),
            tenant_id: "201".into(),
            plugin_id: PLUGIN.into(),
            plugin_version: "1.0.0".into(),
            manifest_digest: self.host.0.deployment.manifest_digest.clone(),
            deployment_id: "life-1".into(),
            grant_id: "grant-1".into(),
            grant_revision: 1,
            configuration_digest: configuration_digest(&json!({})).unwrap(),
            staff_id: "staff-9".into(),
            org_id: None,
            permission: plugin_permission(PLUGIN, "read").unwrap(),
            operation: operation.into(),
            request_id: "lifecycle-7".into(),
            reason: None,
            issued_at_ms: now,
            expires_at_ms: now + 1000,
        };
        Self::request(
            operation,
            input,
            self.calls.sign_staff(&claims, now).unwrap(),
        )
    }
}

fn unbound_input() -> Value {
    json!({"entity": "poll-1", "reason": "recalled", "serverMsgId": "m-1"})
}

#[tokio::test]
async fn a_system_ticket_reaches_the_declared_unbound_operation_for_its_entity() {
    let f = fixture();
    f.enable(1, true).await;
    let verified = f
        .host
        .verify_system(&f.system(UNBOUND, &unbound_input(), 1))
        .await
        .unwrap();
    assert_eq!(
        (
            verified.tenant_id.as_str(),
            verified.operation.as_str(),
            verified.entity.as_str(),
            verified.reason.as_str(),
            verified.request_id.as_str(),
        ),
        ("201", UNBOUND, "poll-1", "recalled", "lifecycle-7")
    );
}

#[tokio::test]
async fn the_input_must_name_the_tickets_entity_and_reason() {
    let f = fixture();
    f.enable(1, true).await;
    for input in [
        json!({"entity": "poll-2", "reason": "recalled"}),
        json!({"entity": "poll-1", "reason": "post_failed"}),
        json!({"reason": "recalled"}),
        json!({}),
    ] {
        assert!(
            f.host
                .verify_system(&f.system(UNBOUND, &input, 1))
                .await
                .is_err(),
            "{input}"
        );
    }
    // A system ticket claiming a user is refused.
    let mut request = f.system(UNBOUND, &unbound_input(), 1);
    request
        .get_mut()
        .metadata
        .insert("user_id".into(), "u1".into());
    assert!(f.host.verify_system(&request).await.is_err());
}

#[tokio::test]
async fn system_tickets_reach_lifecycle_operations_only_and_no_other_ticket_does() {
    let f = fixture();
    f.enable(1, true).await;
    let vote = json!({"pollId": "poll-1"});
    // A system ticket for a user operation or an admin operation.
    for operation in ["example.life.vote", "example.life.list"] {
        assert!(
            f.host
                .verify_system(&f.system(operation, &unbound_input(), 1))
                .await
                .is_err(),
            "{operation}"
        );
    }
    // A user or staff ticket for the unbound operation, through any entry.
    assert!(
        f.host
            .verify_call(&f.user(UNBOUND, &unbound_input()))
            .await
            .is_err()
    );
    assert!(
        f.host
            .verify_staff(&f.staff(UNBOUND, &unbound_input()))
            .await
            .is_err()
    );
    assert!(
        f.host
            .verify_system(&f.user(UNBOUND, &unbound_input()))
            .await
            .is_err()
    );
    assert!(
        f.host
            .verify_system(&f.staff(UNBOUND, &unbound_input()))
            .await
            .is_err()
    );
    // A system ticket presented as a user or staff call.
    assert!(
        f.host
            .verify_call(&f.system(UNBOUND, &unbound_input(), 1))
            .await
            .is_err()
    );
    assert!(
        f.host
            .verify_staff(&f.system(UNBOUND, &unbound_input(), 1))
            .await
            .is_err()
    );
    // The user operation still takes user tickets.
    assert!(
        f.host
            .verify_call(&f.user("example.life.vote", &vote))
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn a_stale_revision_or_disabled_tenant_refuses_system_tickets() {
    let f = fixture();
    f.enable(2, true).await;
    assert!(
        f.host
            .verify_system(&f.system(UNBOUND, &unbound_input(), 1))
            .await
            .is_err()
    );
    assert!(
        f.host
            .verify_system(&f.system(UNBOUND, &unbound_input(), 2))
            .await
            .is_ok()
    );
    f.enable(3, false).await;
    assert!(
        f.host
            .verify_system(&f.system(UNBOUND, &unbound_input(), 3))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn a_projection_that_does_not_match_the_ticket_is_unavailable_so_core_retries() {
    let code = |result: Result<VerifiedPluginSystem, Status>| result.unwrap_err().code();
    let f = fixture();
    // No tenant state here yet.
    assert_eq!(
        code(
            f.host
                .verify_system(&f.system(UNBOUND, &unbound_input(), 1))
                .await
        ),
        tonic::Code::Unavailable
    );
    f.enable(2, true).await;
    // Another grant revision, another configuration.
    assert_eq!(
        code(
            f.host
                .verify_system(&f.system(UNBOUND, &unbound_input(), 1))
                .await
        ),
        tonic::Code::Unavailable
    );
    let signed = |claims: &PluginSystemCallClaims| {
        Fixture::request(
            UNBOUND,
            &unbound_input(),
            f.calls.sign_system(claims, now_ms()).unwrap(),
        )
    };
    let reconfigured = PluginSystemCallClaims {
        configuration_digest: configuration_digest(&json!({"limit": 1})).unwrap(),
        ..f.system_claims(UNBOUND, "poll-1", 2)
    };
    assert_eq!(
        code(f.host.verify_system(&signed(&reconfigured)).await),
        tonic::Code::Unavailable
    );
    // A version switch: Core's projection names another version or deployment.
    for switched in [
        PluginSystemCallClaims {
            plugin_version: "1.0.1".into(),
            ..f.system_claims(UNBOUND, "poll-1", 2)
        },
        PluginSystemCallClaims {
            deployment_id: "life-2".into(),
            ..f.system_claims(UNBOUND, "poll-1", 2)
        },
        PluginSystemCallClaims {
            manifest_digest: format!("sha256:{}", "b".repeat(64)),
            ..f.system_claims(UNBOUND, "poll-1", 2)
        },
    ] {
        assert_eq!(
            code(f.host.verify_system(&signed(&switched)).await),
            tonic::Code::Unavailable,
            "{switched:?}"
        );
    }
    // Core rotated its signing key before this deployment was given the new one.
    let rotated = PluginCallSigner::new("call-key-2".into(), [84; 32]).unwrap();
    let request = Fixture::request(
        UNBOUND,
        &unbound_input(),
        rotated
            .sign_system(&f.system_claims(UNBOUND, "poll-1", 2), now_ms())
            .unwrap(),
    );
    assert_eq!(
        code(f.host.verify_system(&request).await),
        tonic::Code::Unavailable
    );
    // The tenant is disabled here.
    f.enable(3, false).await;
    assert_eq!(
        code(
            f.host
                .verify_system(&f.system(UNBOUND, &unbound_input(), 3))
                .await
        ),
        tonic::Code::Unavailable
    );
    f.enable(4, true).await;
    assert!(
        f.host
            .verify_system(&f.system(UNBOUND, &unbound_input(), 4))
            .await
            .is_ok()
    );

    // Not a projection question: refused for good.
    let forged = PluginCallSigner::new("call-key".into(), [85; 32]).unwrap();
    let request = Fixture::request(
        UNBOUND,
        &unbound_input(),
        forged
            .sign_system(&f.system_claims(UNBOUND, "poll-1", 4), now_ms())
            .unwrap(),
    );
    assert_eq!(
        code(f.host.verify_system(&request).await),
        tonic::Code::Unauthenticated,
        "a known key id whose signature does not verify"
    );
    assert_eq!(
        code(
            f.host
                .verify_system(&Fixture::request(
                    UNBOUND,
                    &unbound_input(),
                    "garbage".into()
                ))
                .await
        ),
        tonic::Code::Unauthenticated
    );
    assert_eq!(
        code(
            f.host
                .verify_system(&f.user(UNBOUND, &unbound_input()))
                .await
        ),
        tonic::Code::Unauthenticated,
        "a user ticket"
    );
    assert_eq!(
        code(
            f.host
                .verify_system(&f.system(
                    UNBOUND,
                    &json!({"entity": "poll-2", "reason": "recalled"}),
                    4
                ))
                .await
        ),
        tonic::Code::PermissionDenied
    );
    assert_eq!(
        code(
            f.host
                .verify_system(&f.system("example.life.vote", &unbound_input(), 4))
                .await
        ),
        tonic::Code::PermissionDenied
    );
}
