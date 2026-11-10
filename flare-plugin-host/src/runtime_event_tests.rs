use super::*;
use crate::protocol::{event::*, signature::PublisherTrustStore};
use base64::{Engine, engine::general_purpose::STANDARD};
use ed25519_dalek::{Signer, SigningKey};
use serde_json::json;
#[path = "runtime_event_service_tests.rs"]
mod service_tests;

async fn fixture() -> (
    PluginRuntimeHost,
    PluginCallSigner,
    PluginMessageEvent,
    PluginEventClaims,
) {
    let raw=serde_json::to_vec(&json!({"manifestVersion":1,"pluginId":"observer","version":"1.0.0","apiVersion":"1.0",
      "displayName":"Observer","publisher":{"publisherId":"test","displayName":"Test"},
      "runtime":{"kind":"grpc","service":"test.Plugin","protocol":"flare.capability.v1"},
      "dependencies":[{"pluginId":"tasks","versionRange":">=1.0.0, <2.0.0","optional":true}],
      "capabilities":[{"id":"observer","operations":[{"id":"receive","inputSchema":{"type":"object"},"outputSchema":{"type":"object"},"permissionIds":["observe"],"hostCapabilities":["plugin.service.dispatch"],"idempotent":true,"timeoutMs":1000},{"id":"daily","inputSchema":{"type":"object"},"outputSchema":{"type":"object"},"permissionIds":["observe"],"idempotent":true,"timeoutMs":1000}]}],
      "permissions":[{"id":"observe","description":"Messages","dataScopes":[MESSAGE_EVENTS_SCOPE,MESSAGE_BODY_SCOPE,"plugin.service.dispatch"],"riskLevel":"high"}],
      "eventSubscriptions":[{"eventType":MESSAGE_CREATED,"schemaVersion":1,"operation":"observer.receive","permissionIds":["observe"],"includeBody":true}],
      "features":[{"id":"observe","label":{"en-US":"Observe"},"gates":{"operations":["observer.receive"]}},{"id":"daily","label":{"en-US":"Daily"},"gates":{"operations":["observer.daily"]}}],
      "artifact":{"digest":format!("sha256:{}","a".repeat(64)),"signatureRef":"test"}})).unwrap();
    let publisher = SigningKey::from_bytes(&[90; 32]);
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
    let digest = manifest.digest().to_string();
    let calls = PluginCallSigner::new("core".into(), [91; 32]).unwrap();
    let mut verifier = PluginCallVerifier::default();
    verifier.insert("core".into(), calls.public_key()).unwrap();
    let host = PluginRuntimeHost::new(
        manifest,
        PluginDeployment {
            deployment_id: "deployment".into(),
            plugin_id: "observer".into(),
            plugin_version: "1.0.0".into(),
            manifest_digest: digest.clone(),
            artifact_digest: format!("sha256:{}", "a".repeat(64)),
            grpc_authority: "http://127.0.0.1:1".into(),
        },
        verifier,
        Arc::new(|_| Ok(())),
        Arc::new(crate::state_store::MemoryStore::default()),
    )
    .unwrap();
    let now = now_ms();
    host.apply_tenant(
        tenant_state_request(
            &calls,
            PluginTenantState {
                organization_id: "org".into(),
                deployment_id: "deployment".into(),
                app_id: "app".into(),
                grant_id: "grant".into(),
                revision: 1,
                manifest_digest: digest.clone(),
                configuration_json: b"{}".to_vec(),
                configuration_digest: configuration_digest(&json!({})).unwrap(),
                enabled: true,
                valid_until_ms: now + 60000,
                validate_configuration: true,
                approved_operations: vec!["observer.receive".into(), "observer.daily".into()],
                permissions: vec!["observe".into()],
                data_scopes: vec![
                    MESSAGE_EVENTS_SCOPE.into(),
                    MESSAGE_BODY_SCOPE.into(),
                    "plugin.service.dispatch".into(),
                ],
            },
        )
        .unwrap(),
    )
    .await
    .unwrap();
    let event = PluginMessageEvent {
        id: "message-1".into(),
        event_type: MESSAGE_CREATED.into(),
        schema_version: 1,
        app: "app".into(),
        time: now,
        data: MessageCreatedData {
            server_message_id: "message-1".into(),
            conversation_id: "group".into(),
            actor_id: "member".into(),
            conversation_type: "group".into(),
            text: Some("签到".into()),
        },
    };
    let claims = PluginEventClaims {
        tenant_id: "app".into(),
        plugin_id: "observer".into(),
        plugin_version: "1.0.0".into(),
        manifest_digest: digest,
        deployment_id: "deployment".into(),
        grant_id: "grant".into(),
        grant_revision: 1,
        configuration_digest: configuration_digest(&json!({})).unwrap(),
        enabled_features: ["observe".into(), "daily".into()].into(),
        active_since_ms: 1000,
        operation: "observer.receive".into(),
        event_id: event.id.clone(),
        request_digest: digest_bytes(&serde_json::to_vec(&event).unwrap()),
        actor_id: "member".into(),
        conversation_id: "group".into(),
        issued_at_ms: now,
        expires_at_ms: now + 30000,
    };
    (host, calls, event, claims)
}
fn request(
    event: &PluginMessageEvent,
    claims: &PluginEventClaims,
    signer: &PluginCallSigner,
) -> Request<GenericRequest> {
    let mut request = Request::new(GenericRequest {
        operation: claims.operation.clone(),
        request_id: event.id.clone(),
        payload: Some(prost_types::Any {
            type_url: String::new(),
            value: serde_json::to_vec(event).unwrap(),
        }),
        ..Default::default()
    });
    request.metadata_mut().insert(
        PLUGIN_CALL_HEADER,
        signer
            .sign_event(claims, now_ms())
            .unwrap()
            .parse()
            .unwrap(),
    );
    request
}
#[tokio::test]
async fn event_verifier_binds_payload_deployment_application_grant_and_refuses_user_domain() {
    let (host, signer, event, claims) = fixture().await;
    let verified = host
        .verify_event_call(&request(&event, &claims, &signer))
        .await
        .unwrap();
    assert_eq!(verified.actor_id(), "member");
    assert_eq!(verified.conversation_id(), "group");
    assert!(verified.operation_enabled("observer.daily"));
    assert!(!verified.operation_enabled("observer.receive"));
    let mut closed_daily = claims.clone();
    closed_daily.enabled_features.remove("daily");
    let event_without_daily = host
        .verify_event_call(&request(&event, &closed_daily, &signer))
        .await
        .unwrap();
    assert!(!event_without_daily.operation_enabled("observer.daily"));
    assert_eq!(event_without_daily.configuration(), &json!({}));
    let mut closed_callback = claims.clone();
    closed_callback.enabled_features.remove("observe");
    assert!(
        host.verify_event_call(&request(&event, &closed_callback, &signer))
            .await
            .is_err()
    );
    assert!(
        host.verify_call(&request(&event, &claims, &signer))
            .await
            .is_err()
    );
    let mut changed = event.clone();
    changed.data.text = Some("other".into());
    assert!(
        host.verify_event_call(&request(&changed, &claims, &signer))
            .await
            .is_err()
    );
    for which in 0..4 {
        let mut bad = claims.clone();
        match which {
            0 => bad.deployment_id = "other".into(),
            1 => bad.grant_revision = 2,
            2 => bad.tenant_id = "other".into(),
            _ => bad.configuration_digest = format!("sha256:{}", "c".repeat(64)),
        }
        assert!(
            host.verify_event_call(&request(&event, &bad, &signer))
                .await
                .is_err()
        );
    }
    let mut unsigned = Request::new(request(&event, &claims, &signer).into_inner());
    unsigned
        .get_mut()
        .metadata
        .insert("user_id".into(), "member".into());
    assert!(host.verify_event_call(&unsigned).await.is_err());
}
