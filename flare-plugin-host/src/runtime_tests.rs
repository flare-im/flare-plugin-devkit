use super::*;
use base64::{Engine, engine::general_purpose::STANDARD};
use ed25519_dalek::{
    Signer, SigningKey,
    pkcs8::{EncodePrivateKey, EncodePublicKey},
};
use flare_plugin_protocol::{
    invocation::{PluginCallClaims, PluginCallSigner},
    signature::PublisherTrustStore,
};
use serde_json::json;

fn fixture() -> (PluginRuntimeHost, TokenService, PluginCallSigner) {
    fixture_with_store(Arc::new(crate::state_store::MemoryStore::default()))
}
fn fixture_with_store(
    store: Arc<dyn crate::state_store::TenantStateStore>,
) -> (PluginRuntimeHost, TokenService, PluginCallSigner) {
    let raw=serde_json::to_vec(&json!({"manifestVersion":1,"pluginId":"example.echo","version":"1.0.0","apiVersion":"1.0","displayName":"Echo","publisher":{"publisherId":"test","displayName":"Test"},"runtime":{"kind":"grpc","service":"example.echo.v1.PluginService","protocol":"flare.capability.v1"},"capabilities":[{"id":"example.echo","operations":[{"id":"run","inputSchema":{"type":"object"},"outputSchema":{"type":"object"},"permissionIds":[],"idempotent":true,"timeoutMs":1000}]}],"permissions":[],"artifact":{"digest":format!("sha256:{}","a".repeat(64)),"signatureRef":"test"}})).unwrap();
    let publisher = SigningKey::from_bytes(&[61; 32]);
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
    let key = SigningKey::from_bytes(&[62; 32]);
    let issuer = TokenService::from_ed25519_private_key(
        &key.to_pkcs8_pem(Default::default()).unwrap(),
        "flare-platform",
        60,
    )
    .unwrap();
    let calls = PluginCallSigner::new("call-key".into(), [63; 32]).unwrap();
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
        Arc::new(|value| {
            if value.get("reject").is_some() {
                Err(Status::invalid_argument("rejected"))
            } else {
                Ok(())
            }
        }),
        store,
    )
    .unwrap();
    (host, issuer, calls)
}
fn state(host: &PluginRuntimeHost, revision: u64, enabled: bool) -> PluginTenantState {
    PluginTenantState {
        organization_id: "org-1".into(),
        deployment_id: "echo-1".into(),
        app_id: "201".into(),
        grant_id: "grant-1".into(),
        revision,
        manifest_digest: host.0.deployment.manifest_digest.clone(),
        configuration_json: b"{}".to_vec(),
        configuration_digest: configuration_digest(&json!({})).unwrap(),
        enabled,
        valid_until_ms: now_ms() + 300000,
        validate_configuration: true,
        approved_operations: Vec::new(),
        permissions: Vec::new(),
        data_scopes: Vec::new(),
    }
}
fn assertion(
    issuer: &TokenService,
    state: PluginTenantState,
    tenant: &str,
    subject: &str,
    audience: &str,
) -> Request<PluginTenantState> {
    let token = issuer
        .generate_with_ed25519(subject, Some(tenant), "control", audience)
        .unwrap();
    let mut req = Request::new(state);
    req.metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    req
}
fn signed(issuer: &TokenService, state: PluginTenantState) -> Request<PluginTenantState> {
    assertion(
        issuer,
        state,
        "201",
        "flare-platform-worker",
        "flare-plugin-runtime:echo-1:org-1",
    )
}

#[tokio::test]
async fn background_upgrade_resolves_only_this_deployments_current_tenant_grant() {
    let memory = Arc::new(crate::state_store::MemoryStore::default());
    let (host, issuer, calls) = fixture_with_store(memory.clone());
    let mut current = state(&host, 1, true);
    current.grant_id = "new-installation".into();
    host.apply_tenant(signed(&issuer, current.clone()))
        .await
        .unwrap();
    assert_eq!(
        host.current_grant_id("201", "old-installation", true)
            .await
            .unwrap(),
        "new-installation"
    );
    assert_eq!(
        host.current_grant_id("201", "old-installation", false)
            .await
            .unwrap(),
        "new-installation"
    );
    assert!(
        host.current_grant_id("202", "old-installation", true)
            .await
            .is_err()
    );
    assert!(
        host.verify_call(&invocation(&host, &calls, 1))
            .await
            .is_err(),
        "user calls must still name their exact admitted grant"
    );

    current.enabled = false;
    current.revision = 2;
    host.apply_tenant(signed(&issuer, current.clone()))
        .await
        .unwrap();
    assert!(
        host.current_grant_id("201", "old-installation", false)
            .await
            .is_err(),
        "disabled installations cannot publish"
    );
    assert_eq!(
        host.current_grant_id("201", "old-installation", true)
            .await
            .unwrap(),
        "new-installation",
        "disabled installations retain recovery only"
    );

    memory
        .0
        .write()
        .unwrap()
        .values_mut()
        .for_each(|s| s.valid_until_ms = 1);
    assert!(
        host.current_grant_id("201", "old-installation", true)
            .await
            .is_err(),
        "expired control assertions never authorize recovery"
    );
    current.enabled = true;
    current.revision = 3;
    host.apply_tenant(signed(&issuer, current.clone()))
        .await
        .unwrap();
    let mut other = current;
    other.grant_id = "ambiguous-installation".into();
    host.apply_tenant(signed(&issuer, other)).await.unwrap();
    assert!(
        host.current_grant_id("201", "old-installation", true)
            .await
            .is_err(),
        "never choose arbitrarily between installations"
    );
}
fn invocation(
    host: &PluginRuntimeHost,
    signer: &PluginCallSigner,
    revision: u64,
) -> Request<GenericRequest> {
    let now = now_ms();
    let claims = PluginCallClaims {
        tenant_id: "201".into(),
        plugin_id: "example.echo".into(),
        plugin_version: "1.0.0".into(),
        manifest_digest: host.0.deployment.manifest_digest.clone(),
        deployment_id: "echo-1".into(),
        grant_id: "grant-1".into(),
        grant_revision: revision,
        configuration_digest: configuration_digest(&json!({})).unwrap(),
        actor_id: "u1".into(),
        operation: "example.echo.run".into(),
        request_id: "r1".into(),
        conversation_id: None,
        issued_at_ms: now,
        expires_at_ms: now + 1000,
    };
    let mut req = Request::new(GenericRequest {
        operation: claims.operation.clone(),
        request_id: "r1".into(),
        ..Default::default()
    });
    req.metadata_mut().insert(
        PLUGIN_CALL_HEADER,
        signer.sign(&claims, now).unwrap().parse().unwrap(),
    );
    req
}
#[tokio::test]
async fn installation_configuration_enable_disable_and_stale_replay_are_fenced() {
    let memory = Arc::new(crate::state_store::MemoryStore::default());
    let (host, issuer, calls) = fixture_with_store(memory.clone());
    assert!(
        host.verify_call(&invocation(&host, &calls, 1))
            .await
            .is_err()
    );
    host.apply_tenant(signed(&issuer, state(&host, 1, false)))
        .await
        .unwrap();
    assert!(
        host.verify_call(&invocation(&host, &calls, 1))
            .await
            .is_err()
    );
    host.apply_tenant(signed(&issuer, state(&host, 2, true)))
        .await
        .unwrap();
    assert_eq!(
        host.verify_call(&invocation(&host, &calls, 2))
            .await
            .unwrap()
            .tenant_id,
        "201"
    );
    host.apply_tenant(signed(&issuer, state(&host, 2, true)))
        .await
        .unwrap();
    assert!(
        host.apply_tenant(signed(&issuer, state(&host, 2, false)))
            .await
            .is_err()
    );
    host.apply_tenant(signed(&issuer, state(&host, 3, false)))
        .await
        .unwrap();
    assert!(
        host.verify_call(&invocation(&host, &calls, 2))
            .await
            .is_err()
    );
    assert!(
        host.apply_tenant(signed(&issuer, state(&host, 2, true)))
            .await
            .is_err()
    );
    // Tombstones remain fenced even after expiry.
    memory
        .0
        .write()
        .unwrap()
        .values_mut()
        .next()
        .unwrap()
        .valid_until_ms = 1;
    assert!(
        host.apply_tenant(signed(&issuer, state(&host, 2, true)))
            .await
            .is_err()
    );
}
#[tokio::test]
async fn runtime_rejects_wrong_principal_audience_tenant_and_digest() {
    let (host, issuer, _) = fixture();
    for (tenant, subject, audience) in [
        (
            "202",
            "flare-platform-worker",
            "flare-plugin-runtime:echo-1:org-1",
        ),
        ("201", "employee", "flare-plugin-runtime:echo-1:org-1"),
        ("201", "flare-platform-worker", "flare-core-projection"),
    ] {
        assert!(
            host.apply_tenant(assertion(
                &issuer,
                state(&host, 1, true),
                tenant,
                subject,
                audience
            ))
            .await
            .is_err()
        );
    }
    let mut bad = state(&host, 1, true);
    bad.configuration_json = b"{\"reject\":true}".to_vec();
    bad.configuration_digest = configuration_digest(&json!({"reject":true})).unwrap();
    assert!(host.apply_tenant(signed(&issuer, bad)).await.is_err());
    let mut bad = state(&host, 1, true);
    bad.configuration_digest = format!("sha256:{}", "b".repeat(64));
    assert!(host.apply_tenant(signed(&issuer, bad)).await.is_err());
    let mut req = signed(&issuer, state(&host, 1, true));
    let auth = req.metadata().get("authorization").unwrap().clone();
    req.metadata_mut().append("authorization", auth);
    assert!(host.apply_tenant(req).await.is_err());
}
#[tokio::test]
async fn invocation_requires_dedicated_ticket_and_current_config_and_body_bindings() {
    let (host, issuer, calls) = fixture();
    host.apply_tenant(signed(&issuer, state(&host, 1, true)))
        .await
        .unwrap();
    // A ticket that does not match its call is refused for good.
    let code = |result: Result<VerifiedPluginUser, Status>| result.err().map(|e| e.code());
    let mut req = invocation(&host, &calls, 1);
    req.get_mut().operation = "example.echo.other".into();
    assert_eq!(
        code(host.verify_call(&req).await),
        Some(tonic::Code::PermissionDenied)
    );
    let mut req = invocation(&host, &calls, 1);
    req.get_mut()
        .metadata
        .insert("tenant_id".into(), "202".into());
    assert_eq!(
        code(host.verify_call(&req).await),
        Some(tonic::Code::PermissionDenied)
    );
    let mut req = invocation(&host, &calls, 1);
    req.get_mut().request_id = "other".into();
    assert_eq!(
        code(host.verify_call(&req).await),
        Some(tonic::Code::PermissionDenied)
    );
    let mut req = invocation(&host, &calls, 1);
    let ticket = req.metadata().get(PLUGIN_CALL_HEADER).unwrap().clone();
    req.metadata_mut().append(PLUGIN_CALL_HEADER, ticket);
    assert!(host.verify_call(&req).await.is_err());
    let mut req = invocation(&host, &calls, 1);
    req.metadata_mut().remove(PLUGIN_CALL_HEADER);
    req.metadata_mut()
        .insert("x-flare-ctx", "original-cell-context".parse().unwrap());
    assert!(host.verify_call(&req).await.is_err());
    let changed = json!({"option":true});
    let mut update = state(&host, 2, true);
    update.configuration_json = serde_json::to_vec(&changed).unwrap();
    update.configuration_digest = configuration_digest(&changed).unwrap();
    host.apply_tenant(signed(&issuer, update)).await.unwrap();
    // Core signed under the previous revision: the projections have not converged yet, so the
    // call did not run and Core may answer "outcome unknown" instead of a rejection.
    assert_eq!(
        code(host.verify_call(&invocation(&host, &calls, 1)).await),
        Some(tonic::Code::Unavailable)
    );
    host.set_draining(true);
    assert!(
        host.verify_call(&invocation(&host, &calls, 2))
            .await
            .is_err()
    );
}
#[tokio::test]
async fn shared_replica_and_restart_preserve_fences_and_lease_expiry_denies_calls() {
    let memory = Arc::new(crate::state_store::MemoryStore::default());
    let (host, issuer, calls) = fixture_with_store(memory.clone());
    host.apply_tenant(signed(&issuer, state(&host, 1, true)))
        .await
        .unwrap();
    let ticket = invocation(&host, &calls, 1);
    assert!(host.verify_call(&ticket).await.is_ok());
    let (fresh, _, _) = fixture_with_store(memory.clone());
    assert!(fresh.verify_call(&ticket).await.is_ok());
    fresh
        .apply_tenant(signed(&issuer, state(&host, 2, false)))
        .await
        .unwrap();
    assert!(host.verify_call(&ticket).await.is_err());
    assert!(
        host.apply_tenant(signed(&issuer, state(&host, 1, true)))
            .await
            .is_err()
    );
    memory
        .0
        .write()
        .unwrap()
        .values_mut()
        .next()
        .unwrap()
        .valid_until_ms = 1;
    assert!(host.verify_call(&ticket).await.is_err());
}

#[tokio::test]
async fn disabled_preparation_validates_config_but_emergency_deny_remains_available() {
    let (host, issuer, _) = fixture();
    let mut invalid = state(&host, 1, false);
    let config = json!({"reject":true});
    invalid.configuration_json = serde_json::to_vec(&config).unwrap();
    invalid.configuration_digest = configuration_digest(&config).unwrap();
    assert!(
        host.apply_tenant(signed(&issuer, invalid.clone()))
            .await
            .is_err()
    );
    invalid.validate_configuration = false;
    assert!(
        !host
            .apply_tenant(signed(&issuer, invalid))
            .await
            .unwrap()
            .into_inner()
            .enabled
    );
}

#[tokio::test]
async fn drain_tracks_cloned_admission_and_cancellation() {
    let (host, issuer, calls) = fixture();
    host.apply_tenant(signed(&issuer, state(&host, 1, true)))
        .await
        .unwrap();
    let admitted = host
        .verify_call(&invocation(&host, &calls, 1))
        .await
        .unwrap();
    let retained = admitted.clone();
    let provider = host.admit_work().unwrap();
    assert_eq!(host.in_flight(), 2);
    drop(admitted);
    assert!(host.drain(std::time::Duration::ZERO).await.is_err());
    assert!(
        host.verify_call(&invocation(&host, &calls, 1))
            .await
            .is_err()
    );
    drop(retained);
    assert!(host.admit_work().is_err());
    assert_eq!(host.in_flight(), 1);
    drop(provider);
    host.drain(std::time::Duration::from_secs(1)).await.unwrap();
    assert_eq!(host.in_flight(), 0);
}

#[tokio::test]
#[ignore = "requires explicitly owned FLARE_PLUGIN_TEST_DATABASE_URL"]
async fn postgres_replicas_restart_durable_tombstones_and_concurrent_revision() {
    use crate::state_store::{PostgresTenantStateStore, TenantStateStore};
    let url = std::env::var("FLARE_PLUGIN_TEST_DATABASE_URL").unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();
    let owned:bool=sqlx::query_scalar("SELECT current_database() LIKE 'flare_plugin_test_%' AND shobj_description((SELECT oid FROM pg_database WHERE datname=current_database()),'pg_database')='flare-plugin-owned/v1'").fetch_one(&pool).await.unwrap();
    assert!(owned, "refuse an unowned fixture");
    sqlx::query("CREATE SCHEMA host_durability_test")
        .execute(&pool)
        .await
        .unwrap();
    let mut tx = pool.begin().await.unwrap();
    sqlx::query("SET LOCAL search_path=host_durability_test")
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::raw_sql(include_str!("../initial/runtime.sql"))
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let options: sqlx::postgres::PgConnectOptions = url.parse().unwrap();
    let options = options.options([("search_path", "host_durability_test")]);
    let store = Arc::new(PostgresTenantStateStore::new(
        sqlx::PgPool::connect_with(options.clone()).await.unwrap(),
    ));
    let (a, issuer, calls) = fixture_with_store(store.clone());
    let (b, _, _) = fixture_with_store(Arc::new(PostgresTenantStateStore::new(
        sqlx::PgPool::connect_with(options.clone()).await.unwrap(),
    )));
    a.apply_tenant(signed(&issuer, state(&a, 1, true)))
        .await
        .unwrap();
    let active = b.verify_call(&invocation(&b, &calls, 1)).await.unwrap();
    assert_eq!(active.configuration, json!({}));
    let old = state(&a, 1, true);
    let newer = state(&a, 3, false);
    let middle = state(&a, 2, true);
    let _ = tokio::join!(
        a.apply_tenant(signed(&issuer, middle)),
        b.apply_tenant(signed(&issuer, newer))
    );
    assert_eq!(
        store
            .get("echo-1", "201", "grant-1")
            .await
            .unwrap()
            .unwrap()
            .revision,
        3
    );
    assert!(a.verify_call(&invocation(&a, &calls, 1)).await.is_err());
    assert!(a.apply_tenant(signed(&issuer, old.clone())).await.is_err());
    assert!(
        store
            .get("echo-1", "202", "grant-1")
            .await
            .unwrap()
            .is_none()
    );
    // In-flight work retains its admitted config; disabled state denies new work.
    assert_eq!(active.grant_revision, 1);
    drop(a);
    drop(b);
    drop(store);
    let (restarted, _, _) = fixture_with_store(Arc::new(PostgresTenantStateStore::new(
        sqlx::PgPool::connect_with(options).await.unwrap(),
    )));
    assert!(restarted.apply_tenant(signed(&issuer, old)).await.is_err());
    assert!(
        restarted
            .verify_call(&invocation(&restarted, &calls, 1))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn data_tickets_bind_purpose_body_grant_and_revision() {
    use flare_plugin_protocol::data::*;
    let (host, issuer, _) = fixture();
    host.apply_tenant(signed(&issuer, state(&host, 1, false)))
        .await
        .unwrap();
    let data = DataRequest {
        app_id: "201".into(),
        deployment_id: "echo-1".into(),
        manifest_digest: host.manifest_digest().into(),
        grant_id: "grant-1".into(),
        grant_revision: 1,
        job_id: "job-1".into(),
        fence: 1,
        action: DataAction::Export,
    };
    let token = issuer
        .generate_with_ed25519(
            "flare-platform-worker",
            Some("201"),
            TOKEN_USE,
            &data.audience().unwrap(),
        )
        .unwrap();
    let request = |data: &DataRequest, token: &str| {
        let mut req = Request::new(GenericRequest {
            operation: OPERATION.into(),
            request_id: data.job_id.clone(),
            payload: Some(prost_types::Any {
                type_url: String::new(),
                value: serde_json::to_vec(data).unwrap(),
            }),
            ..Default::default()
        });
        req.metadata_mut()
            .insert("authorization", format!("Bearer {token}").parse().unwrap());
        req
    };
    host.verify_data_request(&request(&data, &token))
        .await
        .unwrap(); // disabled exports are valid
    for changed in [
        DataRequest {
            fence: 2,
            ..data.clone()
        },
        DataRequest {
            app_id: "202".into(),
            ..data.clone()
        },
        DataRequest {
            action: DataAction::Read {
                offset: 0,
                digest: configuration_digest(&json!({})).unwrap(),
            },
            ..data.clone()
        },
    ] {
        assert!(
            host.verify_data_request(&request(&changed, &token))
                .await
                .is_err()
        );
    }
    let wrong = issuer
        .generate_with_ed25519(
            "flare-platform-worker",
            Some("201"),
            "control",
            "flare-plugin-runtime:echo-1:org-1",
        )
        .unwrap();
    assert!(
        host.verify_data_request(&request(&data, &wrong))
            .await
            .is_err()
    );
    host.apply_tenant(signed(&issuer, state(&host, 2, false)))
        .await
        .unwrap();
    assert!(
        host.verify_data_request(&request(&data, &token))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn inspect_reports_the_data_protocol_only_when_the_binary_declares_it() {
    let (host, _, _) = fixture();
    let plain = PluginRuntime::inspect(&host, Request::new(PluginRuntimeInspectRequest {}))
        .await
        .unwrap()
        .into_inner();
    assert!(plain.protocols.is_empty());
    let (host, _, _) = fixture();
    let host = host.with_data_protocol().unwrap();
    let info = PluginRuntime::inspect(&host, Request::new(PluginRuntimeInspectRequest {}))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(info.protocols, [flare_plugin_protocol::data::OPERATION]);
    // The host is not shared yet only before serving.
    let shared = host.clone();
    assert!(host.with_data_protocol().is_err());
    drop(shared);
}
