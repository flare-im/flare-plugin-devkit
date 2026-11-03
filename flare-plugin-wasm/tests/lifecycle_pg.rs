//! Real signed gRPC + WASM + shared PG, exclusively in the owned fixture.
use base64::{Engine, engine::general_purpose::STANDARD};
use ed25519_dalek::{
    Signer, SigningKey,
    pkcs8::{EncodePrivateKey, EncodePublicKey},
};
use flare_grpc_proto::capability::{
    GenericRequest, PluginTenantState, extension_plugin_client::ExtensionPluginClient,
    extension_plugin_server::ExtensionPluginServer, plugin_runtime_client::PluginRuntimeClient,
    plugin_runtime_server::PluginRuntimeServer,
};
use flare_plugin_host::{PluginRuntimeHost, state_store::PostgresTenantStateStore};
use flare_plugin_protocol::{
    invocation::{PLUGIN_CALL_HEADER, PluginCallClaims, PluginCallSigner, PluginCallVerifier},
    runtime::{PluginDeployment, configuration_digest, digest_bytes},
    signature::PublisherTrustStore,
};
use flare_plugin_wasm::{ABI_VERSION, WasmSandbox, service::WasmPluginService};
use flare_server_core::TokenService;
use serde_json::{Value, json};
use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tonic::{Request, Status, transport::Channel};

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}
struct Fixture {
    manifest: flare_plugin_protocol::signature::VerifiedManifest,
    sandbox: Arc<WasmSandbox>,
    issuer: TokenService,
    platform_public: String,
    calls: PluginCallSigner,
}
impl Fixture {
    fn new() -> Self {
        let bytes = wat::parse_str(r#"(module
            (memory (export "memory") 1 2)
            (func (export "flare_alloc") (param i32) (result i32) i32.const 4096)
            (func (export "flare_validate") (param i32 i32) (result i32) i32.const 0)
            (func (export "flare_call") (param $p i32) (param $n i32) (result i64)
                (i64.or (i64.shl (i64.extend_i32_u (local.get $p)) (i64.const 32)) (i64.extend_i32_u (local.get $n)))))"#).unwrap();
        let raw = serde_json::to_vec(&json!({"manifestVersion":1,"pluginId":"fixture.wasm","version":"1.0.0","apiVersion":"1.0","displayName":"WASM lifecycle fixture","publisher":{"publisherId":"fixture","displayName":"Fixture"},"runtime":{"kind":"wasm","module":"fixture.wasm","abiVersion":ABI_VERSION,"maxMemoryPages":2,"maxFuel":100000},"capabilities":[{"id":"fixture.wasm","operations":[{"id":"run","inputSchema":{"type":"object"},"outputSchema":{"type":"object"},"permissionIds":[],"timeoutMs":2000,"idempotent":true}]}],"permissions":[],"artifact":{"digest":digest_bytes(&bytes),"signatureRef":"fixture"}})).unwrap();
        let publisher = SigningKey::from_bytes(&[81; 32]);
        let mut trust = PublisherTrustStore::default();
        trust
            .insert(
                "fixture".into(),
                "fixture".into(),
                publisher.verifying_key().to_bytes(),
            )
            .unwrap();
        let manifest = trust
            .verify(
                &raw,
                "fixture",
                &STANDARD.encode(publisher.sign(&raw).to_bytes()),
            )
            .unwrap();
        let sandbox = Arc::new(WasmSandbox::new(&manifest, &bytes).unwrap());
        let platform = SigningKey::from_bytes(&[82; 32]);
        let issuer = TokenService::from_ed25519_private_key(
            &platform.to_pkcs8_pem(Default::default()).unwrap(),
            "flare-platform",
            60,
        )
        .unwrap();
        Self {
            manifest,
            sandbox,
            issuer,
            platform_public: platform
                .verifying_key()
                .to_public_key_pem(Default::default())
                .unwrap(),
            calls: PluginCallSigner::new("wasm-fixture".into(), [83; 32]).unwrap(),
        }
    }
    fn host(&self, pool: sqlx::PgPool) -> PluginRuntimeHost {
        let mut verifier = PluginCallVerifier::default();
        verifier
            .insert(self.calls.key_id().into(), self.calls.public_key())
            .unwrap();
        let sandbox = self.sandbox.clone();
        PluginRuntimeHost::new(
            self.manifest.clone(),
            PluginDeployment {
                deployment_id: "wasm-1".into(),
                plugin_id: "fixture.wasm".into(),
                plugin_version: "1.0.0".into(),
                manifest_digest: self.manifest.digest().into(),
                artifact_digest: self
                    .manifest
                    .manifest()
                    .artifact
                    .as_ref()
                    .unwrap()
                    .digest
                    .clone(),
                grpc_authority: "http://127.0.0.1:7903".into(),
            },
            &self.platform_public,
            verifier,
            Arc::new(move |value| {
                sandbox
                    .validate_configuration(value)
                    .map_err(|_| Status::invalid_argument("configuration rejected"))
            }),
            Arc::new(PostgresTenantStateStore::new(pool)),
        )
        .unwrap()
    }
    fn state(&self, tenant: &str, revision: u64, enabled: bool) -> PluginTenantState {
        let configuration = json!({"tenantSetting":tenant});
        PluginTenantState {
            deployment_id: "wasm-1".into(),
            app_id: tenant.into(),
            grant_id: format!("grant-{tenant}"),
            revision,
            manifest_digest: self.manifest.digest().into(),
            configuration_digest: configuration_digest(&configuration).unwrap(),
            configuration_json: serde_json::to_vec(&configuration).unwrap(),
            enabled,
            valid_until_ms: now() + 60000,
            validate_configuration: true,
        }
    }
    fn control(&self, state: PluginTenantState) -> Request<PluginTenantState> {
        let token = self
            .issuer
            .generate_with_ed25519(
                "flare-platform-worker",
                Some(&state.app_id),
                "control",
                "flare-plugin-runtime:wasm-1",
            )
            .unwrap();
        let mut request = Request::new(state);
        request
            .metadata_mut()
            .insert("authorization", format!("Bearer {token}").parse().unwrap());
        request
    }
    fn call(&self, tenant: &str, revision: u64) -> Request<GenericRequest> {
        let state = self.state(tenant, revision, true);
        let time = now();
        let claims = PluginCallClaims {
            tenant_id: tenant.into(),
            plugin_id: "fixture.wasm".into(),
            plugin_version: "1.0.0".into(),
            manifest_digest: self.manifest.digest().into(),
            deployment_id: "wasm-1".into(),
            grant_id: state.grant_id,
            grant_revision: revision,
            configuration_digest: state.configuration_digest,
            actor_id: format!("user-{tenant}"),
            operation: "fixture.wasm.run".into(),
            request_id: format!("req-{tenant}-{revision}"),
            conversation_id: Some("conversation-1".into()),
            issued_at_ms: time,
            expires_at_ms: time + 10000,
        };
        let mut request = Request::new(GenericRequest {
            operation: claims.operation.clone(),
            request_id: claims.request_id.clone(),
            payload: Some(prost_types::Any {
                type_url: "json".into(),
                value: br#"{"hello":"wasm"}"#.to_vec(),
            }),
            ..Default::default()
        });
        request.metadata_mut().insert(
            PLUGIN_CALL_HEADER,
            self.calls.sign(&claims, time).unwrap().parse().unwrap(),
        );
        request
    }
}
struct Server {
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    work: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
    channel: Channel,
}
impl Server {
    async fn start(host: PluginRuntimeHost, sandbox: Arc<WasmSandbox>) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = format!("http://{}", listener.local_addr().unwrap());
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let service = WasmPluginService::new(host.clone(), sandbox, None).unwrap();
        let work = tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(PluginRuntimeServer::new(host))
                .add_service(ExtensionPluginServer::new(service))
                .serve_with_incoming_shutdown(
                    tokio_stream::wrappers::TcpListenerStream::new(listener),
                    async {
                        let _ = stopped.await;
                    },
                ),
        );
        let channel = Channel::from_shared(address)
            .unwrap()
            .connect_timeout(Duration::from_secs(2))
            .connect()
            .await
            .unwrap();
        Self {
            stop: Some(stop),
            work,
            channel,
        }
    }
    async fn close(mut self) {
        let _ = self.stop.take().unwrap().send(());
        (&mut self.work).await.unwrap().unwrap();
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        self.work.abort();
    }
}
async fn invoke(server: &Server, request: Request<GenericRequest>) -> Result<Value, Status> {
    invoke_channel(server.channel.clone(), request).await
}
async fn invoke_channel(
    channel: Channel,
    request: Request<GenericRequest>,
) -> Result<Value, Status> {
    let response = ExtensionPluginClient::new(channel)
        .call(request)
        .await?
        .into_inner();
    assert!(response.ok);
    Ok(serde_json::from_slice(&response.payload.unwrap().value).unwrap())
}

#[tokio::test]
#[ignore = "requires explicitly owned disposable plugin PostgreSQL fixture"]
async fn wasm_owned_postgres_signed_replicas_lifecycle_and_tenant_isolation() {
    let url = std::env::var("FLARE_PLUGIN_TEST_DATABASE_URL").unwrap();
    let admin = sqlx::PgPool::connect(&url).await.unwrap();
    let owned:bool=sqlx::query_scalar("SELECT current_database() LIKE 'flare_plugin_test_%' AND shobj_description((SELECT oid FROM pg_database WHERE datname=current_database()),'pg_database')='flare-plugin-owned/v1'").fetch_one(&admin).await.unwrap();
    assert!(owned, "refuse unowned fixture");
    sqlx::query("CREATE SCHEMA wasm_lifecycle_test")
        .execute(&admin)
        .await
        .unwrap();
    let mut tx = admin.begin().await.unwrap();
    sqlx::query("SET LOCAL search_path=wasm_lifecycle_test")
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::raw_sql(include_str!("../../flare-plugin-host/initial/runtime.sql"))
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let options: sqlx::postgres::PgConnectOptions = url.parse().unwrap();
    let options = options.options([("search_path", "wasm_lifecycle_test")]);
    let pool = sqlx::PgPool::connect_with(options).await.unwrap();
    let fixture = Fixture::new();
    let a_host = fixture.host(pool.clone());
    let b_host = fixture.host(pool.clone());
    let a = Server::start(a_host.clone(), fixture.sandbox.clone()).await;
    let b = Server::start(b_host.clone(), fixture.sandbox.clone()).await;
    let mut control = PluginRuntimeClient::new(a.channel.clone());
    assert!(invoke(&b, fixture.call("201", 1)).await.is_err());
    control
        .apply_tenant(fixture.control(fixture.state("201", 1, false)))
        .await
        .unwrap();
    assert!(invoke(&b, fixture.call("201", 1)).await.is_err());
    control
        .apply_tenant(fixture.control(fixture.state("201", 2, true)))
        .await
        .unwrap();
    control
        .apply_tenant(fixture.control(fixture.state("202", 1, true)))
        .await
        .unwrap();
    let mut calls = tokio::task::JoinSet::new();
    for index in 0..16 {
        let server = if index % 2 == 0 { &a } else { &b };
        let tenant = if index % 3 == 0 { "202" } else { "201" };
        let revision = if tenant == "202" { 1 } else { 2 };
        let channel = server.channel.clone();
        let request = fixture.call(tenant, revision);
        calls.spawn(async move {
            let value = invoke_channel(channel, request).await;
            (tenant, value)
        });
    }
    // Bound parallelism to service capacity per replica. The capacity contract
    // allows load shedding; every accepted result must retain its own tenant.
    let mut accepted = 0;
    while let Some(result) = calls.join_next().await {
        let (tenant, value) = result.unwrap();
        match value {
            Ok(value) => {
                accepted += 1;
                assert_eq!(value["context"]["tenantId"], tenant);
                assert_eq!(value["configuration"]["tenantSetting"], tenant);
                assert_eq!(value["payload"]["hello"], "wasm");
                assert!(value.to_string().find("x-flare-plugin-call").is_none());
            }
            Err(status) => assert_eq!(status.code(), tonic::Code::ResourceExhausted),
        }
    }
    assert!(accepted > 0);
    let mut forged = fixture.call("201", 2);
    forged.get_mut().request_id = "different-request".into();
    assert!(invoke(&a, forged).await.is_err());
    let mut control_b = PluginRuntimeClient::new(b.channel.clone());
    control_b
        .apply_tenant(fixture.control(fixture.state("201", 3, false)))
        .await
        .unwrap();
    assert!(invoke(&a, fixture.call("201", 2)).await.is_err());
    assert!(
        control
            .apply_tenant(fixture.control(fixture.state("201", 2, true)))
            .await
            .is_err()
    );
    assert!(invoke(&a, fixture.call("202", 1)).await.is_ok());
    a_host.drain(Duration::from_secs(1)).await.unwrap();
    assert!(invoke(&a, fixture.call("202", 1)).await.is_err());
    assert!(invoke(&b, fixture.call("202", 1)).await.is_ok());
    a.close().await;
    b.close().await;
    let restarted = Server::start(fixture.host(pool.clone()), fixture.sandbox.clone()).await;
    assert!(invoke(&restarted, fixture.call("201", 2)).await.is_err());
    assert!(invoke(&restarted, fixture.call("202", 1)).await.is_ok());
    PluginRuntimeClient::new(restarted.channel.clone())
        .apply_tenant(fixture.control(fixture.state("201", 4, true)))
        .await
        .unwrap();
    assert!(invoke(&restarted, fixture.call("201", 4)).await.is_ok());
    restarted.close().await;
    pool.close().await;
    admin.close().await;
}
