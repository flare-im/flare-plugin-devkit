use base64::{Engine, engine::general_purpose::STANDARD};
use ed25519_dalek::{Signer, SigningKey};
use flare_plugin_protocol::{
    runtime::digest_bytes,
    signature::{PublisherTrustStore, VerifiedManifest},
};
use flare_plugin_wasm::*;
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::{Duration, Instant};

fn signed(bytes: &[u8], host: bool, pages: u32, fuel: u64) -> VerifiedManifest {
    signed_with_schema(bytes, host, pages, fuel, None).unwrap()
}
fn signed_with_schema(
    bytes: &[u8],
    host: bool,
    pages: u32,
    fuel: u64,
    schema: Option<serde_json::Value>,
) -> Result<VerifiedManifest, flare_plugin_protocol::ProtocolError> {
    let key = SigningKey::from_bytes(&[62; 32]);
    let scope = if host {
        vec!["im.conversation.read"]
    } else {
        vec![]
    };
    let mut value = json!({"manifestVersion":1,"pluginId":"example.wasm","version":"0.1.0","apiVersion":"1.0","displayName":"Isolated WASM","publisher":{"publisherId":"fixture","displayName":"Fixture"},"runtime":{"kind":"wasm","module":"plugin.wasm","abiVersion":ABI_VERSION,"maxMemoryPages":pages,"maxFuel":fuel},"capabilities":[{"id":"example.wasm","operations":[{"id":"run","inputSchema":{"type":"object"},"outputSchema":{"type":"object"},"permissionIds":["example.wasm.run"],"hostCapabilities":scope,"idempotent":true,"timeoutMs":1000}]}],"permissions":[{"id":"example.wasm.run","description":"Read the bound conversation","dataScopes":scope,"riskLevel":"high"}],"features":[{"id":"run","label":{"en-US":"Run"},"gates":{"operations":["example.wasm.run"]}}],"artifact":{"digest":digest_bytes(bytes),"signatureRef":"fixture"}});
    if let Some(schema) = schema {
        value["capabilities"][0]["operations"][0]["inputSchema"] = schema;
    }
    let raw = serde_json::to_vec(&value).unwrap();
    let mut trust = PublisherTrustStore::default();
    trust
        .insert(
            "fixture-key".into(),
            "fixture".into(),
            key.verifying_key().to_bytes(),
        )
        .unwrap();
    trust.verify(
        &raw,
        "fixture-key",
        &STANDARD.encode(key.sign(&raw).to_bytes()),
    )
}
fn module(body: &str) -> Vec<u8> {
    wat::parse_str(format!(
        r#"(module
    (memory (export "memory") 1 8)
    (func (export "flare_alloc") (param i32) (result i32) (i32.const 4096))
    (func (export "flare_validate") (param i32 i32) (result i32) (i32.const 0))
    (func (export "flare_call") (param $ptr i32) (param $len i32) (result i64) {body})
    )"#
    ))
    .unwrap()
}
fn invocation(tenant: &str) -> Invocation {
    Invocation {
        operation: "example.wasm.run".into(),
        context: InvocationContext {
            tenant_id: tenant.into(),
            user_id: format!("user-{tenant}"),
            request_id: "request-1".into(),
            conversation_id: Some("conversation-1".into()),
        },
        configuration: json!({"tenantSetting":tenant}),
        payload: json!({"text":"hello"}),
    }
}
fn run(
    sandbox: &WasmSandbox,
    tenant: &str,
    callback: Option<Arc<ResourceCallback>>,
) -> Result<serde_json::Value, SandboxError> {
    sandbox.execute(
        &invocation(tenant),
        callback,
        Arc::new(AtomicBool::new(false)),
        Instant::now() + Duration::from_secs(1),
    )
}

#[test]
fn real_guest_reads_only_its_own_invocation_and_fresh_store() {
    let bytes = module(
        "(i64.or (i64.shl (i64.extend_i32_u (local.get $ptr)) (i64.const 32)) (i64.extend_i32_u (local.get $len)))",
    );
    let sandbox = WasmSandbox::new(&signed(&bytes, false, 2, 100000), &bytes).unwrap();
    sandbox.validate_configuration(&json!({})).unwrap();
    for tenant in ["tenant-a", "tenant-b", "tenant-a"] {
        let response = run(&sandbox, tenant, None).unwrap();
        assert_eq!(response["context"]["tenantId"], tenant);
        assert_eq!(response["configuration"]["tenantSetting"], tenant);
        assert!(response["context"].get("delegation").is_none());
    }
    let mut input = invocation("a");
    input.operation = "undeclared.run".into();
    assert_eq!(
        sandbox.execute(
            &input,
            None,
            Arc::new(AtomicBool::new(false)),
            Instant::now() + Duration::from_secs(1)
        ),
        Err(SandboxError::Payload)
    );
}

#[test]
fn host_enforces_input_output_configuration_schemas_without_external_resolution() {
    let bytes = module("(i64.const 0)");
    let mut input = invocation("a");
    input.payload = json!("not an object");
    let sandbox = WasmSandbox::new(&signed(&bytes, false, 2, 10000), &bytes).unwrap();
    assert_eq!(
        sandbox.execute(
            &input,
            None,
            Arc::new(AtomicBool::new(false)),
            Instant::now() + Duration::from_secs(1)
        ),
        Err(SandboxError::Payload)
    );
    // A well-formed JSON string is still rejected by the operation's object schema.
    let bytes = wat::parse_str(
        r#"(module (memory (export "memory") 1 2) (data (i32.const 0) "\22text\22")
        (func (export "flare_alloc") (param i32) (result i32) i32.const 4096)
        (func (export "flare_validate") (param i32 i32) (result i32) i32.const 0)
        (func (export "flare_call") (param i32 i32) (result i64) i64.const 6))"#,
    )
    .unwrap();
    let sandbox = WasmSandbox::new(&signed(&bytes, false, 2, 10000), &bytes).unwrap();
    assert_eq!(run(&sandbox, "a", None), Err(SandboxError::Payload));
    assert!(
        sandbox
            .validate_configuration(&json!(["not an object"]))
            .is_err()
    );
    for schema in [
        json!({"$ref":"https://untrusted.invalid/schema"}),
        json!({"$ref":"file:///etc/passwd"}),
    ] {
        assert!(
            signed_with_schema(&bytes, false, 2, 10000, Some(schema)).is_err(),
            "rejected by the signed manifest before module loading"
        );
    }
    let manifest = signed_with_schema(
        &bytes,
        false,
        2,
        10000,
        Some(json!({"type":"string","pattern":"(?=unsafe)"})),
    )
    .unwrap();
    assert!(
        WasmSandbox::new(&manifest, &bytes).is_err(),
        "non-linear regex patterns are rejected"
    );
    for schema in [
        json!({"$ref":"#"}),
        json!({"$ref":"#/$defs/a","$defs":{"a":{"$ref":"#/$defs/a"}}}),
        json!({"type":"object","$dynamicRef":"#"}),
    ] {
        let manifest = signed_with_schema(&bytes, false, 2, 10000, Some(schema)).unwrap();
        assert!(
            WasmSandbox::new(&manifest, &bytes).is_err(),
            "host schema work is bounded separately from guest fuel"
        );
    }
    let manifest=signed_with_schema(&bytes,false,2,10000,Some(json!({"type":"object","properties":{"text":{"$ref":"#/$defs/text"}},"$defs":{"text":{"type":"string","maxLength":16}}}))).unwrap();
    assert!(
        WasmSandbox::new(&manifest, &bytes).is_ok(),
        "acyclic local definitions remain supported"
    );
}

#[test]
fn artifact_import_memory_and_abi_checks_happen_before_serving() {
    let good = module("(i64.const 0)");
    let manifest = signed(&good, false, 2, 1000);
    let mut altered = good.clone();
    altered.push(0);
    assert!(matches!(
        WasmSandbox::new(&manifest, &altered),
        Err(SandboxError::Artifact)
    ));
    for import in [
        r#"(import "wasi_snapshot_preview1" "proc_exit" (func (param i32)))"#,
        r#"(import "env" "network" (func))"#,
    ] {
        let bytes = wat::parse_str(format!("(module {import})")).unwrap();
        assert!(matches!(
            WasmSandbox::new(&signed(&bytes, false, 2, 1000), &bytes),
            Err(SandboxError::Abi)
        ));
    }
    let bytes=wat::parse_str(r#"(module (memory (export "memory") 1) (func (export "flare_call") (result i32) i32.const 0))"#).unwrap();
    assert!(matches!(
        WasmSandbox::new(&signed(&bytes, false, 2, 1000), &bytes),
        Err(SandboxError::Abi)
    ));
    let bytes = module("(drop (memory.grow (i32.const 1))) (i64.const 0)");
    let sandbox = WasmSandbox::new(&signed(&bytes, false, 1, 1000), &bytes).unwrap();
    assert!(run(&sandbox, "a", None).is_err());
    assert!(WasmSandbox::new(&signed(&good, false, 1025, 1000), &good).is_err());
}

#[test]
fn fuel_traps_bounds_cancellation_and_recovery_are_bounded() {
    let bytes = module("(loop $forever (br $forever)) (i64.const 0)");
    let sandbox = WasmSandbox::new(&signed(&bytes, false, 2, 1000), &bytes).unwrap();
    assert_eq!(run(&sandbox, "a", None), Err(SandboxError::Execution));
    assert_eq!(run(&sandbox, "b", None), Err(SandboxError::Execution));
    for body in ["unreachable", "(i64.const -1)", "(i64.const 65537)"] {
        let bytes = module(body);
        let sandbox = WasmSandbox::new(&signed(&bytes, false, 2, 1000), &bytes).unwrap();
        assert!(run(&sandbox, "a", None).is_err());
    }
    let bytes = module(
        "(i64.or (i64.shl (i64.extend_i32_u (local.get $ptr)) (i64.const 32)) (i64.extend_i32_u (local.get $len)))",
    );
    let sandbox = WasmSandbox::new(&signed(&bytes, false, 2, 10000), &bytes).unwrap();
    assert_eq!(
        sandbox.execute(
            &invocation("a"),
            None,
            Arc::new(AtomicBool::new(true)),
            Instant::now() + Duration::from_secs(1)
        ),
        Err(SandboxError::Execution)
    );
    assert!(run(&sandbox, "after-trap", None).is_ok());
}

fn host_module(calls: usize) -> Vec<u8> {
    let request = br#"{"capability":"im.conversation.read"}"#;
    let data = request
        .iter()
        .map(|byte| format!("\\{byte:02x}"))
        .collect::<String>();
    let call = format!(
        "(local.set $n (call $host (i32.const 0) (i32.const {}) (i32.const 1024) (i32.const 1024)))",
        request.len()
    );
    wat::parse_str(format!(
        r#"(module
        (import "flare_host" "call" (func $host (param i32 i32 i32 i32) (result i32)))
        (memory (export "memory") 1 2) (data (i32.const 0) "{data}")
        (func (export "flare_alloc") (param i32) (result i32) i32.const 4096)
        (func (export "flare_validate") (param i32 i32) (result i32) i32.const 0)
        (func (export "flare_call") (param i32 i32) (result i64) (local $n i32)
            {} (i64.or (i64.const 4398046511104) (i64.extend_i32_u (local.get $n)))))"#,
        call.repeat(calls)
    ))
    .unwrap()
}
#[test]
fn host_callbacks_require_declared_scopes_and_have_per_store_limits() {
    let bytes = host_module(1);
    let sandbox = WasmSandbox::new(&signed(&bytes, true, 2, 100000), &bytes).unwrap();
    let hits = Arc::new(AtomicUsize::new(0));
    let counter = hits.clone();
    let callback: Arc<ResourceCallback> = Arc::new(move |_| {
        counter.fetch_add(1, Ordering::SeqCst);
        Ok(json!({"conversationId":"conversation-1"}))
    });
    assert_eq!(
        run(&sandbox, "a", Some(callback.clone())).unwrap()["conversationId"],
        "conversation-1"
    );
    assert!(run(&sandbox, "a", None).is_err());
    let denied = WasmSandbox::new(&signed(&bytes, false, 2, 100000), &bytes).unwrap();
    assert!(run(&denied, "a", Some(callback.clone())).is_err());
    assert_eq!(hits.load(Ordering::SeqCst), 1);
    let bytes = host_module(9);
    let sandbox = WasmSandbox::new(&signed(&bytes, true, 2, 100000), &bytes).unwrap();
    assert!(run(&sandbox, "a", Some(callback)).is_err());
    assert_eq!(
        hits.load(Ordering::SeqCst),
        9,
        "ninth callback traps before invoking host"
    );
}
