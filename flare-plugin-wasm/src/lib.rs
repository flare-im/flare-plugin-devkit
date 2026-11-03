//! Server WASM runtime. No WASI, ambient credentials, filesystem or sockets.
pub mod service;
use flare_plugin_protocol::{
    manifest::PluginRuntime,
    resource::{MAX_RESOURCE_BYTES, MAX_RESOURCE_CALLS, ResourceRequest},
    signature::VerifiedManifest,
};
use serde::Serialize;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};
use wasmi::{
    Caller, CompilationMode, Config, Engine, Linker, Memory, Module, Store, StoreLimits,
    StoreLimitsBuilder,
};

pub const ABI_VERSION: &str = "flare.plugin.json.v1";
pub const MAX_MODULE_BYTES: usize = 1024 * 1024;
pub type ResourceCallback = dyn Fn(&ResourceRequest) -> Result<Value, SandboxError> + Send + Sync;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SandboxError {
    #[error("invalid or untrusted WASM artifact")]
    Artifact,
    #[error("WASM ABI or import is not supported")]
    Abi,
    #[error("WASM invocation resource limit or trap")]
    Execution,
    #[error("invalid WASM input or output")]
    Payload,
    #[error("host resource access denied or unavailable")]
    Resource,
}
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InvocationContext {
    pub tenant_id: String,
    pub user_id: String,
    pub request_id: String,
    pub conversation_id: Option<String>,
}
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Invocation {
    pub operation: String,
    pub context: InvocationContext,
    pub configuration: Value,
    pub payload: Value,
}

pub struct WasmSandbox {
    engine: Engine,
    module: Module,
    pages: u32,
    fuel: u64,
    operations: BTreeMap<String, BTreeSet<flare_plugin_protocol::resource::HostCapability>>,
    manifest_digest: String,
    schemas: BTreeMap<String, (jsonschema::Validator, jsonschema::Validator)>,
    configuration_schema: jsonschema::Validator,
}
struct CallState {
    limits: StoreLimits,
    callback: Option<Arc<ResourceCallback>>,
    calls: usize,
    deadline: Instant,
    cancelled: Arc<AtomicBool>,
    allowed: BTreeSet<flare_plugin_protocol::resource::HostCapability>,
}
fn trapped(_: impl std::fmt::Display) -> SandboxError {
    SandboxError::Execution
}
fn host_error() -> wasmi::Error {
    wasmi::Error::new("host resource access denied or limit exceeded")
}

/// The interpreter's fuel does not meter host-side schema evaluation. Bound
/// the schema graph separately, and reject recursive/dynamic references before
/// compiling it. Acyclic local definitions and linear-time patterns work.
fn bounded_schema(schema: &Value) -> Result<(), SandboxError> {
    fn walk(
        root: &Value,
        value: &Value,
        active: &mut BTreeSet<String>,
        depth: usize,
        budget: &mut usize,
    ) -> Result<(), SandboxError> {
        if depth > 32 || *budget == 0 {
            return Err(SandboxError::Artifact);
        }
        *budget -= 1;
        match value {
            Value::Object(object) => {
                if object.contains_key("$dynamicRef") {
                    return Err(SandboxError::Artifact);
                }
                if let Some(reference) = object.get("$ref") {
                    let reference = reference.as_str().ok_or(SandboxError::Artifact)?;
                    let pointer = reference.strip_prefix('#').ok_or(SandboxError::Artifact)?;
                    if !active.insert(reference.into()) {
                        return Err(SandboxError::Artifact);
                    }
                    let target = root.pointer(pointer).ok_or(SandboxError::Artifact)?;
                    walk(root, target, active, depth + 1, budget)?;
                    active.remove(reference);
                }
                for child in object.values() {
                    walk(root, child, active, depth + 1, budget)?;
                }
            }
            Value::Array(values) => {
                for child in values {
                    walk(root, child, active, depth + 1, budget)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    walk(schema, schema, &mut ["#".into()].into(), 0, &mut 2048)
}

impl WasmSandbox {
    pub fn new(manifest: &VerifiedManifest, bytes: &[u8]) -> Result<Self, SandboxError> {
        let PluginRuntime::Wasm {
            abi_version,
            max_memory_pages,
            max_fuel,
            ..
        } = &manifest.manifest().runtime
        else {
            return Err(SandboxError::Abi);
        };
        if abi_version != ABI_VERSION
            || !(1..=1024).contains(max_memory_pages)
            || !(1..=10_000_000).contains(max_fuel)
        {
            return Err(SandboxError::Abi);
        }
        if bytes.len() > MAX_MODULE_BYTES
            || !bytes.starts_with(b"\0asm")
            || manifest
                .manifest()
                .artifact
                .as_ref()
                .is_none_or(|a| a.digest != flare_plugin_protocol::runtime::digest_bytes(bytes))
        {
            return Err(SandboxError::Artifact);
        }
        let mut config = Config::default();
        config
            .consume_fuel(true)
            .compilation_mode(CompilationMode::Eager)
            .wasm_multi_memory(false)
            .wasm_custom_page_sizes(false)
            .set_max_recursion_depth(128)
            .set_min_stack_height(1024)
            .set_max_stack_height(65536)
            .set_max_cached_stacks(0);
        let engine = Engine::new(&config);
        let module = Module::new(&engine, bytes).map_err(|_| SandboxError::Artifact)?;
        let mut imports = 0;
        for import in module.imports() {
            imports += 1;
            if imports > 1 || import.module() != "flare_host" || import.name() != "call" {
                return Err(SandboxError::Abi);
            }
        }
        let operations = manifest
            .manifest()
            .capabilities
            .iter()
            .flat_map(|c| {
                c.operations.iter().map(|o| {
                    (
                        format!("{}.{}", c.id, o.id),
                        o.host_capabilities.iter().copied().collect(),
                    )
                })
            })
            .collect();
        // Release-time compilation only; ambient HTTP/file schema resolution
        // is disabled in both Cargo features and validator options.
        let compile = |value: &Value| {
            bounded_schema(value)?;
            jsonschema::options()
                .offline()
                .with_pattern_options(jsonschema::PatternOptions::regex().size_limit(262144))
                .build(value)
                .map_err(|_| SandboxError::Artifact)
        };
        let mut schemas = BTreeMap::new();
        for capability in &manifest.manifest().capabilities {
            for operation in &capability.operations {
                schemas.insert(
                    format!("{}.{}", capability.id, operation.id),
                    (
                        compile(&operation.input_schema)?,
                        compile(&operation.output_schema)?,
                    ),
                );
            }
        }
        let configuration_schema = compile(
            &manifest
                .manifest()
                .configuration_schema
                .clone()
                .unwrap_or(Value::Bool(true)),
        )?;
        let sandbox = Self {
            engine,
            module,
            pages: *max_memory_pages,
            fuel: *max_fuel,
            operations,
            manifest_digest: manifest.digest().into(),
            schemas,
            configuration_schema,
        };
        let (store, instance, _) = sandbox.instantiate(
            None,
            Arc::new(AtomicBool::new(false)),
            Instant::now() + Duration::from_secs(1),
            BTreeSet::new(),
        )?;
        instance
            .get_typed_func::<i32, i32>(&store, "flare_alloc")
            .map_err(|_| SandboxError::Abi)?;
        instance
            .get_typed_func::<(i32, i32), i32>(&store, "flare_validate")
            .map_err(|_| SandboxError::Abi)?;
        instance
            .get_typed_func::<(i32, i32), i64>(&store, "flare_call")
            .map_err(|_| SandboxError::Abi)?;
        Ok(sandbox)
    }

    pub fn from_directory(
        manifest: &VerifiedManifest,
        directory: &std::path::Path,
    ) -> Result<Self, SandboxError> {
        use std::io::Read;
        let PluginRuntime::Wasm { module, .. } = &manifest.manifest().runtime else {
            return Err(SandboxError::Abi);
        };
        let directory = directory
            .canonicalize()
            .map_err(|_| SandboxError::Artifact)?;
        let file = directory
            .join(module)
            .canonicalize()
            .map_err(|_| SandboxError::Artifact)?;
        if !file.starts_with(&directory) {
            return Err(SandboxError::Artifact);
        }
        let mut bytes = Vec::new();
        std::fs::File::open(file)
            .map_err(|_| SandboxError::Artifact)?
            .take(MAX_MODULE_BYTES as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| SandboxError::Artifact)?;
        Self::new(manifest, &bytes)
    }

    fn instantiate(
        &self,
        callback: Option<Arc<ResourceCallback>>,
        cancelled: Arc<AtomicBool>,
        deadline: Instant,
        allowed: BTreeSet<flare_plugin_protocol::resource::HostCapability>,
    ) -> Result<(Store<CallState>, wasmi::Instance, Memory), SandboxError> {
        let limits = StoreLimitsBuilder::new()
            .memory_size(self.pages as usize * 65536)
            .memories(1)
            .instances(1)
            .tables(1)
            .table_elements(1024)
            .trap_on_grow_failure(true)
            .build();
        let mut store = Store::new(
            &self.engine,
            CallState {
                limits,
                callback,
                calls: 0,
                deadline,
                cancelled,
                allowed,
            },
        );
        store.limiter(|s| &mut s.limits);
        store.set_fuel(self.fuel).map_err(trapped)?;
        let mut linker = Linker::new(&self.engine);
        linker
            .func_wrap(
                "flare_host",
                "call",
                |mut caller: Caller<'_, CallState>,
                 ptr: i32,
                 len: i32,
                 out: i32,
                 capacity: i32|
                 -> Result<i32, wasmi::Error> {
                    let state = caller.data_mut();
                    if state.calls >= MAX_RESOURCE_CALLS
                        || state.cancelled.load(Ordering::Acquire)
                        || Instant::now() >= state.deadline
                    {
                        return Err(host_error());
                    }
                    state.calls += 1;
                    let callback = state.callback.clone().ok_or_else(host_error)?;
                    let memory = caller
                        .get_export("memory")
                        .and_then(|e| e.into_memory())
                        .ok_or_else(host_error)?;
                    let ptr = usize::try_from(ptr).map_err(|_| host_error())?;
                    let len = usize::try_from(len).map_err(|_| host_error())?;
                    let out = usize::try_from(out).map_err(|_| host_error())?;
                    let capacity = usize::try_from(capacity).map_err(|_| host_error())?;
                    if len > 4096 || capacity > MAX_RESOURCE_BYTES {
                        return Err(host_error());
                    }
                    let mut request = vec![0; len];
                    memory
                        .read(&caller, ptr, &mut request)
                        .map_err(|_| host_error())?;
                    let request: ResourceRequest =
                        serde_json::from_slice(&request).map_err(|_| host_error())?;
                    request.validate().map_err(|_| host_error())?;
                    if !caller.data().allowed.contains(&request.capability()) {
                        return Err(host_error());
                    }
                    let response =
                        serde_json::to_vec(&callback(&request).map_err(|_| host_error())?)
                            .map_err(|_| host_error())?;
                    if response.len() > capacity
                        || caller.data().cancelled.load(Ordering::Acquire)
                        || Instant::now() >= caller.data().deadline
                    {
                        return Err(host_error());
                    }
                    memory
                        .write(&mut caller, out, &response)
                        .map_err(|_| host_error())?;
                    Ok(response.len() as i32)
                },
            )
            .map_err(|_| SandboxError::Abi)?;
        let instance = linker
            .instantiate_and_start(&mut store, &self.module)
            .map_err(trapped)?;
        let memory = instance
            .get_memory(&store, "memory")
            .ok_or(SandboxError::Abi)?;
        Ok((store, instance, memory))
    }

    fn input(
        &self,
        store: &mut Store<CallState>,
        instance: wasmi::Instance,
        memory: Memory,
        bytes: &[u8],
    ) -> Result<i32, SandboxError> {
        if bytes.len() > MAX_RESOURCE_BYTES {
            return Err(SandboxError::Payload);
        }
        let alloc = instance
            .get_typed_func::<i32, i32>(&*store, "flare_alloc")
            .map_err(|_| SandboxError::Abi)?;
        let ptr = alloc
            .call(&mut *store, bytes.len() as i32)
            .map_err(trapped)?;
        memory
            .write(
                store,
                usize::try_from(ptr).map_err(|_| SandboxError::Payload)?,
                bytes,
            )
            .map_err(|_| SandboxError::Payload)?;
        Ok(ptr)
    }

    pub fn validate_configuration(&self, configuration: &Value) -> Result<(), SandboxError> {
        flare_plugin_protocol::runtime::configuration_digest(configuration)
            .map_err(|_| SandboxError::Payload)?;
        if !self.configuration_schema.is_valid(configuration) {
            return Err(SandboxError::Payload);
        }
        let bytes = serde_json::to_vec(configuration).map_err(|_| SandboxError::Payload)?;
        let (mut store, instance, memory) = self.instantiate(
            None,
            Arc::new(AtomicBool::new(false)),
            Instant::now() + Duration::from_secs(1),
            BTreeSet::new(),
        )?;
        let ptr = self.input(&mut store, instance, memory, &bytes)?;
        let validate = instance
            .get_typed_func::<(i32, i32), i32>(&store, "flare_validate")
            .map_err(|_| SandboxError::Abi)?;
        if validate
            .call(&mut store, (ptr, bytes.len() as i32))
            .map_err(trapped)?
            != 0
        {
            return Err(SandboxError::Payload);
        }
        Ok(())
    }

    pub fn execute(
        &self,
        invocation: &Invocation,
        callback: Option<Arc<ResourceCallback>>,
        cancelled: Arc<AtomicBool>,
        deadline: Instant,
    ) -> Result<Value, SandboxError> {
        if cancelled.load(Ordering::Acquire) || Instant::now() >= deadline {
            return Err(SandboxError::Execution);
        }
        let (input_schema, output_schema) = self
            .schemas
            .get(&invocation.operation)
            .ok_or(SandboxError::Payload)?;
        if !input_schema.is_valid(&invocation.payload) {
            return Err(SandboxError::Payload);
        }
        let bytes = serde_json::to_vec(invocation).map_err(|_| SandboxError::Payload)?;
        let allowed = self
            .operations
            .get(&invocation.operation)
            .ok_or(SandboxError::Payload)?
            .clone();
        let (mut store, instance, memory) =
            self.instantiate(callback, cancelled, deadline, allowed)?;
        let ptr = self.input(&mut store, instance, memory, &bytes)?;
        let call = instance
            .get_typed_func::<(i32, i32), i64>(&store, "flare_call")
            .map_err(|_| SandboxError::Abi)?;
        let packed = call
            .call(&mut store, (ptr, bytes.len() as i32))
            .map_err(trapped)? as u64;
        let ptr = (packed >> 32) as usize;
        let len = (packed & u64::from(u32::MAX)) as usize;
        if len > MAX_RESOURCE_BYTES
            || store.data().cancelled.load(Ordering::Acquire)
            || Instant::now() >= deadline
        {
            return Err(SandboxError::Payload);
        }
        let mut response = vec![0; len];
        memory
            .read(&store, ptr, &mut response)
            .map_err(|_| SandboxError::Payload)?;
        let response: Value =
            serde_json::from_slice(&response).map_err(|_| SandboxError::Payload)?;
        if !output_schema.is_valid(&response) {
            return Err(SandboxError::Payload);
        }
        Ok(response)
    }
}
