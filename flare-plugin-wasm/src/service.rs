//! The same PluginRuntime control API and invocation tickets as gRPC plugins.
use crate::*;
use flare_grpc_proto::capability::{
    GenericRequest, GenericResponse, extension_plugin_server::ExtensionPlugin,
};
use flare_plugin_host::{PluginRuntimeHost, resource::HostResourceClient};
use tonic::{Request, Response, Status};

#[derive(Clone)]
pub struct WasmPluginService {
    host: PluginRuntimeHost,
    sandbox: Arc<WasmSandbox>,
    resources: Option<HostResourceClient>,
    slots: Arc<tokio::sync::Semaphore>,
}
struct Cancellation(Arc<AtomicBool>);
impl Drop for Cancellation {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}
impl WasmPluginService {
    pub fn new(
        host: PluginRuntimeHost,
        sandbox: Arc<WasmSandbox>,
        resources: Option<HostResourceClient>,
    ) -> Result<Self, SandboxError> {
        if host.manifest_digest() != sandbox.manifest_digest {
            return Err(SandboxError::Artifact);
        }
        Ok(Self {
            host,
            sandbox,
            resources,
            slots: Arc::new(tokio::sync::Semaphore::new(4)),
        })
    }
}
#[tonic::async_trait]
impl ExtensionPlugin for WasmPluginService {
    async fn call(
        &self,
        request: Request<GenericRequest>,
    ) -> Result<Response<GenericResponse>, Status> {
        let user = flare_plugin_host::verify_user_call(&request, &self.host).await?;
        let permit = self
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("WASM capacity exhausted"))?;
        let body = request.into_inner();
        let bytes = body
            .payload
            .as_ref()
            .map(|p| p.value.as_slice())
            .unwrap_or(b"null");
        if bytes.len() > 16384 {
            return Err(Status::resource_exhausted("WASM input limit"));
        }
        let payload: Value = serde_json::from_slice(bytes)
            .map_err(|_| Status::invalid_argument("invalid WASM payload"))?;
        let op = self
            .host
            .manifest()
            .operation(&body.operation)
            .ok_or_else(|| Status::permission_denied("operation denied"))?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| Status::unavailable("clock unavailable"))?
            .as_millis() as i64;
        let remaining = user.expires_at_ms.saturating_sub(now);
        if remaining <= 0 {
            return Err(Status::permission_denied("WASM invocation expired"));
        }
        let deadline = Instant::now()
            + Duration::from_millis((remaining as u64).min(u64::from(op.timeout_ms)));
        let invocation = Invocation {
            operation: body.operation,
            context: InvocationContext {
                tenant_id: user.tenant_id.clone(),
                user_id: user.user_id.clone(),
                request_id: user.request_id.clone(),
                conversation_id: user.conversation_id.clone(),
            },
            configuration: user.configuration.clone(),
            payload,
        };
        let callback = self.resources.clone().map(|client| {
            let user = user.clone();
            let handle = tokio::runtime::Handle::current();
            Arc::new(move |request: &ResourceRequest| {
                handle
                    .block_on(client.read(&user, request))
                    .map_err(|_| SandboxError::Resource)
            }) as Arc<ResourceCallback>
        });
        let sandbox = self.sandbox.clone();
        let cancelled = Arc::new(AtomicBool::new(false));
        let _cancel = Cancellation(cancelled.clone());
        let work = tokio::task::spawn_blocking(move || {
            // Both admission and capacity stay owned by actual work even when
            // the caller disconnects or times out. Guest fuel remains bounded.
            let _permit = permit;
            let _admission = user;
            sandbox.execute(&invocation, callback, cancelled, deadline)
        });
        let value = tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), work)
            .await
            .map_err(|_| Status::deadline_exceeded("WASM deadline exceeded"))?
            .map_err(|_| Status::internal("WASM worker failed"))?
            .map_err(|_| Status::failed_precondition("WASM execution rejected"))?;
        Ok(Response::new(GenericResponse {
            ok: true,
            request_id: body.request_id,
            payload: Some(prost_types::Any {
                type_url: "type.googleapis.com/flare.capability.v1.PayloadJson".into(),
                value: serde_json::to_vec(&value)
                    .map_err(|_| Status::internal("WASM response encoding"))?,
            }),
            ..Default::default()
        }))
    }
}
