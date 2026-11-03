//! 一个能跑起来的最小插件：两个 operation，一次声明，一个 gRPC 服务。
//!
//! 它做的事没有业务价值 —— 价值在于**它是完整的**：
//! 共享部署、租户配置、专用调用凭据、JSON 编解码与优雅停机，
//! 一个真实插件需要的每一环这里都有，而且都短到能一眼看完。

use std::net::SocketAddr;
use std::sync::Arc;

use flare_grpc_proto::capability::extension_plugin_server::{
    ExtensionPlugin, ExtensionPluginServer,
};
use flare_grpc_proto::capability::plugin_runtime_server::PluginRuntimeServer;
use flare_grpc_proto::capability::{GenericRequest, GenericResponse};
use flare_plugin_host::PluginRuntimeHost;
use tonic::{Request, Response, Status, transport::Server};

/// 声明清单。**它就是边界** —— 不在这里的 operation，平台会在到达本进程前拒掉。
const OPERATIONS: &[&str] = &["example.echo.say", "example.echo.upper"];

struct EchoPlugin {
    runtime: PluginRuntimeHost,
}

#[tonic::async_trait]
impl ExtensionPlugin for EchoPlugin {
    async fn call(
        &self,
        request: Request<GenericRequest>,
    ) -> Result<Response<GenericResponse>, Status> {
        if request.get_ref().operation == "health_check" {
            return Ok(Response::new(GenericResponse {
                ok: true,
                request_id: request.get_ref().request_id.clone(),
                ..Default::default()
            }));
        }
        let identity = flare_plugin_host::verify_user_call(&request, &self.runtime).await?;
        let req = request.into_inner();

        // Any.value 直接就是 JSON 字节，不是 protobuf 编码。
        let payload: serde_json::Value = match req.payload.as_ref() {
            Some(any) => serde_json::from_slice(&any.value)
                .map_err(|e| Status::invalid_argument(format!("payload 不是合法 JSON: {e}")))?,
            None => serde_json::Value::Null,
        };
        let text = payload.get("text").and_then(|v| v.as_str()).unwrap_or("");

        let result = match req.operation.as_str() {
            "example.echo.say" => serde_json::json!({
                "echo": text,
                "tenant_id": identity.tenant_id,
                "user_id": identity.user_id,
            }),
            "example.echo.upper" => serde_json::json!({ "echo": text.to_uppercase() }),

            // 走到这里说明声明清单与本 match 不同步。
            // 明确回错，别静默成功 —— 静默成功会让调用方以为业务做了。
            other => {
                return Ok(Response::new(GenericResponse {
                    ok: false,
                    payload: None,
                    error_code: "UNKNOWN_OPERATION".into(),
                    error_message: format!("{other} 已声明但未实现"),
                    request_id: req.request_id,
                }));
            }
        };

        Ok(Response::new(GenericResponse {
            ok: true,
            payload: Some(prost_types::Any {
                type_url: "type.googleapis.com/flare.capability.v1.PayloadJson".into(),
                value: serde_json::to_vec(&result).map_err(|e| Status::internal(e.to_string()))?,
            }),
            error_code: String::new(),
            error_message: String::new(),
            request_id: req.request_id,
        }))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listen: SocketAddr = std::env::var("ECHO_PLUGIN_LISTEN")
        .unwrap_or_else(|_| "127.0.0.1:7901".into())
        .parse()?;
    let runtime = PluginRuntimeHost::from_env(Arc::new(|config| {
        if config.as_object().is_none_or(|v| !v.is_empty()) {
            return Err(Status::invalid_argument("echo configuration must be empty"));
        }
        Ok(())
    }))
    .await?;
    if runtime.manifest().operation_ids() != OPERATIONS.iter().map(|s| s.to_string()).collect() {
        return Err("echo manifest operations do not match implementation".into());
    }
    let plugin = EchoPlugin {
        runtime: runtime.clone(),
    };
    flare_plugin_host::serve_bounded(
        runtime.clone(),
        std::time::Duration::from_secs(30),
        |stop| {
            Server::builder()
                .timeout(std::time::Duration::from_secs(30))
                .concurrency_limit_per_connection(64)
                .max_concurrent_streams(Some(64))
                .add_service(PluginRuntimeServer::new(runtime.clone()))
                .add_service(ExtensionPluginServer::new(plugin))
                .serve_with_shutdown(listen, async move {
                    let _ = stop.await;
                })
        },
    )
    .await?;
    Ok(())
}

/// 声明清单必须与上面 `call` 里的分发臂一致，两个方向都会出事：
///   声明有、实现没有 → 请求打过来，你回 UNKNOWN_OPERATION，看着像插件坏了
///   实现有、声明没有 → 平台直接拒绝，你的进程收不到，看着像权限问题
///
/// 两种症状的排查方向完全不同，所以用一条测试把它们钉在一起。
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn declaration_matches_implementation() {
        let implemented = include_str!("echo_plugin.rs");
        for op in OPERATIONS {
            assert!(
                implemented.contains(&format!("\"{op}\" =>")),
                "{op} 声明了却没有对应的分发臂"
            );
        }
    }
}
