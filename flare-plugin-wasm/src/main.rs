use flare_grpc_proto::capability::{
    extension_plugin_server::ExtensionPluginServer, plugin_runtime_server::PluginRuntimeServer,
};
use flare_plugin_host::{
    PluginRuntimeHost, resource::HostResourceClient, runtime::ConfigurationValidator, serve_bounded,
};
use flare_plugin_wasm::{WasmSandbox, service::WasmPluginService};
use std::{sync::Arc, time::Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let directory = std::env::var("FLARE_PLUGIN_ARTIFACT_DIR")?;
    let (host, sandbox) = PluginRuntimeHost::from_env_with(|manifest| {
        let sandbox = Arc::new(WasmSandbox::from_directory(
            manifest,
            std::path::Path::new(&directory),
        )?);
        let validator = sandbox.clone();
        let validate: Arc<ConfigurationValidator> = Arc::new(move |configuration| {
            validator
                .validate_configuration(configuration)
                .map_err(|_| tonic::Status::invalid_argument("WASM configuration rejected"))
        });
        Ok((validate, sandbox))
    })
    .await?;
    let resources = match std::env::var("FLARE_PLUGIN_HOST_BRIDGE_URL") {
        Ok(url) => Some(HostResourceClient::connect(&url).await?),
        Err(std::env::VarError::NotPresent) => None,
        Err(_) => return Err("invalid host bridge configuration".into()),
    };
    let service = WasmPluginService::new(host.clone(), sandbox, resources)?;
    let address = std::env::var("FLARE_PLUGIN_LISTEN")
        .unwrap_or_else(|_| "127.0.0.1:7903".into())
        .parse()?;
    let control = host.clone();
    serve_bounded(host, Duration::from_secs(30), move |stop| async move {
        tonic::transport::Server::builder()
            .timeout(Duration::from_secs(30))
            .add_service(PluginRuntimeServer::new(control))
            .add_service(
                ExtensionPluginServer::new(service)
                    .max_decoding_message_size(32768)
                    .max_encoding_message_size(66560),
            )
            .serve_with_shutdown(address, async {
                let _ = stop.await;
            })
            .await
    })
    .await?;
    Ok(())
}
