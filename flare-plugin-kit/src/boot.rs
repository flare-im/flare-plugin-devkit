//! Runtime host wiring: verified manifest and tenant state from the operator
//! environment, the release check, bounded serving and background workers.
use flare_grpc_proto::capability::{
    extension_plugin_server::{ExtensionPlugin, ExtensionPluginServer},
    plugin_runtime_server::PluginRuntimeServer,
};
use flare_plugin_host::PluginRuntimeHost;
use serde_json::Value;
use std::{collections::BTreeSet, future::Future, net::SocketAddr, sync::Arc, time::Duration};

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// What this binary implements. The signed manifest must match it exactly.
#[derive(Debug, Clone, Copy)]
pub struct Release {
    pub plugin_id: &'static str,
    pub version: &'static str,
    pub operations: &'static [&'static str],
}

/// The deployment's runtime host from the documented `FLARE_PLUGIN_*`
/// environment (signed manifest, publisher trust, deployment, call keys,
/// platform key, tenant state database and the optional runtime credential).
/// Tenant configurations are checked with `validate` before they are accepted.
pub async fn runtime_from_env(
    release: Release,
    validate: fn(&Value) -> Result<(), &'static str>,
) -> Result<PluginRuntimeHost, Box<dyn std::error::Error>> {
    let runtime = PluginRuntimeHost::from_env(Arc::new(move |value| {
        validate(value).map_err(tonic::Status::invalid_argument)
    }))
    .await?;
    ensure_release(&runtime, release)?;
    Ok(runtime)
}

/// The signed manifest names this plugin and version and declares exactly
/// the operations this binary dispatches.
pub fn ensure_release(
    runtime: &PluginRuntimeHost,
    release: Release,
) -> Result<(), Box<dyn std::error::Error>> {
    let manifest = runtime.manifest();
    if manifest.plugin_id != release.plugin_id {
        return Err("plugin manifest identity mismatch".into());
    }
    if manifest.version != release.version {
        return Err("plugin release version mismatch".into());
    }
    let declared: BTreeSet<String> = release.operations.iter().map(|s| s.to_string()).collect();
    if manifest.operation_ids() != declared {
        return Err("plugin manifest operations mismatch".into());
    }
    Ok(())
}

/// Serve the runtime control service and the business service on one
/// listener until SIGINT/SIGTERM, then drain within 30 seconds.
pub async fn serve<S: ExtensionPlugin>(
    runtime: PluginRuntimeHost,
    listen: SocketAddr,
    service: S,
) -> Result<(), flare_plugin_host::ServeError> {
    flare_plugin_host::serve_bounded(runtime.clone(), Duration::from_secs(30), |stop| {
        tonic::transport::Server::builder()
            .timeout(Duration::from_secs(30))
            .concurrency_limit_per_connection(64)
            .max_concurrent_streams(Some(64))
            .add_service(PluginRuntimeServer::new(runtime.clone()))
            .add_service(ExtensionPluginServer::new(service))
            .serve_with_shutdown(listen, async move {
                let _ = stop.await;
            })
    })
    .await
}

/// Run `task` every `period` while the runtime admits work. Each run holds a
/// process admission, so draining waits for it; a failed run is logged and
/// retried on the next tick.
pub fn spawn_periodic<F, Fut>(
    runtime: PluginRuntimeHost,
    name: &'static str,
    period: Duration,
    mut task: F,
) -> tokio::task::JoinHandle<()>
where
    F: FnMut() -> Fut + Send + 'static,
    Fut: Future<Output = Result<(), BoxError>> + Send,
{
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(period);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            let Ok(_work) = runtime.admit_work() else {
                break;
            };
            if let Err(error) = task().await {
                eprintln!("plugin background work {name} failed: {error}");
            }
        }
    })
}

/// A listen address from `variable`, or `default` when unset.
pub fn listen_from_env(
    variable: &str,
    default: &str,
) -> Result<SocketAddr, Box<dyn std::error::Error>> {
    Ok(std::env::var(variable)
        .unwrap_or_else(|_| default.into())
        .parse()?)
}
