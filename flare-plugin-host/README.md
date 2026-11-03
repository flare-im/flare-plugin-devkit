# flare-plugin-host

English · [中文](README.zh-CN.md)

The endpoint SDK for Flare plugins: validates declarations, derives them from publisher-verified platform manifests, announces gRPC endpoints and withdraws them during shutdown. Registration, platform deployment and tenant approval are separate operations.

## Installation

```toml
flare-plugin-host = "0.1.1"
```

## Shared runtime

`PluginRuntimeHost` serves the existing listener's `PluginRuntime` control service, validates platform worker assertions and tenant configuration, and fences invocation tickets by deployment, app, grant revision and configuration digest. `verify_user_call(...).await` returns the admitted identity and configuration snapshot. Production state is PostgreSQL-backed, shared across replicas and restart durable; no positive authorization cache or in-memory production fallback is used. No Cell/private platform key or endpoint self-registration is required. Register the immutable shared deployment with the platform, then apply/approve/configure/enable per app. See [Echo runtime reference](docs/echo-plugin.md) for explicit schema/role provisioning, drain and lease limits.

## Diagnostic registration contract

`PluginDeclaration` requires one concrete tenant, plugin id, entry capability, address, semantic plugin version, API version `1.0`, a lowercase 64-character manifest SHA-256 and a nonempty unique operation list containing the entry capability. Core repeats validation. Incomplete, undeclared and unverified instances cannot receive calls.

Prefer `PluginDeclaration::from_manifest` with a publisher-verified `protocol::signature::VerifiedManifest`. Address and tenant are host configuration, outside the published manifest.

```rust
use flare_plugin_host::{PluginDeclaration, HostError, protocol::signature::VerifiedManifest};

fn declaration(manifest: &VerifiedManifest) -> Result<PluginDeclaration, HostError> {
    PluginDeclaration::from_manifest("tenant-a".into(), "plugin.internal:9000".into(), manifest)
}
```

## Registration credentials

Third-party hosts use `PluginHost::connect_with_workload` (or a custom `RegistrationTokenProvider`). `workload::WorkloadTokenProvider` reopens an operator-provisioned projected token for every exchange, including after kubelet file rotation. The broker in the existing Social platform authenticates with Kubernetes TokenReview, checks the exact service-account name and UID, immutable deployment and approved tenant installation, then returns a 60-second credential bound to the complete registration RPC body. Core accepts it only in the register/deregister RPCs via `x-flare-plugin-registration`; it is never a Cell context. Plugins receive no platform/Cell signing key. The broker is optional for platform-projected endpoint discovery and never participates in business dispatch. Ask your platform operator for the broker endpoint and workload settings.

Trusted first-party composition may use `connect_with_signer`/`signer_from_env`; never mount that root signing key into an untrusted plugin. Core binds service registration to its signed plugin identity, tenant and request id. Missing credentials fail locally; cross-plugin registration/withdrawal is denied.

For business calls, use `verify_user_call(request, &runtime)` to verify the independent `x-flare-plugin-call` ticket against the current acknowledged tenant state. The original Cell `x-flare-ctx` is never a business-call credential for this runtime. Enforce resource permissions in the business handler. `GenericRequest.metadata` is informational and must not establish tenant/user identity. Publisher signature verification and artifact trust are independent checks. `announce` does not grant tenant approval or bypass user operation permissions.

## Plugin server structure

Business plugin servers follow the [DDD + CQRS standard](docs/plugin-ddd-cqrs.md): domain, application (separate command and query handlers), infrastructure and interface layers, with the shared skeleton from `flare-plugin-kit`.

## Staff calls and runtime writes

The admin operations a manifest's `admin` section declares (`PluginManifest::admin_operations()`) are invoked by tenant staff through Core `dispatch_admin`; verify them with `verify_staff_call(request, &runtime)`, which applies the same deployment, grant revision and configuration checks as `verify_user_call`, requires the permission code the operation declares and, for an operation declaring `requiresReason`, the operator's stated reason, and refuses user tickets. User tickets cannot reach admin operations. A deployment publishes entity shared state with no call in flight (deadlines, console changes) through `HostResourceClient::publish_entity_state`, signed with its own runtime key (`FLARE_PLUGIN_RUNTIME_KEY_ID`, `FLARE_PLUGIN_RUNTIME_SIGNING_KEY`) for the exact request bytes; Core registers the public key per deployment in `FLARE_PLUGIN_RUNTIME_PUBLIC_KEYS`. See [Build your own plugin](docs/build-your-own-plugin.md).

## Validation

Run `cargo test --all-targets` and `cargo test --doc` here. The example uses the signed release and public-key configuration documented in `docs/echo-plugin.md`.

## License

Apache-2.0; see [LICENSE](LICENSE).
