# flare-plugin-host

English · [中文](README.zh-CN.md)

The process SDK for Flare plugins: serves the tenant state Core delivers, verifies the tickets Core signs, and reaches host capabilities. A plugin process never registers itself: the operator registers its deployment with Core.

## Installation

```toml
flare-plugin-host = "0.1.1"
```

## Shared runtime

`PluginRuntimeHost` serves the existing listener's `PluginRuntime` control service, validates Core tenant state tickets and tenant configuration, and fences invocation tickets by deployment, app, grant revision and configuration digest. `verify_user_call(...).await` returns the admitted identity and configuration snapshot. Production state is PostgreSQL-backed, shared across replicas and restart durable; no positive authorization cache or in-memory production fallback is used. No Cell/private platform key or endpoint self-registration is required. Register the immutable shared deployment with the platform, then apply/approve/configure/enable per app. See [Echo runtime reference](docs/echo-plugin.md) for explicit schema/role provisioning, drain and lease limits.

## Tickets

For business calls, use `verify_user_call(request, &runtime)` to verify the independent `x-flare-plugin-call` ticket against the current acknowledged tenant state. The original Cell `x-flare-ctx` is never a business-call credential for this runtime. Enforce resource permissions in the business handler. `GenericRequest.metadata` is informational and must not establish tenant/user identity. Publisher signature verification and artifact trust are independent checks. `announce` does not grant tenant approval or bypass user operation permissions.

## Plugin server structure

Business plugin servers follow the [DDD + CQRS standard](docs/plugin-ddd-cqrs.md): domain, application (separate command and query handlers), infrastructure and interface layers, with the shared skeleton from `flare-plugin-kit`.

## Staff calls and runtime writes

The admin operations a manifest's `admin` section declares (`PluginManifest::admin_operations()`) are invoked by tenant staff through Core `dispatch_admin`; verify them with `verify_staff_call(request, &runtime)`, which applies the same deployment, grant revision and configuration checks as `verify_user_call`, requires the permission code the operation declares and, for an operation declaring `requiresReason`, the operator's stated reason, and refuses user tickets. User tickets cannot reach admin operations. A deployment publishes entity shared state with no call in flight (deadlines, console changes) through `HostResourceClient::publish_entity_state`, signed with its own runtime key (`FLARE_PLUGIN_RUNTIME_KEY_ID`, `FLARE_PLUGIN_RUNTIME_SIGNING_KEY`) for the exact request bytes; Core registers the public key per deployment in `FLARE_PLUGIN_RUNTIME_PUBLIC_KEYS`. See [Build your own plugin](docs/build-your-own-plugin.md).

## Validation

Run `cargo test --all-targets` and `cargo test --doc` here. The example uses the signed release and public-key configuration documented in `docs/echo-plugin.md`.

## License

Apache-2.0; see [LICENSE](LICENSE).
