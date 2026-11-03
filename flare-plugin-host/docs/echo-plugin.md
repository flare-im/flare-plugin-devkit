# Echo plugin: shared runtime reference

The example serves `PluginRuntime` and `ExtensionPlugin` on one listener. It accepts tenant configuration through authenticated platform-worker assertions and accepts business calls only with dedicated Core plugin tickets. It does not load Cell or platform private keys and needs no endpoint self-registration.

```sh
cargo run --manifest-path flare-plugin-host/Cargo.toml --example echo_plugin
```

Provision a publisher-signed Manifest with `example.echo.say` and `example.echo.upper`, gRPC runtime and artifact digest. Configuration is an empty object. Register the same signed release and shared deployment through the existing platform API; apply, approve, configure and enable per app. See the workspace [runtime operations guide](../../../docs/design/plugin-runtime-operations.md).

| Variable | Meaning |
|---|---|
| `ECHO_PLUGIN_LISTEN` | Optional bind address; defaults to `127.0.0.1:7901` |
| `FLARE_PLUGIN_MANIFEST_PATH` | Original publisher-signed release Manifest file |
| `FLARE_PLUGIN_MANIFEST_SIGNATURE` | Detached Ed25519 signature, Base64 |
| `FLARE_PLUGIN_PUBLISHER_KEY_ID` | Pinned publisher key ID |
| `FLARE_PLUGIN_PUBLISHER_ID` | Pinned publisher identity |
| `FLARE_PLUGIN_PUBLISHER_PUBLIC_KEY` | Base64 raw 32-byte public key |
| `FLARE_PLUGIN_DEPLOYMENT_ID` | Operator-registered immutable deployment identity |
| `FLARE_PLUGIN_GRPC_AUTHORITY` | Operator-registered shared service URL |
| `FLARE_PLATFORM_PUBLIC_KEY` | Platform Ed25519 public key in PEM form |
| `FLARE_PLUGIN_CALL_PUBLIC_KEYS` | JSON array of `{key_id, public_key_base64}`; independent invocation keys |
| `FLARE_PLUGIN_RUNTIME_DATABASE_URL` | PostgreSQL state store shared by replicas of this deployment; distinct least-privilege role |
| `FLARE_PLUGIN_RUNTIME_KEY_ID` | Optional, with the next one: key id of this deployment's runtime credential key |
| `FLARE_PLUGIN_RUNTIME_SIGNING_KEY` | Optional: Base64 raw 32-byte Ed25519 seed for host writes with no call in flight; Core registers the public key for this deployment only in `FLARE_PLUGIN_RUNTIME_PUBLIC_KEYS` |

Provision `initial/runtime.sql` explicitly with a schema owner, then apply `initial/grants.sql` with psql `-v runtime_role=<unique-plugin-role>`. The runtime process never performs DDL. Do not share a state-store role/schema with an unrelated plugin. Replicas must use the same authoritative database; asynchronous read replicas are unsuitable for authorization.

`verify_user_call(...).await` returns verified app/user/request identity, grant revision, and a configuration snapshot for the admitted call. Business code still validates payloads, object ownership/membership and request idempotency. Running work keeps that admitted version/configuration. Every new admission strongly reads the shared state; database failure denies admission. Revision tombstones survive expiry and process restart. Restarted hosts may resume only unexpired, matching durable grants.

SIGINT/SIGTERM closes admission and drains retained guards for up to 30 seconds. This example also bounds RPC processing to 30 seconds. `admit_work` covers independently authenticated provider/background work; it supplies no authorization. Deployments must also bound their business tasks and set a supervisor termination grace period above the drain/RPC budgets. A drain deadline is not proof that every external side effect was cancelled; reconcile unknown outcomes with the same business command ID.

Endpoint registration remains diagnostics for older first-party integrations. It does not confer business execution authority.
