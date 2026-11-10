# Flare Plugin DevKit

English · [Simplified Chinese](README.zh-CN.md)

Build gRPC or WebAssembly plugins for Flare IM. The repository contains the `flare-plugin` command, the Apache-2.0 host library, shared plugin scaffolding, WASM runtime, and a manifest schema and example template.

Start with [Build your own plugin](flare-plugin-host/docs/build-your-own-plugin.md). The [echo plugin](flare-plugin-host/examples/echo_plugin.rs) demonstrates the host contract.

## The flare-plugin command

Install it from this repository, with `flare-protocol` checked out alongside:

```sh
cargo install --path flare-plugin-cli
```

| Command | What it does |
|---|---|
| `flare-plugin new plugin acme.poll` | Creates `acme.poll/` with a manifest, README and changelog |
| `flare-plugin new hook spam-filter` | Creates a `pre_send` hook service in `spam-filter/` |
| `flare-plugin check <dir>` | Validates `manifest.json`, prints warnings and the derived plugin kind |
| `flare-plugin pack <dir> --key <file> --key-id <id>` | Signs the manifest and writes a reproducible `.fpkg` |
| `flare-plugin publish <file> --url <console> --token <token>` | Uploads a package to the platform |
| `flare-plugin dev <dir> --app <id> --url <core admin> --token <token>` | Runs a local process as the plugin in an application flagged for development, and re-registers it when `manifest.json` changes |

`--key` takes a PKCS#8 PEM Ed25519 private key. `--token` also reads `FLARE_PLUGIN_TOKEN`, which keeps it out of your shell history.

Clone [flare-protocol](https://github.com/flare-im/flare-protocol), [flare-protocol-grpc](https://github.com/flare-im/flare-protocol-grpc), and [flare-server-runtime](https://github.com/flare-im/flare-server-runtime) alongside this repository. Run `cargo test --manifest-path flare-plugin-host/Cargo.toml` from the repository root. The protobuf compiler must be on PATH.

The template contains placeholder artifact metadata. Replace it with the digest and signature reference from your release pipeline. `flare-plugin check` warns until you do.

Use the public sibling source checkouts for development and CI. The manual registry packaging check requires the matching `flare-plugin-protocol` crate to be available on crates.io.

See the [manifest schema](schema/) and [template](template/) when validating or generating a plugin manifest.
