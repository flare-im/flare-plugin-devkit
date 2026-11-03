# Flare Plugin DevKit

Build gRPC or WebAssembly plugins for Flare IM. The repository contains the Apache-2.0 host library, shared plugin scaffolding, WASM runtime, and a manifest schema and example template.

Start with [Build your own plugin](flare-plugin-host/docs/build-your-own-plugin.md). The [echo plugin](flare-plugin-host/examples/echo_plugin.rs) demonstrates the host contract.

Clone [flare-protocol](https://github.com/flare-im/flare-protocol), [flare-protocol-grpc](https://github.com/flare-im/flare-protocol-grpc), and [flare-server-runtime](https://github.com/flare-im/flare-server-runtime) alongside this repository. Run `cargo test --manifest-path flare-plugin-host/Cargo.toml` from the repository root. The protobuf compiler must be on PATH.

The template contains placeholder artifact metadata. Replace it with the digest and signature produced by your publishing pipeline.
