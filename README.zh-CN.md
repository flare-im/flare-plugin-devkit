# Flare 插件 DevKit

[English](README.md) · 简体中文

用公开的 Apache-2.0 宿主库、脚手架和 WASM 运行时开发 Flare IM 插件。本仓还提供 Manifest schema 和示例模板。

- [插件开发指南](flare-plugin-host/docs/build-your-own-plugin.md)介绍 gRPC 或 WASM 的开发入口。
- [Echo 示例](flare-plugin-host/examples/echo_plugin.rs)演示宿主契约。
- [Manifest schema](schema/) 和[模板](template/)用于校验和生成插件声明。

将 `flare-protocol`、`flare-protocol-grpc` 和 `flare-server-runtime` 并列检出，并安装 `protoc`。

```sh
cargo test --manifest-path flare-plugin-host/Cargo.toml
```

本地与 CI 使用公开源码依赖。手动验证 registry 打包时，需要 crates.io 上存在对应版本的 `flare-plugin-protocol`。发布时用流水线产出的摘要和签名替换示例 Manifest 的占位值。
