# Flare 插件 DevKit

[English](README.md) · 简体中文

用 `flare-plugin` 命令、公开的 Apache-2.0 宿主库、脚手架和 WASM 运行时开发 Flare IM 插件。本仓还提供 Manifest schema 和示例模板。

- [插件开发指南](flare-plugin-host/docs/build-your-own-plugin.md)介绍 gRPC 或 WASM 的开发入口。
- [Echo 示例](flare-plugin-host/examples/echo_plugin.rs)演示宿主契约。
- [Manifest schema](schema/) 和[模板](template/)用于校验和生成插件声明。

## flare-plugin 命令

并列检出 `flare-protocol` 后，从本仓安装：

```sh
cargo install --path flare-plugin-cli
```

| 命令 | 作用 |
|---|---|
| `flare-plugin new plugin acme.poll` | 生成 `acme.poll/`，含 Manifest、README 和更新日志 |
| `flare-plugin new hook spam-filter` | 在 `spam-filter/` 生成 `pre_send` Hook 服务 |
| `flare-plugin check <dir>` | 校验 `manifest.json`，输出警告和推导出的插件类型 |
| `flare-plugin pack <dir> --key <file> --key-id <id>` | 签名 Manifest，写出可复现的 `.fpkg` |
| `flare-plugin publish <file> --url <控制台> --token <令牌>` | 把包上传到平台 |
| `flare-plugin dev <dir> --app <id> --url <Core 管理地址> --token <令牌>` | 把本机进程作为插件接到标记为开发用途的应用上，`manifest.json` 变了就重新登记 |

`--key` 接收 PKCS#8 PEM 格式的 Ed25519 私钥。`--token` 也读 `FLARE_PLUGIN_TOKEN`，这样令牌不会进 shell 历史。

## 构建

将 `flare-protocol`、`flare-protocol-grpc` 和 `flare-server-runtime` 并列检出，并安装 `protoc`。

```sh
cargo test --manifest-path flare-plugin-host/Cargo.toml
```

本地与 CI 使用公开源码依赖。手动验证 registry 打包时，需要 crates.io 上存在对应版本的 `flare-plugin-protocol`。发布时用流水线产出的摘要和签名引用替换示例 Manifest 的占位值，`flare-plugin check` 在替换前会一直警告。
