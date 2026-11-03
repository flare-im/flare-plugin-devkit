# 插件开发文档

给**第三方插件作者**的完整资料。平台不需要为你的插件改一行代码。

## 从哪开始

| 我想…… | 看这个 |
|---|---|
| 直接看能跑的代码 | [`examples/echo_plugin.rs`](../examples/echo_plugin.rs)（约 130 行，`cargo run --example echo_plugin`） |
| 搞清楚平台和插件之间到底传什么 | [插件契约](./plugin-contract.md) |
| 从零做一个插件 | [自己开发一个插件](./build-your-own-plugin.md) |
| 按什么结构写插件服务端 | [插件服务端规范：DDD + CQRS](./plugin-ddd-cqrs.md) |
| 查某个字段的确切语义 | [`flare-plugin-host` API 文档](https://docs.rs/flare-plugin-host) |

## 三分钟版本

```bash
cargo add flare-plugin-host
```

1. 实现 `ExtensionPlugin.Call`：一个 RPC，`(operation, JSON) -> JSON`。
2. 启动后 `announce()`：声明你能处理的全部 operation，以及计费单位。
3. 平台把你记进路由簿；声明之外的调用到不了你的进程。

三步的每一步都有一个不报错的失败方式，文档里逐条标了出来 ——
这也是为什么建议用 `flare-plugin-host` 而不是手写注册调用。
