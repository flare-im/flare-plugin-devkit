# flare-plugin-host

[English](README.md) · 中文

插件进程接入 SDK：接收 Core 下发的租户状态，校验 Core 签的票据，访问宿主能力。进程不自己注册：部署由运维登记到 Core。

## 共享运行时

`PluginRuntimeHost` 与业务插件共用 gRPC listener，验证 Core 的租户状态票据和租户配置，以修订号隔离启停和配置。平台登记共享部署后，按应用申请、审批、配置和启用；此路径无需持有 Cell 私钥，也不依赖 endpoint 自注册。`verify_user_call(...).await` 返回准入时的身份与配置快照。生产宿主使用共享 PostgreSQL 强读授权、持久修订墓碑，支持副本和重启；不回退进程内授权缓存。SIGINT/SIGTERM 停止新准入并排空，provider/后台工作通过 `admit_work` 计入排空，但仍须独立授权。显式 schema/SQL 角色、运行方式与租约限制见 [Echo 示例](docs/echo-plugin.md) 和工作区 [运行时规范](../../docs/design/plugin-runtime-operations.md)。

## 安装

```toml
flare-plugin-host = "0.1.1"
```

## 票据

业务调用使用 `verify_user_call(request, &runtime).await` 校验独立的 `x-flare-plugin-call`，核对共享部署、应用、grant 修订和配置摘要，再检查资源权限。宿主不接受原始 Cell `x-flare-ctx` 作为业务调用凭据。`GenericRequest.metadata` 中的 tenant/user 不可作为身份真源。endpoint 注册不代表发布者/制品可信或租户审批通过。

## 插件服务端结构

业务插件服务端按 [DDD + CQRS 规范](docs/plugin-ddd-cqrs.md) 组织：领域、用例（命令与查询的 handler 分开）、基础设施、接口四层，公共骨架用 `flare-plugin-kit`。

## 员工调用与主动写入

manifest `admin` 段声明的管理操作（`PluginManifest::admin_operations()`）由租户员工经 Core `dispatch_admin` 调用，用 `verify_staff_call(request, &runtime)` 校验：部署、grant 修订号与配置摘要的检查和 `verify_user_call` 相同，权限码须等于该操作声明的码，`requiresReason` 的操作须带原因，用户票据一律拒绝；用户票据也到不了管理操作。没有调用在途时（截止、控制台变更），部署经 `HostResourceClient::publish_entity_state` 发布实体共享状态，用自己的运行时密钥（`FLARE_PLUGIN_RUNTIME_KEY_ID`、`FLARE_PLUGIN_RUNTIME_SIGNING_KEY`）对请求原文签名；Core 在 `FLARE_PLUGIN_RUNTIME_PUBLIC_KEYS` 里按部署登记公钥。详见 [开发插件](docs/build-your-own-plugin.md)。

## 检查

在此目录运行 `cargo test --all-targets` 和 `cargo test --doc`。示例所需 `FLARE_PLUGIN_MANIFEST_SHA256` 由发布流程提供。租户审批、启停和调用者 operation 权限独立执行。

## 许可

Apache-2.0；见 [LICENSE](LICENSE)。
