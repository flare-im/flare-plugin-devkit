# flare-plugin-host

[English](README.md) · 中文

插件 endpoint 接入 SDK：校验声明、从发布者验签后的平台 Manifest 生成声明、登记 gRPC 地址和停机注销。endpoint 注册、平台部署和租户审批是不同操作。

## 共享运行时

`PluginRuntimeHost` 与业务插件共用 gRPC listener，验证平台 worker 断言和租户配置，以修订号隔离启停和配置。平台登记共享部署后，按应用申请、审批、配置和启用；此路径无需持有 Cell 私钥，也不依赖 endpoint 自注册。`verify_user_call(...).await` 返回准入时的身份与配置快照。生产宿主使用共享 PostgreSQL 强读授权、持久修订墓碑，支持副本和重启；不回退进程内授权缓存。SIGINT/SIGTERM 停止新准入并排空，provider/后台工作通过 `admit_work` 计入排空，但仍须独立授权。显式 schema/SQL 角色、运行方式与租约限制见 [Echo 示例](docs/echo-plugin.md) 和工作区 [运行时规范](../../docs/design/plugin-runtime-operations.md)。

## 安装

```toml
flare-plugin-host = "0.1.1"
```

## 合同

`PluginDeclaration` 必须包含具体租户、插件 ID、入口 capability、地址、语义版本、API 版本 `1.0`、64 位小写十六进制 Manifest SHA-256，以及非空、不重复并包含入口的 operation 清单。Core 复用同一校验；声明缺失、未验证或越过声明的调用均拒绝。

优先使用 `PluginDeclaration::from_manifest` 从 `protocol::signature::VerifiedManifest` 生成声明。tenant 和地址来自宿主配置。

## 凭据

第三方插件使用 `connect_with_token_provider` 和控制面提供的 `RegistrationTokenProvider`：broker 验证 workload 并授权插件/部署/租户，为每个 request id 签发短时上下文。插件进程不持有 Cell 私钥；broker 和 RPC 均有超时。具体 broker adapter 由控制面接入。

`connect_with_signer`/`signer_from_env` 仅用于可信第一方组合，不能把根签名密钥挂入第三方插件。Core 校验 service actor 与请求中的插件 ID、租户、request id 一致；无凭据在本地失败，不能注册或注销其他插件。

业务调用使用 `verify_user_call(request, &runtime).await` 校验独立的 `x-flare-plugin-call`，核对共享部署、应用、grant 修订和配置摘要，再检查资源权限。宿主不接受原始 Cell `x-flare-ctx` 作为业务调用凭据。`GenericRequest.metadata` 中的 tenant/user 不可作为身份真源。endpoint 注册不代表发布者/制品可信或租户审批通过。

## 插件服务端结构

业务插件服务端按 [DDD + CQRS 规范](docs/plugin-ddd-cqrs.md) 组织：领域、用例（命令与查询的 handler 分开）、基础设施、接口四层，公共骨架用 `flare-plugin-kit`。

## 员工调用与主动写入

manifest `admin` 段声明的管理操作（`PluginManifest::admin_operations()`）由租户员工经 Core `dispatch_admin` 调用，用 `verify_staff_call(request, &runtime)` 校验：部署、grant 修订号与配置摘要的检查和 `verify_user_call` 相同，权限码须等于该操作声明的码，`requiresReason` 的操作须带原因，用户票据一律拒绝；用户票据也到不了管理操作。没有调用在途时（截止、控制台变更），部署经 `HostResourceClient::publish_entity_state` 发布实体共享状态，用自己的运行时密钥（`FLARE_PLUGIN_RUNTIME_KEY_ID`、`FLARE_PLUGIN_RUNTIME_SIGNING_KEY`）对请求原文签名；Core 在 `FLARE_PLUGIN_RUNTIME_PUBLIC_KEYS` 里按部署登记公钥。详见 [开发插件](docs/build-your-own-plugin.md)。

## 检查

在此目录运行 `cargo test --all-targets` 和 `cargo test --doc`。示例所需 `FLARE_PLUGIN_MANIFEST_SHA256` 由发布流程提供。租户审批、启停和调用者 operation 权限独立执行。

## 许可

Apache-2.0；见 [LICENSE](LICENSE)。
