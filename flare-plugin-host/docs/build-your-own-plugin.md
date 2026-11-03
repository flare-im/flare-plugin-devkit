# 开发插件

插件服务端必须按 [DDD + CQRS 规范](./plugin-ddd-cqrs.md) 组织：领域、用例（命令与查询分开）、基础设施、接口四层；公共骨架用 `flare-plugin-kit`。

1. 从 `../../template/platform-plugin.template.json` 建立平台清单，定义稳定 ID、版本、capability/operation、最小权限与数据范围、依赖、事件、Hook、UI 和卸载数据策略。
2. 使用 `flare-plugin-protocol` 的 `PluginManifest::validate` 校验语义。JSON Schema 校验结构；权限引用、重复 operation、API 兼容与依赖范围也必须通过。
3. 可信发布流程生成制品摘要，使用独立发布者密钥对精确清单字节签名。平台 `PublisherTrustStore` 验签后获得 `VerifiedManifest`，`PluginRegistry` 固定版本和依赖摘要。
4. 实现 `ExtensionPlugin.Call`，只处理声明的 operation。上下文必须来自 `PluginRuntimeHost` 校验的 `x-flare-plugin-call`，并使用返回的配置快照；metadata 中的 tenant/user 仅用于诊断。
5. 组合 `PluginRuntimeHost` 与 `PluginRuntimeServer`，提供租户配置 validator，与业务服务共用 listener。使用发布者验签后的 Manifest 和平台登记的 deployment 绑定。
6. 宿主仅持有发布者、平台控制和独立调用 signer 的公钥。平台审批及配置由既有 scheduler 投递；业务处理器使用 `verify_user_call`。诊断注册是可选路径，不授予业务调用权。
7. 平台发布、依赖校验、租户申请/审批、配置和启用独立执行。注册成功不等于批准，也不能绕过用户 operation 权限。
8. 停机通过 `set_draining` 停止新请求，业务实现完成或取消已准入任务。升级与回滚编排仍按后续阶段验收；新旧版本隔离业务和配置。

插件独立拥有业务数据并以 tenant 强制分区。查询、缓存、事件订阅、对象存储 key 和密钥都包含租户范围。业务命令提供幂等键；跨服务副作用使用 outbox/幂等消费。卸载默认保留数据，清理需要独立授权和审计。

## 可选的工作负载自注册

平台投影已经包含共享部署地址；正常安装、配置、启停和业务调用不依赖注册 broker。需要宿主自注册时，使用 `PluginHost::connect_with_workload(endpoint, plugin_id).await`，再调用 `announce`。`plugin_id` 及声明应来自已验证的 Manifest，租户应来自已批准的部署编排。

运维提供 `FLARE_PLUGIN_BROKER_URL`（HTTPS 完整路径 `/api/v1/internal/plugins/registration-tokens`）、`FLARE_PLUGIN_WORKLOAD_TOKEN_FILE`（投影文件绝对路径）、`FLARE_PLUGIN_DEPLOYMENT_ID`，可选 `FLARE_PLUGIN_BROKER_CA_FILE`。投影 audience 固定为 `flare-plugin-broker`；使用只读目录挂载，不使用 `subPath`。不要挂载平台或 Cell 私钥。

宿主每次重新打开令牌文件，支持 Kubernetes 原子替换轮换。平台核对 TokenReview 中的 ServiceAccount 名称、UID、Pod 绑定，以及不可变部署和租户审批；只返回完整请求绑定、60 秒有效的 `plugin_registration` 凭据。Core 仅在注册/注销入口接受，不能用于用户调用、资源回调或配置投影。错误会显式返回，不回退裸注册。配置变更由运维发布；租户的安装和启停无需重启 IM 服务。

自定义 `RegistrationTokenProvider` 接收完整的 `RegistrationRequest`，不能只按插件、租户和 request ID 签发通用身份。共享部署退出单个副本时使用 runtime drain；不要对整个共享服务执行 `withdraw`。注销是租户/插件地址簿操作，不能代替多实例排空。

## 读取当前会话资源

在目标 operation 的 `hostCapabilities` 中声明 `im.conversation.read`，并在该 operation 的权限 `dataScopes` 中声明相同范围。平台审批精确版本与权限后，Core 仍按原用户、会话成员、租户功能开关和当前授权投影逐次校验。权限扩展发布新版本并重新申请；发布模板不是已验证制品。

业务处理器先调用 `flare_plugin_host::verify_user_call(&request, &host).await?`，保留返回的 `VerifiedPluginUser`（含准入 guard 和配置快照），再将它传给 `resource::HostResourceClient::read`。请求类型使用 `flare_plugin_protocol::resource::ResourceRequest::ConversationRead {}`；响应可反序列化为 `ConversationView`。另外两个只读能力是分页参与人和当前群成员。API 不接受插件指定 tenant、actor、conversation、group、URL 或 SQL；群 ID 由 Core 从已授权会话导出。

客户端仅连接运维指定的 `FLARE_PLUGIN_HOST_BRIDGE_URL`，不从租户配置选择上游。它转交独立调用票据，不获取用户 bearer 或 Cell 签名密钥。Core 对同一原始调用提供跨实例最多 8 次回调预算，并在返回资源前再次检查授权。请求失败应返回受控错误；不要用缓存授权或自报身份绕过拒绝。

服务端 WASM 使用同一 Host/Runtime RPC，详见 `flare-plugin-wasm/README.md`；guest 仅可通过受限 `flare_host.call` 访问已声明的只读能力。不会获得文件、网络、进程或原始调用票据。

运行 `cargo test --all-targets`、`cargo test --doc` 和协议测试。故障用例包含越权 tenant/operation、无效签名、缺少声明、插件超时/崩溃、重复命令、禁用消息降级和升级 drain。

## 管理页与员工调用

管理面在 manifest 顶层 `admin` 段声明：`admin.permissions`（员工权限，授予码 `plugin:<pluginId>:<permission>`）、`admin.operations`（即 `PluginManifest::admin_operations()`，每条声明 `mode`、所需权限，写操作按需 `requiresReason` 与 `auditTarget`）、`admin.pages` 与 `admin.navigation`。员工在控制台操作时，Core `dispatch_admin` 核对员工持有该操作声明的码、`requiresReason` 的操作带了原因，再签发员工票据，仍经 `ExtensionPlugin.Call` 的 `x-flare-plugin-call` 送达，不带会话和 user_id。

业务处理器按 operation 选择校验：管理操作用 `flare_plugin_host::verify_staff_call(&request, &host).await?`，其余用 `verify_user_call`。`verify_staff_call` 的部署、版本、Manifest 摘要、request id、grant 修订号、配置摘要与启用校验和用户票据相同，并要求操作属于 `admin_operations()`、权限码等于该操作声明的码、声明 `requiresReason` 的操作带着原因。用户票据调用管理操作、员工票据调用普通操作都会被拒绝。返回的 `VerifiedPluginStaff` 只按租户授权，实体 ID 由输入给出，插件仍须核对租户归属；`reason` 是员工填写的原因，供插件写自己的业务审计。

## 主动发布实体状态

用户调用的结果可以带 `data.sharedState`，由 Core 发布给会话全体成员。没有调用在途的变更（截止自动结束、控制台关闭）用部署自己的运行时凭据写：

1. 在会产生状态变更的 operation 上声明 `hostCapabilities: ["im.plugin.entity_state.publish"]`，并在它的某个权限 `dataScopes` 中声明同名范围。Core 只发布声明了写能力的操作的结果，运行时凭据也要求至少一个已审批的操作声明它。
2. 运维为每个部署生成独立的 Ed25519 密钥：私钥种子（32 字节，标准 base64）配置为 `FLARE_PLUGIN_RUNTIME_SIGNING_KEY`，密钥 id 配置为 `FLARE_PLUGIN_RUNTIME_KEY_ID`；公钥登记到 Core capability 的 `FLARE_PLUGIN_RUNTIME_PUBLIC_KEYS`（`[{"key_id","deployment_id","public_key_base64"}]`，每把只验证登记的部署）。两个变量要么都配、要么都不配；都不配时插件不能主动发布。
3. 调用 `resource::HostResourceClient::publish_entity_state(&host, tenant, grant_id, state)`。宿主读取该租户 grant 的当前修订号，确认已启用且未过期，再对请求体原文签发 30 秒的 `x-flare-plugin-runtime` 凭据。返回 `EntityStatePublishView`，`published`、`duplicate`、`stale` 都算成功。

`version` 对每个实体严格递增，Core 只保留最高版本；同一版本从结果和主动发布各送一次是幂等的。
