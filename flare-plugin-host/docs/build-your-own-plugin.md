# 开发插件

插件服务端必须按 [DDD + CQRS 规范](./plugin-ddd-cqrs.md) 组织：领域、用例（命令与查询分开）、基础设施、接口四层；公共骨架用 `flare-plugin-kit`。

1. 运行 `flare-plugin new plugin acme.poll` 生成插件目录，再改 `manifest.json`：稳定 ID、版本、capability/operation、最小权限与数据范围、依赖、事件、功能、UI 和卸载数据策略。安装方法见 [DevKit README](../../README.zh-CN.md)。
2. 运行 `flare-plugin check <dir>`。它用 `PluginManifest::from_json` 校验语义（权限引用、重复 operation、API 兼容与依赖范围），输出警告和推导出的插件类型。
3. 可信发布流程生成制品摘要，写进 `artifact`。图标和截图放进 `assets/`，`pack` 把它们的摘要写进所签的 Manifest，磁盘上的文件不变。运行 `flare-plugin pack <dir> --key publisher.pem --key-id <id>`：用独立发布者密钥对精确清单字节签名，写出 `<pluginId>-<version>.fpkg`。相同目录和密钥得到逐字节相同的包。
4. 运行 `flare-plugin publish <file> --url <控制台地址>`，令牌放在 `FLARE_PLUGIN_TOKEN`，需要 `plugins:catalog:write`；也可以在控制台版本页选择文件上传。平台 `PublisherTrustStore` 验签后获得 `VerifiedManifest`，素材入库，`PluginRegistry` 固定版本和依赖摘要。版本登记后不上架，上架是单独的操作。
5. 实现 `ExtensionPlugin.Call`，只处理声明的 operation。上下文必须来自 `PluginRuntimeHost` 校验的 `x-flare-plugin-call`，并使用返回的配置快照；metadata 中的 tenant/user 仅用于诊断。
6. 组合 `PluginRuntimeHost` 与 `PluginRuntimeServer`，提供租户配置 validator，与业务服务共用 listener。使用发布者验签后的 Manifest 和平台登记的 deployment 绑定。
7. 宿主仅持有发布者、平台控制和独立调用 signer 的公钥。平台审批及配置由既有 scheduler 投递；业务处理器使用 `verify_user_call`。诊断注册是可选路径，不授予业务调用权。
8. 平台发布、依赖校验、租户申请/审批、配置和启用独立执行。注册成功不等于批准，也不能绕过用户 operation 权限。
9. 停机通过 `set_draining` 停止新请求，业务实现完成或取消已准入任务。升级与回滚编排仍按后续阶段验收；新旧版本隔离业务和配置。

插件独立拥有业务数据并以 tenant 强制分区。查询、缓存、事件订阅、对象存储 key 和密钥都包含租户范围。业务命令提供幂等键；跨服务副作用使用 outbox/幂等消费。卸载默认保留数据，清理需要独立授权和审计。

## 本机开发

`flare-plugin dev` 把本机进程接到一个开发应用上，不走打包、上架、申请和审批：

```sh
flare-plugin dev ./acme.poll --app 201 --url http://127.0.0.1:50051 \
  --token "$FLARE_PLUGIN_TOKEN" --process http://127.0.0.1:7900
```

它把 `manifest.json` 登记成未签名的开发版本，加一个指向 `--process` 的部署，再写入这个应用的状态。之后每秒看一次文件，内容变了就重新登记。令牌是这个应用的员工令牌，需要 `core:plugins:write`；`--url` 是 Core 管理网关，不是控制台。配置用 `--config <file>` 传，默认 `{}`，文件变了同样重新登记。按 Ctrl-C 退出，已登记的内容留着，进程不再应答后 Core 把插件从应用里摘掉。

要满足三个条件，缺一个 Core 都拒绝：

1. Capability 启动时设了 `FLARE_PLUGIN_DEV_MODE=1`。只有值为 `1` 才生效。发版构建和部署模板从不设它，发版监管进程也不继承。没有这个开关，已有的开发数据一律不生效。
2. 这个应用被标记为开发用途。平台员工用 `core:plugins:platform:write` 调用 `PUT /api/v1/admin/platform/plugin-registry/dev-apps/{app_id}`，请求体 `{"development":true}`。写 `false` 同时删除这个应用的开发绑定。
3. 这个版本号没有被正式版本占用，插件 ID 和 capability 命名空间不属于别的发布者或插件。

开发版本不进市场，也不出现在注册表列表、版本详情和部署列表里，不能发布，不能用 `plugin_config.put` 绑定，投影也不能引用它。声明的操作和权限全部授予，功能按 `defaultEnabled`，同一插件已有的正式安装被它顶替。这些只发生在开发应用里。

本机进程按正常方式组合 `PluginRuntimeHost`，环境变量要和登记内容对上：

- `FLARE_PLUGIN_DEPLOYMENT_ID`：默认 `dev-<pluginId>`，用 `--deployment-id` 改，必须以 `dev-` 开头。两个应用同时开发同一插件时各用一个。
- `FLARE_PLUGIN_GRPC_AUTHORITY`：和 `--process` 相同，命令也读这个变量。地址要从 Capability 所在的位置能连上。
- `FLARE_PLUGIN_MANIFEST_PATH`：指向同一个 `manifest.json`。Core 记录的摘要按文件字节算，所以不会像 `pack` 那样补素材表。
- `FLARE_PLUGIN_CALL_PUBLIC_KEYS`：用开发栈的公钥。
- 宿主启动时仍然校验 Manifest 签名。用自己的开发密钥签，把公钥写进 `FLARE_PLUGIN_PUBLISHER_PUBLIC_KEY`。

Manifest 必须带 `artifact`，占位内容也行。宿主只在启动时读一次 Manifest，改完要带新签名重启进程，否则摘要对不上，调用报投影过期。改同一版本号的 Manifest 时，别的应用如果在用这个版本的旧内容，登记会被拒绝，换一个版本号。

目前的限制：`assets/` 不上传；Storage 读写服务只看平台投影，开发插件发的卡片不会绑定实体，要验证卡片仍然走 `scripts/plugin-online-lab.py`。这个脚本验收完整的发布、审批和投影链路，`dev` 不替代它。

## 发布者密钥

用 OpenSSL 生成 Ed25519 密钥，再取出平台要登记的公钥：

```sh
openssl genpkey -algorithm ed25519 -out publisher.pem
openssl pkey -in publisher.pem -pubout -outform DER | tail -c 32 | base64
```

运维把公钥写进平台的 `FLARE_PLUGIN_PUBLISHER_KEYS`：`[{"key_id","publisher_id","public_key_base64"}]`。`publisher_id` 必须等于 Manifest 的 `publisher.publisherId`。私钥不要进仓库。

## Hook 服务

Hook 不是插件，不走打包和发布。运行 `flare-plugin new hook spam-filter` 生成一个只实现 `HookPlugin.Call` 的 `pre_send` 服务，在 `Hook::pre_send` 里写规则。生成的 `hooks.toml` 是部署配置样例；平台和应用的 Hook 在控制台配置。

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
