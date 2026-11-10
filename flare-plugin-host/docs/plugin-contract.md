# 插件线上合同

gRPC 插件实现 `flare.capability.v1.ExtensionPlugin/Call`。`GenericRequest` 带 operation、payload、request id 和诊断 metadata；`GenericResponse` 带 ok、payload、结构化错误和 request id。业务字段由版本化 operation schema 定义，Core 不解析插件业务含义。

## 身份与权限

插件使用 `verify_user_call` 验证 `x-flare-plugin-call` 专用调用票据，绑定应用、用户、operation、request id、版本、deployment、grant revision、配置摘要与有效期；同时核对宿主当前已确认的租户状态。原始 `x-flare-ctx` 不作为业务调用凭据。不得用 `GenericRequest.metadata.tenant_id/user_id` 建立身份。资源 ACL 和 operation 权限由对应能力 owner 检查。

同一请求头还有两种票据，签名域各自独立，互相验不过：员工在控制台调管理操作的员工票据（`verify_staff_call`，只放行管理操作），以及平台调用实体生命周期回调的系统票据（`verify_system_call`）。message-content 贡献声明 `entityLifecycle.unbound` 后，绑定的卡片被撤回（`reason: recalled`）或平台没能代发卡片（`reason: post_failed`）时，Core 以系统票据调用这个操作，输入 `{entity, reason, serverMsgId?}`；宿主要求操作就是声明的 unbound、输入里的 `entity` 与 `reason` 等于票据，没有用户和会话。投递至少一次、同一条通知的 request id 不变，插件按实体幂等处理（投票取消、红包退款）。用户票据与员工票据都调不了 unbound 操作。

两种票据的额外核对：员工票据的权限码必须等于 manifest 为该操作声明的码（`admin_permission_for`：`admin.operations[].permission` 对应的 `plugin:<pluginId>:<permission>`），持有本插件的其他权限码也调不了；声明 `requiresReason` 的操作，票据必须带员工填写的 `reason`，否则回 `PermissionDenied`。系统票据被拒时宿主按原因回不同的码：本部署的投影与票据对不上（grant revision 或配置摘要不同、还没有或已过期的租户状态、租户已停用、版本切换中插件/版本/manifest 摘要/deployment 不同、签名密钥还没下发到本部署）回 `Unavailable`，Core 稍后重投；票据在本部署持有的密钥下验不过回 `Unauthenticated`；票据与这次调用不符（操作、request id、实体、原因、租户、带了用户）回 `PermissionDenied`。用户票据与员工票据按同样的规则区分：投影没对上回 `Unavailable`（调用没有执行，Core 按「结果未知、可重试」回给调用方，而不是插件拒绝），验不过回 `Unauthenticated`，与调用不符回 `PermissionDenied`。插件应原样返回 `verify_system_call`、`verify_user_call` 与 `verify_staff_call` 的错误，不要改写成业务拒绝。

发布者 Manifest 签名、运行时注册身份和租户审批是独立检查。发布者密钥来自 `PublisherTrustStore`；第三方插件注册使用控制面签发的短时 Service 上下文；tenant grant 与启用状态由控制面管理。商业授权不参与检查。

## 共享部署与路由

实际业务目标只能来自经平台 worker 认证、带单调版本及租约的 Core 本地授权投影。`PluginRuntime` Inspect/ApplyTenant 与业务 gRPC 共用 listener，平台地址登记必须匹配不可变签名版本与制品摘要。租户配置确认和运行授权投递分离，禁用/撤权投递无需先访问宿主。当前单实例宿主、十分钟租约和后续生命周期边界见工作区 `docs/design/plugin-runtime-operations.md`。

## 诊断注册

注册必须带语义版本、API `1.0`、64 位小写 Manifest SHA-256 和非空唯一的 declared operations。入口必须在清单中。Core 在注册和分发时复用结构校验，未声明/未验证记录仅供诊断，禁止调用。Service 只能登记/注销自己的 plugin id，request id 和租户必须与签名匹配；服务身份只能列出自身实例。

注册声明不验证制品的实际字节，也不证明平台审批完成；这些由 Registry/控制面及运行时执行。注册不能成为插件自行增加权限的入口。

## 可选依赖与服务委托

`dependencies[].optional` 只表示安装时允许缺少这个插件。可选插件已经安装且版本符合 `versionRange` 时，可以经现有 `PluginServiceRouter` 调用它已声明的服务；仍然必须核对源插件的服务分发能力、双方当前应用授权、目标服务操作及权限/数据范围，以及相应的 `User` 或 `Recovery` 票据。未声明的目标、版本不兼容、目标缺席或审批不足都拒绝。协议 v1 的依赖匹配使用同一版本范围规则，不用把可选依赖改成必装来启用服务调用。

## 失败与生命周期

身份、租户、权限和声明错误必须拒绝；可用性错误返回明确错误。超时不代表业务未执行，调用者使用稳定幂等键查询或重试。事件至少一次交付，消费者去重并提供重试/DLQ。

运行状态与租户启用分离。后续生命周期验收要求新版本先验证/启动/健康，再切换新请求，旧版本 drain 在途业务。停用后历史 APP_CARD/CUSTOM 消息用通用安全摘要展示，动作不可执行。卸载默认保留数据，删除需要显式策略和独立授权。
## 异步消息观察

托管插件用签名 Manifest 的 `eventSubscriptions` 声明 `im.message.created`（`schemaVersion: 1`）、专用幂等 `operation`、`permissionIds` 和默认 `false` 的 `includeBody`。元数据需要显式批准 `messages.events.read`；正文另需 `messages.body.read`。目前只投递原始成员文本新消息，机器人、系统、转发与操作事件不进入该事件。

Core 的后台持久消费者观察已由 Ingest 接受的 JetStream 主流，不进入同步发送路径。事件含 `id`、`type`、`schemaVersion`、`app`、`time`、`data`；`time` 是服务端摄入时间，`data` 中的发送者、会话与服务端消息 ID 来自主流。Core 只投递审批与配置生效水位之后的消息，该水位持久化并签入票据，可从 `active_since_ms` 读取；普通投影租约与健康续期不推进水位，新审批或配置不会重新解释历史正文。投递至少一次、不保证顺序；消费者有界并发、退避重试与持久死信。插件必须按稳定事件 ID 幂等处理；回放使用原始时间，不能把历史消息当成到达当天的新动作。流有容量和保留期上限。

Core 将已批准的期望订阅与当前可路由的部署分别读取：平台投影租约或 runtime/deployment 健康 TTL 过期不会删除期望订阅，待投事件继续退避重试，恢复后仍用原事件 ID；单个绑定的版本或清单暂不可用也保留待恢复状态，其他可用订阅方继续接收，即使只有待恢复绑定也不会确认掉事件。开发模式采用同样的隔离规则。明确卸载、关闭、撤销审批或关闭功能才停止对应订阅。策略读端只有明确的 `PolicyDenied` 表示停用，存储故障等错误保留事件重试，不能当成关闭后确认掉。重试耗尽进入持久 DLQ。

生效水位由迁移 `0028` 持久化。运行时健康续租保留已存的期望配置密文，相同明文配置的重复写入也复用原密文；包含非空 secret 时不会因为加密随机 nonce 改变而推进水位、误丢积压。实际审批、功能、启停或配置变更才推进对应水位。

回调使用独立的 Core 事件签名域，绑定原始载荷、插件部署、应用授权修订及配置。插件必须先调用 `verify_event_call`，取得不可自行构造的 `VerifiedPluginEvent`。事件不是普通用户票据；`read_event` 只允许检查签名发送者是否仍为签名会话成员，`call_service_event` 通过 Core 转投受限的 `User` 模式服务操作并保留可信会话。事件驱动的业务组合必须用 `operation_enabled` 检查对应用户操作的功能开关，该结果来自 Core 签名功能快照与当前授权，不能由事件正文或配置自报。
