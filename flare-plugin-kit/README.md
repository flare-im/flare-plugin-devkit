# flare-plugin-kit

业务插件（投票、接龙、任务、日程、群公告）共用的服务端骨架，建在 `flare-plugin-host` 之上，对应 [插件服务端规范：DDD + CQRS](../flare-plugin-host/docs/plugin-ddd-cqrs.md) 里的公共部分。插件只保留自己的规则和表，其余由本 crate 提供。红包暂未迁入。

| 模块 | 提供什么 |
| --- | --- |
| `boot` | `runtime_from_env`：从运维环境组装 `PluginRuntimeHost`（验签 Manifest、租户状态库、调用公钥、可选运行时凭据），用配置校验函数把关租户配置；`ensure_release` 核对 Manifest 的插件 ID、版本和操作集与二进制一致；`serve` 在同一 listener 上提供运行时控制面和业务服务，SIGINT/SIGTERM 后 30 秒内排空；`spawn_periodic` 跑持有准入的后台任务 |
| `database` | 连接池：每个连接设置插件 schema 的 `search_path`、`statement_timeout`（默认 5 s）和 `lock_timeout`（默认 2 s）；`db_now` 用数据库时钟 |
| `migration` | 按版本号连续的迁移步骤，由 schema owner 在一个事务里执行（advisory lock 串行），不降级；`grant` 给运行角色授最小权限；运行进程只做 `check` |
| `commands` | 命令幂等表 `plugin_commands`：键为（租户, 操作者, commandId），没有 commandId 时用 Core 的 request id；同请求重放原结果，不同请求报 `COMMAND_CONFLICT`；`forget_entity` 删掉某个实体的旧结果，`prune` 做保留期清理。键空间互不相交：命令部分是 `command:<commandId>`、`request:<requestId>`，或插件自定的 `CommandKey::fixed`（`<scope>:<id>`，如每个投票人每个投票一个键）；操作者部分按 `Actor` 分类，成员是 user id，员工是 `$staff:<id>`，平台和任务是 `$<name>`。用户票据只准入 `[A-Za-z0-9_:-]` 的 user id，所以无论租户怎么给用户取名，都占不到员工和平台的键。新代码用 `CommandKey::of`；`CommandKey::new` 按原样存操作者，不接受 `$` 开头 |
| `outbox`、`events` | 审计事件与业务变更同事务写入 `plugin_outbox`，租约投递到 NATS JetStream（流由运维预建，插件只检查），事件 ID 稳定，消费方按 ID 去重；可整体关闭 |
| `caller`、`membership` | `UserCaller`、`StaffCaller` 持有准入直到释放；`require_member` 要求票据绑定的会话就是实体所在会话，并经 `im.conversation.read` 确认成员身份；`group_roles` 经 `social.group.member.read` 查群角色 |
| `entity_state` | `with_states` 把 `sharedState`、`messageState` 放进调用结果；`stage` 在业务事务里登记实体的最新状态；`publish_due` 用运行时凭据补发 Core 还没确认的版本（租约、指数退避，最多 12 次；已结束的实体不重试） |
| `error` | `RejectionCode` 风格的拒绝码；`respond` 生成 `GenericResponse`：成功、明确拒绝（`ok:false` + `errorCode`）或结果未知（`UNAVAILABLE`） |

## 表

`initial/kit.sql` 的三张表放在插件自己的 schema 里，随插件第一步迁移创建；运行角色权限见 `initial/kit-grants.sql`。

- `plugin_commands`：命令结果，按创建时间清理。
- `plugin_outbox`：待投递和已投递的事件，`(app_id, event_key)` 唯一。
- `plugin_entity_states`：每个实体一行，`version` 是最新版本，`published_version` 是 Core 已确认的版本。

## 共享状态的两条路

1. 用户调用的结果带 `sharedState`，Core 立即发布；结果里同时有调用者自己的 `messageState`。
2. 每次变更也在业务事务里 `stage`。结果已带上的状态延迟 3 秒，其余（截止、控制台变更）立即到期；`publish_due` 每秒发送到期的状态。Core 只保留最高版本，同版本重复是幂等的。

主动发布需要运行时凭据和资源桥同时配置（`entity_state::host_publisher`）；任一缺失时只有调用结果这一条路。

已结束的实体不重试：Core 对卡片已撤回的实体回 `FAILED_PRECONDITION` `PLUGIN_MESSAGE_RECALLED`，对没有卡片的实体（平台放弃代发，或卡片刚落库还没绑定）回 `PLUGIN_CARD_UNBOUND`。前者这一版立即不再发；后者在这一版登记后的 2 分钟（`UNBOUND_GRACE_MS`）内照常退避重试，之后不再发。之后登记的新版本各有自己的尝试次数。

## 测试

`testing` feature 提供不经票据的 `UserCaller::unverified`、`StaffCaller::unverified`，只给依赖本 crate 的插件测试用，不能在发布构建里开启。数据库行为由各插件的 PostgreSQL 用例覆盖（见 `../flare-poll`）。

```sh
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
```
