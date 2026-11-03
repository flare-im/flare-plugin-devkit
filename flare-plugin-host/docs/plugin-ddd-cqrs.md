# 插件服务端规范：DDD + CQRS

插件服务端必须按 DDD + CQRS 组织。这是插件开发的基础原则，新插件按本文起步，既有插件改造时也以本文为目标结构。

插件以 `ExtensionPlugin.Call` 作为 gRPC 入口，通过宿主资源桥调用 IM 能力，并发布实体状态。

## 铁律

1. **对外契约只由 Manifest 定义**。operation 名称、输入输出 schema、错误码、卡片载荷和共享状态的字段，都写在合同 crate 里，并由它导出 Manifest。内部重构不能改变这些形状。
2. **命令改写模型，查询读读模型**。每个 operation 先归类：会改变业务状态的是命令，只读的是查询。两者使用不同的 handler、不同的仓储 trait、不同的 Postgres 实现。管理页的 rows 来源和 detail 都是查询。
3. **聚合纯净**。聚合里只放没有 IO 的不变量、校验和归一化，例如投票的选项是否合法、能否改票、截止后能否操作。凡是要查外部事实的判断（是不是会话成员、在群里是什么角色、余额够不够），都留在命令 handler 里，经端口去查。
4. **状态变更和它的副作用同一个事务**。写模型、实体状态的版本号、outbox 事件，在同一个数据库事务里提交。共享状态（`data.sharedState`）在提交之后，用写后回读的结果构造，版本号就是这次事务递增后的值，不能在提交前凭内存猜。
5. **身份与授权在接口层完成**。用户 operation 用 `verify_user_call`，管理 operation 用 `verify_staff_call`，宿主主动写入用运行时凭据。handler 只接收校验后的身份（`VerifiedPluginUser` / `VerifiedPluginStaff`），不读 metadata 里的自报身份。
6. **命令幂等**。命令表和业务写入在同一事务里，重放时返回第一次的结果。幂等键优先用输入里的 `commandId`；卡片动作和 choices 块只能绑定已声明的输入，带不上 commandId，这时用 Core 签发的 request id。
7. **测试即门禁**。每个 operation 都有合同测试；仓储有 PG 测试；跨宿主的完整链路在插件在线实验台上用真实账号验收。

## 目录结构

```text
flare-<plugin>/src/
├── domain/
│   ├── mod.rs          领域模型、常量、领域事件
│   ├── aggregate.rs    纯校验、不变量与归一化（无 IO）
│   ├── write_repo.rs   写仓储 trait（含写后回读）
│   ├── read_repo.rs    读仓储 trait（返回拼好的读模型）
│   └── events.rs       发往 outbox 的领域事件
├── application/
│   ├── commands.rs     命令 DTO（来自已校验的 operation 输入）
│   ├── queries.rs      查询 DTO、分页归一
│   └── handlers/
│       ├── command_handler.rs   持 write、read（回读）与端口（成员、角色、状态发布）
│       └── query_handler.rs     只持 read 与只读端口（例如核对调用者是不是会话成员）
├── infrastructure/
│   ├── postgres_write.rs   写仓储实现（同事务写命令表、状态版本、outbox）
│   ├── postgres_read.rs    读仓储实现
│   ├── mapping.rs          行映射与写后回读，读写共用
│   └── host.rs             端口的宿主实现：资源桥读取、实体状态发布
├── interface/
│   └── call.rs         ExtensionPlugin.Call：验票，按 operation 分派到命令或查询
└── main.rs             装配：运行时宿主、连接池、handler、服务
```

公共骨架由 `flare-plugin-kit` 提供：运行时宿主接线、连接池、迁移、outbox、命令幂等表、成员与群角色端口、实体状态发布客户端、错误码映射。插件只写自己的领域、用例和仓储。

## 用例流

**命令**

```text
Call(operation, ticket)
  → verify_user_call / verify_staff_call            接口层
  → CommandHandler::handle_x(identity, cmd)
      → 端口：成员、角色等外部事实
      → Aggregate::validate_x(...)                  纯校验
      → write_repo.x(...)                          同一事务：命令表 + 写模型 + 状态版本 + outbox
      → read_repo 回读
  → 结果：data（含 data.message 或 data.sharedState、data.messageState）
```

**查询**

```text
Call(operation, ticket) → 验票 → QueryHandler::handle_x(identity, q) → read_repo.x(...)
```

查询一侧绝不写库，也不发布状态。需要外部事实时（比如明细只给会话成员看），经只读端口去查。

**宿主主动发布**（例如截止自动结束）：定时任务调用命令 handler 的同一个用例，提交后用运行时凭据经 `im.plugin.entity_state.publish` 发布。不能绕开聚合直接改表。

## 共享状态与卡片

- 卡片载荷只放创建时就确定的内容和初始状态。之后变化的部分放进实体共享状态，比如计数、是否结束、条目摘要。共享状态不超过 32 个键、8 KiB。
- 只跟当前用户有关的状态（例如「我选了哪几项」）放在 `data.messageState`，不进共享状态。
- 要发布共享状态的 operation，必须在 Manifest 里声明 `im.plugin.entity_state.publish`；没有声明的，Core 会忽略结果里的 sharedState。
- 客户端没有业务逻辑。卡片、表单和管理页都由 Manifest 里的 UiDocument 声明，由核心 SDK 解析、Kit 渲染。插件不为某一端写界面代码。

## 读模型

- 每个实体、每个用户都有界的查询（一次投票的明细、一次接龙的条目）直接查写模型的索引，不另建投影。
- 管理页的列表按租户分页，查询要走「租户 + 创建时间」这类索引，并带游标。
- 只有出现跨实体的扩散读热点时，才加读模型，并且和写模型同事务维护。

## 新插件清单

1. 合同 crate：PLUGIN_ID、operation 常量、配置与校验、输入输出类型、卡片载荷、共享状态值、Manifest 导出与自检。
2. 服务 crate：按上面的目录建，迁移、命令表、outbox。
3. 合同测试，以及 PG 仓储测试。
4. catalog 条目，摘要要和导出的 plugin.json 一致。
5. 在插件在线实验台登记、发布、启用，用多个账号验收「发起、参与、全员状态更新、控制台管理」。

## 存量

红包插件（`flare-red-packet`）早于本规范，领域、存储、用例目前还混在 `lib.rs` 里。之后改动红包时，应迁移到本文的结构，迁移时契约不变。
