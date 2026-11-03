# 服务端 WASM 插件宿主

这是可选的进程内 WASM 执行器。进程通过现有 `ExtensionPlugin` 和 `PluginRuntime` gRPC 合同接入平台，复用签名发布、租户审批、持久授权、启停、修订栅栏和有界退出。市场不参与执行，也没有购买、订阅、许可证或计费依赖。

## 发布与运行

Manifest 使用公共 `flare-plugin-protocol` 的 `runtime.kind=wasm`：

```json
{
  "kind": "wasm",
  "module": "plugin.wasm",
  "abiVersion": "flare.plugin.json.v1",
  "maxMemoryPages": 64,
  "maxFuel": 1000000
}
```

模块路径必须位于运维提供的 `FLARE_PLUGIN_ARTIFACT_DIR` 内；读取后复核签名 Manifest 的精确 SHA-256，不信任文件名、目录登记或 Inspect 自报。模块上限 1 MiB。发布者和平台使用分离密钥，第三方进程不持有 Cell 或平台私钥。

先按 `../flare-plugin-host/initial/runtime.sql` 与 `grants.sql` 显式创建运行 schema/角色；宿主不执行 DDL。基础环境变量与 gRPC 宿主相同，额外提供：

| 变量 | 用途 |
|---|---|
| `FLARE_PLUGIN_ARTIFACT_DIR` | 只读发布产物目录 |
| `FLARE_PLUGIN_HOST_BRIDGE_URL` | 可选的受信 Core 资源桥固定地址；声明宿主能力的模块需要配置 |
| `FLARE_PLUGIN_LISTEN` | 本进程 gRPC 监听，默认 `127.0.0.1:7903` |

进程启动读取同一份已验证 Manifest，编译一次模块；每个配置校验、业务调用都创建 fresh Store。租户安装和启停不重启进程；模块版本不可变，更换模块应发布新版本/部署，再执行已有审批与版本切换流程。

## ABI v1

模块导出：

- `memory`：一个线性内存。
- `flare_alloc(length: i32) -> i32`：返回可写入输入 UTF-8 JSON 的地址。
- `flare_validate(pointer: i32, length: i32) -> i32`：租户配置校验，0 表示接受。
- `flare_call(pointer: i32, length: i32) -> i64`：返回高 32 位为地址、低 32 位为长度的 UTF-8 JSON。

调用输入为 `{operation, context:{tenantId,userId,requestId,conversationId}, configuration, payload}`。调用 ticket 不进入客体内存。

唯一可选 import 是 `flare_host.call(pointer,length,output,capacity)->i32`，输入为公共 `ResourceRequest`，返回实际写入的 JSON 字节数。宿主执行 Manifest 中当前 operation 的 `hostCapabilities` 白名单，再通过 Core 资源桥复核当前授权、用户策略与对象成员关系。没有 WASI、网络、文件、环境、时钟或进程 import。

## 边界

- 每次执行最多 1024 页（64 MiB）、1000 万 fuel；栈深度 128、栈高 65536；最多一个内存、实例、表，表元素最多 1024；增长失败立即 trap。
- 业务并发 4，配置校验并发 4；实际 worker 持有容量和生命周期 guard。调用取消/超时不提前释放尚在工作的 worker 槽位；后续宿主调用和输出检查取消状态。
- RPC 业务输入最多 16 KiB，客体总输入及输出最多 64 KiB。配置复用对象/16 KiB/32 层限制。最多 8 次宿主调用；Core 另有跨副本、租户隔离的共享额度。
- 客体燃料不计量宿主 Schema 工作。因此 Schema 单独限制为 2048 节点/32 层，只允许无环本地引用；不支持动态/循环引用，拒绝外部引用；正则使用线性引擎。Schema 在发布加载时编译，调用前后验证输入和输出，配置同时验证声明的 Schema 和客体 validator。
- 任意 trap、越界、超额返回或校验失败只拒绝该次调用；后续调用使用新 Store。CPU、进程总内存、发布目录只读挂载和进程崩溃重启仍由既有部署 supervisor/container 策略约束。
- 当前资源桥是只读能力。需要账本、钱包、媒体栈、模型服务或独立持久业务状态的插件使用 gRPC 运行方式；两种方式共享管理合同，不给 WASM 追加未隔离的系统调用。

引擎与校验器依赖锁定为 [Wasmi 2.0.0](https://docs.rs/wasmi/2.0.0/wasmi/) 和 [jsonschema 0.59.0 的离线解析](https://docs.rs/jsonschema/0.59.0/jsonschema/struct.ValidationOptions.html#method.offline)，默认文件/HTTP 解析特性关闭。

## 验证

`rtk proxy cargo test --locked --offline --manifest-path flare-plugin-devkit/flare-plugin-wasm/Cargo.toml --all-targets` 验证真实模块、导入拒绝、燃料、内存、Schema、宿主白名单和 trap 后恢复。

根目录 `scripts/verify-plugin-platform.py` 还执行一次性 PostgreSQL 中的真实签名 RPC：双实例、并发租户配置、安装/启用/禁用、排空、墓碑重启和重新启用。该验收不会读取或重置既有数据库。
