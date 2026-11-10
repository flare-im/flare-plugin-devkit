//! Plugin process SDK and shared platform protocol access.
//!
//! A plugin process does not register itself: the operator registers its deployment with Core and
//! Core delivers each tenant's state ([`PluginRuntimeHost`]). Every ticket it accepts comes from
//! Core and verifies with `FLARE_PLUGIN_CALL_PUBLIC_KEYS`.
//!
//! Publisher manifest signatures use an independent publisher trust store in [`protocol`].

mod call_context;
pub mod event;
pub use event::{VerifiedPluginEvent, verify_event_call};
mod lifecycle;
pub mod resource;
pub mod runtime;
pub mod service;
pub mod state_store;
pub use call_context::{
    VerifiedPluginStaff, VerifiedPluginSystem, VerifiedPluginUser, verify_staff_call,
    verify_system_call, verify_user_call,
};
pub use lifecycle::{RuntimeWork, ServeError, serve_bounded, shutdown_signal};
pub use runtime::{PluginRuntimeHost, RuntimeWrite, runtime_signer_from_env, tenant_state_request};

/// 完整文档。
///
/// 放在 crate 里而不是仓库里：外部读者拿到的是 crates.io 上的包与
/// [docs.rs](https://docs.rs/flare-plugin-host)，包外的目录他们看不到。
pub mod docs {
    /// 平台与插件之间的线上契约：一条 RPC 的每个字段是什么意思。
    #[doc = include_str!("../docs/plugin-contract.md")]
    pub mod plugin_contract {}

    /// 从零做一个插件的完整流程，含常见错误的症状→原因对照表。
    #[doc = include_str!("../docs/build-your-own-plugin.md")]
    pub mod build_your_own_plugin {}

    /// 可运行示例 `echo_plugin` 的说明（`cargo run --example echo_plugin`）。
    #[doc = include_str!("../docs/echo-plugin.md")]
    pub mod echo_plugin {}

    /// 插件服务端规范：DDD + CQRS 的铁律、目录结构、用例流与共享状态约定。
    #[doc = include_str!("../docs/plugin-ddd-cqrs.md")]
    pub mod plugin_ddd_cqrs {}
}

pub use flare_plugin_protocol as protocol;
