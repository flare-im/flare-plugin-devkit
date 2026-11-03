//! Plugin endpoint SDK and shared platform protocol access.
//!
//! A complete version/API/manifest-digest/operation declaration is required.
//! Undeclared or unverified endpoints cannot receive business calls. Registration
//! describes a deployment address; tenant approval and enablement are separate.
//!
//! Third-party hosts use [`PluginHost::connect_with_token_provider`] with a trusted
//! control-plane broker. Each credential is bound to the complete registration request;
//! no Cell signing key is mounted into that plugin. [`workload::WorkloadTokenProvider`]
//! reopens projected workload tokens for automatic rotation. [`PluginHost::connect_with_signer`] remains
//! available for trusted first-party composition only.
//!
//! Publisher manifest signatures use an independent publisher trust store in
//! [`protocol`]; registration identity is not proof of publisher/artifact trust.

mod call_context;
mod lifecycle;
pub mod resource;
pub mod runtime;
pub mod service;
pub mod state_store;
pub mod workload;
pub use call_context::{
    VerifiedPluginStaff, VerifiedPluginSystem, VerifiedPluginUser, verify_staff_call,
    verify_system_call, verify_user_call,
};
pub use lifecycle::{RuntimeWork, ServeError, serve_bounded, shutdown_signal};
pub use runtime::{PluginRuntimeHost, RuntimeWrite, runtime_signer_from_env};

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

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use flare_grpc_proto::capability::capability_service_client::CapabilityServiceClient;
use flare_grpc_proto::capability::{
    DeregisterPluginEndpointRequest, RegisterPluginEndpointRequest,
};
use flare_server_core::{
    SignedActorType, SignedContextIdentity, SignedContextSigner, set_signed_context_token,
};
use tonic::transport::Channel;

pub use flare_plugin_protocol as protocol;

pub use protocol::workload::{EndpointDeclaration, RegistrationOperation, RegistrationRequest};

pub type RegistrationTokenFuture<'a> =
    Pin<Box<dyn Future<Output = Result<String, HostError>> + Send + 'a>>;

pub trait RegistrationTokenProvider: Send + Sync {
    fn token<'a>(&'a self, request: &'a RegistrationRequest) -> RegistrationTokenFuture<'a>;
}

struct DelegatedRegistrar {
    plugin_id: String,
    provider: Arc<dyn RegistrationTokenProvider>,
}

#[derive(Debug, thiserror::Error)]
pub enum HostError {
    #[error("连接 capability 失败：{0}")]
    Connect(String),
    #[error("capability 拒绝了注册：{0}")]
    Rejected(String),
    #[error("gRPC 调用失败：{0}")]
    Rpc(String),
    #[error("声明不完整：{0}")]
    Invalid(&'static str),
    #[error("签发注册上下文失败：{0}")]
    Sign(String),
}

/// 插件对 capability 的完整声明。
///
/// 所有声明字段均须完整，并由 host 和 Core 复用同一结构校验。
#[derive(Debug, Clone)]
pub struct PluginDeclaration {
    pub tenant_id: String,
    pub plugin_id: String,
    /// 本次注册的能力 id。必须出现在 `declared_operations` 里，
    /// 否则 capability 会当场拒绝（清单与注册对不上）。
    ///
    /// 它**不限制**插件承接的范围 —— 范围由 `declared_operations` 决定。
    /// 这里填一个最好认的入口即可，它主要出现在路由簿与日志里。
    pub capability_id: String,
    /// 本插件的 gRPC 地址，capability 按它回调。
    pub grpc_authority: String,
    pub plugin_version: String,
    pub api_version: String,
    /// 插件清单（plugin.json）的 sha256，用于确认部署物与目录一致。
    pub manifest_sha256: String,
    /// 本插件能处理的 operation 全集；为空时拒绝注册。
    pub declared_operations: Vec<String>,
    /// 附加标签，例如 `health_protocol` 声明特殊探活协议。
    pub labels: HashMap<String, String>,
}

impl PluginDeclaration {
    /// Derive the endpoint declaration from a verified immutable manifest.
    /// Tenant approval and the deployment address are supplied by the host.
    pub fn from_manifest(
        tenant_id: String,
        grpc_authority: String,
        verified: &protocol::signature::VerifiedManifest,
    ) -> Result<Self, HostError> {
        let manifest = verified.manifest();
        if !matches!(
            manifest.runtime,
            protocol::manifest::PluginRuntime::Grpc { .. }
                | protocol::manifest::PluginRuntime::Wasm { .. }
        ) {
            return Err(HostError::Invalid(
                "a server endpoint requires a gRPC or WASM manifest",
            ));
        }
        let operations: Vec<_> = manifest.operation_ids().into_iter().collect();
        let declaration = Self {
            tenant_id,
            plugin_id: manifest.plugin_id.clone(),
            capability_id: operations
                .first()
                .cloned()
                .ok_or(HostError::Invalid("operations required"))?,
            grpc_authority,
            plugin_version: manifest.version.clone(),
            api_version: manifest.api_version.clone(),
            manifest_sha256: verified
                .digest()
                .strip_prefix("sha256:")
                .ok_or(HostError::Invalid("invalid manifest digest"))?
                .to_owned(),
            declared_operations: operations,
            labels: HashMap::new(),
        };
        declaration.validate()?;
        Ok(declaration)
    }

    /// 本地自检：把「注册后才发现对不上」提前到调用前。
    ///
    /// capability 侧也会校验（那是权威判定），这里重复一遍是为了让插件作者
    /// 在自己的进程里就看到错误，而不是去翻 capability 的日志。
    pub fn validate(&self) -> Result<(), HostError> {
        if self.tenant_id.trim().is_empty() || self.tenant_id == "*" {
            return Err(HostError::Invalid("one concrete tenant_id is required"));
        }
        if self.plugin_id.trim().is_empty() {
            return Err(HostError::Invalid("plugin_id 不能为空"));
        }
        if self.capability_id.trim().is_empty() {
            return Err(HostError::Invalid("capability_id 不能为空"));
        }
        if self.grpc_authority.trim().is_empty() {
            return Err(HostError::Invalid("grpc_authority 不能为空"));
        }
        flare_plugin_protocol::registration::validate_declaration(
            &self.plugin_version,
            &self.api_version,
            &self.manifest_sha256,
            &self.declared_operations,
            &self.capability_id,
        )
        .map_err(HostError::Invalid)?;
        Ok(())
    }
}

/// 注册签名私钥的环境变量：Cell 私钥（PEM 或单行 base64），与各签名方同名。
pub const SIGNING_KEY_ENV: &str = "FLARE_INTERNAL_CTX_SIGNING_KEY";
/// 可选：该私钥在 capability 的 `FLARE_INTERNAL_CTX_PUBLIC_KEYS` 里登记的密钥 id。
/// 不配时令牌不带 `kid`，由 capability 的单把 Cell 公钥验签。
pub const SIGNING_KID_ENV: &str = "FLARE_INTERNAL_CTX_KID";

/// 从环境读取注册签名器（[`SIGNING_KEY_ENV`]、[`SIGNING_KID_ENV`]）。
///
/// 没配私钥返回 `Ok(None)`，由调用方决定跳过声明还是裸连；配了但无效返回错误，
/// 不静默退化成不签名的注册。
pub fn signer_from_env() -> Result<Option<SignedContextSigner>, HostError> {
    signer_from(
        std::env::var(SIGNING_KEY_ENV).ok(),
        std::env::var(SIGNING_KID_ENV).ok(),
    )
}

fn signer_from(
    key: Option<String>,
    kid: Option<String>,
) -> Result<Option<SignedContextSigner>, HostError> {
    let Some(key) = key.filter(|k| !k.trim().is_empty()) else {
        return Ok(None);
    };
    let signer = SignedContextSigner::from_private_key(&key)
        .map_err(|_| HostError::Sign(format!("{SIGNING_KEY_ENV} is invalid")))?;
    match kid.map(|k| k.trim().to_owned()).filter(|k| !k.is_empty()) {
        Some(kid) => signer
            .with_kid(kid)
            .map(Some)
            .map_err(|_| HostError::Sign(format!("{SIGNING_KID_ENV} is invalid"))),
        None => Ok(Some(signer)),
    }
}

/// 签发注册上下文的身份：Cell 私钥签名器与插件自己的 id。
struct Registrar {
    signer: SignedContextSigner,
    actor_id: String,
}

impl Registrar {
    /// 一张只为这次调用签发的 Service 上下文：`actor_id` 是插件 id，`tenant_id`
    /// 是声明的租户，`request_id` 唯一标识这次注册/注销。
    fn signed_request<T>(
        &self,
        payload: T,
        tenant_id: &str,
        request_id: &str,
    ) -> Result<tonic::Request<T>, HostError> {
        let identity = SignedContextIdentity::new(
            SignedActorType::Service,
            self.actor_id.as_str(),
            tenant_id,
            request_id,
        );
        let token = self
            .signer
            .sign(&identity)
            .map_err(|e| HostError::Sign(e.to_string()))?;
        let mut request = tonic::Request::new(payload);
        set_signed_context_token(&mut request, &token)
            .map_err(|e| HostError::Sign(e.to_string()))?;
        Ok(request)
    }
}

/// 注册/注销调用的请求 id：插件 id、操作、纳秒时间戳与进程内序号，在 capability 的
/// 审计里可区分每次调用（同一纳秒内的两次调用靠序号区分）。
fn request_id(plugin_id: &str, operation: &str) -> String {
    static SEQUENCE: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default();
    let sequence = SEQUENCE.fetch_add(1, Ordering::Relaxed);
    format!("{plugin_id}-{operation}-{nanos}-{sequence}")
}

/// 与 capability 控制面的连接。
pub struct PluginHost {
    client: CapabilityServiceClient<Channel>,
    registrar: Option<Registrar>,
    delegated_registrar: Option<DelegatedRegistrar>,
}

impl PluginHost {
    /// 连接 capability，并以 Service 身份签每一次注册/注销。
    ///
    /// `signer` 持 Cell 私钥（`FLARE_INTERNAL_CTX_SIGNING_KEY`，可用
    /// `SignedContextSigner::with_kid` 声明密钥 id）；`actor_id` 是插件自己的 id，
    /// 必须与声明里的 `plugin_id` 一致，capability 按它核对登记记录。
    pub async fn connect_with_signer(
        endpoint: impl Into<String>,
        signer: SignedContextSigner,
        actor_id: impl Into<String>,
    ) -> Result<Self, HostError> {
        let actor_id = actor_id.into();
        if actor_id.trim().is_empty() {
            return Err(HostError::Invalid("签名身份的 actor_id 不能为空"));
        }
        let client = Self::client(endpoint.into()).await?;
        Ok(Self {
            client,
            registrar: Some(Registrar { signer, actor_id }),
            delegated_registrar: None,
        })
    }

    /// A workload-authenticated control-plane broker authorizes registrations;
    /// the plugin only receives a short-lived, request-bound registration credential.
    pub async fn connect_with_token_provider(
        endpoint: impl Into<String>,
        plugin_id: impl Into<String>,
        provider: Arc<dyn RegistrationTokenProvider>,
    ) -> Result<Self, HostError> {
        let plugin_id = plugin_id.into();
        if plugin_id.trim().is_empty() {
            return Err(HostError::Invalid("plugin_id required"));
        }
        Ok(Self {
            client: Self::client(endpoint.into()).await?,
            registrar: None,
            delegated_registrar: Some(DelegatedRegistrar {
                plugin_id,
                provider,
            }),
        })
    }

    /// Automatic projected service-account exchange. Operator configuration
    /// supplies the broker address, CA, token path and immutable deployment id.
    pub async fn connect_with_workload(
        endpoint: impl Into<String>,
        plugin_id: impl Into<String>,
    ) -> Result<Self, HostError> {
        Self::connect_with_token_provider(
            endpoint,
            plugin_id,
            Arc::new(workload::WorkloadTokenProvider::from_env()?),
        )
        .await
    }

    async fn client(endpoint: String) -> Result<CapabilityServiceClient<Channel>, HostError> {
        let channel = Channel::from_shared(endpoint.clone())
            .map_err(|e| HostError::Connect(format!("{endpoint}: {e}")))?
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(10))
            .connect()
            .await
            .map_err(|e| HostError::Connect(format!("{endpoint}: {e}")))?;
        Ok(CapabilityServiceClient::new(channel))
    }

    /// 是否具备签名身份（可信 signer 或控制面 token provider）。
    pub fn signs_registration(&self) -> bool {
        self.registrar.is_some() || self.delegated_registrar.is_some()
    }

    fn require_plugin_identity(&self, plugin_id: &str) -> Result<(), HostError> {
        let identity = self
            .delegated_registrar
            .as_ref()
            .map(|r| r.plugin_id.as_str())
            .or_else(|| self.registrar.as_ref().map(|r| r.actor_id.as_str()));
        if identity == Some(plugin_id) {
            Ok(())
        } else {
            Err(HostError::Invalid(
                "plugin identity does not match registration credentials",
            ))
        }
    }

    async fn request<T>(
        &self,
        payload: T,
        tenant_id: &str,
        request_id: &str,
        intent: Option<&RegistrationRequest>,
    ) -> Result<tonic::Request<T>, HostError> {
        if let Some(delegated) = &self.delegated_registrar {
            let token = tokio::time::timeout(
                Duration::from_secs(5),
                delegated.provider.token(
                    intent.ok_or(HostError::Invalid("complete registration intent required"))?,
                ),
            )
            .await
            .map_err(|_| HostError::Sign("registration token broker timed out".into()))?
            .map_err(|_| HostError::Sign("registration token broker refused credentials".into()))?;
            let mut request = tonic::Request::new(payload);
            let mut token: tonic::metadata::MetadataValue<tonic::metadata::Ascii> =
                token
                    .parse()
                    .map_err(|_| HostError::Sign("invalid registration token".into()))?;
            token.set_sensitive(true);
            request
                .metadata_mut()
                .insert(protocol::workload::REGISTRATION_HEADER, token);
            return Ok(request);
        }
        match &self.registrar {
            Some(registrar) => registrar.signed_request(payload, tenant_id, request_id),
            None => Err(HostError::Invalid(
                "signed registration credentials required",
            )),
        }
    }

    /// 向 capability 声明本插件。
    ///
    /// 声明注册成功不代表平台审批通过；声明之外的调用会被 capability 拒绝。
    pub async fn announce(&mut self, declaration: &PluginDeclaration) -> Result<(), HostError> {
        declaration.validate()?;
        self.require_plugin_identity(&declaration.plugin_id)?;
        let request_id = request_id(&declaration.plugin_id, "announce");
        let intent = RegistrationRequest {
            plugin_id: declaration.plugin_id.clone(),
            tenant_id: declaration.tenant_id.clone(),
            request_id: request_id.clone(),
            action: RegistrationOperation::Register(EndpointDeclaration {
                capability_id: declaration.capability_id.clone(),
                grpc_authority: declaration.grpc_authority.clone(),
                plugin_version: declaration.plugin_version.clone(),
                api_version: declaration.api_version.clone(),
                manifest_sha256: declaration.manifest_sha256.clone(),
                declared_operations: declaration.declared_operations.clone(),
                labels: declaration.labels.clone().into_iter().collect(),
            }),
        };
        let request = self
            .request(
                RegisterPluginEndpointRequest {
                    tenant_id: declaration.tenant_id.clone(),
                    plugin_id: declaration.plugin_id.clone(),
                    capability_id: declaration.capability_id.clone(),
                    grpc_authority: declaration.grpc_authority.clone(),
                    labels: declaration.labels.clone(),
                    request_id: request_id.clone(),
                    plugin_version: declaration.plugin_version.clone(),
                    api_version: declaration.api_version.clone(),
                    manifest_sha256: declaration.manifest_sha256.clone(),
                    declared_operations: declaration.declared_operations.clone(),
                },
                &declaration.tenant_id,
                &request_id,
                Some(&intent),
            )
            .await?;
        let response = self
            .client
            .register_plugin_endpoint(request)
            .await
            .map_err(|e| HostError::Rpc(e.to_string()))?
            .into_inner();

        if !response.accepted {
            return Err(HostError::Rejected(response.message));
        }
        tracing::info!(
            plugin_id = %declaration.plugin_id,
            capability_id = %declaration.capability_id,
            operations = declaration.declared_operations.len(),
            "plugin announced to capability"
        );
        Ok(())
    }

    /// 优雅摘除：停机前告诉 capability 别再往这里派活。
    ///
    /// 不调用也不会坏 —— capability 的健康检查最终会把它摘掉。但那要等一个
    /// 检查周期，期间的调用会打到正在退出的进程上。
    /// 注销粒度是 `(tenant, plugin)`，与注册粒度一致 —— 一个插件进程在路由簿里
    /// 只有一条记录，承接范围由 `declared_operations` 表达，所以一次注销即摘干净。
    pub async fn withdraw(&mut self, tenant_id: &str, plugin_id: &str) -> Result<(), HostError> {
        self.require_plugin_identity(plugin_id)?;
        let request_id = request_id(plugin_id, "withdraw");
        let intent = RegistrationRequest {
            plugin_id: plugin_id.into(),
            tenant_id: tenant_id.into(),
            request_id: request_id.clone(),
            action: RegistrationOperation::Deregister,
        };
        let request = self
            .request(
                DeregisterPluginEndpointRequest {
                    tenant_id: tenant_id.to_string(),
                    plugin_id: plugin_id.to_string(),
                    request_id: request_id.clone(),
                },
                tenant_id,
                &request_id,
                Some(&intent),
            )
            .await?;
        self.client
            .deregister_plugin_endpoint(request)
            .await
            .map_err(|e| HostError::Rpc(e.to_string()))?;
        tracing::info!(plugin_id, "plugin withdrawn from capability");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn declaration() -> PluginDeclaration {
        PluginDeclaration {
            tenant_id: "0".into(),
            plugin_id: "p1".into(),
            capability_id: "vendorx.do".into(),
            grpc_authority: "127.0.0.1:1".into(),
            plugin_version: "1.0.0".into(),
            api_version: "1.0".into(),
            manifest_sha256: "a".repeat(64),
            declared_operations: vec!["vendorx.do".into()],
            labels: HashMap::new(),
        }
    }

    #[test]
    fn complete_declaration_is_valid() {
        declaration().validate().expect("完整声明应当通过");
    }

    /// 空声明会让插件静默退化成 unverified —— 本库的存在意义就是不让它发生。
    #[test]
    fn empty_declared_operations_is_rejected_locally() {
        let mut d = declaration();
        d.declared_operations.clear();
        assert!(matches!(d.validate(), Err(HostError::Invalid(_))));
    }

    /// 注册的能力不在自己的声明里 —— capability 会拒，这里提前拦下。
    #[test]
    fn capability_id_must_be_declared() {
        let mut d = declaration();
        d.declared_operations = vec!["vendorx.other".into()];
        assert!(matches!(d.validate(), Err(HostError::Invalid(_))));
    }

    #[tokio::test]
    async fn delegated_registration_uses_request_bound_credentials_without_a_private_key() {
        struct Broker;
        impl RegistrationTokenProvider for Broker {
            fn token<'a>(
                &'a self,
                request: &'a RegistrationRequest,
            ) -> RegistrationTokenFuture<'a> {
                Box::pin(async move {
                    assert_eq!(
                        (
                            request.plugin_id.as_str(),
                            request.tenant_id.as_str(),
                            request.request_id.as_str()
                        ),
                        ("p1", "tenant-a", "request-1")
                    );
                    Ok("delegated-signed-context".into())
                })
            }
        }
        let host = PluginHost {
            client: CapabilityServiceClient::new(
                Channel::from_static("http://127.0.0.1:1").connect_lazy(),
            ),
            registrar: None,
            delegated_registrar: Some(DelegatedRegistrar {
                plugin_id: "p1".into(),
                provider: Arc::new(Broker),
            }),
        };
        assert!(host.signs_registration());
        host.require_plugin_identity("p1").unwrap();
        assert!(host.require_plugin_identity("another-plugin").is_err());
        let intent = RegistrationRequest {
            plugin_id: "p1".into(),
            tenant_id: "tenant-a".into(),
            request_id: "request-1".into(),
            action: RegistrationOperation::Deregister,
        };
        let request = host
            .request((), "tenant-a", "request-1", Some(&intent))
            .await
            .unwrap();
        assert_eq!(
            request
                .metadata()
                .get(protocol::workload::REGISTRATION_HEADER)
                .unwrap(),
            "delegated-signed-context"
        );
    }

    /// 注册请求的 `x-flare-ctx` 是插件自己的 Service 身份：`actor_id` 是插件 id，
    /// `tenant_id` 是声明的租户，`request_id` 与请求体一致，并且每次调用都是一张新令牌。
    /// A runtime is needed only for the lazy channel of the unsigned host below.
    #[tokio::test]
    async fn signed_registration_carries_the_plugin_service_identity() {
        use ed25519_dalek::SigningKey;
        use ed25519_dalek::pkcs8::{EncodePrivateKey, EncodePublicKey};
        use flare_server_core::SignedContextVerifier;
        use flare_server_core::flare_core_transport::grpc::middleware::signed_context::SIGNED_CONTEXT_HEADER;

        let key = SigningKey::from_bytes(&[73; 32]);
        let signer =
            SignedContextSigner::from_private_key(&key.to_pkcs8_pem(Default::default()).unwrap())
                .unwrap()
                .with_kid("bridge")
                .unwrap();
        let verifier = SignedContextVerifier::from_public_key(
            &key.verifying_key()
                .to_public_key_pem(Default::default())
                .unwrap(),
        )
        .unwrap();
        let registrar = Registrar {
            signer,
            actor_id: "p1".into(),
        };
        let d = declaration();
        let first = registrar
            .signed_request((), &d.tenant_id, "p1-announce-1")
            .unwrap();
        let token = first
            .metadata()
            .get(SIGNED_CONTEXT_HEADER)
            .expect("x-flare-ctx is set")
            .to_str()
            .unwrap()
            .to_string();
        let claims = verifier.verify(&token).unwrap();
        assert_eq!(claims.actor_type, SignedActorType::Service);
        assert_eq!(claims.actor_id, "p1");
        assert_eq!(claims.tenant_id, "0");
        assert_eq!(claims.request_id, "p1-announce-1");
        assert!(claims.roles.is_empty());
        assert_eq!(claims.kid, "bridge");
        let second = registrar
            .signed_request((), &d.tenant_id, "p1-announce-2")
            .unwrap();
        assert_ne!(
            second.metadata().get(SIGNED_CONTEXT_HEADER).unwrap(),
            token.as_str(),
            "every call is signed afresh"
        );
        // Missing credentials fail locally; no unsigned registration is sent.
        let bare = PluginHost {
            client: CapabilityServiceClient::new(
                Channel::from_static("http://127.0.0.1:1").connect_lazy(),
            ),
            registrar: None,
            delegated_registrar: None,
        };
        assert!(!bare.signs_registration());
        assert!(bare.request((), "0", "p1-announce-3", None).await.is_err());
        // Request ids are distinct across calls of the same operation.
        assert_ne!(request_id("p1", "announce"), request_id("p1", "announce"));
    }

    /// No key: no signer (the caller decides); an invalid key or kid is an error, never a
    /// silent fall back to unsigned registration; a valid kid is carried.
    #[test]
    fn signer_from_env_values() {
        use ed25519_dalek::SigningKey;
        use ed25519_dalek::pkcs8::EncodePrivateKey;

        assert!(signer_from(None, Some("bridge".into())).unwrap().is_none());
        assert!(signer_from(Some("  ".into()), None).unwrap().is_none());
        assert!(matches!(
            signer_from(Some("not a key".into()), None),
            Err(HostError::Sign(_))
        ));
        let pem = SigningKey::from_bytes(&[9; 32])
            .to_pkcs8_pem(Default::default())
            .unwrap()
            .to_string();
        assert_eq!(
            signer_from(Some(pem.clone()), None).unwrap().unwrap().kid(),
            None
        );
        assert_eq!(
            signer_from(Some(pem.clone()), Some(" bridge ".into()))
                .unwrap()
                .unwrap()
                .kid(),
            Some("bridge")
        );
        assert!(matches!(
            signer_from(Some(pem), Some("bad kid!".into())),
            Err(HostError::Sign(_))
        ));
    }
}
