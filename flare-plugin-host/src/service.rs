//! Generic trusted service router shared by Core and in-process acceptance rigs.
//! The directory is the authenticated local control projection, never endpoint
//! registration, a tenant URL, or the caller's claimed grant.
use crate::protocol::{
    authorization::PluginInvocation,
    invocation::{PluginCallSigner, PluginCallVerifier, PluginRuntimeVerifier},
    resource::{HostCapability, MAX_RESOURCE_BYTES, ResourceWriteRequest},
    runtime::PluginRuntimeSnapshot,
    service::*,
};
use crate::{PluginRuntimeHost, VerifiedPluginUser};
use flare_grpc_proto::capability::{
    GenericRequest, extension_plugin_client::ExtensionPluginClient,
};
use serde_json::Value;
use std::{sync::Arc, time::Duration};
use tonic::{Request, Status};
/// The message of the refusal for a `user` mode operation whose feature the application closed
/// or never published. The Core adapter turns exactly this answer into `PLUGIN_FEATURE_DISABLED`.
pub const FEATURE_DISABLED: &str = "service feature disabled";
#[tonic::async_trait]
pub trait ServiceDirectory: Send + Sync {
    async fn snapshot(&self, tenant: &str) -> Result<Arc<PluginRuntimeSnapshot>, Status>;
    async fn check_policy(&self, tenant: &str, operation: &str) -> Result<(), Status>;
    /// Shared admission ledger, keyed by the original Core request for member
    /// calls and by the signed workload request for recovery.
    async fn reserve_service(
        &self,
        claims: &PluginServiceClaims,
        origin_request: &str,
    ) -> Result<(), Status>;
}
pub struct PluginServiceRouter {
    directory: Arc<dyn ServiceDirectory>,
    calls: PluginCallVerifier,
    runtimes: PluginRuntimeVerifier,
    signer: Arc<PluginCallSigner>,
    slots: tokio::sync::Semaphore,
}
impl PluginServiceRouter {
    pub fn new(
        directory: Arc<dyn ServiceDirectory>,
        calls: PluginCallVerifier,
        runtimes: PluginRuntimeVerifier,
        signer: Arc<PluginCallSigner>,
    ) -> Self {
        Self {
            directory,
            calls,
            runtimes,
            signer,
            slots: tokio::sync::Semaphore::new(32),
        }
    }
    pub async fn dispatch(
        &self,
        token: &str,
        request_id: &str,
        bytes: &[u8],
    ) -> Result<PluginServiceResponse, Status> {
        let _slot = self
            .slots
            .try_acquire()
            .map_err(|_| Status::resource_exhausted("service route capacity"))?;
        tokio::time::timeout(
            Duration::from_secs(3),
            self.dispatch_once(token, request_id, bytes),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("service delegation deadline"))?
    }
    async fn dispatch_once(
        &self,
        token: &str,
        request_id: &str,
        bytes: &[u8],
    ) -> Result<PluginServiceResponse, Status> {
        if bytes.len() > MAX_SERVICE_REQUEST_BYTES {
            return Err(Status::resource_exhausted("service input limit"));
        }
        let now = now_ms();
        let runtime = self
            .runtimes
            .verify(token, bytes, now)
            .map_err(|_| denied())?;
        if runtime.capability != HostCapability::ServiceDispatch || runtime.request_id != request_id
        {
            return Err(denied());
        }
        let request: ResourceWriteRequest = serde_json::from_slice(bytes).map_err(|_| denied())?;
        let ResourceWriteRequest::ServiceDispatch(request) = request else {
            return Err(denied());
        };
        request.validate().map_err(|_| denied())?;
        let snapshot = self.directory.snapshot(&runtime.tenant_id).await?;
        snapshot.validate(now).map_err(|_| denied())?;
        // Service authorization deliberately has a shorter stale-state window than
        // diagnostic/client catalogs. Control renews this projection at <= 10 seconds.
        if snapshot.tenant_id != runtime.tenant_id
            || now.saturating_sub(snapshot.issued_at_ms) > MAX_SERVICE_TTL_MS
        {
            return Err(Status::unavailable("service projection stale"));
        }
        let source = snapshot
            .plugins
            .iter()
            .find(|p| p.grant.grant_id == runtime.grant_id)
            .ok_or_else(denied)?;
        if runtime.plugin_id != source.grant.plugin_id
            || runtime.plugin_version != source.grant.plugin_version
            || runtime.manifest_digest != source.grant.manifest_digest
            || runtime.deployment_id != source.deployment.deployment_id
            || runtime.grant_revision != source.grant.revision
        {
            return Err(denied());
        }
        let source_manifest = source.manifest();
        let target = snapshot
            .plugins
            .iter()
            .filter(|p| {
                p.grant.plugin_id == request.target_plugin_id
                    && source_manifest.requires(&p.grant.plugin_id, &p.grant.plugin_version)
                    && (request.mode == ServiceMode::Recovery
                        || p.grant.enabled && !p.grant.revoked)
            })
            .max_by_key(|p| (p.grant.enabled && !p.grant.revoked, p.grant.revision))
            .ok_or_else(denied)?;
        let target_manifest = target.manifest();
        if !source_manifest.requires(&target.grant.plugin_id, &target.grant.plugin_version) {
            return Err(denied());
        }
        let service = target_manifest
            .service_operations
            .iter()
            .find(|s| s.operation == request.operation && s.mode == request.mode)
            .ok_or_else(denied)?;
        // Closing a payment stops new user calls; `recovery` reconciliation is never gated.
        if service.mode == ServiceMode::User
            && target
                .closed()
                .service_operations
                .contains(&service.operation)
        {
            return Err(Status::permission_denied(FEATURE_DISABLED));
        }
        let target_op = target_manifest
            .operation(&service.operation)
            .ok_or_else(denied)?;
        if !target
            .grant
            .approved_operations
            .contains(&request.operation)
            || !target_op
                .permission_ids
                .iter()
                .all(|p| target.grant.approved_permissions.contains(p))
        {
            return Err(denied());
        }
        if target_manifest
            .permissions
            .iter()
            .filter(|p| target_op.permission_ids.contains(&p.id))
            .any(|p| {
                p.data_scopes
                    .iter()
                    .any(|scope| !target.grant.approved_data_scopes.contains(scope))
            })
        {
            return Err(denied());
        }
        let (actor, conversation, origin, budget_request, expires) =
            if let Some(ticket) = request.actor_ticket.as_deref() {
                // An event authenticates its actor only in this restricted delegation domain.
                // Never turn it into a user ticket or accept JSON identity from the plugin.
                let user = match self.calls.verify(ticket, now) {
                    Ok(user) => {
                        if source_manifest
                            .event_subscriptions
                            .iter()
                            .any(|s| s.operation == user.operation)
                        {
                            return Err(denied());
                        }
                        user
                    }
                    Err(_) => {
                        let event = self
                            .calls
                            .verify_event_origin(ticket, now)
                            .map_err(|_| denied())?;
                        source
                            .authorize_event(
                                crate::protocol::event::MESSAGE_CREATED,
                                &event.operation,
                                &event.actor_id,
                                now,
                            )
                            .map_err(|_| denied())?;
                        // Internal adaptation only: this object is never signed or returned as a user credential.
                        crate::protocol::invocation::PluginCallClaims {
                            tenant_id: event.tenant_id,
                            plugin_id: event.plugin_id,
                            plugin_version: event.plugin_version,
                            manifest_digest: event.manifest_digest,
                            deployment_id: event.deployment_id,
                            grant_id: event.grant_id,
                            grant_revision: event.grant_revision,
                            configuration_digest: event.configuration_digest,
                            actor_id: event.actor_id,
                            operation: event.operation,
                            request_id: event.event_id,
                            conversation_id: Some(event.conversation_id),
                            issued_at_ms: event.issued_at_ms,
                            expires_at_ms: event.expires_at_ms,
                        }
                    }
                };
                if user.tenant_id != runtime.tenant_id
                    || user.plugin_id != runtime.plugin_id
                    || user.plugin_version != runtime.plugin_version
                    || user.manifest_digest != runtime.manifest_digest
                    || user.deployment_id != runtime.deployment_id
                    || user.grant_id != runtime.grant_id
                    || user.grant_revision != runtime.grant_revision
                    || user.configuration_digest != source.configuration_digest
                    || user.expires_at_ms > source.grant.valid_until_ms
                {
                    return Err(denied());
                }
                let op = source_manifest
                    .operation(&user.operation)
                    .ok_or_else(denied)?;
                if source_manifest.admin_operations().contains(&user.operation)
                    || source_manifest
                        .service_operation_ids()
                        .contains(&user.operation)
                    || !op
                        .host_capabilities
                        .contains(&HostCapability::ServiceDispatch)
                {
                    return Err(denied());
                }
                self.directory
                    .check_policy(&runtime.tenant_id, &user.operation)
                    .await?;
                self.directory
                    .check_policy(&runtime.tenant_id, &request.operation)
                    .await?;
                source
                    .authorize(&PluginInvocation {
                        tenant_id: &runtime.tenant_id,
                        actor_id: &user.actor_id,
                        operation: &user.operation,
                        actor_permissions: &source.grant.approved_permissions,
                        platform_enabled: true,
                        now_ms: now,
                    })
                    .map_err(|_| denied())?;
                target
                    .authorize(&PluginInvocation {
                        tenant_id: &runtime.tenant_id,
                        actor_id: &user.actor_id,
                        operation: &request.operation,
                        actor_permissions: &target.grant.approved_permissions,
                        platform_enabled: true,
                        now_ms: now,
                    })
                    .map_err(|_| denied())?;
                (
                    Some(user.actor_id),
                    user.conversation_id,
                    Some(user.operation),
                    user.request_id,
                    user.expires_at_ms.min(runtime.expires_at_ms),
                )
            } else {
                let approved = source_manifest
                    .capabilities
                    .iter()
                    .flat_map(|c| {
                        c.operations
                            .iter()
                            .map(move |op| (format!("{}.{}", c.id, op.id), op))
                    })
                    .any(|(id, op)| {
                        op.host_capabilities
                            .contains(&HostCapability::ServiceDispatch)
                            && source.grant.approved_operations.contains(&id)
                            && op
                                .permission_ids
                                .iter()
                                .all(|p| source.grant.approved_permissions.contains(p))
                            && source_manifest
                                .permissions
                                .iter()
                                .filter(|p| op.permission_ids.contains(&p.id))
                                .all(|p| {
                                    p.data_scopes.iter().all(|scope| {
                                        source.grant.approved_data_scopes.contains(scope)
                                    })
                                })
                    });
                if request.mode != ServiceMode::Recovery || !approved {
                    return Err(denied());
                }
                (
                    None,
                    None,
                    None,
                    request_id.to_owned(),
                    runtime.expires_at_ms,
                )
            };
        let claims = PluginServiceClaims {
            tenant_id: runtime.tenant_id,
            source: ServicePrincipal::projected(source),
            target: ServicePrincipal::projected(target),
            actor_id: actor,
            conversation_id: conversation,
            origin_operation: origin,
            mode: request.mode,
            operation: request.operation.clone(),
            business_ref: request.business_ref,
            request_id: request_id.into(),
            input_digest: input_digest(&request.input).map_err(|_| denied())?,
            issued_at_ms: now,
            expires_at_ms: (now + MAX_SERVICE_TTL_MS)
                .min(expires)
                .min(source.grant.valid_until_ms)
                .min(target.grant.valid_until_ms)
                .min(snapshot.valid_until_ms),
        };
        self.directory
            .reserve_service(&claims, &budget_request)
            .await?;
        let ticket = self
            .signer
            .sign_service(&claims, now)
            .map_err(|_| denied())?;
        // Only the operator-owned authority in the authenticated projection is used.
        let mut client = ExtensionPluginClient::connect(target.deployment.grpc_authority.clone())
            .await
            .map_err(|_| Status::unavailable("target service unavailable"))?
            .max_decoding_message_size(MAX_RESOURCE_BYTES)
            .max_encoding_message_size(MAX_SERVICE_REQUEST_BYTES + 4096);
        let mut call = Request::new(GenericRequest {
            operation: request.operation,
            request_id: request_id.into(),
            payload: Some(prost_types::Any {
                type_url: String::new(),
                value: serde_json::to_vec(&request.input).map_err(|_| denied())?,
            }),
            ..Default::default()
        });
        let mut header: tonic::metadata::MetadataValue<tonic::metadata::Ascii> =
            ticket.parse().map_err(|_| denied())?;
        header.set_sensitive(true);
        call.metadata_mut().insert(SERVICE_HEADER, header);
        let response = client.call(call).await?.into_inner();
        if response.request_id != request_id {
            return Err(Status::data_loss("service response identity"));
        }
        let data = response
            .payload
            .map(|p| {
                serde_json::from_slice(&p.value).map_err(|_| Status::data_loss("service response"))
            })
            .transpose()?;
        Ok(PluginServiceResponse {
            ok: response.ok,
            error_code: (!response.error_code.is_empty()).then_some(response.error_code),
            data,
        })
    }
}
fn denied() -> Status {
    Status::permission_denied("plugin service delegation denied")
}
pub(crate) fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|t| t.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}
/// Opaque validated delegation; only Host can construct one.
pub struct VerifiedPluginService {
    pub(crate) claims: PluginServiceClaims,
    pub organization_id: Option<String>,
    pub(crate) _admission: crate::RuntimeWork,
}
impl VerifiedPluginService {
    pub fn claims(&self) -> &PluginServiceClaims {
        &self.claims
    }
}
impl PluginRuntimeHost {
    pub async fn verify_service_call(
        &self,
        request: &Request<GenericRequest>,
    ) -> Result<VerifiedPluginService, Status> {
        self.verify_service(request).await
    }
}
/// Current operator-projected installation used to settle existing obligations.
#[derive(Clone, Copy)]
pub struct ServiceRecoveryScope<'a> {
    pub tenant_id: &'a str,
    pub grant_id: &'a str,
}
impl crate::resource::HostResourceClient {
    /// Delegate an event-derived actor, without manufacturing an ordinary user invocation.
    pub async fn call_service_event(
        &self,
        runtime: &PluginRuntimeHost,
        caller: &crate::VerifiedPluginEvent,
        target: &str,
        operation: &str,
        business_ref: &str,
        input: Value,
    ) -> Result<PluginServiceResponse, Status> {
        let request = PluginServiceRequest {
            target_plugin_id: target.into(),
            operation: operation.into(),
            business_ref: business_ref.into(),
            mode: ServiceMode::User,
            actor_ticket: Some(caller.delegation.to_string()),
            input,
        };
        self.send_service(runtime, caller.tenant_id(), caller.grant_id(), request)
            .await
    }
    pub async fn call_service(
        &self,
        runtime: &PluginRuntimeHost,
        caller: &VerifiedPluginUser,
        target: &str,
        operation: &str,
        business_ref: &str,
        input: Value,
    ) -> Result<PluginServiceResponse, Status> {
        let request = PluginServiceRequest {
            target_plugin_id: target.into(),
            operation: operation.into(),
            business_ref: business_ref.into(),
            mode: ServiceMode::User,
            actor_ticket: Some(caller.delegation.to_string()),
            input,
        };
        self.send_service(runtime, &caller.tenant_id, &caller.grant_id, request)
            .await
    }
    pub async fn recover_service(
        &self,
        runtime: &PluginRuntimeHost,
        scope: ServiceRecoveryScope<'_>,
        target: &str,
        operation: &str,
        business_ref: &str,
        input: Value,
    ) -> Result<PluginServiceResponse, Status> {
        let grant = runtime
            .current_grant_id(scope.tenant_id, scope.grant_id, true)
            .await?;
        let request = PluginServiceRequest {
            target_plugin_id: target.into(),
            operation: operation.into(),
            business_ref: business_ref.into(),
            mode: ServiceMode::Recovery,
            actor_ticket: None,
            input,
        };
        self.send_service(runtime, scope.tenant_id, &grant, request)
            .await
    }
    async fn send_service(
        &self,
        runtime: &PluginRuntimeHost,
        tenant: &str,
        grant: &str,
        request: PluginServiceRequest,
    ) -> Result<PluginServiceResponse, Status> {
        let write = runtime
            .sign_runtime_write(
                tenant,
                grant,
                &ResourceWriteRequest::ServiceDispatch(request),
            )
            .await?;
        serde_json::from_value(self.write(&write).await?)
            .map_err(|_| Status::data_loss("service reply"))
    }
}
