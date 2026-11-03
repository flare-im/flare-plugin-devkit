//! Conversation facts from Core's resource bridge, read under the member's
//! own ticket. A conversation id, type or member list supplied by a client is
//! never trusted, and a denied read denies the operation.
use crate::{KitError, UserCaller};
use flare_plugin_host::{protocol::resource::*, resource::HostResourceClient};
use serde_json::Value;

#[tonic::async_trait]
pub trait Directory: Send + Sync {
    async fn read(&self, caller: &UserCaller, request: &ResourceRequest)
    -> Result<Value, KitError>;
}

/// No bridge configured: every membership check fails with
/// `HOST_RESOURCE_UNAVAILABLE`.
pub struct Unavailable;
#[tonic::async_trait]
impl Directory for Unavailable {
    async fn read(&self, _: &UserCaller, _: &ResourceRequest) -> Result<Value, KitError> {
        Err(KitError::ResourceUnavailable)
    }
}

/// Core's `PluginHostBridge` at an operator-fixed address.
pub struct HostDirectory(pub HostResourceClient);
#[tonic::async_trait]
impl Directory for HostDirectory {
    async fn read(
        &self,
        caller: &UserCaller,
        request: &ResourceRequest,
    ) -> Result<Value, KitError> {
        let invocation = caller
            .admission
            .as_ref()
            .ok_or(KitError::ResourceUnavailable)?;
        self.0
            .read(invocation, request)
            .await
            .map_err(|status| match status.code() {
                // Core, IM and Social answer a non-member and a revoked or
                // undeclared capability with the same denial.
                tonic::Code::PermissionDenied | tonic::Code::NotFound => KitError::NotMember,
                _ => KitError::ResourceUnavailable,
            })
    }
}

/// `FLARE_PLUGIN_HOST_BRIDGE_URL`: the bridge client, or `None` when unset.
pub async fn bridge_from_env() -> Result<Option<HostResourceClient>, Box<dyn std::error::Error>> {
    match std::env::var("FLARE_PLUGIN_HOST_BRIDGE_URL") {
        Ok(url) => Ok(Some(HostResourceClient::connect(&url).await?)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(_) => Err("invalid FLARE_PLUGIN_HOST_BRIDGE_URL".into()),
    }
}

/// The caller's ticket is bound to `conversation_id` and the host confirms
/// the caller is (still) a member of it.
pub async fn require_member(
    directory: &dyn Directory,
    caller: &UserCaller,
    conversation_id: &str,
) -> Result<ConversationView, KitError> {
    match caller.conversation() {
        Some(bound) if bound == conversation_id => {}
        Some(_) => return Err(KitError::NotMember),
        None => return Err(KitError::Invalid),
    }
    bound_conversation(directory, caller).await
}

/// The conversation the caller's ticket is bound to, confirmed by the host.
pub async fn bound_conversation(
    directory: &dyn Directory,
    caller: &UserCaller,
) -> Result<ConversationView, KitError> {
    let bound = caller.conversation().ok_or(KitError::Invalid)?;
    let view: ConversationView = serde_json::from_value(
        directory
            .read(caller, &ResourceRequest::ConversationRead {})
            .await?,
    )
    .map_err(|_| KitError::ResourceUnavailable)?;
    if view.conversation_id != bound {
        return Err(KitError::NotMember);
    }
    Ok(view)
}

/// `user`'s roles in the group of the caller's (group) conversation, from
/// Social (`social.group.member.read`). `None` when Social does not confirm a
/// membership; an unreachable bridge is an error, never "no role".
pub async fn group_roles(
    directory: &dyn Directory,
    caller: &UserCaller,
    user: &str,
) -> Result<Option<Vec<i32>>, KitError> {
    let lookup = ResourceRequest::SocialGroupMemberRead {
        user_id: user.into(),
    };
    match directory.read(caller, &lookup).await {
        Ok(value) => Ok(serde_json::from_value::<GroupMemberView>(value)
            .ok()
            .filter(|member| member.user_id == user)
            .map(|member| member.roles)),
        Err(KitError::NotMember) => Ok(None),
        Err(error) => Err(error),
    }
}
