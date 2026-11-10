use super::*;
use crate::event::VerifiedPluginEvent;

impl PluginRuntimeHost {
    /// Verify Core's event domain and exact payload before exposing actor or conversation.
    pub async fn verify_event_call(
        &self,
        request: &Request<GenericRequest>,
    ) -> Result<VerifiedPluginEvent, Status> {
        if self.0.lifecycle.is_draining() {
            return Err(Status::unavailable("plugin draining"));
        }
        if request.metadata().contains_key(PLUGIN_RUNTIME_HEADER)
            || !request.get_ref().metadata.is_empty()
        {
            return Err(denied());
        }
        let mut values = request.metadata().get_all(PLUGIN_CALL_HEADER).iter();
        let token = values
            .next()
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| Status::unauthenticated("event ticket required"))?;
        if values.next().is_some() {
            return Err(Status::unauthenticated("one event ticket required"));
        }
        let body = request.get_ref();
        let bytes = body.payload.as_ref().ok_or_else(denied)?.value.as_slice();
        let now = now_ms();
        let claims = self
            .0
            .calls
            .verify_event(token, bytes, now)
            .map_err(|_| Status::unauthenticated("invalid event ticket"))?;
        let d = &self.0.deployment;
        if claims.plugin_id != d.plugin_id
            || claims.plugin_version != d.plugin_version
            || claims.manifest_digest != d.manifest_digest
            || claims.deployment_id != d.deployment_id
        {
            return Err(projection_stale());
        }
        let event = claims.matches_event(bytes).map_err(|_| denied())?;
        let subscription = self
            .manifest()
            .event_subscriptions
            .iter()
            .find(|s| {
                s.event_type == event.event_type
                    && s.schema_version == event.schema_version
                    && s.operation == body.operation
            })
            .ok_or_else(denied)?;
        if claims.operation != body.operation
            || claims.event_id != body.request_id
            || event.data.text.is_some() && !subscription.include_body
        {
            return Err(denied());
        }
        let state = self
            .0
            .states
            .get(&d.deployment_id, &claims.tenant_id, &claims.grant_id)
            .await?
            .ok_or_else(projection_stale)?;
        if !state.enabled
            || state.revision != claims.grant_revision
            || state.configuration_digest != claims.configuration_digest
            || state.valid_until_ms <= now
            || claims.expires_at_ms > state.valid_until_ms
        {
            return Err(projection_stale());
        }
        if !state.approved_operations.contains(&claims.operation)
            || !subscription
                .permission_ids
                .iter()
                .all(|id| state.permissions.contains(id))
            || !state
                .data_scopes
                .iter()
                .any(|s| s == crate::protocol::event::MESSAGE_EVENTS_SCOPE)
            || subscription.include_body
                && !state
                    .data_scopes
                    .iter()
                    .any(|s| s == crate::protocol::event::MESSAGE_BODY_SCOPE)
        {
            return Err(denied());
        }
        let manifest = self.manifest();
        if claims
            .enabled_features
            .iter()
            .any(|id| !manifest.features.iter().any(|f| &f.id == id))
        {
            return Err(denied());
        }
        let features = claims
            .enabled_features
            .iter()
            .map(|id| (id.clone(), true))
            .collect();
        let closed = manifest.closed_operations(&features);
        if closed.contains(&claims.operation) {
            return Err(denied());
        }
        let allowed_operations = manifest
            .user_operation_ids()
            .into_iter()
            .filter(|operation| {
                state.approved_operations.contains(operation)
                    && !closed.contains(operation)
                    && manifest.operation(operation).is_some_and(|op| {
                        op.permission_ids.iter().all(|id| {
                            state.permissions.contains(id)
                                && manifest
                                    .permissions
                                    .iter()
                                    .find(|p| &p.id == id)
                                    .is_some_and(|permission| {
                                        permission
                                            .data_scopes
                                            .iter()
                                            .all(|scope| state.data_scopes.contains(scope))
                                    })
                        })
                    })
            })
            .collect();
        Ok(VerifiedPluginEvent {
            event,
            grant_id: claims.grant_id,
            grant_revision: claims.grant_revision,
            expires_at_ms: claims.expires_at_ms,
            active_since_ms: claims.active_since_ms,
            delegation: token.into(),
            configuration: serde_json::from_slice(&state.configuration_json)
                .map_err(|_| denied())?,
            allowed_operations,
            _admission: self.0.lifecycle.admit()?,
        })
    }
}

#[cfg(test)]
#[path = "runtime_event_tests.rs"]
mod tests;
