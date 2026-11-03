//! Safe callback client to Core's `PluginHostBridge`.
//!
//! - Reads ([`HostResourceClient::read`]) go out only under the delegation of a
//!   verified user invocation, scoped to the conversation its ticket is bound to.
//! - The one write, [`HostResourceClient::publish_entity_state`], goes out under
//!   this deployment's runtime credential (`x-flare-plugin-runtime`), for
//!   changes no user call carries: a deadline passing, a console change. Core
//!   accepts it only for an entity bound to a message of this plugin in that
//!   tenant, while the grant is enabled.
use crate::{PluginRuntimeHost, RuntimeWrite, VerifiedPluginUser};
use flare_grpc_proto::capability::{
    GenericRequest, GenericResponse, plugin_host_bridge_client::PluginHostBridgeClient,
};
use flare_plugin_protocol::{
    invocation::{PLUGIN_CALL_HEADER, PLUGIN_RUNTIME_HEADER},
    resource::{
        EntityStatePublishView, MAX_RESOURCE_BYTES, MAX_RESOURCE_REQUEST_BYTES, ResourceRequest,
        ResourceWriteRequest,
    },
    state::SharedState,
};
use tonic::{Request, Status, transport::Channel};

// `im.realtime.deliver` (`deliver_realtime`, `deliver_realtime_as_runtime`).
#[path = "resource_realtime.rs"]
mod realtime;

const BRIDGE_DEADLINE: std::time::Duration = std::time::Duration::from_secs(2);

#[derive(Clone)]
pub struct HostResourceClient(PluginHostBridgeClient<Channel>);
impl HostResourceClient {
    pub async fn connect(authority: &str) -> Result<Self, Status> {
        if !flare_plugin_protocol::runtime::valid_grpc_authority(authority) {
            return Err(Status::invalid_argument("invalid host bridge authority"));
        }
        let client = tokio::time::timeout(
            BRIDGE_DEADLINE,
            PluginHostBridgeClient::connect(authority.to_owned()),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("host bridge connection deadline"))?
        .map_err(|_| Status::unavailable("host bridge unavailable"))?;
        Ok(Self(
            client
                .max_decoding_message_size(MAX_RESOURCE_BYTES + 1024)
                .max_encoding_message_size(
                    flare_plugin_protocol::service::MAX_SERVICE_REQUEST_BYTES + 1024,
                ),
        ))
    }
    pub async fn read(
        &self,
        invocation: &VerifiedPluginUser,
        resource: &ResourceRequest,
    ) -> Result<serde_json::Value, Status> {
        resource
            .validate()
            .map_err(|_| Status::invalid_argument("invalid resource request"))?;
        let bytes = serde_json::to_vec(resource)
            .map_err(|_| Status::invalid_argument("invalid resource request"))?;
        if bytes.len() > MAX_RESOURCE_REQUEST_BYTES {
            return Err(Status::resource_exhausted("resource request limit"));
        }
        let mut request = Request::new(envelope(
            resource.capability().id(),
            &invocation.request_id,
            bytes,
        ));
        insert_credential(&mut request, PLUGIN_CALL_HEADER, &invocation.delegation)?;
        self.send(request, &invocation.request_id).await
    }

    /// Publish `state` for an entity of this plugin under the runtime
    /// credential of `tenant_id`'s grant `grant_id` (see
    /// [`PluginRuntimeHost::sign_runtime_write`]). `published`, `duplicate` and
    /// `stale` are all successes: Core keeps the highest version.
    pub async fn publish_entity_state(
        &self,
        runtime: &PluginRuntimeHost,
        tenant_id: &str,
        grant_id: &str,
        state: SharedState,
    ) -> Result<EntityStatePublishView, Status> {
        let (entity, version) = (state.entity.clone(), state.version);
        let write = runtime
            .sign_runtime_write(
                tenant_id,
                grant_id,
                &ResourceWriteRequest::EntityStatePublish(state),
            )
            .await?;
        let view: EntityStatePublishView = serde_json::from_value(self.write(&write).await?)
            .map_err(|_| Status::data_loss("invalid entity state publish response"))?;
        if view.entity != entity || view.version != version {
            return Err(Status::data_loss("entity state publish response mismatch"));
        }
        Ok(view)
    }

    /// Send one signed runtime write exactly as signed.
    pub async fn write(&self, write: &RuntimeWrite) -> Result<serde_json::Value, Status> {
        let mut request = Request::new(envelope(
            write.capability.id(),
            &write.request_id,
            write.body.clone(),
        ));
        insert_credential(&mut request, PLUGIN_RUNTIME_HEADER, &write.credential)?;
        self.send(request, &write.request_id).await
    }

    async fn send(
        &self,
        request: Request<GenericRequest>,
        request_id: &str,
    ) -> Result<serde_json::Value, Status> {
        let response: GenericResponse =
            tokio::time::timeout(BRIDGE_DEADLINE, self.0.clone().call(request))
                .await
                .map_err(|_| Status::deadline_exceeded("host resource deadline"))??
                .into_inner();
        if !response.ok || response.request_id != request_id {
            return Err(Status::permission_denied("resource response mismatch"));
        }
        serde_json::from_slice(
            &response
                .payload
                .ok_or_else(|| Status::data_loss("resource response missing"))?
                .value,
        )
        .map_err(|_| Status::data_loss("invalid resource response"))
    }
}

fn envelope(operation: &str, request_id: &str, bytes: Vec<u8>) -> GenericRequest {
    GenericRequest {
        operation: operation.into(),
        request_id: request_id.into(),
        payload: Some(prost_types::Any {
            type_url: "type.googleapis.com/flare.capability.v1.PayloadJson".into(),
            value: bytes,
        }),
        ..Default::default()
    }
}

fn insert_credential(
    request: &mut Request<GenericRequest>,
    header: &'static str,
    credential: &str,
) -> Result<(), Status> {
    let mut value: tonic::metadata::MetadataValue<tonic::metadata::Ascii> = credential
        .parse()
        .map_err(|_| Status::unauthenticated("invalid resource credential"))?;
    value.set_sensitive(true);
    request.metadata_mut().insert(header, value);
    Ok(())
}
