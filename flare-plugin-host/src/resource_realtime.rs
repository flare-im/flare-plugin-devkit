//! `im.realtime.deliver`: a packet to the online devices of members of one
//! conversation. Not stored, at most once, never pushed offline: the plugin's
//! own durable state stays the truth. Under a user call's delegation the
//! delivery must name the conversation the ticket is bound to; under this
//! deployment's runtime credential (no call in flight: a deadline passing, a
//! console change) any conversation of the tenant.
use super::*;
use flare_plugin_protocol::resource::{RealtimeDelivery, RealtimeDeliveryView};

impl HostResourceClient {
    /// Deliver `delivery` under the delegation of a verified user call whose
    /// operation declares `im.realtime.deliver`.
    pub async fn deliver_realtime(
        &self,
        invocation: &VerifiedPluginUser,
        delivery: RealtimeDelivery,
    ) -> Result<RealtimeDeliveryView, Status> {
        if invocation.conversation_id.as_deref() != Some(delivery.conversation_id.as_str()) {
            return Err(Status::invalid_argument(
                "realtime delivery outside the ticket's conversation",
            ));
        }
        let write = ResourceWriteRequest::RealtimeDeliver(delivery);
        write
            .validate()
            .map_err(|_| Status::invalid_argument("invalid realtime delivery"))?;
        let body = serde_json::to_vec(&write)
            .map_err(|_| Status::invalid_argument("invalid realtime delivery"))?;
        if body.len() > MAX_RESOURCE_REQUEST_BYTES {
            return Err(Status::resource_exhausted("host write request limit"));
        }
        let mut request = Request::new(envelope(
            write.capability().id(),
            &invocation.request_id,
            body,
        ));
        insert_credential(&mut request, PLUGIN_CALL_HEADER, &invocation.delegation)?;
        let answer = self.send(request, &invocation.request_id).await?;
        checked(&write, answer)
    }

    /// Deliver `delivery` under the runtime credential of `tenant_id`'s grant
    /// `grant_id` (see [`PluginRuntimeHost::sign_runtime_write`]).
    pub async fn deliver_realtime_as_runtime(
        &self,
        runtime: &PluginRuntimeHost,
        tenant_id: &str,
        grant_id: &str,
        delivery: RealtimeDelivery,
    ) -> Result<RealtimeDeliveryView, Status> {
        let write = ResourceWriteRequest::RealtimeDeliver(delivery);
        let signed = runtime
            .sign_runtime_write(tenant_id, grant_id, &write)
            .await?;
        let answer = self.write(&signed).await?;
        checked(&write, answer)
    }
}

/// Core's answer covers this delivery: `skipped` names requested users only,
/// in request order.
fn checked(
    write: &ResourceWriteRequest,
    answer: serde_json::Value,
) -> Result<RealtimeDeliveryView, Status> {
    let ResourceWriteRequest::RealtimeDeliver(delivery) = write else {
        return Err(Status::internal("not a realtime delivery"));
    };
    let view: RealtimeDeliveryView = serde_json::from_value(answer)
        .map_err(|_| Status::data_loss("invalid realtime delivery response"))?;
    let mut requested = delivery.user_ids.iter();
    let in_order = view
        .skipped
        .iter()
        .all(|skipped| requested.any(|user| user == skipped));
    if view.delivery_id.is_empty() || !in_order {
        return Err(Status::data_loss("realtime delivery response mismatch"));
    }
    Ok(view)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::PluginRuntimeHost;
    use flare_grpc_proto::capability::plugin_host_bridge_server::{
        PluginHostBridge, PluginHostBridgeServer,
    };
    use flare_plugin_protocol::invocation::{PluginRuntimeSigner, PluginRuntimeVerifier};
    use serde_json::json;
    use std::sync::{Arc, Mutex};

    /// Core's bridge: checks the runtime credential, records what it got and
    /// skips `stranger`.
    struct Bridge {
        verifier: PluginRuntimeVerifier,
        seen: Arc<Mutex<Vec<(String, RealtimeDelivery)>>>,
    }
    #[tonic::async_trait]
    impl PluginHostBridge for Bridge {
        async fn call(
            &self,
            request: Request<GenericRequest>,
        ) -> Result<tonic::Response<GenericResponse>, Status> {
            let credential = request
                .metadata()
                .get(PLUGIN_RUNTIME_HEADER)
                .and_then(|v| v.to_str().ok())
                .ok_or_else(|| Status::unauthenticated("runtime credential"))?
                .to_owned();
            let body = request.into_inner();
            let bytes = body.payload.unwrap().value;
            self.verifier
                .verify(&credential, &bytes, now())
                .map_err(|_| Status::unauthenticated("runtime credential"))?;
            let ResourceWriteRequest::RealtimeDeliver(delivery) =
                serde_json::from_slice(&bytes).unwrap()
            else {
                return Err(Status::invalid_argument("not a delivery"));
            };
            let skipped: Vec<String> = delivery
                .user_ids
                .iter()
                .filter(|u| u.as_str() == "stranger")
                .cloned()
                .collect();
            let view = RealtimeDeliveryView {
                delivery_id: "d-1".into(),
                delivered: (delivery.user_ids.len() - skipped.len()) as u32,
                skipped,
            };
            self.seen
                .lock()
                .unwrap()
                .push((body.operation.clone(), delivery));
            Ok(tonic::Response::new(GenericResponse {
                ok: true,
                request_id: body.request_id,
                payload: Some(prost_types::Any {
                    type_url: String::new(),
                    value: serde_json::to_vec(&view).unwrap(),
                }),
                ..Default::default()
            }))
        }
    }
    fn now() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
    }
    fn delivery(users: &[&str]) -> RealtimeDelivery {
        RealtimeDelivery {
            conversation_id: "conversation-1".into(),
            user_ids: users.iter().map(|u| u.to_string()).collect(),
            packet_type: "call.snapshot".into(),
            payload: json!({"callId": "c-1", "version": 2})
                .as_object()
                .unwrap()
                .clone(),
            ttl_ms: 30_000,
        }
    }

    #[tokio::test]
    async fn runtime_deliveries_are_signed_for_the_exact_bytes_and_checked() {
        use crate::state_store::TenantStateStore;
        let (host, store) = crate::runtime::status_fixture::host_with_store();
        let signer = PluginRuntimeSigner::new("runtime-key".into(), [93; 32]).unwrap();
        let verifier = PluginRuntimeVerifier::from_public_keys_json(
            &json!([{"key_id": "runtime-key", "deployment_id": "echo-1",
                     "public_key_base64": base64::Engine::encode(
                         &base64::engine::general_purpose::STANDARD, signer.public_key())}])
            .to_string(),
        )
        .unwrap();
        let host: PluginRuntimeHost = host.with_runtime_signer(signer).unwrap();
        store
            .apply(crate::runtime::status_fixture::tenant(&host, 1, true))
            .await
            .unwrap();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let address = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap();
        let server = tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(PluginHostBridgeServer::new(Bridge {
                    verifier,
                    seen: seen.clone(),
                }))
                .serve(address),
        );
        let authority = format!("http://{address}");
        let client = {
            let mut attempts = 0;
            loop {
                match HostResourceClient::connect(&authority).await {
                    Ok(client) => break client,
                    Err(_) if attempts < 50 => {
                        attempts += 1;
                        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                    }
                    Err(error) => panic!("bridge unavailable: {error}"),
                }
            }
        };
        let view = client
            .deliver_realtime_as_runtime(
                &host,
                "201",
                "grant-1",
                delivery(&["alice", "stranger", "bob"]),
            )
            .await
            .unwrap();
        assert_eq!(view.delivered, 2);
        assert_eq!(view.skipped, vec!["stranger".to_string()]);
        {
            let seen = seen.lock().unwrap();
            assert_eq!(seen.len(), 1);
            assert_eq!(seen[0].0, "im.realtime.deliver");
            assert_eq!(seen[0].1, delivery(&["alice", "stranger", "bob"]));
        }
        // Invalid deliveries are never signed or sent.
        let mut duplicate = delivery(&["alice", "alice"]);
        assert!(
            client
                .deliver_realtime_as_runtime(&host, "201", "grant-1", duplicate.clone())
                .await
                .is_err()
        );
        duplicate.user_ids = vec!["alice".into()];
        duplicate.ttl_ms = 0;
        assert!(
            client
                .deliver_realtime_as_runtime(&host, "201", "grant-1", duplicate)
                .await
                .is_err()
        );
        // No grant for this tenant here.
        assert!(
            client
                .deliver_realtime_as_runtime(&host, "202", "grant-1", delivery(&["alice"]))
                .await
                .is_err()
        );
        assert_eq!(seen.lock().unwrap().len(), 1);
        // Core answering for other users is a mismatch, not a success.
        let mismatch = checked(
            &ResourceWriteRequest::RealtimeDeliver(delivery(&["alice"])),
            json!({"deliveryId": "d-2", "delivered": 0, "skipped": ["mallory"]}),
        );
        assert!(mismatch.is_err());
        server.abort();
    }
}
