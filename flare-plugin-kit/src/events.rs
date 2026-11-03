//! Plugin-owned event transport: NATS JetStream with a stream the operator
//! provisions. The plugin only inspects the stream; its broker identity may
//! publish to its own subjects and nothing else. Core is not involved.
use crate::outbox::{EventSink, OutboxEvent};
use flare_server_core::{
    Context, EventEnvelope,
    mq::producer::{Producer, ProducerError},
    nats::{NatsProducer, NatsProducerConfig, NatsStreamSpec},
};
use std::{collections::HashMap, sync::Arc, time::Duration};

/// Where one plugin's events go. Deliberately not `Debug`: the URL may carry
/// broker credentials.
pub struct EventsConfig {
    url: String,
    replicas: usize,
    stream: &'static str,
    /// `flare.plugins.<name>`; events go to `<prefix>.<tenant>.<kind>`.
    subject_prefix: &'static str,
    /// Envelope source and event id prefix.
    source: &'static str,
    kinds: &'static [&'static str],
}
impl EventsConfig {
    pub fn new(
        url: String,
        replicas: usize,
        stream: &'static str,
        subject_prefix: &'static str,
        source: &'static str,
        kinds: &'static [&'static str],
    ) -> Result<Self, &'static str> {
        if url.is_empty() || !(1..=5).contains(&replicas) || kinds.is_empty() {
            return Err("invalid plugin event transport configuration");
        }
        Ok(Self {
            url,
            replicas,
            stream,
            subject_prefix,
            source,
            kinds,
        })
    }
    /// `<PREFIX>_NATS_URL` (absent: events off) and `<PREFIX>_REPLICAS`
    /// (default 1).
    pub fn from_env(
        prefix: &str,
        stream: &'static str,
        subject_prefix: &'static str,
        source: &'static str,
        kinds: &'static [&'static str],
    ) -> Result<Option<Self>, &'static str> {
        let url = match std::env::var(format!("{prefix}_NATS_URL")) {
            Ok(url) => url,
            Err(std::env::VarError::NotPresent) => return Ok(None),
            Err(_) => return Err("invalid plugin event transport URL"),
        };
        let replicas = std::env::var(format!("{prefix}_REPLICAS"))
            .unwrap_or_else(|_| "1".into())
            .parse()
            .map_err(|_| "invalid plugin event replica count")?;
        Self::new(url, replicas, stream, subject_prefix, source, kinds).map(Some)
    }
    pub fn source(&self) -> &'static str {
        self.source
    }
    pub fn subjects(&self) -> String {
        format!("{}.*.>", self.subject_prefix)
    }
}
impl NatsProducerConfig for EventsConfig {
    fn nats_url(&self) -> &str {
        &self.url
    }
    fn timeout_ms(&self) -> u64 {
        1000
    }
    // The durable outbox owns retries.
    fn retries(&self) -> u32 {
        0
    }
    fn retry_backoff_ms(&self) -> u64 {
        0
    }
    fn stream_specs(&self) -> Vec<NatsStreamSpec> {
        vec![
            NatsStreamSpec::new(self.stream, vec![self.subjects()])
                .with_max_bytes(256 * 1024 * 1024)
                .with_num_replicas(self.replicas),
        ]
    }
}

pub struct NatsEventSink {
    producer: NatsProducer,
    subject_prefix: &'static str,
    source: &'static str,
    kinds: &'static [&'static str],
}
impl NatsEventSink {
    /// Connect and check the provisioned stream exists; never creates it.
    pub async fn connect(config: &EventsConfig) -> Result<Self, ProducerError> {
        let producer = tokio::time::timeout(
            Duration::from_secs(5),
            NatsProducer::connect_existing(config),
        )
        .await
        .map_err(|_| ProducerError::Timeout("event transport startup timed out".into()))??;
        Ok(Self {
            producer,
            subject_prefix: config.subject_prefix,
            source: config.source,
            kinds: config.kinds,
        })
    }
}

/// The subject and envelope of `event`, refusing anything a tenant id or kind
/// could turn into a wildcard or header injection.
fn envelope(
    event: &OutboxEvent,
    subject_prefix: &str,
    source: &str,
    kinds: &[&str],
) -> Result<(String, EventEnvelope), ()> {
    if event.tenant_id.is_empty()
        || event.tenant_id.len() > 32
        || !event
            .tenant_id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
        || !kinds.contains(&event.kind.as_str())
        || event.id.is_empty()
        || event.id.len() > 512
        || event.id.chars().any(char::is_control)
        || event.sequence == 0
        || !event.payload.is_object()
    {
        return Err(());
    }
    let payload = serde_json::to_vec(&event.payload).map_err(|_| ())?;
    if payload.len() > 65536 {
        return Err(());
    }
    let mut envelope = EventEnvelope::new(&event.kind, &event.tenant_id, event.sequence, payload)
        .with_timestamp_ms(event.created_at_ms)
        .with_source(source);
    envelope.event_id.clone_from(&event.id);
    Ok((
        format!("{subject_prefix}.{}.{}", event.tenant_id, event.kind),
        envelope,
    ))
}

#[tonic::async_trait]
impl EventSink for NatsEventSink {
    async fn deliver(&self, event: &OutboxEvent) -> Result<(), ()> {
        let (subject, envelope) = envelope(event, self.subject_prefix, self.source, self.kinds)?;
        let ctx = Arc::new(Context::with_request_id(&event.id).with_tenant_id(&event.tenant_id));
        self.producer
            .send(
                &ctx,
                &subject,
                Some(&event.tenant_id),
                envelope.to_json_bytes().map_err(|_| ())?,
                Some(HashMap::from([
                    ("x-message-id".into(), event.id.clone()),
                    ("content-type".into(), "application/json".into()),
                ])),
            )
            .await
            .map_err(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn subjects_and_envelopes_are_bounded() {
        let mut event = OutboxEvent {
            id: "poll:201:created:p1".into(),
            tenant_id: "201".into(),
            kind: "poll.created".into(),
            payload: serde_json::json!({"pollId": "p1"}),
            fence: 1,
            sequence: 1,
            created_at_ms: 1000,
        };
        let kinds = &["poll.created"];
        let (subject, message) =
            envelope(&event, "flare.plugins.poll", "flare.poll", kinds).unwrap();
        assert_eq!(subject, "flare.plugins.poll.201.poll.created");
        assert_eq!(message.event_id, event.id);
        event.tenant_id = "201.>".into();
        assert!(envelope(&event, "flare.plugins.poll", "flare.poll", kinds).is_err());
        event.tenant_id = "201".into();
        event.kind = "poll.unknown".into();
        assert!(envelope(&event, "flare.plugins.poll", "flare.poll", kinds).is_err());
    }
}
