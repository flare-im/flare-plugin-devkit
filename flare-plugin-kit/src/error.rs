//! Rejections a client can tell apart, and the plugin's `GenericResponse`.
//!
//! A definite rejection travels as `ok: false` with its `errorCode`
//! (`SCREAMING_SNAKE_CASE`, one of the plugin contract's codes). An unknown
//! outcome (storage failed mid-command) is a gRPC `UNAVAILABLE`: the caller
//! retries with the same command id instead of assuming nothing happened.
use flare_grpc_proto::capability::GenericResponse;
use serde_json::Value;
use tonic::{Response, Status};

/// Codes shared by every plugin built on the kit; plugins add their own.
pub mod codes {
    pub const INVALID_REQUEST: &str = "INVALID_REQUEST";
    pub const ACCESS_DENIED: &str = "ACCESS_DENIED";
    pub const COMMAND_CONFLICT: &str = "COMMAND_CONFLICT";
    pub const NOT_CONVERSATION_MEMBER: &str = "NOT_CONVERSATION_MEMBER";
    pub const HOST_RESOURCE_UNAVAILABLE: &str = "HOST_RESOURCE_UNAVAILABLE";
    pub const INCOMPATIBLE_DATA_SCHEMA: &str = "INCOMPATIBLE_DATA_SCHEMA";
}

#[derive(Debug, thiserror::Error)]
pub enum KitError {
    #[error("invalid plugin request")]
    Invalid,
    #[error("plugin access denied")]
    Denied,
    #[error("command id reused with another request")]
    Conflict,
    #[error("not a member of the conversation")]
    NotMember,
    #[error("host resource bridge unavailable")]
    ResourceUnavailable,
    #[error("plugin data schema is incompatible with this binary")]
    IncompatibleSchema,
    #[error("plugin persistence unavailable")]
    Storage(#[from] sqlx::Error),
}

/// An error that may carry a definite rejection code.
pub trait Rejection: std::fmt::Display {
    /// `None` when the outcome is unknown.
    fn code(&self) -> Option<&'static str>;
}
impl Rejection for KitError {
    fn code(&self) -> Option<&'static str> {
        Some(match self {
            Self::Invalid => codes::INVALID_REQUEST,
            Self::Denied => codes::ACCESS_DENIED,
            Self::Conflict => codes::COMMAND_CONFLICT,
            Self::NotMember => codes::NOT_CONVERSATION_MEMBER,
            Self::ResourceUnavailable => codes::HOST_RESOURCE_UNAVAILABLE,
            Self::IncompatibleSchema => codes::INCOMPATIBLE_DATA_SCHEMA,
            Self::Storage(_) => return None,
        })
    }
}

const PAYLOAD_TYPE: &str = "type.googleapis.com/flare.capability.v1.PayloadJson";

/// The response to one plugin call: the JSON result, a definite rejection with
/// its code, or `UNAVAILABLE` for an unknown outcome.
pub fn respond<E: Rejection>(
    request_id: &str,
    outcome: Result<Value, E>,
) -> Result<Response<GenericResponse>, Status> {
    match outcome {
        Ok(value) => Ok(Response::new(GenericResponse {
            ok: true,
            request_id: request_id.into(),
            payload: Some(prost_types::Any {
                type_url: PAYLOAD_TYPE.into(),
                value: serde_json::to_vec(&value)
                    .map_err(|_| Status::internal("plugin response encoding"))?,
            }),
            ..Default::default()
        })),
        Err(error) => match error.code() {
            Some(code) => Ok(Response::new(GenericResponse {
                ok: false,
                request_id: request_id.into(),
                error_code: code.into(),
                error_message: "plugin command rejected".into(),
                ..Default::default()
            })),
            None => Err(Status::unavailable("plugin outcome unknown")),
        },
    }
}

/// Decode a call payload into its typed request; malformed is `Invalid`.
pub fn decode<T: serde::de::DeserializeOwned>(payload: &[u8]) -> Result<T, KitError> {
    serde_json::from_slice(payload).map_err(|_| KitError::Invalid)
}

/// Encode a typed result.
pub fn encode<T: serde::Serialize>(value: &T) -> Result<Value, KitError> {
    serde_json::to_value(value).map_err(|_| KitError::Invalid)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn definite_rejections_carry_codes_and_unknown_outcomes_do_not() {
        let codes = [
            KitError::Invalid,
            KitError::Denied,
            KitError::Conflict,
            KitError::NotMember,
            KitError::ResourceUnavailable,
            KitError::IncompatibleSchema,
        ]
        .map(|e| e.code().unwrap());
        assert_eq!(
            codes
                .iter()
                .collect::<std::collections::BTreeSet<_>>()
                .len(),
            codes.len()
        );
        let rejected = respond::<KitError>("r1", Err(KitError::NotMember))
            .unwrap()
            .into_inner();
        assert!(!rejected.ok);
        assert_eq!(rejected.error_code, codes::NOT_CONVERSATION_MEMBER);
        assert_eq!(rejected.request_id, "r1");
        let unknown = respond::<KitError>("r1", Err(KitError::Storage(sqlx::Error::PoolClosed)));
        assert_eq!(unknown.unwrap_err().code(), tonic::Code::Unavailable);
        let ok = respond::<KitError>("r1", Ok(serde_json::json!({"a": 1})))
            .unwrap()
            .into_inner();
        assert!(ok.ok);
        assert_eq!(ok.payload.unwrap().value, br#"{"a":1}"#);
    }
}
