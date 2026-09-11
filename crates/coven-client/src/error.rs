use std::io;

use serde_json::Value;
use thiserror::Error;

pub(crate) const UNIX_CONNECT_OPERATION: &str = "failed to connect to Coven daemon socket";
pub(crate) const UNIX_CONFIGURE_WRITES_OPERATION: &str =
    "failed to configure nonblocking Coven daemon socket writes";
pub(crate) const WINDOWS_CONNECT_OPERATION: &str = "failed to connect to Coven daemon pipe";
pub(crate) const WINDOWS_CONFIGURE_WRITES_OPERATION: &str =
    "failed to configure nonblocking Coven daemon pipe writes";

#[derive(Clone, Debug, PartialEq)]
pub struct DaemonError {
    pub code: String,
    pub message: String,
    pub details: Value,
}

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("invalid execution transcript request: {0}")]
    InvalidTranscriptRequest(crate::execution::ContractError),
    #[error("Coven daemon returned an invalid transcript response: {0}")]
    InvalidTranscriptResponse(crate::execution::ContractError),
    #[error("invalid execution catalog request: {0}")]
    InvalidCatalogRequest(crate::execution::ContractError),
    #[error("Coven daemon returned an invalid catalog response: {0}")]
    InvalidCatalogResponse(crate::execution::ContractError),
    #[error(transparent)]
    InvalidExecutionRequest(#[from] crate::execution::ContractError),
    #[error("Coven daemon returned an invalid execution response: {0}")]
    InvalidExecutionResponse(crate::execution::ContractError),
    #[error("Coven daemon returned an invalid source response: {0}")]
    InvalidSourceResponse(crate::execution::ContractError),
    #[error("failed to discover owner-local Coven endpoint: {0}")]
    Discovery(String),
    #[error("{operation}: {source}")]
    Io {
        operation: &'static str,
        #[source]
        source: io::Error,
    },
    #[error("Coven daemon response exceeded the {max_bytes}-byte body limit")]
    ResponseTooLarge { max_bytes: usize },
    #[error(
        "Coven daemon request body of {actual_bytes} bytes exceeded the {max_bytes}-byte limit"
    )]
    RequestTooLarge {
        max_bytes: usize,
        actual_bytes: usize,
    },
    #[error("invalid Coven daemon HTTP response: {0}")]
    InvalidHttpResponse(String),
    #[error("Coven daemon response was not valid UTF-8")]
    InvalidUtf8(#[source] std::string::FromUtf8Error),
    #[error("failed to parse Coven daemon response: {0}")]
    InvalidJson(#[source] serde_json::Error),
    #[error("Coven daemon API mismatch: expected {expected}, got {actual}")]
    ProtocolVersion {
        expected: &'static str,
        actual: String,
    },
    #[error("Coven daemon does not advertise capabilities.structuredErrors")]
    StructuredErrorsUnavailable,
    #[error("Coven daemon does not advertise required capabilities.{capability}")]
    CapabilityUnavailable { capability: &'static str },
    #[error("Coven daemon health reported not ready")]
    HealthNotReady,
    #[error("Coven daemon instance changed; health negotiation is required")]
    DaemonInstanceChanged,
    #[error("Coven daemon rejected request with HTTP {status}: {error}")]
    Daemon { status: u16, error: DaemonError },
    #[error("Coven daemon rejected request with HTTP {0}")]
    HttpStatus(u16),
    #[error("invalid Coven API route parameter: {0}")]
    InvalidRouteParameter(&'static str),
    #[error(
        "cannot safely stop a legacy BASE Coven daemon on {platform}: identity-bound process \
         signaling is unavailable; upgrade Coven and retry, or restart the daemon manually"
    )]
    LegacyShutdownUpgradeRequired { platform: &'static str },
    #[error("Coven daemon client is not implemented on this platform")]
    UnsupportedPlatform,
}

impl ClientError {
    /// Return whether the transport proved that no request bytes were sent.
    pub fn request_was_definitely_not_sent(&self) -> bool {
        matches!(
            self,
            Self::Io {
                operation: UNIX_CONNECT_OPERATION
                    | UNIX_CONFIGURE_WRITES_OPERATION
                    | WINDOWS_CONNECT_OPERATION
                    | WINDOWS_CONFIGURE_WRITES_OPERATION,
                ..
            } | Self::DaemonInstanceChanged
                | Self::InvalidExecutionRequest(_)
                | Self::InvalidCatalogRequest(_)
                | Self::InvalidTranscriptRequest(_)
        )
    }
}

impl std::fmt::Display for DaemonError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({})", self.message, self.code)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn io_error(operation: &'static str) -> ClientError {
        ClientError::Io {
            operation,
            source: io::Error::from(io::ErrorKind::ConnectionReset),
        }
    }

    #[test]
    fn request_delivery_classification_is_exact() {
        for operation in [
            UNIX_CONNECT_OPERATION,
            UNIX_CONFIGURE_WRITES_OPERATION,
            WINDOWS_CONNECT_OPERATION,
            WINDOWS_CONFIGURE_WRITES_OPERATION,
        ] {
            assert!(io_error(operation).request_was_definitely_not_sent());
        }

        assert!(ClientError::DaemonInstanceChanged.request_was_definitely_not_sent());
        assert!(
            ClientError::InvalidTranscriptRequest(crate::execution::ContractError("authority"))
                .request_was_definitely_not_sent()
        );
        assert!(
            !ClientError::InvalidTranscriptResponse(crate::execution::ContractError("response"))
                .request_was_definitely_not_sent()
        );
        assert!(
            ClientError::InvalidCatalogRequest(crate::execution::ContractError("authority"))
                .request_was_definitely_not_sent()
        );
        assert!(
            !ClientError::InvalidCatalogResponse(crate::execution::ContractError("response"))
                .request_was_definitely_not_sent()
        );
        assert!(!io_error("failed to write Coven daemon request").request_was_definitely_not_sent());
        assert!(!io_error("failed to connect to Coven daemon socket later")
            .request_was_definitely_not_sent());
    }
}
