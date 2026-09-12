//! Owner-local, bounded terminal-control wire contract.
//!
//! Terminal observation and terminal mutation have different authority
//! boundaries.  [`crate::terminal`] owns the long-lived observe-only stream;
//! this module owns the short JSON requests used by an attachment that was
//! explicitly admitted for control.  Every request repeats the complete
//! stream/execution identity.  A daemon must therefore reject a request from
//! an old stream, execution generation, or owner epoch even when the caller
//! still has a syntactically valid attachment UUID.
//!
//! The module deliberately does not open a socket or retry a request.  The
//! parent `DaemonClient` integration supplies one owner-local, peer-bound
//! POST through [`TerminalControlTransport`].  A transport error is returned
//! as-is and an accepted `unknown` mutation receipt is returned as-is: neither
//! result is converted into another mutation attempt.

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{execution::ContractError, ClientError};

/// The version of the owner-local terminal-control JSON contract.
pub const TERMINAL_CONTROL_PROTOCOL_VERSION: u16 = 1;

/// Stable contract identifier for capability and diagnostics surfaces.
pub const CONTRACT: &str = "coven.terminal-control.v1";

/// The sole route used by the typed client integration.
pub const TERMINAL_CONTROL_PATH: &str = "/api/v1/terminal/control";

/// Maximum encoded request and response body accepted by this contract.
///
/// The daemon applies the same bound before deserializing.  In particular, a
/// request with a legal input byte count can still be rejected when its JSON
/// array encoding crosses this limit.
pub const MAX_TERMINAL_CONTROL_BODY_BYTES: usize = 64 * 1024;

/// Alias naming the request-side limit for callers that want to be explicit.
pub const MAX_TERMINAL_CONTROL_REQUEST_BYTES: usize = MAX_TERMINAL_CONTROL_BODY_BYTES;

/// Alias naming the response-side limit for callers that want to be explicit.
pub const MAX_TERMINAL_CONTROL_RESPONSE_BYTES: usize = MAX_TERMINAL_CONTROL_BODY_BYTES;

/// The hard cap shared by the controlled PTY writer and this wire contract.
pub const MAX_TERMINAL_CONTROL_INPUT_BYTES: usize = 64 * 1024;

/// Compatibility spelling used by the daemon's controlled writer.
pub const MAX_CONTROLLED_INPUT_BYTES: usize = MAX_TERMINAL_CONTROL_INPUT_BYTES;

/// The maximum lease duration accepted by the daemon owner, in milliseconds.
pub const MAX_TERMINAL_LEASE_DURATION_MS: u64 = 5 * 60 * 1000;

/// Identity echoed by an attach handshake and repeated on every control
/// request.  These are display-safe wire values rather than native process or
/// socket capabilities.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TerminalControlIdentity {
    pub session_id: String,
    pub stream_id: String,
    pub stream_generation: u64,
    pub execution_generation: u64,
    pub authority_epoch: u64,
    pub attachment_id: String,
}

impl TerminalControlIdentity {
    /// Construct a validated identity from the fields returned by a control
    /// capable attach handshake.
    pub fn new(
        session_id: impl Into<String>,
        stream_id: impl Into<String>,
        stream_generation: u64,
        execution_generation: u64,
        authority_epoch: u64,
        attachment_id: impl Into<String>,
    ) -> Result<Self, ContractError> {
        let identity = Self {
            session_id: session_id.into(),
            stream_id: stream_id.into(),
            stream_generation,
            execution_generation,
            authority_epoch,
            attachment_id: attachment_id.into(),
        };
        identity.validate()?;
        Ok(identity)
    }

    /// Validate all identity fields before they are serialized or used to
    /// bind a response.
    pub fn validate(&self) -> Result<(), ContractError> {
        validate_session_id(&self.session_id)?;
        validate_uuid_text(&self.stream_id, "terminalControl.streamId")?;
        validate_uuid_text(&self.attachment_id, "terminalControl.attachmentId")?;
        if self.stream_generation == 0 {
            return Err(ContractError("terminalControl.streamGeneration"));
        }
        if self.execution_generation == 0 {
            return Err(ContractError("terminalControl.executionGeneration"));
        }
        if self.authority_epoch == 0 {
            return Err(ContractError("terminalControl.authorityEpoch"));
        }
        Ok(())
    }

    /// Whether this identity is exactly equal to another identity.  The
    /// explicit method makes response-binding call sites self-documenting.
    pub fn matches(&self, other: &Self) -> bool {
        self == other
    }

    /// Build the control identity from fields returned by a control-capable
    /// terminal attach.  The owner only publishes execution and epoch values
    /// for a control attachment; their absence therefore fails closed.
    pub fn from_attach_response(
        response: &crate::terminal::TerminalAttachResponse,
    ) -> Result<Self, ContractError> {
        if response.protocol_version != crate::terminal::TERMINAL_PROTOCOL_VERSION {
            return Err(ContractError("terminalControl.protocolVersion"));
        }
        if response.authority != crate::terminal::TerminalAuthority::OwnerLocalIpc {
            return Err(ContractError("terminalControl.authority"));
        }
        if response.backend != crate::terminal::TerminalBackend::PtyRaw {
            return Err(ContractError("terminalControl.backend"));
        }
        if response.codec != crate::terminal::TerminalCodec::RawBytesV1 {
            return Err(ContractError("terminalControl.codec"));
        }
        if !response.permissions.observe {
            return Err(ContractError("terminalControl.observePermission"));
        }
        if !response.permissions.control {
            return Err(ContractError("terminalControl.controlPermission"));
        }
        let execution_generation = response
            .execution_generation
            .ok_or(ContractError("terminalControl.executionGeneration"))?;
        let authority_epoch = response
            .authority_epoch
            .ok_or(ContractError("terminalControl.authorityEpoch"))?;
        Self::new(
            response.session_id.clone(),
            response.stream_id.clone(),
            response.stream_generation,
            execution_generation,
            authority_epoch,
            response.attachment_id.clone(),
        )
    }
}

/// Closed set of mutations and lease transitions accepted by the owner.
///
/// `bytes` is intentionally a JSON byte array.  This matches the daemon's
/// `Vec<u8>` DTO, avoids introducing a base64 dependency in the small client,
/// and lets the encoded body bound be checked before any I/O.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "snake_case",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum TerminalControlAction {
    Acquire {
        #[serde(alias = "durationMs")]
        requested_duration_ms: u64,
    },
    Renew {
        lease_generation: u64,
        #[serde(alias = "durationMs")]
        requested_duration_ms: u64,
    },
    Release {
        lease_generation: u64,
    },
    Takeover {
        #[serde(alias = "durationMs")]
        requested_duration_ms: u64,
    },
    Input {
        lease_generation: u64,
        mutation_seq: u64,
        bytes: Vec<u8>,
    },
    Resize {
        lease_generation: u64,
        mutation_seq: u64,
        rows: u16,
        cols: u16,
        #[serde(default)]
        pixel_width: u16,
        #[serde(default)]
        pixel_height: u16,
    },
    Poll {
        mutation_seq: u64,
    },
}

impl TerminalControlAction {
    pub fn acquire(requested_duration_ms: u64) -> Self {
        Self::Acquire {
            requested_duration_ms,
        }
    }

    pub fn renew(lease_generation: u64, requested_duration_ms: u64) -> Self {
        Self::Renew {
            lease_generation,
            requested_duration_ms,
        }
    }

    pub const fn release(lease_generation: u64) -> Self {
        Self::Release { lease_generation }
    }

    pub fn takeover(requested_duration_ms: u64) -> Self {
        Self::Takeover {
            requested_duration_ms,
        }
    }

    pub fn input(lease_generation: u64, mutation_seq: u64, bytes: Vec<u8>) -> Self {
        Self::Input {
            lease_generation,
            mutation_seq,
            bytes,
        }
    }

    pub const fn resize(
        lease_generation: u64,
        mutation_seq: u64,
        rows: u16,
        cols: u16,
        pixel_width: u16,
        pixel_height: u16,
    ) -> Self {
        Self::Resize {
            lease_generation,
            mutation_seq,
            rows,
            cols,
            pixel_width,
            pixel_height,
        }
    }

    pub const fn poll(mutation_seq: u64) -> Self {
        Self::Poll { mutation_seq }
    }

    pub fn validate(&self) -> Result<(), ContractError> {
        match self {
            Self::Acquire {
                requested_duration_ms,
            }
            | Self::Takeover {
                requested_duration_ms,
            } => validate_duration(*requested_duration_ms),
            Self::Renew {
                lease_generation,
                requested_duration_ms,
            } => {
                validate_generation(*lease_generation, "terminalControl.leaseGeneration")?;
                validate_duration(*requested_duration_ms)
            }
            Self::Release { lease_generation } => {
                validate_generation(*lease_generation, "terminalControl.leaseGeneration")
            }
            Self::Input {
                lease_generation,
                mutation_seq,
                bytes,
            } => {
                validate_generation(*lease_generation, "terminalControl.leaseGeneration")?;
                validate_sequence(*mutation_seq)?;
                if bytes.len() > MAX_TERMINAL_CONTROL_INPUT_BYTES {
                    return Err(ContractError("terminalControl.inputBytes"));
                }
                Ok(())
            }
            Self::Resize {
                lease_generation,
                mutation_seq,
                rows,
                cols,
                ..
            } => {
                validate_generation(*lease_generation, "terminalControl.leaseGeneration")?;
                validate_sequence(*mutation_seq)?;
                if *rows == 0 || *cols == 0 {
                    return Err(ContractError("terminalControl.resize"));
                }
                Ok(())
            }
            Self::Poll { mutation_seq } => validate_sequence(*mutation_seq),
        }
    }

    fn expected_outcome(&self) -> ExpectedOutcome {
        match self {
            Self::Acquire { .. } => ExpectedOutcome::Acquire,
            Self::Renew { .. } => ExpectedOutcome::Renew,
            Self::Release { .. } => ExpectedOutcome::Release,
            Self::Takeover { .. } => ExpectedOutcome::Takeover,
            Self::Input { mutation_seq, .. } | Self::Resize { mutation_seq, .. } => {
                ExpectedOutcome::Mutation(*mutation_seq)
            }
            Self::Poll { mutation_seq } => ExpectedOutcome::Poll(*mutation_seq),
        }
    }
}

/// One flat request envelope.  The fields are deliberately repeated here
/// rather than nested under `identity`: the owner route can require and bind
/// every identity field before dispatching any action.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TerminalControlRequest {
    pub session_id: String,
    pub stream_id: String,
    pub stream_generation: u64,
    pub execution_generation: u64,
    pub authority_epoch: u64,
    pub attachment_id: String,
    pub action: TerminalControlAction,
}

impl TerminalControlRequest {
    pub fn new(identity: TerminalControlIdentity, action: TerminalControlAction) -> Self {
        Self {
            session_id: identity.session_id,
            stream_id: identity.stream_id,
            stream_generation: identity.stream_generation,
            execution_generation: identity.execution_generation,
            authority_epoch: identity.authority_epoch,
            attachment_id: identity.attachment_id,
            action,
        }
    }

    pub fn identity(&self) -> TerminalControlIdentity {
        TerminalControlIdentity {
            session_id: self.session_id.clone(),
            stream_id: self.stream_id.clone(),
            stream_generation: self.stream_generation,
            execution_generation: self.execution_generation,
            authority_epoch: self.authority_epoch,
            attachment_id: self.attachment_id.clone(),
        }
    }

    pub fn validate(&self) -> Result<(), ContractError> {
        self.identity().validate()?;
        self.action.validate()
    }

    /// Serialize after semantic and encoded-size validation.  Callers should
    /// use this helper instead of serializing a request and checking its size
    /// afterward, because it is the boundary before transport I/O.
    pub fn to_bounded_json(&self) -> Result<Vec<u8>, TerminalControlClientError> {
        self.validate()
            .map_err(TerminalControlClientError::InvalidRequest)?;
        let body = serde_json::to_vec(self).map_err(TerminalControlClientError::Json)?;
        if body.len() > MAX_TERMINAL_CONTROL_BODY_BYTES {
            return Err(TerminalControlClientError::RequestTooLarge {
                max_bytes: MAX_TERMINAL_CONTROL_BODY_BYTES,
                actual_bytes: body.len(),
            });
        }
        Ok(body)
    }

    /// Parse a daemon-bound request only after enforcing the same body limit
    /// used by the owner IPC parser.
    pub fn from_bounded_json(body: &[u8]) -> Result<Self, TerminalControlClientError> {
        if body.len() > MAX_TERMINAL_CONTROL_BODY_BYTES {
            return Err(TerminalControlClientError::RequestTooLarge {
                max_bytes: MAX_TERMINAL_CONTROL_BODY_BYTES,
                actual_bytes: body.len(),
            });
        }
        let request: Self =
            serde_json::from_slice(body).map_err(TerminalControlClientError::Json)?;
        request
            .validate()
            .map_err(TerminalControlClientError::InvalidRequest)?;
        Ok(request)
    }
}

/// Wire status of an admitted mutation.  `Unknown` is a terminal client
/// decision: the side effect may have happened, so callers must reconcile the
/// existing sequence rather than resubmit it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TerminalMutationStatus {
    Pending,
    Completed,
    Rejected,
    Stopped,
    Unknown,
}

/// Compatibility spelling for callers that use the daemon's shorter name.
pub type MutationStatus = TerminalMutationStatus;

impl TerminalMutationStatus {
    pub const fn is_pending(self) -> bool {
        matches!(self, Self::Pending)
    }

    pub const fn is_completed(self) -> bool {
        matches!(self, Self::Completed)
    }

    pub const fn is_unknown(self) -> bool {
        matches!(self, Self::Unknown)
    }
}

/// Bounded receipt returned for input, resize, and poll operations.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TerminalMutationReceipt {
    pub mutation_seq: u64,
    pub status: TerminalMutationStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

impl TerminalMutationReceipt {
    pub fn validate(&self) -> Result<(), ContractError> {
        validate_sequence(self.mutation_seq)?;
        if self.reason.as_deref().is_some_and(|reason| {
            reason.is_empty() || reason.len() > 256 || reason.chars().any(char::is_control)
        }) {
            return Err(ContractError("terminalControl.receiptReason"));
        }
        Ok(())
    }

    pub const fn is_unknown(&self) -> bool {
        self.status.is_unknown()
    }
}

/// Typed outcome for each control action.  The action-specific tags prevent a
/// successful acquire from being mistaken for a mutation receipt by a caller
/// that has received a malformed or mismatched daemon response.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "snake_case",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum TerminalControlOutcome {
    Acquired {
        lease_generation: u64,
        next_mutation_seq: u64,
    },
    Renewed {
        lease_generation: u64,
        next_mutation_seq: u64,
    },
    Released,
    TakenOver {
        lease_generation: u64,
        next_mutation_seq: u64,
    },
    Mutation {
        mutation_seq: u64,
        status: TerminalMutationStatus,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
    Polled {
        mutation_seq: u64,
        status: TerminalMutationStatus,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
}

/// Compatibility spelling that emphasizes result semantics.
pub type TerminalControlResult = TerminalControlOutcome;

impl TerminalControlOutcome {
    pub fn validate(&self) -> Result<(), ContractError> {
        match self {
            Self::Acquired {
                lease_generation,
                next_mutation_seq,
            }
            | Self::Renewed {
                lease_generation,
                next_mutation_seq,
            }
            | Self::TakenOver {
                lease_generation,
                next_mutation_seq,
            } => {
                validate_generation(*lease_generation, "terminalControl.leaseGeneration")?;
                validate_sequence(*next_mutation_seq)
            }
            Self::Released => Ok(()),
            Self::Mutation {
                mutation_seq,
                status,
                reason,
            }
            | Self::Polled {
                mutation_seq,
                status,
                reason,
            } => TerminalMutationReceipt {
                mutation_seq: *mutation_seq,
                status: *status,
                reason: reason.clone(),
            }
            .validate(),
        }
    }

    fn validate_for(&self, expected: ExpectedOutcome) -> Result<(), ContractError> {
        self.validate()?;
        let matches = match (expected, self) {
            (ExpectedOutcome::Acquire, Self::Acquired { .. })
            | (ExpectedOutcome::Renew, Self::Renewed { .. })
            | (ExpectedOutcome::Release, Self::Released)
            | (ExpectedOutcome::Takeover, Self::TakenOver { .. }) => true,
            (ExpectedOutcome::Mutation(sequence), Self::Mutation { mutation_seq, .. })
            | (ExpectedOutcome::Poll(sequence), Self::Polled { mutation_seq, .. }) => {
                sequence == *mutation_seq
            }
            _ => false,
        };
        if matches {
            Ok(())
        } else {
            Err(ContractError("terminalControl.responseKind"))
        }
    }
}

/// Reply envelope.  The daemon echoes the full identity so a response from a
/// replaced terminal cannot be accepted merely because its action succeeded.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TerminalControlReply {
    pub session_id: String,
    pub stream_id: String,
    pub stream_generation: u64,
    pub execution_generation: u64,
    pub authority_epoch: u64,
    pub attachment_id: String,
    #[serde(alias = "result")]
    pub outcome: TerminalControlOutcome,
}

impl TerminalControlReply {
    pub fn identity(&self) -> TerminalControlIdentity {
        TerminalControlIdentity {
            session_id: self.session_id.clone(),
            stream_id: self.stream_id.clone(),
            stream_generation: self.stream_generation,
            execution_generation: self.execution_generation,
            authority_epoch: self.authority_epoch,
            attachment_id: self.attachment_id.clone(),
        }
    }

    pub fn validate(&self) -> Result<(), ContractError> {
        self.identity().validate()?;
        self.outcome.validate()
    }

    pub fn validate_for(&self, request: &TerminalControlRequest) -> Result<(), ContractError> {
        request.validate()?;
        self.validate()?;
        if !self.identity().matches(&request.identity()) {
            return Err(ContractError("terminalControl.responseIdentity"));
        }
        self.outcome.validate_for(request.action.expected_outcome())
    }
}

/// Errors raised before or during the one typed terminal-control call.
#[derive(Debug, Error)]
pub enum TerminalControlClientError {
    #[error("invalid terminal control request: {0}")]
    InvalidRequest(#[source] ContractError),
    #[error("Coven daemon returned an invalid terminal control response: {0}")]
    InvalidResponse(#[source] ContractError),
    #[error("terminal control JSON was invalid: {0}")]
    Json(#[source] serde_json::Error),
    #[error("terminal control body of {actual_bytes} bytes exceeded the {max_bytes}-byte limit")]
    RequestTooLarge {
        max_bytes: usize,
        actual_bytes: usize,
    },
    #[error(
        "terminal control response of {actual_bytes} bytes exceeded the {max_bytes}-byte limit"
    )]
    ResponseTooLarge {
        max_bytes: usize,
        actual_bytes: usize,
    },
    #[error(transparent)]
    Transport(#[from] ClientError),
}

/// The only transport seam used by [`call_terminal_control`].
///
/// The `DaemonClient` integration should implement this in `http.rs` by
/// ensuring health, using the negotiated peer-bound request helper, and
/// returning the successful response body.  It must map non-2xx responses to
/// the existing `ClientError` envelope.  Keeping the route implicit here
/// prevents callers from substituting an arbitrary path or a TCP fallback.
pub trait TerminalControlTransport {
    fn post_terminal_control(&mut self, body: &[u8]) -> Result<Vec<u8>, ClientError>;
}

/// Perform exactly one bounded owner-local terminal-control POST and decode
/// its typed reply.  This function never retries, including for a transport
/// error or an `Unknown` mutation outcome.
pub fn call_terminal_control<T: TerminalControlTransport>(
    transport: &mut T,
    request: &TerminalControlRequest,
) -> Result<TerminalControlReply, TerminalControlClientError> {
    let body = request.to_bounded_json()?;
    let response_body = transport
        .post_terminal_control(&body)
        .map_err(TerminalControlClientError::Transport)?;
    if response_body.len() > MAX_TERMINAL_CONTROL_RESPONSE_BYTES {
        return Err(TerminalControlClientError::ResponseTooLarge {
            max_bytes: MAX_TERMINAL_CONTROL_RESPONSE_BYTES,
            actual_bytes: response_body.len(),
        });
    }
    let reply: TerminalControlReply =
        serde_json::from_slice(&response_body).map_err(TerminalControlClientError::Json)?;
    reply
        .validate_for(request)
        .map_err(TerminalControlClientError::InvalidResponse)?;
    Ok(reply)
}

/// Convenience trait for callers that want method syntax after the root
/// `DaemonClient` implements [`TerminalControlTransport`].
pub trait TerminalControlClient {
    fn terminal_control(
        &mut self,
        request: &TerminalControlRequest,
    ) -> Result<TerminalControlReply, TerminalControlClientError>;
}

impl<T: TerminalControlTransport> TerminalControlClient for T {
    fn terminal_control(
        &mut self,
        request: &TerminalControlRequest,
    ) -> Result<TerminalControlReply, TerminalControlClientError> {
        call_terminal_control(self, request)
    }
}

#[derive(Clone, Copy)]
enum ExpectedOutcome {
    Acquire,
    Renew,
    Release,
    Takeover,
    Mutation(u64),
    Poll(u64),
}

fn validate_session_id(value: &str) -> Result<(), ContractError> {
    if value.is_empty() || value.len() > 128 || value.chars().any(char::is_control) {
        return Err(ContractError("terminalControl.sessionId"));
    }
    Ok(())
}

fn validate_generation(value: u64, field: &'static str) -> Result<(), ContractError> {
    if value == 0 {
        Err(ContractError(field))
    } else {
        Ok(())
    }
}

fn validate_sequence(value: u64) -> Result<(), ContractError> {
    validate_generation(value, "terminalControl.mutationSeq")
}

fn validate_duration(value: u64) -> Result<(), ContractError> {
    if value == 0 || value > MAX_TERMINAL_LEASE_DURATION_MS {
        Err(ContractError("terminalControl.durationMs"))
    } else {
        Ok(())
    }
}

fn validate_uuid_text(value: &str, field: &'static str) -> Result<(), ContractError> {
    let bytes = value.as_bytes();
    if bytes.len() != 36
        || !matches!(bytes.get(8), Some(b'-'))
        || !matches!(bytes.get(13), Some(b'-'))
        || !matches!(bytes.get(18), Some(b'-'))
        || !matches!(bytes.get(23), Some(b'-'))
    {
        return Err(ContractError(field));
    }
    let mut nonzero = false;
    for (index, byte) in bytes.iter().enumerate() {
        if matches!(index, 8 | 13 | 18 | 23) {
            continue;
        }
        if !byte.is_ascii_hexdigit() {
            return Err(ContractError(field));
        }
        nonzero |= *byte != b'0';
    }
    if nonzero {
        Ok(())
    } else {
        Err(ContractError(field))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    const SESSION: &str = "engine/session-1";
    const STREAM: &str = "00000000-0000-0000-0000-000000000001";
    const ATTACHMENT: &str = "00000000-0000-0000-0000-000000000002";

    fn identity() -> TerminalControlIdentity {
        TerminalControlIdentity::new(SESSION, STREAM, 3, 7, 11, ATTACHMENT).unwrap()
    }

    fn make_request(action: TerminalControlAction) -> TerminalControlRequest {
        TerminalControlRequest::new(identity(), action)
    }

    fn control_attach_response() -> crate::terminal::TerminalAttachResponse {
        crate::terminal::TerminalAttachResponse {
            protocol_version: crate::terminal::TERMINAL_PROTOCOL_VERSION,
            authority: crate::terminal::TerminalAuthority::OwnerLocalIpc,
            session_id: SESSION.to_owned(),
            attachment_id: ATTACHMENT.to_owned(),
            stream_id: STREAM.to_owned(),
            stream_generation: 3,
            backend: crate::terminal::TerminalBackend::PtyRaw,
            codec: crate::terminal::TerminalCodec::RawBytesV1,
            permissions: crate::terminal::TerminalPermissions {
                observe: true,
                control: true,
            },
            execution_generation: Some(7),
            authority_epoch: Some(11),
            max_frame_bytes: crate::terminal::DEFAULT_MAX_FRAME_BYTES,
            replay_bytes: crate::terminal::DEFAULT_MAX_REPLAY_BYTES,
            cursor: crate::terminal::TerminalCursor {
                offset: 0,
                sequence: 1,
            },
        }
    }

    fn reply_for(request: &TerminalControlRequest, outcome: TerminalControlOutcome) -> Vec<u8> {
        serde_json::to_vec(&TerminalControlReply {
            session_id: request.session_id.clone(),
            stream_id: request.stream_id.clone(),
            stream_generation: request.stream_generation,
            execution_generation: request.execution_generation,
            authority_epoch: request.authority_epoch,
            attachment_id: request.attachment_id.clone(),
            outcome,
        })
        .unwrap()
    }

    #[test]
    fn request_is_flat_and_carries_every_identity_field() {
        let request = make_request(TerminalControlAction::input(9, 12, vec![1, 2, 3]));
        let value: serde_json::Value =
            serde_json::from_slice(&request.to_bounded_json().unwrap()).unwrap();
        for field in [
            "sessionId",
            "streamId",
            "streamGeneration",
            "executionGeneration",
            "authorityEpoch",
            "attachmentId",
            "action",
        ] {
            assert!(value.get(field).is_some(), "missing {field}");
        }
        assert_eq!(value["action"]["kind"], "input");
        assert_eq!(value["action"]["bytes"], serde_json::json!([1, 2, 3]));
    }

    #[test]
    fn action_is_closed_and_unknown_fields_are_rejected() {
        let unknown_kind = serde_json::json!({
            "sessionId": SESSION,
            "streamId": STREAM,
            "streamGeneration": 3,
            "executionGeneration": 7,
            "authorityEpoch": 11,
            "attachmentId": ATTACHMENT,
            "action": {"kind": "write", "mutationSeq": 1}
        });
        assert!(TerminalControlRequest::from_bounded_json(
            &serde_json::to_vec(&unknown_kind).unwrap()
        )
        .is_err());

        let unknown_field = serde_json::json!({
            "sessionId": SESSION,
            "streamId": STREAM,
            "streamGeneration": 3,
            "executionGeneration": 7,
            "authorityEpoch": 11,
            "attachmentId": ATTACHMENT,
            "action": {"kind": "release", "leaseGeneration": 1, "extra": true}
        });
        assert!(TerminalControlRequest::from_bounded_json(
            &serde_json::to_vec(&unknown_field).unwrap()
        )
        .is_err());
    }

    #[test]
    fn invalid_identity_and_action_values_are_rejected_before_transport() {
        let mut request = make_request(TerminalControlAction::release(1));
        request.stream_generation = 0;
        assert!(matches!(
            request.to_bounded_json(),
            Err(TerminalControlClientError::InvalidRequest(_))
        ));

        let mut request = make_request(TerminalControlAction::release(1));
        request.attachment_id = "00000000-0000-0000-0000-000000000000".to_owned();
        assert!(request.validate().is_err());

        let request = make_request(TerminalControlAction::input(1, 0, vec![1]));
        assert!(request.validate().is_err());

        let request = make_request(TerminalControlAction::resize(1, 1, 0, 80, 0, 0));
        assert!(request.validate().is_err());
    }

    #[test]
    fn attach_identity_requires_a_control_capable_owner_handshake() {
        let response = control_attach_response();
        assert!(TerminalControlIdentity::from_attach_response(&response).is_ok());

        let mut response = control_attach_response();
        response.protocol_version = 0;
        assert!(TerminalControlIdentity::from_attach_response(&response).is_err());

        let mut response = control_attach_response();
        response.permissions.observe = false;
        assert!(TerminalControlIdentity::from_attach_response(&response).is_err());

        let mut response = control_attach_response();
        response.permissions.control = false;
        assert!(TerminalControlIdentity::from_attach_response(&response).is_err());
    }

    #[test]
    fn input_and_encoded_body_limits_are_bounded() {
        let request = make_request(TerminalControlAction::input(
            1,
            1,
            vec![0_u8; MAX_TERMINAL_CONTROL_INPUT_BYTES + 1],
        ));
        assert!(matches!(
            request.to_bounded_json(),
            Err(TerminalControlClientError::InvalidRequest(_))
        ));

        // This input is semantically legal but its JSON array is larger than
        // the body cap; no transport call is permitted in that case.
        let request = make_request(TerminalControlAction::input(
            1,
            1,
            vec![0_u8; MAX_TERMINAL_CONTROL_INPUT_BYTES],
        ));
        assert!(matches!(
            request.to_bounded_json(),
            Err(TerminalControlClientError::RequestTooLarge { .. })
        ));
    }

    struct FakeTransport {
        calls: Cell<u32>,
        body: Vec<u8>,
    }

    impl TerminalControlTransport for FakeTransport {
        fn post_terminal_control(&mut self, _body: &[u8]) -> Result<Vec<u8>, ClientError> {
            self.calls.set(self.calls.get() + 1);
            Ok(self.body.clone())
        }
    }

    #[test]
    fn unknown_receipt_is_returned_without_retry() {
        let request = make_request(TerminalControlAction::input(9, 12, vec![1]));
        let body = reply_for(
            &request,
            TerminalControlOutcome::Mutation {
                mutation_seq: 12,
                status: TerminalMutationStatus::Unknown,
                reason: None,
            },
        );
        let mut transport = FakeTransport {
            calls: Cell::new(0),
            body,
        };
        let reply = call_terminal_control(&mut transport, &request).unwrap();
        assert_eq!(transport.calls.get(), 1);
        assert!(matches!(
            reply.outcome,
            TerminalControlOutcome::Mutation {
                status: TerminalMutationStatus::Unknown,
                ..
            }
        ));
    }

    #[test]
    fn response_identity_and_action_kind_are_bound_to_request() {
        let request = make_request(TerminalControlAction::release(1));
        let mut body = reply_for(&request, TerminalControlOutcome::Released);
        let mut value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        value["streamGeneration"] = serde_json::json!(4);
        body = serde_json::to_vec(&value).unwrap();
        let mut transport = FakeTransport {
            calls: Cell::new(0),
            body,
        };
        assert!(matches!(
            call_terminal_control(&mut transport, &request),
            Err(TerminalControlClientError::InvalidResponse(_))
        ));
        assert_eq!(transport.calls.get(), 1);

        let request = make_request(TerminalControlAction::release(1));
        let body = reply_for(
            &request,
            TerminalControlOutcome::Mutation {
                mutation_seq: 1,
                status: TerminalMutationStatus::Completed,
                reason: None,
            },
        );
        let mut transport = FakeTransport {
            calls: Cell::new(0),
            body,
        };
        assert!(matches!(
            call_terminal_control(&mut transport, &request),
            Err(TerminalControlClientError::InvalidResponse(_))
        ));
    }

    #[test]
    fn lease_grants_require_a_nonzero_next_mutation_sequence() {
        let missing = r#"{"kind":"acquired","leaseGeneration":1}"#;
        assert!(serde_json::from_str::<TerminalControlOutcome>(missing).is_err());
        let invalid = TerminalControlOutcome::Acquired {
            lease_generation: 1,
            next_mutation_seq: 0,
        };
        assert!(invalid.validate().is_err());
        let valid = TerminalControlOutcome::TakenOver {
            lease_generation: 2,
            next_mutation_seq: 17,
        };
        assert!(valid.validate().is_ok());
    }

    #[test]
    fn missing_identity_fields_do_not_deserialize() {
        let value = serde_json::json!({
            "sessionId": SESSION,
            "streamId": STREAM,
            "streamGeneration": 3,
            "executionGeneration": 7,
            "authorityEpoch": 11,
            "action": {"kind": "release", "leaseGeneration": 1}
        });
        assert!(
            TerminalControlRequest::from_bounded_json(&serde_json::to_vec(&value).unwrap())
                .is_err()
        );
    }
}
