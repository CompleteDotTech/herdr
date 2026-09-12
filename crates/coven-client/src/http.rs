use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;

use crate::{
    error::DaemonError,
    models::{Health, HealthCapabilities, ReadEndpoint, WriteEndpoint, PROTOCOL_VERSION},
    transport::{self, HttpResponse, PeerIdentity},
    ClientError, DaemonEndpoint,
};

const HEALTH_PATH: &str = "/api/v1/health";
const RESERVED_SESSION_DETAIL_ROUTE_ERROR: &str =
    "ReadEndpoint::Session id collides with a reserved BASE v1 nested route";

pub struct DaemonClient {
    endpoint: DaemonEndpoint,
    negotiated: Option<NegotiatedDaemon>,
}

struct NegotiatedDaemon {
    capabilities: HealthCapabilities,
    peer_identity: PeerIdentity,
    execution_authority: Option<crate::execution::ExecutionAuthority>,
}

impl DaemonClient {
    /// Open an owner-local, observe-only terminal stream. The request body is
    /// intentionally used for the session id so reserved characters never
    /// become an HTTP route component. The returned reader retains the same
    /// authenticated transport after the bounded JSON negotiation.
    pub fn attach_terminal(
        &mut self,
        request: crate::terminal::TerminalAttachRequest,
    ) -> Result<crate::terminal::TerminalReader, ClientError> {
        self.ensure_health()?;
        let body = serde_json::to_vec(&request).map_err(ClientError::InvalidJson)?;
        if body.len() > crate::terminal::MAX_TERMINAL_ATTACH_BODY_BYTES {
            return Err(ClientError::RequestTooLarge {
                max_bytes: crate::terminal::MAX_TERMINAL_ATTACH_BODY_BYTES,
                actual_bytes: body.len(),
            });
        }
        let expected_peer = self
            .negotiated
            .as_ref()
            .ok_or(ClientError::HealthNotReady)?
            .peer_identity
            .clone();
        let connection = match transport::open_terminal(
            &self.endpoint,
            "/api/v1/terminal/attach",
            &body,
            &expected_peer,
        ) {
            Ok(connection) => connection,
            Err(error) => {
                if matches!(error, ClientError::DaemonInstanceChanged) {
                    self.negotiated = None;
                }
                return Err(error);
            }
        };
        if !(200..300).contains(&connection.response.status) {
            return Err(daemon_error(
                connection.response.status,
                connection.response.body,
            )?);
        }
        crate::terminal::TerminalReader::from_connection(connection, &request)
    }

    /// Open the version-2 checkpoint-aware terminal stream. The request uses
    /// the same owner-local attach route as RawBytesV1, but the client sends
    /// and requires an explicit CTS2 protocol/codec acknowledgement before
    /// interpreting any retained bytes as frames.
    pub fn attach_terminal_checkpoint(
        &mut self,
        request: crate::terminal_checkpoint::TerminalCheckpointAttachRequest,
    ) -> Result<crate::terminal_checkpoint::TerminalCheckpointReader, ClientError> {
        if !self.endpoint.is_owner_local() {
            return Err(ClientError::InvalidHttpResponse(
                "checkpoint terminal attachment requires owner-local IPC".to_owned(),
            ));
        }
        let body = request.to_bounded_json()?;
        self.ensure_health()?;
        let expected_peer = self
            .negotiated
            .as_ref()
            .ok_or(ClientError::HealthNotReady)?
            .peer_identity
            .clone();
        let connection = match transport::open_terminal_checkpoint(
            &self.endpoint,
            crate::terminal_checkpoint::TERMINAL_CHECKPOINT_ATTACH_PATH,
            &body,
            &expected_peer,
        ) {
            Ok(connection) => connection,
            Err(error) => {
                if matches!(error, ClientError::DaemonInstanceChanged) {
                    self.negotiated = None;
                }
                return Err(error);
            }
        };
        if !(200..300).contains(&connection.response.status) {
            return Err(daemon_error(
                connection.response.status,
                connection.response.body,
            )?);
        }
        crate::terminal_checkpoint::TerminalCheckpointReader::from_connection(connection, &request)
    }

    /// Bounded read with no launch/control side effects. POST carries a closed
    /// typed query so opaque native session references never become URL paths.
    pub fn read_execution_source(
        &mut self,
        request: &crate::source::SourceRead,
    ) -> Result<crate::source::SourceReply, ClientError> {
        request.validate()?;
        self.ensure_health()?;
        let missing = (!self
            .capabilities()?
            .execution_source_contracts
            .iter()
            .any(|c| c == crate::source::CONTRACT))
        .then_some("executionSourceContracts");
        self.reject_missing_capability(missing)?;
        let body = serde_json::to_vec(request).map_err(ClientError::InvalidJson)?;
        if body.len() > crate::source::MAX_READ_BYTES {
            return Err(ClientError::RequestTooLarge {
                max_bytes: crate::source::MAX_READ_BYTES,
                actual_bytes: body.len(),
            });
        }
        let response = self.send_bound(
            "POST",
            "/api/v1/execution-source/read",
            Some(&body),
            transport::MAX_RESPONSE_BODY_BYTES,
        )?;
        let reply: crate::source::SourceReply = self.decode(response)?;
        reply
            .validate_for(request)
            .map_err(ClientError::InvalidSourceResponse)?;
        Ok(reply)
    }

    /// Submit exactly one request. Transport ambiguity is returned to the
    /// caller, never converted into a retry. Reconcile with lookup before
    /// making any new execution decision.
    pub fn execution_request(
        &mut self,
        request: &crate::execution::ExecutionRequest,
    ) -> Result<crate::execution::ExecutionOutcome, ClientError> {
        let digest = request.digest()?;
        self.ensure_health()?;
        let capabilities = self.capabilities()?;
        let missing = if !capabilities
            .execution_request_contracts
            .iter()
            .any(|v| v == crate::execution::CONTRACT)
        {
            Some("executionRequestContracts")
        } else if !capabilities
            .execution_request_operations
            .iter()
            .any(|v| v == request.intent.capability())
        {
            Some("executionRequestOperations")
        } else {
            None
        };
        self.reject_missing_capability(missing)?;
        if self
            .negotiated
            .as_ref()
            .and_then(|n| n.execution_authority.as_ref())
            != Some(&request.authority)
        {
            return Err(ClientError::InvalidExecutionRequest(
                crate::execution::ContractError("authority"),
            ));
        }
        let body = request.canonical_bytes()?;
        let response = self.send_bound(
            "POST",
            "/api/v1/execution-requests",
            Some(&body),
            transport::MAX_RESPONSE_BODY_BYTES,
        )?;
        let outcome: crate::execution::ExecutionOutcome = self.decode(response)?;
        outcome
            .validate()
            .map_err(ClientError::InvalidExecutionResponse)?;
        if outcome.request_id != request.request_id
            || outcome.scope != request.scope
            || outcome.payload_digest != digest
        {
            return Err(ClientError::InvalidExecutionResponse(
                crate::execution::ContractError("response.binding"),
            ));
        }
        request
            .intent
            .target()
            .validate_outcome(&outcome)
            .map_err(ClientError::InvalidExecutionResponse)?;
        Ok(outcome)
    }

    pub fn lookup_execution_request(
        &mut self,
        scope: &crate::execution::ExecutionScope,
        request_id: &crate::execution::RequestId,
        expected_digest: &str,
        expected_target: &crate::execution::ExecutionTarget,
    ) -> Result<crate::execution::ExecutionOutcome, ClientError> {
        scope.validate()?;
        expected_target.validate()?;
        crate::execution::validate_digest(expected_digest)?;
        let outcome: crate::execution::ExecutionOutcome =
            self.get_json(ReadEndpoint::ExecutionRequest {
                project_id: scope.project_id.clone(),
                profile_id: scope.profile_id.clone(),
                request_id: request_id.clone(),
            })?;
        outcome
            .validate()
            .map_err(ClientError::InvalidExecutionResponse)?;
        if outcome.request_id != *request_id
            || outcome.scope != *scope
            || outcome.payload_digest != expected_digest
        {
            return Err(ClientError::InvalidExecutionResponse(
                crate::execution::ContractError("response.binding"),
            ));
        }
        expected_target
            .validate_outcome(&outcome)
            .map_err(ClientError::InvalidExecutionResponse)?;
        Ok(outcome)
    }

    pub fn new(endpoint: DaemonEndpoint) -> Self {
        Self {
            endpoint,
            negotiated: None,
        }
    }

    pub fn health(&mut self) -> Result<Health, ClientError> {
        // Clear any prior negotiation before every attempt: a failure below
        // must never leave `ensure_health` trusting a stale success from an
        // earlier call, which would let dependent requests bypass
        // revalidation against a now-incompatible daemon.
        self.negotiated = None;
        let transport = transport::request(&self.endpoint, "GET", HEALTH_PATH, None)?;
        let health: Health = self.decode(transport.response)?;
        if health.api_version != PROTOCOL_VERSION {
            return Err(ClientError::ProtocolVersion {
                expected: PROTOCOL_VERSION,
                actual: health.api_version,
            });
        }
        if !health.capabilities.structured_errors {
            return Err(ClientError::StructuredErrorsUnavailable);
        }
        if !health.ok {
            return Err(ClientError::HealthNotReady);
        }
        self.negotiated = Some(NegotiatedDaemon {
            capabilities: health.capabilities.clone(),
            peer_identity: transport.peer_identity,
            execution_authority: health.execution_authority.clone(),
        });
        Ok(health)
    }

    pub fn get_json<T: DeserializeOwned>(
        &mut self,
        endpoint: ReadEndpoint,
    ) -> Result<T, ClientError> {
        let path = read_path(&endpoint)?;
        self.ensure_health()?;
        self.require_read_capabilities(&endpoint)?;
        let response = self.send_bound("GET", &path, None, transport::MAX_RESPONSE_BODY_BYTES)?;
        self.decode(response)
    }

    pub fn post_json<T: DeserializeOwned, B: Serialize>(
        &mut self,
        endpoint: WriteEndpoint,
        body: &B,
    ) -> Result<T, ClientError> {
        self.ensure_health()?;
        self.require_write_capabilities(&endpoint)?;
        let path = write_path(endpoint)?;
        let body = serde_json::to_vec(body).map_err(ClientError::InvalidJson)?;
        let response = self.send_bound(
            "POST",
            &path,
            Some(&body),
            transport::MAX_RESPONSE_BODY_BYTES,
        )?;
        self.decode(response)
    }

    pub fn post_empty<B: Serialize>(
        &mut self,
        endpoint: WriteEndpoint,
        body: &B,
    ) -> Result<(), ClientError> {
        self.ensure_health()?;
        self.require_write_capabilities(&endpoint)?;
        let path = write_path(endpoint)?;
        let body = serde_json::to_vec(body).map_err(ClientError::InvalidJson)?;
        let response = self.send_bound(
            "POST",
            &path,
            Some(&body),
            transport::MAX_RESPONSE_BODY_BYTES,
        )?;
        if !(200..300).contains(&response.status) {
            return Err(daemon_error(response.status, response.body)?);
        }
        Ok(())
    }

    fn ensure_health(&mut self) -> Result<(), ClientError> {
        if self.negotiated.is_some() {
            Ok(())
        } else {
            self.health().map(|_| ())
        }
    }

    fn require_read_capabilities(&mut self, endpoint: &ReadEndpoint) -> Result<(), ClientError> {
        let missing = {
            let capabilities = self.capabilities()?;
            match endpoint {
                ReadEndpoint::ExecutionRequest { .. } => (!capabilities
                    .execution_request_contracts
                    .iter()
                    .any(|contract| contract == crate::execution::CONTRACT))
                .then_some("executionRequestContracts"),
                ReadEndpoint::Session { .. } | ReadEndpoint::Sessions { .. } => {
                    (!capabilities.sessions).then_some("sessions")
                }
                ReadEndpoint::Events { after_seq, .. } => {
                    if !capabilities.events {
                        Some("events")
                    } else if after_seq.is_some()
                        && capabilities.event_cursor.as_deref() != Some("sequence")
                    {
                        Some("eventCursor")
                    } else {
                        None
                    }
                }
            }
        };
        self.reject_missing_capability(missing)
    }

    fn require_write_capabilities(&mut self, endpoint: &WriteEndpoint) -> Result<(), ClientError> {
        let capabilities = self.capabilities()?;
        let _ = endpoint;
        let missing = (!capabilities.sessions).then_some("sessions");
        self.reject_missing_capability(missing)
    }

    fn reject_missing_capability(
        &mut self,
        missing: Option<&'static str>,
    ) -> Result<(), ClientError> {
        let Some(capability) = missing else {
            return Ok(());
        };
        let expected_peer = self
            .negotiated
            .as_ref()
            .ok_or(ClientError::HealthNotReady)?
            .peer_identity
            .clone();
        match transport::verify_peer(&self.endpoint, &expected_peer) {
            Ok(()) => Err(ClientError::CapabilityUnavailable { capability }),
            Err(error) => {
                if matches!(error, ClientError::DaemonInstanceChanged) {
                    self.negotiated = None;
                }
                Err(error)
            }
        }
    }

    fn capabilities(&self) -> Result<&HealthCapabilities, ClientError> {
        self.negotiated
            .as_ref()
            .map(|negotiated| &negotiated.capabilities)
            .ok_or(ClientError::HealthNotReady)
    }

    fn send_bound(
        &mut self,
        method: &'static str,
        path: &str,
        body: Option<&[u8]>,
        max_response_body_bytes: usize,
    ) -> Result<HttpResponse, ClientError> {
        let expected_peer = self
            .negotiated
            .as_ref()
            .ok_or(ClientError::HealthNotReady)?
            .peer_identity
            .clone();
        match transport::request_bound(
            &self.endpoint,
            method,
            path,
            body,
            &expected_peer,
            max_response_body_bytes,
        ) {
            Ok(response) => Ok(response.response),
            Err(error) => {
                if matches!(error, ClientError::DaemonInstanceChanged) {
                    self.negotiated = None;
                }
                Err(error)
            }
        }
    }

    fn decode<T: DeserializeOwned>(&self, response: HttpResponse) -> Result<T, ClientError> {
        if !(200..300).contains(&response.status) {
            return Err(daemon_error(response.status, response.body)?);
        }
        serde_json::from_slice(&response.body).map_err(ClientError::InvalidJson)
    }
}

impl crate::terminal_control::TerminalControlTransport for DaemonClient {
    fn post_terminal_control(&mut self, body: &[u8]) -> Result<Vec<u8>, ClientError> {
        if !self.endpoint.is_owner_local() {
            return Err(ClientError::InvalidHttpResponse(
                "terminal control requires owner-local IPC".to_owned(),
            ));
        }
        if body.len() > crate::terminal_control::MAX_TERMINAL_CONTROL_BODY_BYTES {
            return Err(ClientError::RequestTooLarge {
                max_bytes: crate::terminal_control::MAX_TERMINAL_CONTROL_BODY_BYTES,
                actual_bytes: body.len(),
            });
        }
        self.ensure_health()?;
        let response = self.send_bound(
            "POST",
            crate::terminal_control::TERMINAL_CONTROL_PATH,
            Some(body),
            crate::terminal_control::MAX_TERMINAL_CONTROL_RESPONSE_BYTES,
        )?;
        if !(200..300).contains(&response.status) {
            return Err(daemon_error(response.status, response.body)?);
        }
        Ok(response.body)
    }
}

fn daemon_error(status: u16, body: Vec<u8>) -> Result<ClientError, ClientError> {
    let Ok(body) = String::from_utf8(body) else {
        return Ok(ClientError::HttpStatus(status));
    };
    let Ok(value) = serde_json::from_str::<Value>(&body) else {
        return Ok(ClientError::HttpStatus(status));
    };
    let Some(error) = value.get("error") else {
        return Ok(ClientError::HttpStatus(status));
    };
    let Some(code) = error.get("code").and_then(Value::as_str) else {
        return Ok(ClientError::HttpStatus(status));
    };
    let Some(message) = error.get("message").and_then(Value::as_str) else {
        return Ok(ClientError::HttpStatus(status));
    };
    Ok(ClientError::Daemon {
        status,
        error: DaemonError {
            code: code.to_owned(),
            message: message.to_owned(),
            details: error.get("details").cloned().unwrap_or(Value::Null),
        },
    })
}

fn read_path(endpoint: &ReadEndpoint) -> Result<String, ClientError> {
    match endpoint {
        ReadEndpoint::ExecutionRequest {
            project_id,
            profile_id,
            request_id,
        } => Ok(format!(
            "/api/v1/execution-requests/{}/{}/{}",
            project_id.as_str(),
            profile_id.as_str(),
            request_id.as_str()
        )),
        ReadEndpoint::Session { session_id } => {
            validate_session_path_remainder(session_id)?;
            if session_id_collides_with_reserved_detail_route(session_id) {
                return Err(ClientError::InvalidRouteParameter(
                    RESERVED_SESSION_DETAIL_ROUTE_ERROR,
                ));
            }
            Ok(format!("/api/v1/sessions/{session_id}"))
        }
        ReadEndpoint::Sessions {
            limit,
            cursor,
            include_archived,
        } => {
            let mut path = "/api/v1/sessions".to_owned();
            let mut separator = '?';
            if let Some(limit) = limit {
                path.push_str(&format!("{separator}limit={limit}"));
                separator = '&';
            }
            if let Some(cursor) = cursor {
                validate_session_page_cursor(cursor)?;
                path.push_str(&format!("{separator}cursor={cursor}"));
                separator = '&';
            }
            if *include_archived {
                path.push_str(&format!("{separator}includeArchived=true"));
            }
            Ok(path)
        }
        ReadEndpoint::Events {
            session_id,
            after_seq,
            limit,
        } => {
            validate_session_query_value(session_id)?;
            let mut path = format!("/api/v1/events?sessionId={session_id}");
            if let Some(after_seq) = after_seq {
                path.push_str(&format!("&afterSeq={after_seq}"));
            }
            if let Some(limit) = limit {
                path.push_str(&format!("&limit={limit}"));
            }
            Ok(path)
        }
    }
}

fn session_id_collides_with_reserved_detail_route(session_id: &str) -> bool {
    const RESERVED_SUFFIXES: [&str; 3] = ["events", "log", "handoffs"];

    RESERVED_SUFFIXES.contains(&session_id.rsplit('/').next().unwrap_or_default())
        || session_id.starts_with("artifacts/")
        || session_id.contains("/artifacts/")
}

fn write_path(endpoint: WriteEndpoint) -> Result<String, ClientError> {
    match endpoint {
        WriteEndpoint::Sessions => Ok("/api/v1/sessions".to_owned()),
        WriteEndpoint::SessionInput { session_id } => {
            validate_session_path_remainder(&session_id)?;
            Ok(format!("/api/v1/sessions/{session_id}/input"))
        }
        WriteEndpoint::SessionKill { session_id } => {
            validate_session_path_remainder(&session_id)?;
            Ok(format!("/api/v1/sessions/{session_id}/kill"))
        }
    }
}

fn validate_session_path_remainder(value: &str) -> Result<(), ClientError> {
    validate_session_request_target_data(value)?;
    if value.contains('?') {
        return Err(ClientError::InvalidRouteParameter("session id"));
    }
    Ok(())
}

/// Reject anything that is not the daemon's own session page cursor.
///
/// The daemon issues URL-safe base64 without padding, so a well-formed cursor
/// is already representable verbatim in the inherited raw `v1` query. Checking
/// the alphabet keeps a corrupted or hand-composed value from smuggling `&`,
/// `#`, or whitespace into the request target.
fn validate_session_page_cursor(value: &str) -> Result<(), ClientError> {
    if value.is_empty()
        || !value
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_'))
    {
        return Err(ClientError::InvalidRouteParameter(
            "session page cursor is not the daemon's URL-safe base64 cursor",
        ));
    }
    Ok(())
}

fn validate_session_query_value(value: &str) -> Result<(), ClientError> {
    validate_session_request_target_data(value)?;
    if value.contains('&') {
        return Err(ClientError::InvalidRouteParameter(
            "session id is ambiguous in the inherited v1 events query",
        ));
    }
    Ok(())
}

fn validate_session_request_target_data(value: &str) -> Result<(), ClientError> {
    if value.is_empty()
        || value
            .chars()
            .any(|character| character.is_whitespace() || character.is_control())
    {
        return Err(ClientError::InvalidRouteParameter("session id"));
    }
    Ok(())
}

impl DaemonClient {
    /// Read the owner-local managed execution catalog without launch, input,
    /// kill, or storage-initialization side effects.
    pub fn read_execution_catalog(
        &mut self,
        request: &crate::catalog::CatalogRead,
    ) -> Result<crate::catalog::CatalogReply, ClientError> {
        request
            .validate()
            .map_err(ClientError::InvalidCatalogRequest)?;
        self.ensure_health()?;
        let missing = (!self
            .capabilities()?
            .execution_catalog_contracts
            .iter()
            .any(|contract| contract == crate::catalog::CONTRACT))
        .then_some("executionCatalogContracts");
        self.reject_missing_capability(missing)?;

        // A catalog request carries an expected current authority. Health is
        // only a negotiation observation, so the daemon repeats this check at
        // its read boundary; the client rejects an obvious stale/mismatched
        // request before sending bytes.
        if self
            .negotiated
            .as_ref()
            .and_then(|negotiated| negotiated.execution_authority.as_ref())
            != Some(&request.authority)
        {
            return Err(ClientError::InvalidCatalogRequest(
                crate::execution::ContractError("authority"),
            ));
        }

        let body = serde_json::to_vec(request).map_err(ClientError::InvalidJson)?;
        if body.len() > crate::catalog::MAX_READ_BYTES {
            return Err(ClientError::RequestTooLarge {
                max_bytes: crate::catalog::MAX_READ_BYTES,
                actual_bytes: body.len(),
            });
        }
        let response = self.send_bound(
            "POST",
            "/api/v1/execution-catalog/read",
            Some(&body),
            transport::MAX_RESPONSE_BODY_BYTES,
        )?;
        let reply: crate::catalog::CatalogReply = self.decode(response)?;
        reply
            .validate_for(request)
            .map_err(ClientError::InvalidCatalogResponse)?;
        Ok(reply)
    }
}

impl DaemonClient {
    /// Read public bounded output without changing execution or legacy source contracts.
    pub fn read_execution_transcript(
        &mut self,
        request: &crate::transcript::TranscriptRead,
    ) -> Result<crate::transcript::TranscriptReply, ClientError> {
        request
            .validate()
            .map_err(ClientError::InvalidTranscriptRequest)?;
        self.ensure_health()?;
        let missing = (!self
            .capabilities()?
            .execution_transcript_contracts
            .iter()
            .any(|contract| contract == crate::transcript::CONTRACT))
        .then_some("executionTranscriptContracts");
        self.reject_missing_capability(missing)?;

        // A transcript request carries an expected current authority. Health is
        // only a negotiation observation, so the daemon repeats this check at
        // its read boundary; the client rejects an obvious stale/mismatched
        // request before sending bytes.
        if self
            .negotiated
            .as_ref()
            .and_then(|negotiated| negotiated.execution_authority.as_ref())
            != Some(&request.authority)
        {
            return Err(ClientError::InvalidTranscriptRequest(
                crate::execution::ContractError("authority"),
            ));
        }

        let body = serde_json::to_vec(request).map_err(ClientError::InvalidJson)?;
        if body.len() > crate::transcript::MAX_READ_BYTES {
            return Err(ClientError::RequestTooLarge {
                max_bytes: crate::transcript::MAX_READ_BYTES,
                actual_bytes: body.len(),
            });
        }
        let response = self.send_bound(
            "POST",
            "/api/v1/execution-transcript/read",
            Some(&body),
            transport::MAX_RESPONSE_BODY_BYTES,
        )?;
        let reply: crate::transcript::TranscriptReply = self.decode(response)?;
        reply
            .validate_for(request)
            .map_err(ClientError::InvalidTranscriptResponse)?;
        Ok(reply)
    }
}

#[cfg(test)]
mod tests {
    use super::{read_path, write_path, RESERVED_SESSION_DETAIL_ROUTE_ERROR};
    use crate::models::{ReadEndpoint, WriteEndpoint};
    use crate::ClientError;

    #[test]
    fn session_routes_preserve_the_v1_raw_remainder() {
        let cases = [
            ("engine/42", "engine/42"),
            ("engine:42", "engine:42"),
            (r"engine\42", r"engine\42"),
            ("engine%3A42", "engine%3A42"),
            ("engine%2F42", "engine%2F42"),
            (".", "."),
            ("..", ".."),
        ];

        for (session_id, raw) in cases {
            assert_eq!(
                read_path(&ReadEndpoint::Session {
                    session_id: session_id.to_owned(),
                })
                .expect("contract-valid external session id"),
                format!("/api/v1/sessions/{raw}")
            );
            assert_eq!(
                write_path(WriteEndpoint::SessionInput {
                    session_id: session_id.to_owned(),
                })
                .expect("contract-valid external session id"),
                format!("/api/v1/sessions/{raw}/input")
            );
        }
    }

    #[test]
    fn session_listing_selects_the_inherited_shape_it_is_asked_for() {
        let cases = [
            (None, None, false, "/api/v1/sessions"),
            (Some(50), None, false, "/api/v1/sessions?limit=50"),
            (
                None,
                Some("Y3Vyc29y"),
                false,
                "/api/v1/sessions?cursor=Y3Vyc29y",
            ),
            (None, None, true, "/api/v1/sessions?includeArchived=true"),
            (
                Some(50),
                Some("Y3Vyc29y"),
                true,
                "/api/v1/sessions?limit=50&cursor=Y3Vyc29y&includeArchived=true",
            ),
            (
                None,
                Some("Y3Vyc29y"),
                true,
                "/api/v1/sessions?cursor=Y3Vyc29y&includeArchived=true",
            ),
            (
                Some(50),
                None,
                true,
                "/api/v1/sessions?limit=50&includeArchived=true",
            ),
        ];

        for (limit, cursor, include_archived, expected) in cases {
            assert_eq!(
                read_path(&ReadEndpoint::Sessions {
                    limit,
                    cursor: cursor.map(str::to_owned),
                    include_archived,
                })
                .expect("contract-valid session listing"),
                expected
            );
        }
    }

    #[test]
    fn session_listing_rejects_a_cursor_outside_the_daemon_alphabet() {
        for cursor in [
            "",
            "cursor value",
            "cursor&limit=999",
            "cursor#fragment",
            "cursor?other=route",
            "Y3Vyc29y=",
            "cursor/slash",
            "cursor+plus",
            "cursor\nnewline",
        ] {
            let error = read_path(&ReadEndpoint::Sessions {
                limit: None,
                cursor: Some(cursor.to_owned()),
                include_archived: false,
            })
            .expect_err("unsafe session page cursor was accepted");

            assert!(
                matches!(error, ClientError::InvalidRouteParameter(_)),
                "unexpected error for {cursor:?}: {error}"
            );
        }
    }

    #[test]
    fn events_query_preserves_representable_v1_bytes() {
        assert_eq!(
            read_path(&ReadEndpoint::Events {
                session_id: "engine/42%2Fraw+value?literal".to_owned(),
                after_seq: Some(7),
                limit: Some(10),
            })
            .expect("representable raw v1 query value"),
            "/api/v1/events?sessionId=engine/42%2Fraw+value?literal&afterSeq=7&limit=10"
        );
    }

    #[test]
    fn session_routes_reject_only_bytes_the_inherited_wire_cannot_represent() {
        for session_id in ["", "engine 42", "engine\n42", "engine?other=route"] {
            assert!(
                read_path(&ReadEndpoint::Session {
                    session_id: session_id.to_owned(),
                })
                .is_err(),
                "unsafe session id was accepted: {session_id:?}"
            );
            assert!(
                write_path(WriteEndpoint::SessionKill {
                    session_id: session_id.to_owned(),
                })
                .is_err(),
                "unsafe session id was accepted: {session_id:?}"
            );
        }

        assert!(
            read_path(&ReadEndpoint::Events {
                session_id: "engine&limit=999".to_owned(),
                after_seq: None,
                limit: None,
            })
            .is_err(),
            "the raw v1 query grammar cannot represent ampersands in sessionId"
        );
    }

    #[test]
    fn session_detail_rejects_every_reserved_root_and_nested_collision() {
        for session_id in [
            "events",
            "engine/events",
            "/events",
            "log",
            "engine/log",
            "/log",
            "handoffs",
            "engine/handoffs",
            "/handoffs",
            "artifacts/item",
            "artifacts/",
            "engine/artifacts/item",
            "engine/artifacts/",
            "/artifacts/item",
        ] {
            let error = read_path(&ReadEndpoint::Session {
                session_id: session_id.to_owned(),
            })
            .expect_err("reserved BASE nested route must not become a detail endpoint");

            assert!(
                matches!(
                    error,
                    ClientError::InvalidRouteParameter(message)
                        if message == RESERVED_SESSION_DETAIL_ROUTE_ERROR
                ),
                "unexpected error for {session_id:?}: {error}"
            );
        }
    }

    #[test]
    fn session_detail_accepts_reserved_route_near_misses_and_method_only_actions() {
        for session_id in [
            "event",
            "events/item",
            "engine/events/item",
            "logs",
            "log/item",
            "engine/log/item",
            "handoff",
            "handoffs/item",
            "engine/handoffs/item",
            "artifacts",
            "engine/artifacts",
            "artifact/item",
            "artifacts-item/child",
            "input",
            "engine/input",
            "kill",
            "engine/kill",
            "complete",
            "engine/complete",
            "claim",
            "engine/claim",
            "ack",
            "engine/ack",
            "continuations",
            "engine/continuations",
            "external",
        ] {
            assert_eq!(
                read_path(&ReadEndpoint::Session {
                    session_id: session_id.to_owned(),
                })
                .expect("near-miss remains a reachable BASE detail route"),
                format!("/api/v1/sessions/{session_id}"),
                "unexpected path for {session_id:?}"
            );
        }
    }
}
