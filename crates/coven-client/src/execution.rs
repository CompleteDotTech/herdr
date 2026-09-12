//! Versioned owner-local execution requests, distinct from legacy session IO
//! and proof-bearing request adoption. A type definition is not a capability;
//! callers must negotiate [`CONTRACT`] with the actual daemon instance.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const CONTRACT: &str = "coven.execution-request.v2";
pub const MAX_PROMPT_BYTES: usize = 64 * 1024;
pub const MAX_REQUEST_BYTES: usize = 512 * 1024;
const DIGEST_DOMAIN: &[u8] = b"coven.execution-request.v2\0";

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("invalid execution request field: {0}")]
pub struct ContractError(pub &'static str);

macro_rules! scoped_id {
    ($name:ident) => {
        #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(try_from = "String", into = "String")]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, ContractError> {
                let value = value.into();
                if value.is_empty()
                    || value.len() > 128
                    || !value
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
                {
                    return Err(ContractError(stringify!($name)));
                }
                Ok(Self(value))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl TryFrom<String> for $name {
            type Error = ContractError;
            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl From<$name> for String {
            fn from(value: $name) -> Self {
                value.0
            }
        }
    };
}

scoped_id!(RequestId);
scoped_id!(ProjectId);
scoped_id!(ProfileId);
scoped_id!(AuthorityId);

/// Durable execution domain, distinct from transport peer and source cursor.
/// Possession of this binding does not grant permission to execute.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "AuthorityWire")]
pub struct ExecutionAuthority {
    pub id: AuthorityId,
    pub generation: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AuthorityWire {
    id: AuthorityId,
    generation: u64,
}

impl ExecutionAuthority {
    pub fn new(id: AuthorityId, generation: u64) -> Result<Self, ContractError> {
        if generation == 0 || generation > i64::MAX as u64 {
            return Err(ContractError("authorityGeneration"));
        }
        Ok(Self { id, generation })
    }
}

impl TryFrom<AuthorityWire> for ExecutionAuthority {
    type Error = ContractError;

    fn try_from(value: AuthorityWire) -> Result<Self, Self::Error> {
        Self::new(value.id, value.generation)
    }
}

/// A session reference is body data, not a URL segment. Preserve the inherited
/// daemon's unusual identifiers instead of normalizing slashes or `%` bytes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ExecutionSessionId(String);

impl ExecutionSessionId {
    pub fn new(value: impl Into<String>) -> Result<Self, ContractError> {
        let value = value.into();
        if value.is_empty() || value.len() > 512 || value.chars().any(char::is_control) {
            return Err(ContractError("sessionId"));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for ExecutionSessionId {
    type Error = ContractError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<ExecutionSessionId> for String {
    fn from(value: ExecutionSessionId) -> Self {
        value.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ExecutionScope {
    pub project_id: ProjectId,
    pub profile_id: ProfileId,
    pub policy_generation: u64,
}

impl ExecutionScope {
    pub fn validate(&self) -> Result<(), ContractError> {
        if self.policy_generation == 0 || self.policy_generation > i64::MAX as u64 {
            return Err(ContractError("policyGeneration"));
        }
        Ok(())
    }
}

pub fn validate_digest(value: &str) -> Result<(), ContractError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(ContractError("payloadDigest"));
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionMode {
    Transcript,
    Terminal,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionHarness {
    Codex,
    Claude,
    Copilot,
}

impl ExecutionHarness {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Codex => "codex",
            Self::Claude => "claude",
            Self::Copilot => "copilot",
        }
    }
}

// Intentionally no Debug on prompt-bearing types. Diagnostics must not expose
// raw request bodies through a derived error or debug log.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "snake_case",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum ExecutionIntent {
    Launch {
        harness: ExecutionHarness,
        mode: ExecutionMode,
        model: Option<String>,
        cwd: String,
        prompt: String,
    },
    Turn {
        session_id: ExecutionSessionId,
        generation: u64,
        prompt: String,
    },
    Continue {
        session_id: ExecutionSessionId,
        generation: u64,
        prompt: String,
    },
    Stop {
        session_id: ExecutionSessionId,
        generation: u64,
    },
}

/// Expected execution correlation, retained without a raw prompt for lookup.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "snake_case",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum ExecutionTarget {
    Launch,
    Existing {
        session_id: ExecutionSessionId,
        generation: u64,
    },
    Continuation {
        previous_session_id: ExecutionSessionId,
        previous_generation: u64,
    },
}

impl ExecutionTarget {
    pub fn validate(&self) -> Result<(), ContractError> {
        let valid = match self {
            Self::Launch => true,
            Self::Existing { generation, .. } => *generation > 0 && *generation <= i64::MAX as u64,
            Self::Continuation {
                previous_generation,
                ..
            } => *previous_generation > 0 && *previous_generation < i64::MAX as u64,
        };
        if !valid {
            return Err(ContractError("expectedGeneration"));
        }
        Ok(())
    }

    pub fn validate_outcome(&self, outcome: &ExecutionOutcome) -> Result<(), ContractError> {
        self.validate()?;
        let bound = match self {
            Self::Launch => outcome.generation == 1 && outcome.parent_session_id.is_none(),
            Self::Existing {
                session_id,
                generation,
            } => outcome.session_id == *session_id && outcome.generation == *generation,
            Self::Continuation {
                previous_session_id,
                previous_generation,
            } => {
                outcome.session_id != *previous_session_id
                    && outcome.parent_session_id.as_ref() == Some(previous_session_id)
                    && outcome.generation == previous_generation + 1
            }
        };
        if !bound {
            return Err(ContractError("response.executionBinding"));
        }
        Ok(())
    }
}

impl ExecutionIntent {
    pub fn target(&self) -> ExecutionTarget {
        match self {
            Self::Launch { .. } => ExecutionTarget::Launch,
            Self::Turn {
                session_id,
                generation,
                ..
            }
            | Self::Stop {
                session_id,
                generation,
            } => ExecutionTarget::Existing {
                session_id: session_id.clone(),
                generation: *generation,
            },
            Self::Continue {
                session_id,
                generation,
                ..
            } => ExecutionTarget::Continuation {
                previous_session_id: session_id.clone(),
                previous_generation: *generation,
            },
        }
    }

    pub fn capability(&self) -> &'static str {
        match self {
            Self::Launch {
                mode: ExecutionMode::Transcript,
                ..
            } => "launch_transcript",
            Self::Launch {
                mode: ExecutionMode::Terminal,
                ..
            } => "launch_terminal",
            Self::Turn { .. } => "turn",
            Self::Continue { .. } => "continue",
            Self::Stop { .. } => "stop",
        }
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ExecutionRequest {
    pub contract: String,
    pub request_id: RequestId,
    pub scope: ExecutionScope,
    pub authority: ExecutionAuthority,
    /// Unix seconds. Only a new admission is subject to expiry; replay/lookup
    /// of an accepted request remains possible after its original expiry.
    pub expires_at: i64,
    pub intent: ExecutionIntent,
}

impl ExecutionRequest {
    pub fn validate(&self) -> Result<(), ContractError> {
        if self.contract != CONTRACT {
            return Err(ContractError("contract"));
        }
        self.scope.validate()?;
        ExecutionAuthority::new(self.authority.id.clone(), self.authority.generation)?;
        self.intent.target().validate()?;
        if self.expires_at <= 0 {
            return Err(ContractError("expiresAt"));
        }
        let prompt = match &self.intent {
            ExecutionIntent::Launch {
                cwd, prompt, model, ..
            } => {
                if cwd.is_empty() || cwd.len() > 4096 || cwd.contains('\0') {
                    return Err(ContractError("cwd"));
                }
                if model.as_ref().is_some_and(|m| {
                    m.is_empty()
                        || m.trim() != m
                        || m.len() > 256
                        || m.chars().any(char::is_control)
                }) {
                    return Err(ContractError("model"));
                }
                Some(prompt)
            }
            ExecutionIntent::Turn {
                generation, prompt, ..
            }
            | ExecutionIntent::Continue {
                generation, prompt, ..
            } => {
                if *generation == 0 || *generation > i64::MAX as u64 {
                    return Err(ContractError("generation"));
                }
                Some(prompt)
            }
            ExecutionIntent::Stop { generation, .. } => {
                if *generation == 0 || *generation > i64::MAX as u64 {
                    return Err(ContractError("generation"));
                }
                None
            }
        };
        if prompt
            .is_some_and(|p| p.trim().is_empty() || p.len() > MAX_PROMPT_BYTES || p.contains('\0'))
        {
            return Err(ContractError("prompt"));
        }
        Ok(())
    }

    /// Only new admission uses this bound. An exact retained replay remains
    /// valid after expiry and must be checked before calling this method.
    pub fn validate_new_admission(&self, now: i64) -> Result<(), ContractError> {
        self.validate()?;
        if self.expires_at <= now || self.expires_at > now.saturating_add(300) {
            return Err(ContractError("expiresAt"));
        }
        Ok(())
    }

    /// Canonical v2 representation: fixed struct/variant field order, explicit
    /// null for optional model, integer timestamps, unchanged UTF-8 strings.
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, ContractError> {
        self.validate()?;
        let bytes = serde_json::to_vec(self).map_err(|_| ContractError("serialization"))?;
        if bytes.len() > MAX_REQUEST_BYTES {
            return Err(ContractError("requestBytes"));
        }
        Ok(bytes)
    }

    pub fn digest(&self) -> Result<String, ContractError> {
        let mut digest = Sha256::new();
        digest.update(DIGEST_DOMAIN);
        digest.update(self.canonical_bytes()?);
        Ok(digest
            .finalize()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestState {
    Admitted,
    Establishing,
    Running,
    OutcomeKnown,
    Unresolved,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryState {
    NotAttempted,
    Dispatched,
    Unknown,
}

/// Contains no prompt, native harness resume identity, credentials or guessed
/// PID. `session_status` is a separate observation, not an operation result.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ExecutionOutcome {
    pub contract: String,
    pub request_id: RequestId,
    pub scope: ExecutionScope,
    pub payload_digest: String,
    pub session_id: ExecutionSessionId,
    /// Authoritative execution lineage. Continuation must name its parent;
    /// this is never a native harness resume identifier.
    pub parent_session_id: Option<ExecutionSessionId>,
    pub generation: u64,
    pub state: RequestState,
    pub delivery: DeliveryState,
    pub session_status: Option<String>,
    pub exit_code: Option<i32>,
}

impl ExecutionOutcome {
    pub fn validate(&self) -> Result<(), ContractError> {
        if self.contract != CONTRACT {
            return Err(ContractError("response.contract"));
        }
        self.scope.validate()?;
        if self.generation == 0 || self.generation > i64::MAX as u64 {
            return Err(ContractError("response.generation"));
        }
        validate_digest(&self.payload_digest)?;
        let valid_delivery = match self.state {
            RequestState::Admitted | RequestState::Establishing => {
                self.delivery == DeliveryState::NotAttempted
            }
            RequestState::Running => matches!(
                self.delivery,
                DeliveryState::Dispatched | DeliveryState::Unknown
            ),
            RequestState::OutcomeKnown => self.delivery == DeliveryState::Dispatched,
            RequestState::Unresolved => matches!(
                self.delivery,
                DeliveryState::Unknown | DeliveryState::NotAttempted
            ),
        };
        if !valid_delivery {
            return Err(ContractError("response.delivery"));
        }
        if self.session_status.as_ref().is_some_and(|status| {
            !matches!(
                status.as_str(),
                "created"
                    | "starting"
                    | "running"
                    | "idle"
                    | "completed"
                    | "failed"
                    | "cancelled"
                    | "killed"
                    | "orphaned"
            )
        }) {
            return Err(ContractError("response.sessionStatus"));
        }
        if self.state == RequestState::OutcomeKnown && self.exit_code.is_none() {
            return Err(ContractError("response.exitCode"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request() -> ExecutionRequest {
        serde_json::from_str(r#"{"contract":"coven.execution-request.v2","requestId":"r1","scope":{"projectId":"p1","profileId":"local","policyGeneration":1},"authority":{"id":"authority-fixture","generation":1},"expiresAt":2000000000,"intent":{"kind":"launch","harness":"codex","mode":"transcript","model":null,"cwd":".","prompt":"hi"}}"#).unwrap()
    }

    #[test]
    fn ids_reject_route_injection_but_preserve_session_references() {
        for bad in ["", "..", "a/b", "a?x=y", "a%2fb", "a\r\n", "a b"] {
            assert!(RequestId::new(bad).is_err());
        }
        let id = ExecutionSessionId::new("engine/%2F42").unwrap();
        assert_eq!(id.as_str(), "engine/%2F42");
    }

    #[test]
    fn digest_is_order_independent_on_input_but_binds_every_semantic_field() {
        let original = request();
        assert_eq!(
            original.digest().unwrap(),
            "16c94c2c4ebea38170fc849eb6c956a21af1f86600747896e4c1062f3d6cab70"
        );
        let reordered: ExecutionRequest =
            serde_json::from_value(serde_json::to_value(&original).unwrap()).unwrap();
        assert_eq!(original.digest().unwrap(), reordered.digest().unwrap());
        let mut changed = original.clone();
        changed.scope.policy_generation += 1;
        assert_ne!(original.digest().unwrap(), changed.digest().unwrap());
        let mut changed = original.clone();
        if let ExecutionIntent::Launch { prompt, .. } = &mut changed.intent {
            prompt.push(' ');
        }
        assert_ne!(original.digest().unwrap(), changed.digest().unwrap());
    }

    #[test]
    fn unknown_fields_and_noninteger_validity_fail_closed() {
        let mut value = serde_json::to_value(request()).unwrap();
        value["intent"]["env"] = serde_json::json!({"TOKEN":"synthetic"});
        assert!(serde_json::from_value::<ExecutionRequest>(value).is_err());
        let mut value = serde_json::to_value(request()).unwrap();
        value["expiresAt"] = serde_json::json!(2.5);
        assert!(serde_json::from_value::<ExecutionRequest>(value).is_err());
    }

    #[test]
    fn prompt_and_generation_bounds_are_enforced_before_serialization() {
        let mut request = request();
        if let ExecutionIntent::Launch { prompt, .. } = &mut request.intent {
            *prompt = "x".repeat(MAX_PROMPT_BYTES + 1);
        }
        assert!(request.canonical_bytes().is_err());
        request.intent = ExecutionIntent::Stop {
            session_id: ExecutionSessionId::new("s1").unwrap(),
            generation: 0,
        };
        assert!(request.digest().is_err());
    }
    #[test]
    fn outcome_rejects_impossible_delivery_and_unbounded_generations() {
        let mut outcome: ExecutionOutcome = serde_json::from_value(serde_json::json!({
            "contract":CONTRACT, "requestId":"r1", "scope":request().scope,
            "payloadDigest":request().digest().unwrap(), "sessionId":"s1", "generation":1,
            "state":"admitted", "delivery":"dispatched", "sessionStatus":"created", "exitCode":null
        }))
        .unwrap();
        assert!(outcome.validate().is_err());
        outcome.delivery = DeliveryState::NotAttempted;
        assert!(outcome.validate().is_ok());
        outcome.scope.policy_generation = u64::MAX;
        assert!(outcome.validate().is_err());
        outcome.scope.policy_generation = 1;
        outcome.session_status = Some("invented_status".to_owned());
        assert!(outcome.validate().is_err());
    }
    #[test]
    fn continuation_requires_a_new_execution_at_the_next_generation() {
        let mut outcome: ExecutionOutcome = serde_json::from_value(serde_json::json!({
            "contract":CONTRACT,"requestId":"r1","scope":request().scope,
            "payloadDigest":request().digest().unwrap(),"sessionId":"previous","generation":4,
            "state":"admitted","delivery":"not_attempted","sessionStatus":"created","exitCode":null
        }))
        .unwrap();
        let target = ExecutionTarget::Continuation {
            previous_session_id: ExecutionSessionId::new("previous").unwrap(),
            previous_generation: 3,
        };
        assert!(target.validate_outcome(&outcome).is_err());
        outcome.session_id = ExecutionSessionId::new("new-execution").unwrap();
        assert!(target.validate_outcome(&outcome).is_err());
        outcome.parent_session_id = Some(ExecutionSessionId::new("unrelated").unwrap());
        assert!(target.validate_outcome(&outcome).is_err());
        outcome.parent_session_id = Some(ExecutionSessionId::new("previous").unwrap());
        assert!(target.validate_outcome(&outcome).is_ok());
        outcome.generation = 3;
        assert!(target.validate_outcome(&outcome).is_err());
    }
    #[test]
    fn model_selectors_reject_hidden_whitespace_normalization() {
        let mut request = request();
        if let ExecutionIntent::Launch { model, .. } = &mut request.intent {
            *model = Some(" gpt ".to_owned());
        }
        assert!(request.validate().is_err());
    }
    #[test]
    fn authority_identity_and_generation_are_both_digest_bound() {
        let original = request();
        let mut changed = original.clone();
        changed.authority.id = AuthorityId::new("another-authority").unwrap();
        assert_ne!(changed.digest().unwrap(), original.digest().unwrap());
        changed = original.clone();
        changed.authority.generation += 1;
        assert_ne!(changed.digest().unwrap(), original.digest().unwrap());
        let mut legacy = serde_json::to_value(original).unwrap();
        legacy.as_object_mut().unwrap().remove("authority");
        legacy["contract"] = serde_json::json!("coven.execution-request.v1");
        assert!(serde_json::from_value::<ExecutionRequest>(legacy).is_err());
    }
}
