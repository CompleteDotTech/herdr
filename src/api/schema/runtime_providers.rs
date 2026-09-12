use serde::{Deserialize, Serialize};

/// Selects one owner-configured provider instance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RuntimeProviderTarget {
    pub provider_id: String,
}

/// Requests an owner-configured provider execution to be attached to a Herdr terminal.
///
/// The provider id selects trusted owner-local configuration; the request does not carry a
/// daemon path, executable, environment, or credential material.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RuntimeProviderAttachParams {
    pub provider_id: String,
    pub session_id: String,
    pub generation: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_id: Option<String>,
    #[serde(default)]
    pub focus: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
}

/// Selects a provider attachment by its durable Herdr terminal id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RuntimeProviderAttachmentTarget {
    pub terminal_id: String,
}

/// A permission-bearing execution request. Provider scope and authority are
/// selected from owner-local configuration; callers provide only the typed
/// intent payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RuntimeProviderExecuteParams {
    pub provider_id: String,
    pub request_id: String,
    pub expires_at: i64,
    pub intent: RuntimeProviderExecutionIntent,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(
    tag = "kind",
    rename_all = "snake_case",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum RuntimeProviderExecutionIntent {
    Launch {
        harness: RuntimeProviderHarness,
        mode: RuntimeProviderMode,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        model: Option<String>,
        cwd: String,
        prompt: String,
    },
    Turn {
        terminal_id: String,
        prompt: String,
    },
    Continue {
        terminal_id: String,
        prompt: String,
    },
    Stop {
        terminal_id: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeProviderHarness {
    Codex,
    Claude,
    Copilot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeProviderMode {
    Transcript,
    Terminal,
}

/// Reconcile a request after an uncertain response or Herdr restart. The
/// expected digest and target are required so an old request cannot be
/// mistaken for a different execution with the same request id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RuntimeProviderOperationGetParams {
    pub provider_id: String,
    pub request_id: String,
    pub payload_digest: String,
    pub expected_target: RuntimeProviderExecutionTarget,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(
    tag = "kind",
    rename_all = "snake_case",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum RuntimeProviderExecutionTarget {
    Launch,
    Existing {
        session_id: String,
        generation: u64,
    },
    Continuation {
        previous_session_id: String,
        previous_generation: u64,
    },
}

/// Prompt-free operation status. `status` is local observation state while the
/// optional outcome fields retain the daemon's independently validated result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct RuntimeProviderOperationInfo {
    pub provider_id: String,
    pub request_id: String,
    pub ticket: u64,
    pub payload_digest: String,
    pub target: String,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivery: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generation: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostic: Option<String>,
}

/// One configured owner-local provider and the capabilities exposed by its current projection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct RuntimeProviderInfo {
    pub provider_id: String,
    pub host_id: String,
    pub project_id: String,
    pub profile_id: String,
    pub policy_generation: u64,
    pub authority_id: String,
    pub authority_generation: u64,
    pub enabled: bool,
    pub capabilities: Vec<String>,
}

/// A provider-backed terminal attachment projection.
///
/// Lifecycle, activity, connection, and durability remain independent axes. They are strings at
/// this API boundary so a future server can add values without changing the endpoint codec.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ProviderAttachmentInfo {
    pub terminal_id: String,
    pub provider_id: String,
    pub host_id: String,
    pub project_id: String,
    pub profile_id: String,
    pub session_id: String,
    pub generation: u64,
    pub attached: bool,
    pub lifecycle: String,
    pub activity: String,
    pub connection: String,
    pub durability: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostic: Option<String>,
    /// Age of the last validated source reply in this Herdr process.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observation_age_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observation_source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_ledger_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_epoch: Option<u64>,
}
