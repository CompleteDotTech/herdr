//! Owner-local managed execution catalog wire contract.

use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::{
    execution::{
        ContractError, ExecutionAuthority, ExecutionHarness, ExecutionMode, ExecutionScope,
        ExecutionSessionId,
    },
    source::{SourceHealth, SourceId, SourceIdentity, SourceLifecycle},
};

/// Versioned owner-local discovery contract. This is distinct from the
/// execution-request and execution-source contracts because it returns a
/// bounded catalog rather than admitting work or reading event positions.
pub const CONTRACT: &str = "coven.execution-catalog.v1";
pub const MAX_READ_BYTES: usize = 8 * 1024;
pub const MAX_ENTRIES: usize = 100;
pub const MAX_SEARCH_BYTES: usize = 128;
pub const MAX_ROOT_BYTES: usize = 4096;
pub const MAX_MODEL_BYTES: usize = 256;
pub const MAX_TITLE_BYTES: usize = 256;
pub const MAX_TIMESTAMP_BYTES: usize = 64;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CatalogRead {
    pub contract: String,
    pub host_id: SourceId,
    pub scope: ExecutionScope,
    pub authority: ExecutionAuthority,
    pub limit: u16,
    pub include_archived: bool,
    pub search: Option<String>,
    pub cursor: Option<CatalogCursor>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CatalogCursor {
    pub source: SourceIdentity,
    pub scope: ExecutionScope,
    pub authority: ExecutionAuthority,
    pub include_archived: bool,
    pub search: Option<String>,
    /// The catalog's sort order is strictly ascending by this stable managed
    /// session reference. A catalog page is not an immutable snapshot: rows
    /// admitted after a page may appear in later reads according to this key.
    pub after_session_id: ExecutionSessionId,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CatalogReply {
    pub contract: String,
    pub source: SourceIdentity,
    pub scope: ExecutionScope,
    pub authority: ExecutionAuthority,
    pub project: CatalogProject,
    pub health: SourceHealth,
    pub entries: Vec<CatalogEntry>,
    pub next_cursor: Option<CatalogCursor>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CatalogProject {
    /// Server-provided canonical absolute project root. Clients must treat it
    /// as display/policy metadata and never use it to choose a different home.
    pub root: String,
    pub harnesses: Vec<ExecutionHarness>,
    pub modes: Vec<ExecutionMode>,
    pub models: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CatalogEntry {
    pub session_id: ExecutionSessionId,
    pub generation: u64,
    pub harness: ExecutionHarness,
    pub mode: ExecutionMode,
    pub model: Option<String>,
    /// The daemon may return `None` for a malformed or intentionally omitted
    /// display title; a present title is still bounded and control-free.
    pub title: Option<String>,
    pub lifecycle: SourceLifecycle,
    pub archived: bool,
    pub exit_code: Option<i32>,
    pub created_at: String,
    pub updated_at: String,
}

impl CatalogRead {
    pub fn validate(&self) -> Result<(), ContractError> {
        if self.contract != CONTRACT {
            return Err(ContractError("catalog.contract"));
        }
        validate_source_id(&self.host_id, "catalog.hostId")?;
        self.scope.validate()?;
        validate_authority(&self.authority)?;
        if self.limit == 0 || usize::from(self.limit) > MAX_ENTRIES {
            return Err(ContractError("catalog.limit"));
        }
        validate_search(self.search.as_deref())?;
        if let Some(cursor) = &self.cursor {
            cursor.validate()?;
            if cursor.source.host_id != self.host_id
                || cursor.source.profile_id != self.scope.profile_id
                || cursor.scope != self.scope
                || cursor.authority != self.authority
                || cursor.include_archived != self.include_archived
                || cursor.search != self.search
            {
                return Err(ContractError("catalog.cursorBinding"));
            }
        }
        Ok(())
    }

    /// The request body is bounded separately by the HTTP client after its
    /// canonical JSON encoding. This semantic validator remains useful to
    /// callers before serialization.
    pub fn validate_bytes(&self, bytes: &[u8]) -> Result<(), ContractError> {
        self.validate()?;
        if bytes.len() > MAX_READ_BYTES {
            return Err(ContractError("catalog.requestBytes"));
        }
        Ok(())
    }
}

impl CatalogCursor {
    pub fn validate(&self) -> Result<(), ContractError> {
        validate_source_identity(&self.source)?;
        self.scope.validate()?;
        validate_authority(&self.authority)?;
        if self.source.profile_id != self.scope.profile_id {
            return Err(ContractError("catalog.cursorSourceProfile"));
        }
        validate_search(self.search.as_deref())?;
        // ExecutionSessionId's deserializer enforces this, but revalidate a
        // value constructed by a Rust caller rather than trusting its origin.
        ExecutionSessionId::new(self.after_session_id.as_str().to_owned())?;
        Ok(())
    }
}

impl CatalogProject {
    pub fn validate(&self) -> Result<(), ContractError> {
        validate_absolute_path(&self.root)?;
        if self.harnesses.is_empty() || self.harnesses.len() > 3 {
            return Err(ContractError("catalog.project.harnesses"));
        }
        if self.modes.is_empty() || self.modes.len() > 2 {
            return Err(ContractError("catalog.project.modes"));
        }
        if self.models.len() > 32 {
            return Err(ContractError("catalog.project.models"));
        }
        for model in &self.models {
            validate_nonempty_bounded_text(model, MAX_MODEL_BYTES, "catalog.project.model")?;
        }
        Ok(())
    }
}

impl CatalogEntry {
    pub fn validate(&self) -> Result<(), ContractError> {
        ExecutionSessionId::new(self.session_id.as_str().to_owned())?;
        if self.generation == 0 || self.generation > i64::MAX as u64 {
            return Err(ContractError("catalog.entry.generation"));
        }
        if let Some(model) = &self.model {
            validate_nonempty_bounded_text(model, MAX_MODEL_BYTES, "catalog.entry.model")?;
        }
        if let Some(title) = &self.title {
            validate_bounded_text(title, MAX_TITLE_BYTES, "catalog.entry.title")?;
        }
        validate_nonempty_bounded_text(
            &self.created_at,
            MAX_TIMESTAMP_BYTES,
            "catalog.entry.createdAt",
        )?;
        validate_nonempty_bounded_text(
            &self.updated_at,
            MAX_TIMESTAMP_BYTES,
            "catalog.entry.updatedAt",
        )?;
        Ok(())
    }
}

impl CatalogReply {
    /// Validate all server-controlled identity and pagination fields against a
    /// particular request. No caller should render entries before this passes.
    pub fn validate_for(&self, request: &CatalogRead) -> Result<(), ContractError> {
        request.validate()?;
        if self.contract != CONTRACT
            || self.source.host_id != request.host_id
            || self.source.profile_id != request.scope.profile_id
            || self.scope != request.scope
            || self.authority != request.authority
        {
            return Err(ContractError("catalog.responseBinding"));
        }
        if request
            .cursor
            .as_ref()
            .is_some_and(|cursor| cursor.source != self.source)
        {
            return Err(ContractError("catalog.responseCursorSource"));
        }
        validate_source_identity(&self.source)?;
        self.project.validate()?;
        validate_authority(&self.authority)?;
        if !self.health.daemon_live {
            return Err(ContractError("catalog.responseHealth"));
        }
        if self.entries.len() > usize::from(request.limit) || self.entries.len() > MAX_ENTRIES {
            return Err(ContractError("catalog.responseEntries"));
        }
        // A cursor is an exclusive keyset boundary. This rejects replaying
        // the prior page (or returning a row from before that page) even when
        // the entries themselves are strictly ascending.
        let mut previous: Option<&ExecutionSessionId> = request
            .cursor
            .as_ref()
            .map(|cursor| &cursor.after_session_id);
        for entry in &self.entries {
            entry.validate()?;
            if let Some(previous) = previous {
                if entry.session_id.as_str() <= previous.as_str() {
                    return Err(ContractError("catalog.responseOrder"));
                }
            }
            if !request.include_archived && entry.archived {
                return Err(ContractError("catalog.responseArchived"));
            }
            previous = Some(&entry.session_id);
        }

        match (self.entries.last(), self.next_cursor.as_ref()) {
            (None, Some(_)) => Err(ContractError("catalog.responseEmptyCursor")),
            (Some(last), Some(cursor)) => {
                cursor.validate()?;
                if cursor.source != self.source
                    || cursor.scope != self.scope
                    || cursor.authority != self.authority
                    || cursor.include_archived != request.include_archived
                    || cursor.search != request.search
                    || cursor.after_session_id != last.session_id
                {
                    return Err(ContractError("catalog.responseCursorBinding"));
                }
                Ok(())
            }
            (Some(_), None) | (None, None) => Ok(()),
        }
    }
}

fn validate_authority(authority: &ExecutionAuthority) -> Result<(), ContractError> {
    ExecutionAuthority::new(authority.id.clone(), authority.generation)?;
    Ok(())
}

fn validate_source_id(value: &SourceId, field: &'static str) -> Result<(), ContractError> {
    SourceId::new(value.as_str().to_owned())
        .map(|_| ())
        .map_err(|_| ContractError(field))
}

fn validate_source_identity(value: &SourceIdentity) -> Result<(), ContractError> {
    validate_source_id(&value.host_id, "catalog.source.hostId")?;
    validate_source_id(&value.ledger_id, "catalog.source.ledgerId")?;
    if value.epoch == 0 || value.epoch > i64::MAX as u64 {
        return Err(ContractError("catalog.source.epoch"));
    }
    // SourceIdentity's profile id is a scoped id. Reconstructing it through
    // the public serde-compatible constructor keeps Rust-created values under
    // the same syntax bound as wire-created values.
    crate::execution::ProfileId::new(value.profile_id.as_str().to_owned())?;
    Ok(())
}

fn validate_search(value: Option<&str>) -> Result<(), ContractError> {
    if let Some(value) = value {
        validate_bounded_text(value, MAX_SEARCH_BYTES, "catalog.search")?;
    }
    Ok(())
}

fn validate_absolute_path(value: &str) -> Result<(), ContractError> {
    validate_nonempty_bounded_text(value, MAX_ROOT_BYTES, "catalog.project.root")?;
    if !Path::new(value).is_absolute() {
        return Err(ContractError("catalog.project.rootAbsolute"));
    }
    Ok(())
}

fn validate_nonempty_bounded_text(
    value: &str,
    max_bytes: usize,
    field: &'static str,
) -> Result<(), ContractError> {
    if value.is_empty() {
        return Err(ContractError(field));
    }
    validate_bounded_text(value, max_bytes, field)
}

fn validate_bounded_text(
    value: &str,
    max_bytes: usize,
    field: &'static str,
) -> Result<(), ContractError> {
    if value.len() > max_bytes || value.chars().any(char::is_control) {
        return Err(ContractError(field));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scope(project: &str) -> ExecutionScope {
        ExecutionScope {
            project_id: crate::execution::ProjectId::new(project).unwrap(),
            profile_id: crate::execution::ProfileId::new("profile").unwrap(),
            policy_generation: 1,
        }
    }

    fn authority() -> ExecutionAuthority {
        ExecutionAuthority::new(crate::execution::AuthorityId::new("authority").unwrap(), 1)
            .unwrap()
    }

    fn source() -> SourceIdentity {
        SourceIdentity {
            host_id: SourceId::new("host").unwrap(),
            profile_id: crate::execution::ProfileId::new("profile").unwrap(),
            ledger_id: SourceId::new("ledger").unwrap(),
            epoch: 1,
        }
    }

    fn cursor(after_session_id: &str) -> CatalogCursor {
        CatalogCursor {
            source: source(),
            scope: scope("project"),
            authority: authority(),
            include_archived: false,
            search: None,
            after_session_id: ExecutionSessionId::new(after_session_id).unwrap(),
        }
    }

    fn request() -> CatalogRead {
        CatalogRead {
            contract: CONTRACT.to_owned(),
            host_id: SourceId::new("host").unwrap(),
            scope: scope("project"),
            authority: authority(),
            limit: 2,
            include_archived: false,
            search: None,
            cursor: None,
        }
    }

    fn entry(id: &str) -> CatalogEntry {
        CatalogEntry {
            session_id: ExecutionSessionId::new(id).unwrap(),
            generation: 1,
            harness: ExecutionHarness::Codex,
            mode: ExecutionMode::Transcript,
            model: None,
            title: Some("title".to_owned()),
            lifecycle: SourceLifecycle::Completed,
            archived: false,
            exit_code: Some(0),
            created_at: "2026-09-08T00:00:00Z".to_owned(),
            updated_at: "2026-09-08T00:00:01Z".to_owned(),
        }
    }

    fn reply(entries: Vec<CatalogEntry>, next_cursor: Option<CatalogCursor>) -> CatalogReply {
        CatalogReply {
            contract: CONTRACT.to_owned(),
            source: source(),
            scope: scope("project"),
            authority: authority(),
            project: CatalogProject {
                root: "/repo".to_owned(),
                harnesses: vec![ExecutionHarness::Codex],
                modes: vec![ExecutionMode::Transcript],
                models: Vec::new(),
            },
            health: SourceHealth {
                daemon_live: true,
                writer_state: crate::source::WriterState::Healthy,
                writer_queued_bytes: Some(0),
                writer_dropped_output_bytes: Some(0),
            },
            entries,
            next_cursor,
        }
    }

    #[test]
    fn request_rejects_cursor_with_wrong_scope_and_filter() {
        let mut value = request();
        value.cursor = Some(CatalogCursor {
            source: source(),
            scope: scope("other"),
            authority: authority(),
            include_archived: true,
            search: Some("different".to_owned()),
            after_session_id: ExecutionSessionId::new("s1").unwrap(),
        });
        assert!(value.validate().is_err());
    }

    #[test]
    fn request_rejects_oversized_or_control_search() {
        let mut value = request();
        value.search = Some("x".repeat(MAX_SEARCH_BYTES + 1));
        assert!(value.validate().is_err());
        value.search = Some("safe\u{0000}search".to_owned());
        assert!(value.validate().is_err());
    }

    #[test]
    fn response_rejects_more_entries_than_requested() {
        let mut value = request();
        value.limit = 1;
        let response = reply(vec![entry("s1"), entry("s2")], None);
        assert!(response.validate_for(&value).is_err());
    }

    #[test]
    fn response_rejects_non_ascending_entries() {
        let value = request();
        let response = reply(vec![entry("s2"), entry("s1")], None);
        assert!(response.validate_for(&value).is_err());
    }

    #[test]
    fn response_rejects_cursor_without_entries() {
        let value = request();
        let cursor = cursor("s1");
        let response = reply(Vec::new(), Some(cursor));
        assert!(response.validate_for(&value).is_err());
    }

    #[test]
    fn response_rejects_cursor_not_pointing_at_last_entry() {
        let value = request();
        let cursor = cursor("other");
        let response = reply(vec![entry("s1")], Some(cursor));
        assert!(response.validate_for(&value).is_err());
    }

    #[test]
    fn response_rejects_cursor_from_another_source_incarnation() {
        let mut value = request();
        value.cursor = Some(cursor("s1"));
        let mut response = reply(vec![entry("s2")], None);
        response.source.epoch = 2;
        assert!(response.validate_for(&value).is_err());
    }

    #[test]
    fn response_rejects_replaying_the_cursor_boundary() {
        let mut value = request();
        value.cursor = Some(cursor("s1"));
        let response = reply(vec![entry("s1"), entry("s2")], None);
        assert!(response.validate_for(&value).is_err());
    }
}
