//! Owner-local, versioned presentation source. These reads have no execution
//! side effects; capability negotiation and response binding are still required.

use serde::{Deserialize, Serialize};

use crate::execution::{ContractError, ExecutionScope, ExecutionSessionId, ProfileId};

pub const CONTRACT: &str = "coven.execution-source.v1";
pub const MAX_EVENTS: u16 = 256;
pub const MAX_READ_BYTES: usize = 4096;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct SourceId(String);

impl SourceId {
    pub fn new(value: impl Into<String>) -> Result<Self, ContractError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 128
            || !value
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_.:".contains(&b))
        {
            return Err(ContractError("source.id"));
        }
        Ok(Self(value))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl TryFrom<String> for SourceId {
    type Error = ContractError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}
impl From<SourceId> for String {
    fn from(value: SourceId) -> Self {
        value.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SourceIdentity {
    pub host_id: SourceId,
    pub profile_id: ProfileId,
    pub ledger_id: SourceId,
    pub epoch: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SourceCursor {
    pub ledger_id: SourceId,
    pub epoch: u64,
    pub after_seq: i64,
    /// In-place projection revision. This is not an authority epoch and does
    /// not permit sequence rewind in the same ledger/epoch.
    pub revision: u64,
}

impl SourceCursor {
    pub fn validate(&self) -> Result<(), ContractError> {
        if self.epoch == 0
            || self.epoch > i64::MAX as u64
            || self.after_seq < 0
            || self.revision == 0
            || self.revision > i64::MAX as u64
        {
            return Err(ContractError("source.cursor"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SourceRead {
    pub contract: String,
    pub host_id: SourceId,
    pub scope: ExecutionScope,
    pub session_id: ExecutionSessionId,
    pub generation: u64,
    /// None requests an atomic snapshot with its event head.
    pub cursor: Option<SourceCursor>,
    pub limit: u16,
}

impl SourceRead {
    pub fn validate(&self) -> Result<(), ContractError> {
        self.scope.validate()?;
        if self.contract != CONTRACT
            || self.generation == 0
            || self.generation > i64::MAX as u64
            || self.limit == 0
            || self.limit > MAX_EVENTS
        {
            return Err(ContractError("source.read"));
        }
        if let Some(cursor) = &self.cursor {
            cursor.validate()?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceLifecycle {
    Created,
    Starting,
    Running,
    Idle,
    Completed,
    Failed,
    Cancelled,
    Killed,
    Orphaned,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WriterState {
    Healthy,
    Pressured,
    Failed,
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SourceHealth {
    pub daemon_live: bool,
    pub writer_state: WriterState,
    pub writer_queued_bytes: Option<u64>,
    /// Global daemon-lifetime counter; never a session truncation total.
    pub writer_dropped_output_bytes: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SourceProjection {
    pub lifecycle: SourceLifecycle,
    pub exit_code: Option<i32>,
    pub archived: bool,
    /// None when pre-tracking retention or invalid historical data prevents a
    /// complete session count. Zero is only reported when it is known.
    pub dropped_output_bytes: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResetReason {
    SourceChanged,
    RevisionChanged,
    Retention,
    FutureCursor,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "snake_case",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum SourceResult {
    Snapshot {
        cursor: SourceCursor,
        projection: SourceProjection,
    },
    /// Event positions carry no raw terminal text or provider reasoning.
    /// Authoritative presentation changes invalidate the revision and require
    /// a bounded snapshot instead of guessing semantics from raw output.
    Page {
        after: SourceCursor,
        sequences: Vec<i64>,
        next_cursor: Option<SourceCursor>,
    },
    Reset {
        reason: ResetReason,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SourceReply {
    pub contract: String,
    pub source: SourceIdentity,
    pub scope: ExecutionScope,
    pub session_id: ExecutionSessionId,
    pub generation: u64,
    pub health: SourceHealth,
    pub result: SourceResult,
}

impl SourceReply {
    pub fn validate_for(&self, request: &SourceRead) -> Result<(), ContractError> {
        request.validate()?;
        if self.contract != CONTRACT
            || self.source.host_id != request.host_id
            || self.source.profile_id != request.scope.profile_id
            || self.scope != request.scope
            || self.session_id != request.session_id
            || self.generation != request.generation
            || self.source.epoch == 0
            || self.source.epoch > i64::MAX as u64
            || !self.health.daemon_live
        {
            return Err(ContractError("source.responseBinding"));
        }
        let bound_cursor = |cursor: &SourceCursor| -> Result<(), ContractError> {
            cursor.validate()?;
            if cursor.ledger_id != self.source.ledger_id || cursor.epoch != self.source.epoch {
                return Err(ContractError("source.cursorIdentity"));
            }
            Ok(())
        };
        match (&request.cursor, &self.result) {
            (None, SourceResult::Snapshot { cursor, .. }) => bound_cursor(cursor)?,
            (
                Some(expected),
                SourceResult::Page {
                    after,
                    sequences,
                    next_cursor,
                },
            ) => {
                bound_cursor(after)?;
                if expected != after || sequences.len() > usize::from(request.limit) {
                    return Err(ContractError("source.pageBinding"));
                }
                let mut last = after.after_seq;
                for seq in sequences {
                    if *seq <= last {
                        return Err(ContractError("source.sequenceOrder"));
                    }
                    last = *seq;
                }
                match (sequences.is_empty(), next_cursor) {
                    (true, None) => {}
                    (false, Some(next)) => {
                        bound_cursor(next)?;
                        if next.after_seq != last || next.revision != after.revision {
                            return Err(ContractError("source.nextCursor"));
                        }
                    }
                    _ => return Err(ContractError("source.nextCursor")),
                }
            }
            (Some(expected), SourceResult::Reset { reason }) => {
                let changed = expected.ledger_id != self.source.ledger_id
                    || expected.epoch != self.source.epoch;
                if changed != (*reason == ResetReason::SourceChanged) {
                    return Err(ContractError("source.resetIdentity"));
                }
            }
            _ => return Err(ContractError("source.responseKind")),
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request() -> SourceRead {
        serde_json::from_value(serde_json::json!({
            "contract":CONTRACT,"hostId":"host","scope":{"projectId":"project","profileId":"profile","policyGeneration":1},
            "sessionId":"session","generation":1,"limit":2,
            "cursor":{"ledgerId":"ledger","epoch":1,"afterSeq":4,"revision":2}
        })).unwrap()
    }

    fn page(request: &SourceRead) -> SourceReply {
        let after = request.cursor.clone().unwrap();
        SourceReply {
            contract: CONTRACT.to_owned(),
            source: SourceIdentity {
                host_id: request.host_id.clone(),
                profile_id: request.scope.profile_id.clone(),
                ledger_id: after.ledger_id.clone(),
                epoch: after.epoch,
            },
            scope: request.scope.clone(),
            session_id: request.session_id.clone(),
            generation: request.generation,
            health: SourceHealth {
                daemon_live: true,
                writer_state: WriterState::Unknown,
                writer_queued_bytes: None,
                writer_dropped_output_bytes: None,
            },
            result: SourceResult::Page {
                after: after.clone(),
                sequences: vec![7, 19],
                next_cursor: Some(SourceCursor {
                    after_seq: 19,
                    ..after
                }),
            },
        }
    }

    #[test]
    fn page_rejects_unbacked_cursor_revision_and_order() {
        let request = request();
        let good = page(&request);
        good.validate_for(&request).unwrap();
        for problem in 0..4 {
            let mut bad = good.clone();
            if let SourceResult::Page {
                sequences,
                next_cursor,
                ..
            } = &mut bad.result
            {
                match problem {
                    0 => next_cursor.as_mut().unwrap().revision += 1,
                    1 => next_cursor.as_mut().unwrap().after_seq += 1,
                    2 => sequences.reverse(),
                    _ => sequences.push(30),
                }
            }
            assert!(bad.validate_for(&request).is_err());
        }
    }

    #[test]
    fn reset_reason_cannot_hide_source_identity_change() {
        let request = request();
        let mut reply = page(&request);
        reply.source.epoch += 1;
        reply.result = SourceResult::Reset {
            reason: ResetReason::Retention,
        };
        assert!(reply.validate_for(&request).is_err());
        reply.result = SourceResult::Reset {
            reason: ResetReason::SourceChanged,
        };
        reply.validate_for(&request).unwrap();
        reply.source.epoch -= 1;
        assert!(reply.validate_for(&request).is_err());
    }
}
