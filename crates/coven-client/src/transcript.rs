//! Bounded public transcript presentation, distinct from metadata-only source v1.
//! Output is retained/redacted text, not an interactive terminal byte stream.

use serde::{Deserialize, Serialize};

use crate::{
    execution::{ContractError, ExecutionAuthority, ExecutionScope, ExecutionSessionId},
    source::{ResetReason, SourceCursor, SourceHealth, SourceId, SourceIdentity, SourceProjection},
};

pub const CONTRACT: &str = "coven.execution-transcript.v1";
pub const MAX_READ_BYTES: usize = 8192;
pub const MAX_CHUNKS: u16 = 64;
pub const MAX_CHUNK_BYTES: usize = 64 * 1024;
pub const MAX_PAGE_TEXT_BYTES: usize = 256 * 1024;
/// Bound the stored public JSON before parsing it, including legacy redaction.
pub const MAX_STORED_PAYLOAD_BYTES: usize = 2 * 1024 * 1024;
/// Public redaction may expand a legacy text value. Still reject implausible
/// omission counts instead of treating a peer-supplied u64 as unconstrained.
pub const MAX_OMITTED_PREFIX_BYTES: u64 = (MAX_STORED_PAYLOAD_BYTES as u64) * 16;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TranscriptRead {
    pub contract: String,
    pub host_id: SourceId,
    pub scope: ExecutionScope,
    pub authority: ExecutionAuthority,
    pub session_id: ExecutionSessionId,
    pub generation: u64,
    /// None requests a bounded recent tail and its atomic head cursor.
    pub cursor: Option<SourceCursor>,
    pub limit: u16,
}

impl TranscriptRead {
    pub fn validate(&self) -> Result<(), ContractError> {
        self.scope.validate()?;
        ExecutionAuthority::new(self.authority.id.clone(), self.authority.generation)?;
        if self.contract != CONTRACT
            || self.generation == 0
            || self.generation > i64::MAX as u64
            || self.limit == 0
            || self.limit > MAX_CHUNKS
        {
            return Err(ContractError("transcript.read"));
        }
        if let Some(cursor) = &self.cursor {
            cursor.validate()?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TranscriptOmission {
    StoredEventTooLarge,
    MalformedOutput,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "snake_case",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum TranscriptContent {
    Text {
        data: String,
        /// Public text bytes omitted from the beginning of this event to fit
        /// the per-chunk display bound; not a writer-drop counter.
        omitted_prefix_bytes: u64,
    },
    Omitted {
        reason: TranscriptOmission,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TranscriptChunk {
    /// Stable source-sidecar sequence, never legacy SQLite rowid.
    pub seq: i64,
    pub content: TranscriptContent,
}

impl TranscriptChunk {
    pub fn text_bytes(&self) -> usize {
        match &self.content {
            TranscriptContent::Text { data, .. } => data.len(),
            TranscriptContent::Omitted { .. } => 0,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "snake_case",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum TranscriptResult {
    Snapshot {
        cursor: SourceCursor,
        chunks: Vec<TranscriptChunk>,
        /// Some earlier retained text or a pruned prefix is not in this tail.
        prefix_omitted: bool,
    },
    Page {
        after: SourceCursor,
        /// Last scanned stable sequence. It may advance over non-output rows;
        /// the owner never advances past an output row omitted by page budgets.
        cursor: SourceCursor,
        chunks: Vec<TranscriptChunk>,
        has_more: bool,
    },
    Reset {
        reason: ResetReason,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TranscriptReply {
    pub contract: String,
    pub source: SourceIdentity,
    pub scope: ExecutionScope,
    pub authority: ExecutionAuthority,
    pub session_id: ExecutionSessionId,
    pub generation: u64,
    pub health: SourceHealth,
    pub projection: SourceProjection,
    pub result: TranscriptResult,
}

impl TranscriptReply {
    pub fn validate_for(&self, request: &TranscriptRead) -> Result<(), ContractError> {
        request.validate()?;
        if self.contract != CONTRACT
            || self.source.host_id != request.host_id
            || self.source.profile_id != request.scope.profile_id
            || self.scope != request.scope
            || self.authority != request.authority
            || self.session_id != request.session_id
            || self.generation != request.generation
            || self.source.epoch == 0
            || self.source.epoch > i64::MAX as u64
            || !self.health.daemon_live
        {
            return Err(ContractError("transcript.responseBinding"));
        }
        if let Some(cursor) = &request.cursor {
            let changed =
                cursor.ledger_id != self.source.ledger_id || cursor.epoch != self.source.epoch;
            let reset_changed = matches!(
                &self.result,
                TranscriptResult::Reset {
                    reason: ResetReason::SourceChanged
                }
            );
            if changed != reset_changed {
                return Err(ContractError("transcript.sourceChanged"));
            }
        }
        let validate_cursor = |cursor: &SourceCursor| {
            cursor.validate()?;
            if cursor.ledger_id != self.source.ledger_id || cursor.epoch != self.source.epoch {
                return Err(ContractError("transcript.cursorIdentity"));
            }
            Ok(())
        };
        let validate_chunks = |chunks: &[TranscriptChunk], minimum: i64, maximum: i64| {
            if chunks.len() > usize::from(request.limit) {
                return Err(ContractError("transcript.chunkCount"));
            }
            let mut previous = minimum;
            let mut total = 0usize;
            for chunk in chunks {
                if chunk.seq <= previous
                    || chunk.seq > maximum
                    || chunk.text_bytes() > MAX_CHUNK_BYTES
                {
                    return Err(ContractError("transcript.chunkBounds"));
                }
                if matches!(&chunk.content, TranscriptContent::Text { omitted_prefix_bytes, .. } if *omitted_prefix_bytes > MAX_OMITTED_PREFIX_BYTES)
                {
                    return Err(ContractError("transcript.omittedBytes"));
                }
                previous = chunk.seq;
                total = total
                    .checked_add(chunk.text_bytes())
                    .ok_or(ContractError("transcript.textBytes"))?;
                if total > MAX_PAGE_TEXT_BYTES {
                    return Err(ContractError("transcript.textBytes"));
                }
            }
            Ok(())
        };
        match (&request.cursor, &self.result) {
            (None, TranscriptResult::Snapshot { cursor, chunks, .. }) => {
                validate_cursor(cursor)?;
                validate_chunks(chunks, 0, cursor.after_seq)
            }
            (
                Some(expected),
                TranscriptResult::Page {
                    after,
                    cursor,
                    chunks,
                    has_more,
                },
            ) => {
                validate_cursor(after)?;
                validate_cursor(cursor)?;
                if after != expected
                    || cursor.revision != after.revision
                    || cursor.after_seq < after.after_seq
                {
                    return Err(ContractError("transcript.pageCursor"));
                }
                validate_chunks(chunks, after.after_seq, cursor.after_seq)?;
                if *has_more && cursor.after_seq <= after.after_seq {
                    return Err(ContractError("transcript.pageProgress"));
                }
                Ok(())
            }
            (Some(_), TranscriptResult::Reset { .. }) => Ok(()),
            _ => Err(ContractError("transcript.responseKind")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::{SourceLifecycle, WriterState};

    fn request() -> TranscriptRead {
        serde_json::from_value(serde_json::json!({
            "contract":CONTRACT,"hostId":"host",
            "scope":{"projectId":"project","profileId":"profile","policyGeneration":1},
            "authority":{"id":"authority","generation":1},
            "sessionId":"session","generation":1,"limit":2,
            "cursor":{"ledgerId":"ledger","epoch":1,"afterSeq":4,"revision":2}
        }))
        .unwrap()
    }
    fn page(request: &TranscriptRead) -> TranscriptReply {
        let after = request.cursor.clone().unwrap();
        TranscriptReply {
            contract: CONTRACT.into(),
            source: SourceIdentity {
                host_id: request.host_id.clone(),
                profile_id: request.scope.profile_id.clone(),
                ledger_id: after.ledger_id.clone(),
                epoch: after.epoch,
            },
            scope: request.scope.clone(),
            authority: request.authority.clone(),
            session_id: request.session_id.clone(),
            generation: request.generation,
            health: SourceHealth {
                daemon_live: true,
                writer_state: WriterState::Healthy,
                writer_queued_bytes: Some(0),
                writer_dropped_output_bytes: Some(0),
            },
            projection: SourceProjection {
                lifecycle: SourceLifecycle::Running,
                exit_code: None,
                archived: false,
                dropped_output_bytes: Some(0),
            },
            result: TranscriptResult::Page {
                after: after.clone(),
                cursor: SourceCursor {
                    after_seq: 9,
                    ..after
                },
                chunks: vec![
                    TranscriptChunk {
                        seq: 7,
                        content: TranscriptContent::Text {
                            data: "fixture output\n".into(),
                            omitted_prefix_bytes: 0,
                        },
                    },
                    TranscriptChunk {
                        seq: 9,
                        content: TranscriptContent::Omitted {
                            reason: TranscriptOmission::MalformedOutput,
                        },
                    },
                ],
                has_more: true,
            },
        }
    }
    #[test]
    fn page_rejects_wrong_binding_backwards_and_unbacked_progress() {
        let request = request();
        let good = page(&request);
        good.validate_for(&request).unwrap();
        for problem in 0..10 {
            let mut bad = good.clone();
            match problem {
                0 => bad.generation += 1,
                1 => bad.authority.generation += 1,
                2 => bad.scope.policy_generation += 1,
                3 => bad.health.daemon_live = false,
                _ => {
                    if let TranscriptResult::Page {
                        after,
                        cursor,
                        chunks,
                        ..
                    } = &mut bad.result
                    {
                        match problem {
                            4 => after.after_seq += 1,
                            5 => cursor.revision += 1,
                            6 => cursor.after_seq = after.after_seq,
                            7 => chunks.reverse(),
                            8 => {
                                chunks.clear();
                                cursor.after_seq = after.after_seq;
                            }
                            _ => chunks[0].seq = 4,
                        }
                    }
                }
            }
            assert!(bad.validate_for(&request).is_err(), "problem {problem}");
        }
    }
    #[test]
    fn implausible_omission_count_is_rejected() {
        let request = request();
        let mut reply = page(&request);
        if let TranscriptResult::Page { chunks, .. } = &mut reply.result {
            chunks[0].content = TranscriptContent::Text {
                data: String::new(),
                omitted_prefix_bytes: u64::MAX,
            };
        }
        assert!(reply.validate_for(&request).is_err());
    }

    #[test]
    fn bounded_scan_can_advance_over_non_output_rows() {
        let request = request();
        let mut reply = page(&request);
        if let TranscriptResult::Page { chunks, .. } = &mut reply.result {
            chunks.clear();
        }
        reply.validate_for(&request).unwrap();
    }

    #[test]
    fn source_reset_reason_must_match_actual_identity_change() {
        let request = request();
        let mut reply = page(&request);
        reply.source.epoch += 1;
        reply.result = TranscriptResult::Reset {
            reason: ResetReason::Retention,
        };
        assert!(reply.validate_for(&request).is_err());
        reply.result = TranscriptResult::Reset {
            reason: ResetReason::SourceChanged,
        };
        reply.validate_for(&request).unwrap();
        reply.source.epoch -= 1;
        assert!(reply.validate_for(&request).is_err());
    }
    #[test]
    fn byte_and_count_bounds_are_independent() {
        let mut request = request();
        let mut reply = page(&request);
        if let TranscriptResult::Page { chunks, .. } = &mut reply.result {
            chunks[0].content = TranscriptContent::Text {
                data: "x".repeat(MAX_CHUNK_BYTES + 1),
                omitted_prefix_bytes: 0,
            };
        }
        assert!(reply.validate_for(&request).is_err());
        request.limit = MAX_CHUNKS;
        let after = request.cursor.clone().unwrap();
        reply.result = TranscriptResult::Page {
            after: after.clone(),
            cursor: SourceCursor {
                after_seq: 14,
                ..after
            },
            chunks: (5..=14)
                .map(|seq| TranscriptChunk {
                    seq,
                    content: TranscriptContent::Text {
                        data: "x".repeat(MAX_CHUNK_BYTES),
                        omitted_prefix_bytes: 0,
                    },
                })
                .collect(),
            has_more: true,
        };
        assert!(reply.validate_for(&request).is_err());
        request.limit = 1;
        assert!(page(&request).validate_for(&request).is_err());
    }
    #[test]
    fn snapshot_can_only_answer_initial_read_and_output_is_not_input() {
        let mut request = request();
        let mut reply = page(&request);
        let cursor = request.cursor.clone().unwrap();
        reply.result = TranscriptResult::Snapshot {
            cursor,
            chunks: Vec::new(),
            prefix_omitted: true,
        };
        assert!(reply.validate_for(&request).is_err());
        request.cursor = None;
        reply.validate_for(&request).unwrap();
        let invalid = serde_json::json!({"seq":1,"content":{"kind":"input","data":"secret"}});
        assert!(serde_json::from_value::<TranscriptChunk>(invalid).is_err());
    }
}
