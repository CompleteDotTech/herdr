//! Identity shared by every checkpoint-stream message.

use std::{error::Error, fmt};

use serde::{Deserialize, Serialize};

/// Session identifiers are authenticated by the owner-side attach route and
/// are bounded before they become part of a transport identity.
pub const MAX_SESSION_ID_BYTES: usize = 128;

/// Stream, execution, and authority generations are one immutable binding.
/// A message from an old process or a reused session id must fail identity
/// validation even when its output cursor happens to look plausible.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StreamBinding {
    pub session_id: String,
    /// Opaque stream UUID bytes. The all-zero value is invalid.
    pub stream_id: [u8; 16],
    pub stream_generation: u64,
    pub execution_generation: u64,
    pub authority_epoch: u64,
}

impl StreamBinding {
    pub fn new(
        session_id: impl Into<String>,
        stream_id: [u8; 16],
        stream_generation: u64,
        execution_generation: u64,
        authority_epoch: u64,
    ) -> Result<Self, BindingError> {
        let binding = Self {
            session_id: session_id.into(),
            stream_id,
            stream_generation,
            execution_generation,
            authority_epoch,
        };
        binding.validate()?;
        Ok(binding)
    }

    pub fn validate(&self) -> Result<(), BindingError> {
        if self.session_id.is_empty() {
            return Err(BindingError::EmptySessionId);
        }
        if self.session_id.len() > MAX_SESSION_ID_BYTES {
            return Err(BindingError::SessionIdTooLarge {
                actual: self.session_id.len(),
                maximum: MAX_SESSION_ID_BYTES,
            });
        }
        if self.stream_id == [0; 16] {
            return Err(BindingError::ZeroStreamId);
        }
        if self.stream_generation == 0 {
            return Err(BindingError::ZeroStreamGeneration);
        }
        if self.execution_generation == 0 {
            return Err(BindingError::ZeroExecutionGeneration);
        }
        if self.authority_epoch == 0 {
            return Err(BindingError::ZeroAuthorityEpoch);
        }
        Ok(())
    }

    pub fn stream_id_hex(&self) -> String {
        self.stream_id
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BindingError {
    EmptySessionId,
    SessionIdTooLarge { actual: usize, maximum: usize },
    ZeroStreamId,
    ZeroStreamGeneration,
    ZeroExecutionGeneration,
    ZeroAuthorityEpoch,
}

impl fmt::Display for BindingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptySessionId => formatter.write_str("terminal session id is empty"),
            Self::SessionIdTooLarge { actual, maximum } => write!(
                formatter,
                "terminal session id is {actual} bytes; maximum is {maximum}"
            ),
            Self::ZeroStreamId => formatter.write_str("terminal stream id is zero"),
            Self::ZeroStreamGeneration => formatter.write_str("terminal stream generation is zero"),
            Self::ZeroExecutionGeneration => {
                formatter.write_str("terminal execution generation is zero")
            }
            Self::ZeroAuthorityEpoch => formatter.write_str("terminal authority epoch is zero"),
        }
    }
}

impl Error for BindingError {}
