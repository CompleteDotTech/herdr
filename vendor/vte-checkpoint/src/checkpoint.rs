//! Versioned, validated snapshots for the raw VTE parser.
//!
//! The parser deliberately does not derive a snapshot from its Rust layout.
//! This module names every piece of continuation state that can affect the
//! next byte and validates a snapshot before constructing a parser from it.

extern crate alloc;

use alloc::borrow::ToOwned;
use alloc::string::String;
use alloc::vec::Vec;
use core::fmt;
use core::marker::PhantomData;
use core::str;

use serde::de::{self, Deserializer, SeqAccess, Visitor};
use serde::{Deserialize, Serialize};

use super::params::MAX_PARAMS;
use super::{Params, Parser, State, MAX_INTERMEDIATES, MAX_OSC_PARAMS};

/// Maximum OSC bytes retained by the checkpointable parser.
///
/// The upstream `std` parser uses a `Vec`; the fork applies this bound in both
/// builds so an unterminated OSC cannot grow without limit.
pub const MAX_CHECKPOINT_OSC_RAW: usize = 1024 * 1024;

/// Maximum serialized checkpoint size accepted by a caller using the bytes
/// boundary. The parser-level DTO validator also applies field limits.
pub const MAX_CHECKPOINT_BYTES: usize = 4 * 1024 * 1024;

/// The exact upstream source identity used for this fork.
pub const VTE_VERSION: &str = "0.15.0";
pub const UPSTREAM_COMMIT: &str = "3b3da71c34cc1256c7e20981cf03f8eb95e08ffc";

/// Check the serialized envelope size before handing bytes to a Serde
/// deserializer. Callers should apply this to the complete bounded frame,
/// before decoding any fields.
pub fn validate_serialized_size(bytes: &[u8]) -> Result<(), CheckpointError> {
    if bytes.len() > MAX_CHECKPOINT_BYTES {
        return Err(CheckpointError::LimitExceeded {
            field: "serialized_checkpoint",
            actual: bytes.len(),
            maximum: MAX_CHECKPOINT_BYTES,
        });
    }
    Ok(())
}

/// A checkpoint was malformed or exceeded a bounded field limit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CheckpointError {
    /// A structural or semantic invariant did not hold.
    Invalid(&'static str),
    /// A bounded field was larger than the fork permits.
    LimitExceeded { field: &'static str, actual: usize, maximum: usize },
}

impl fmt::Display for CheckpointError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(reason) => write!(f, "invalid checkpoint: {reason}"),
            Self::LimitExceeded { field, actual, maximum } => {
                write!(f, "checkpoint field {field} has size {actual}, maximum is {maximum}")
            },
        }
    }
}

#[cfg(feature = "std")]
impl std::error::Error for CheckpointError {}

/// Parser state at the exact end of a byte prefix.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ParserStateV1 {
    CsiEntry,
    CsiIgnore,
    CsiIntermediate,
    CsiParam,
    DcsEntry,
    DcsIgnore,
    DcsIntermediate,
    DcsParam,
    DcsPassthrough,
    Escape,
    EscapeIntermediate,
    OscString,
    SosPmApcString,
    #[default]
    Ground,
}

impl From<State> for ParserStateV1 {
    fn from(state: State) -> Self {
        match state {
            State::CsiEntry => Self::CsiEntry,
            State::CsiIgnore => Self::CsiIgnore,
            State::CsiIntermediate => Self::CsiIntermediate,
            State::CsiParam => Self::CsiParam,
            State::DcsEntry => Self::DcsEntry,
            State::DcsIgnore => Self::DcsIgnore,
            State::DcsIntermediate => Self::DcsIntermediate,
            State::DcsParam => Self::DcsParam,
            State::DcsPassthrough => Self::DcsPassthrough,
            State::Escape => Self::Escape,
            State::EscapeIntermediate => Self::EscapeIntermediate,
            State::OscString => Self::OscString,
            State::SosPmApcString => Self::SosPmApcString,
            State::Ground => Self::Ground,
        }
    }
}

impl From<ParserStateV1> for State {
    fn from(state: ParserStateV1) -> Self {
        match state {
            ParserStateV1::CsiEntry => Self::CsiEntry,
            ParserStateV1::CsiIgnore => Self::CsiIgnore,
            ParserStateV1::CsiIntermediate => Self::CsiIntermediate,
            ParserStateV1::CsiParam => Self::CsiParam,
            ParserStateV1::DcsEntry => Self::DcsEntry,
            ParserStateV1::DcsIgnore => Self::DcsIgnore,
            ParserStateV1::DcsIntermediate => Self::DcsIntermediate,
            ParserStateV1::DcsParam => Self::DcsParam,
            ParserStateV1::DcsPassthrough => Self::DcsPassthrough,
            ParserStateV1::Escape => Self::Escape,
            ParserStateV1::EscapeIntermediate => Self::EscapeIntermediate,
            ParserStateV1::OscString => Self::OscString,
            ParserStateV1::SosPmApcString => Self::SosPmApcString,
            ParserStateV1::Ground => Self::Ground,
        }
    }
}

/// Complete continuation state for [`Parser`].
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ParserCheckpointV1 {
    /// Explicit schema tag for outer envelopes and diagnostics.
    pub schema: String,
    /// Schema revision for this parser DTO.
    pub version: u16,
    pub state: ParserStateV1,
    pub intermediates: [u8; MAX_INTERMEDIATES],
    pub intermediate_idx: u8,
    pub params_subparams: [u8; MAX_PARAMS],
    pub params: [u16; MAX_PARAMS],
    pub current_subparams: u8,
    pub params_len: u8,
    pub param: u16,
    pub osc_raw: Vec<u8>,
    pub osc_params: [(u32, u32); MAX_OSC_PARAMS],
    pub osc_num_params: u8,
    pub ignoring: bool,
    pub partial_utf8: [u8; 4],
    pub partial_utf8_len: u8,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ParserCheckpointV1Fields {
    schema: String,
    version: u16,
    state: ParserStateV1,
    intermediates: [u8; MAX_INTERMEDIATES],
    intermediate_idx: u8,
    params_subparams: [u8; MAX_PARAMS],
    params: [u16; MAX_PARAMS],
    current_subparams: u8,
    params_len: u8,
    param: u16,
    #[serde(deserialize_with = "deserialize_osc_raw")]
    osc_raw: Vec<u8>,
    osc_params: [(u32, u32); MAX_OSC_PARAMS],
    osc_num_params: u8,
    ignoring: bool,
    partial_utf8: [u8; 4],
    partial_utf8_len: u8,
}

impl<'de> Deserialize<'de> for ParserCheckpointV1 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields = ParserCheckpointV1Fields::deserialize(deserializer)?;
        Ok(Self {
            schema: fields.schema,
            version: fields.version,
            state: fields.state,
            intermediates: fields.intermediates,
            intermediate_idx: fields.intermediate_idx,
            params_subparams: fields.params_subparams,
            params: fields.params,
            current_subparams: fields.current_subparams,
            params_len: fields.params_len,
            param: fields.param,
            osc_raw: fields.osc_raw,
            osc_params: fields.osc_params,
            osc_num_params: fields.osc_num_params,
            ignoring: fields.ignoring,
            partial_utf8: fields.partial_utf8,
            partial_utf8_len: fields.partial_utf8_len,
        })
    }
}

/// Deserialize a sequence while rejecting a declared or observed element
/// count above the caller's field-specific limit before growing the output
/// vector beyond that limit.
pub(crate) fn deserialize_bounded_vec<'de, D, T>(
    deserializer: D,
    maximum: usize,
) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    struct BoundedVecVisitor<T> {
        maximum: usize,
        marker: PhantomData<T>,
    }

    impl<'de, T> Visitor<'de> for BoundedVecVisitor<T>
    where
        T: Deserialize<'de>,
    {
        type Value = Vec<T>;

        fn expecting(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
            formatter.write_str("a bounded sequence")
        }

        fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let size_hint = sequence.size_hint().unwrap_or(0);
            if size_hint > self.maximum {
                return Err(de::Error::custom("checkpoint sequence exceeds its field limit"));
            }

            let mut values = Vec::with_capacity(size_hint);
            while values.len() < self.maximum {
                match sequence.next_element()? {
                    Some(value) => values.push(value),
                    None => return Ok(values),
                }
            }

            if sequence.next_element::<de::IgnoredAny>()?.is_some() {
                return Err(de::Error::custom("checkpoint sequence exceeds its field limit"));
            }
            Ok(values)
        }
    }

    deserializer.deserialize_seq(BoundedVecVisitor { maximum, marker: PhantomData })
}

fn deserialize_osc_raw<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_bounded_vec(deserializer, MAX_CHECKPOINT_OSC_RAW)
}

impl ParserCheckpointV1 {
    /// The stable schema identifier used by this fork.
    pub const SCHEMA: &'static str = "herdr.vte.parser";
    /// The current DTO revision.
    pub const VERSION: u16 = 1;

    /// Validate all lengths, parser arrays, string ranges, and UTF-8 state.
    ///
    /// Validation runs before [`Parser::from_checkpoint`] clones its OSC
    /// payload or installs any parser state.
    pub fn validate(&self) -> Result<(), CheckpointError> {
        if self.schema != Self::SCHEMA {
            return Err(CheckpointError::Invalid("parser schema"));
        }
        if self.version != Self::VERSION {
            return Err(CheckpointError::Invalid("parser version"));
        }
        if usize::from(self.intermediate_idx) > MAX_INTERMEDIATES {
            return Err(CheckpointError::Invalid("intermediate index"));
        }
        let intermediate_idx = usize::from(self.intermediate_idx);
        match self.state {
            ParserStateV1::Escape
            | ParserStateV1::CsiEntry
            | ParserStateV1::DcsEntry
            | ParserStateV1::OscString
            | ParserStateV1::SosPmApcString
                if intermediate_idx != 0 =>
            {
                return Err(CheckpointError::Invalid("intermediate index for parser state"));
            },
            ParserStateV1::EscapeIntermediate
            | ParserStateV1::CsiIntermediate
            | ParserStateV1::DcsIntermediate
                if intermediate_idx == 0 =>
            {
                return Err(CheckpointError::Invalid("missing intermediate byte"));
            },
            ParserStateV1::CsiParam | ParserStateV1::DcsParam if intermediate_idx > 1 => {
                return Err(CheckpointError::Invalid("intermediate index for parser state"));
            },
            _ => {},
        }
        let private_intermediates_allowed = matches!(
            self.state,
            ParserStateV1::Ground
                | ParserStateV1::CsiParam
                | ParserStateV1::DcsParam
                | ParserStateV1::CsiIgnore
                | ParserStateV1::DcsIgnore
                | ParserStateV1::DcsPassthrough
        );
        if matches!(self.state, ParserStateV1::CsiIntermediate | ParserStateV1::DcsIntermediate) {
            // A private marker is collected while in Csi/DcsParam.  A later
            // standard intermediate then moves the parser into the matching
            // Intermediate state, so the reachable two-byte prefix is
            // [private, standard].  The first byte is standard when the
            // parser entered Intermediate directly from Csi/DcsEntry.
            let first = self.intermediates[0];
            let first_is_standard = (0x20..=0x2F).contains(&first);
            let first_is_private = (0x3C..=0x3F).contains(&first);
            let valid = if first_is_private {
                intermediate_idx == 2 && (0x20..=0x2F).contains(&self.intermediates[1])
            } else {
                first_is_standard
                    && self.intermediates[1..intermediate_idx]
                        .iter()
                        .all(|&byte| (0x20..=0x2F).contains(&byte))
            };
            if !valid {
                return Err(CheckpointError::Invalid("intermediate byte"));
            }
        } else if self.intermediates[..intermediate_idx].iter().any(|&byte| {
            !((0x20..=0x2F).contains(&byte)
                || private_intermediates_allowed && (0x3C..=0x3F).contains(&byte))
        }) {
            return Err(CheckpointError::Invalid("intermediate byte"));
        }
        if matches!(self.state, ParserStateV1::CsiParam | ParserStateV1::DcsParam)
            && intermediate_idx == 1
            && !(0x3C..=0x3F).contains(&self.intermediates[0])
        {
            return Err(CheckpointError::Invalid("private intermediate byte"));
        }
        let params_len = usize::from(self.params_len);
        if params_len > MAX_PARAMS {
            return Err(CheckpointError::Invalid("parameter length"));
        }
        validate_params(&self.params_subparams, params_len, self.current_subparams)?;

        if self.osc_raw.len() > MAX_CHECKPOINT_OSC_RAW {
            return Err(CheckpointError::LimitExceeded {
                field: "osc_raw",
                actual: self.osc_raw.len(),
                maximum: MAX_CHECKPOINT_OSC_RAW,
            });
        }
        let osc_num_params = usize::from(self.osc_num_params);
        if osc_num_params > MAX_OSC_PARAMS {
            return Err(CheckpointError::Invalid("OSC parameter count"));
        }
        if !matches!(self.state, ParserStateV1::OscString)
            && (!self.osc_raw.is_empty() || osc_num_params != 0)
        {
            return Err(CheckpointError::Invalid("OSC state outside OSC string"));
        }
        let mut previous_end = 0usize;
        for (index, &(start, end)) in self.osc_params.iter().enumerate().take(osc_num_params) {
            let start =
                usize::try_from(start).map_err(|_| CheckpointError::Invalid("OSC range start"))?;
            let end =
                usize::try_from(end).map_err(|_| CheckpointError::Invalid("OSC range end"))?;
            if start > end || end > self.osc_raw.len() {
                return Err(CheckpointError::Invalid("OSC range bounds"));
            }
            if index == 0 {
                if start != 0 {
                    return Err(CheckpointError::Invalid("OSC first range"));
                }
            } else if start != previous_end {
                return Err(CheckpointError::Invalid("OSC range ordering"));
            }
            previous_end = end;
        }

        let partial_len = usize::from(self.partial_utf8_len);
        if partial_len > self.partial_utf8.len() {
            return Err(CheckpointError::Invalid("partial UTF-8 length"));
        }
        if partial_len != 0 {
            if !matches!(self.state, ParserStateV1::Ground) {
                return Err(CheckpointError::Invalid("partial UTF-8 outside ground"));
            }
            validate_utf8_prefix(&self.partial_utf8[..partial_len])?;
        }
        Ok(())
    }

    /// Construct the standard-library parser after validating the complete
    /// checkpoint.
    #[cfg(feature = "std")]
    pub fn restore(&self) -> Result<Parser, CheckpointError> {
        Parser::from_checkpoint(self)
    }

    /// Construct a no-std parser after validating the complete checkpoint.
    #[cfg(not(feature = "std"))]
    pub fn restore<const OSC_RAW_BUF_SIZE: usize>(
        &self,
    ) -> Result<Parser<OSC_RAW_BUF_SIZE>, CheckpointError> {
        Parser::from_checkpoint(self)
    }
}

impl Default for ParserCheckpointV1 {
    fn default() -> Self {
        Self {
            schema: Self::SCHEMA.to_owned(),
            version: Self::VERSION,
            state: ParserStateV1::Ground,
            intermediates: [0; MAX_INTERMEDIATES],
            intermediate_idx: 0,
            params_subparams: [0; MAX_PARAMS],
            params: [0; MAX_PARAMS],
            current_subparams: 0,
            params_len: 0,
            param: 0,
            osc_raw: Vec::new(),
            osc_params: [(0, 0); MAX_OSC_PARAMS],
            osc_num_params: 0,
            ignoring: false,
            partial_utf8: [0; 4],
            partial_utf8_len: 0,
        }
    }
}

#[cfg(not(feature = "std"))]
impl<const OSC_RAW_BUF_SIZE: usize> Parser<OSC_RAW_BUF_SIZE> {
    /// Export every parser field which affects continuation at the next byte.
    ///
    /// # Panics
    ///
    /// Panics if the parser discarded bytes from an oversized OSC payload. Use
    /// [`Self::try_checkpoint`] when the input source is not already known to
    /// stay within the checkpoint limit.
    pub fn checkpoint(&self) -> ParserCheckpointV1 {
        assert!(
            !self.osc_overflowed(),
            "cannot checkpoint parser after OSC payload overflow; use try_checkpoint"
        );
        let (params_subparams, params, current_subparams, params_len) =
            self.params.checkpoint_parts();
        let osc_params = self.osc_params.map(|(start, end)| (start as u32, end as u32));

        ParserCheckpointV1 {
            schema: ParserCheckpointV1::SCHEMA.to_owned(),
            version: ParserCheckpointV1::VERSION,
            state: self.state.into(),
            intermediates: self.intermediates,
            intermediate_idx: self.intermediate_idx as u8,
            params_subparams,
            params,
            current_subparams,
            params_len: params_len as u8,
            param: self.param,
            osc_raw: self.osc_raw.iter().copied().collect(),
            osc_params,
            osc_num_params: self.osc_num_params as u8,
            ignoring: self.ignoring,
            partial_utf8: self.partial_utf8,
            partial_utf8_len: self.partial_utf8_len as u8,
        }
    }

    /// Export a checkpoint only if the current state is within fork limits.
    pub fn try_checkpoint(&self) -> Result<ParserCheckpointV1, CheckpointError> {
        if self.osc_overflowed() {
            return Err(CheckpointError::LimitExceeded {
                field: "osc_raw",
                actual: MAX_CHECKPOINT_OSC_RAW + 1,
                maximum: MAX_CHECKPOINT_OSC_RAW,
            });
        }
        if self.osc_raw.len() > MAX_CHECKPOINT_OSC_RAW {
            return Err(CheckpointError::LimitExceeded {
                field: "osc_raw",
                actual: self.osc_raw.len(),
                maximum: MAX_CHECKPOINT_OSC_RAW,
            });
        }
        let checkpoint = self.checkpoint();
        checkpoint.validate()?;
        Ok(checkpoint)
    }

    /// Restore a parser after validation, without replaying the input prefix.
    pub fn from_checkpoint(checkpoint: &ParserCheckpointV1) -> Result<Self, CheckpointError> {
        checkpoint.validate()?;
        #[cfg(not(feature = "std"))]
        if checkpoint.osc_raw.len() > OSC_RAW_BUF_SIZE {
            return Err(CheckpointError::LimitExceeded {
                field: "osc_raw",
                actual: checkpoint.osc_raw.len(),
                maximum: OSC_RAW_BUF_SIZE,
            });
        }

        #[cfg(feature = "std")]
        let osc_raw = checkpoint.osc_raw.clone();
        #[cfg(not(feature = "std"))]
        let osc_raw = {
            let mut value = arrayvec::ArrayVec::<u8, OSC_RAW_BUF_SIZE>::new();
            for byte in &checkpoint.osc_raw {
                value.push(*byte);
            }
            value
        };

        let mut osc_params = [(0usize, 0usize); MAX_OSC_PARAMS];
        for (target, &(start, end)) in osc_params.iter_mut().zip(checkpoint.osc_params.iter()) {
            *target = (start as usize, end as usize);
        }

        Ok(Self {
            state: checkpoint.state.into(),
            intermediates: checkpoint.intermediates,
            intermediate_idx: usize::from(checkpoint.intermediate_idx),
            params: Params::from_checkpoint_parts(
                checkpoint.params_subparams,
                checkpoint.params,
                checkpoint.current_subparams,
                usize::from(checkpoint.params_len),
            ),
            param: checkpoint.param,
            osc_raw,
            osc_params,
            osc_num_params: usize::from(checkpoint.osc_num_params),
            ignoring: checkpoint.ignoring,
            partial_utf8: checkpoint.partial_utf8,
            partial_utf8_len: usize::from(checkpoint.partial_utf8_len),
        })
    }

    /// Validate and atomically replace this parser with a checkpoint.
    pub fn restore_checkpoint(
        &mut self,
        checkpoint: &ParserCheckpointV1,
    ) -> Result<(), CheckpointError> {
        let restored = Self::from_checkpoint(checkpoint)?;
        *self = restored;
        Ok(())
    }
}

#[cfg(feature = "std")]
impl Parser {
    /// Export every parser field which affects continuation at the next byte.
    ///
    /// # Panics
    ///
    /// Panics if the parser discarded bytes from an oversized OSC payload. Use
    /// [`Self::try_checkpoint`] when the input source is not already known to
    /// stay within the checkpoint limit.
    pub fn checkpoint(&self) -> ParserCheckpointV1 {
        assert!(
            !self.osc_overflowed(),
            "cannot checkpoint parser after OSC payload overflow; use try_checkpoint"
        );
        let (params_subparams, params, current_subparams, params_len) =
            self.params.checkpoint_parts();
        let osc_params = self.osc_params.map(|(start, end)| (start as u32, end as u32));

        ParserCheckpointV1 {
            schema: ParserCheckpointV1::SCHEMA.to_owned(),
            version: ParserCheckpointV1::VERSION,
            state: self.state.into(),
            intermediates: self.intermediates,
            intermediate_idx: self.intermediate_idx as u8,
            params_subparams,
            params,
            current_subparams,
            params_len: params_len as u8,
            param: self.param,
            osc_raw: self.osc_raw.clone(),
            osc_params,
            osc_num_params: self.osc_num_params as u8,
            ignoring: self.ignoring,
            partial_utf8: self.partial_utf8,
            partial_utf8_len: self.partial_utf8_len as u8,
        }
    }

    /// Export a checkpoint only if the current state is within fork limits.
    pub fn try_checkpoint(&self) -> Result<ParserCheckpointV1, CheckpointError> {
        if self.osc_overflowed() {
            return Err(CheckpointError::LimitExceeded {
                field: "osc_raw",
                actual: MAX_CHECKPOINT_OSC_RAW + 1,
                maximum: MAX_CHECKPOINT_OSC_RAW,
            });
        }
        if self.osc_raw.len() > MAX_CHECKPOINT_OSC_RAW {
            return Err(CheckpointError::LimitExceeded {
                field: "osc_raw",
                actual: self.osc_raw.len(),
                maximum: MAX_CHECKPOINT_OSC_RAW,
            });
        }
        let checkpoint = self.checkpoint();
        checkpoint.validate()?;
        Ok(checkpoint)
    }

    /// Restore a parser after validation, without replaying the input prefix.
    pub fn from_checkpoint(checkpoint: &ParserCheckpointV1) -> Result<Self, CheckpointError> {
        checkpoint.validate()?;
        let mut osc_params = [(0usize, 0usize); MAX_OSC_PARAMS];
        for (target, &(start, end)) in osc_params.iter_mut().zip(checkpoint.osc_params.iter()) {
            *target = (start as usize, end as usize);
        }

        Ok(Self {
            state: checkpoint.state.into(),
            intermediates: checkpoint.intermediates,
            intermediate_idx: usize::from(checkpoint.intermediate_idx),
            params: Params::from_checkpoint_parts(
                checkpoint.params_subparams,
                checkpoint.params,
                checkpoint.current_subparams,
                usize::from(checkpoint.params_len),
            ),
            param: checkpoint.param,
            osc_raw: checkpoint.osc_raw.clone(),
            osc_params,
            osc_num_params: usize::from(checkpoint.osc_num_params),
            #[cfg(feature = "checkpoint")]
            osc_overflowed: false,
            ignoring: checkpoint.ignoring,
            partial_utf8: checkpoint.partial_utf8,
            partial_utf8_len: usize::from(checkpoint.partial_utf8_len),
        })
    }

    /// Validate and atomically replace this parser with a checkpoint.
    pub fn restore_checkpoint(
        &mut self,
        checkpoint: &ParserCheckpointV1,
    ) -> Result<(), CheckpointError> {
        let restored = Self::from_checkpoint(checkpoint)?;
        *self = restored;
        Ok(())
    }
}

fn validate_params(
    subparams: &[u8; MAX_PARAMS],
    len: usize,
    current_subparams: u8,
) -> Result<(), CheckpointError> {
    let current = usize::from(current_subparams);
    if current > MAX_PARAMS || current > len {
        return Err(CheckpointError::Invalid("current subparameter count"));
    }
    if len == 0 {
        if current != 0 {
            return Err(CheckpointError::Invalid("subparameters without parameters"));
        }
        return Ok(());
    }

    let open_group_start = (current != 0).then_some(len - current);
    let mut index = 0usize;
    while index < len {
        let group_len = usize::from(subparams[index]);
        if group_len == 0 || group_len > len - index {
            return Err(CheckpointError::Invalid("parameter group length"));
        }
        if let Some(open_start) = open_group_start {
            if index > open_start {
                return Err(CheckpointError::Invalid("open parameter group boundary"));
            }
            if index == open_start && group_len != current {
                return Err(CheckpointError::Invalid("open subparameter group length"));
            }
        }
        index += group_len;
    }
    if index != len {
        return Err(CheckpointError::Invalid("parameter groups do not cover length"));
    }

    // While a CSI/DCS parameter is still being collected, `current_subparams`
    // counts the already-collected subparameters of that open group. It may
    // equal `len`: the first colon turns the initial parameter into a group,
    // before the next parameter value has arrived.
    Ok(())
}

fn validate_utf8_prefix(bytes: &[u8]) -> Result<(), CheckpointError> {
    if bytes.is_empty() || bytes.len() >= 4 {
        return Err(CheckpointError::Invalid("UTF-8 prefix length"));
    }
    let expected = match bytes[0] {
        0xC2..=0xDF => 2,
        0xE0..=0xEF => 3,
        0xF0..=0xF4 => 4,
        _ => return Err(CheckpointError::Invalid("UTF-8 prefix lead byte")),
    };
    if bytes.len() >= expected {
        return Err(CheckpointError::Invalid("completed UTF-8 scalar"));
    }
    for (index, byte) in bytes.iter().copied().enumerate().skip(1) {
        if !(0x80..=0xBF).contains(&byte) {
            return Err(CheckpointError::Invalid("UTF-8 continuation byte"));
        }
        if index == 1 {
            match bytes[0] {
                0xE0 if byte < 0xA0 => {
                    return Err(CheckpointError::Invalid("overlong UTF-8 prefix"));
                },
                0xED if byte > 0x9F => {
                    return Err(CheckpointError::Invalid("UTF-8 surrogate prefix"));
                },
                0xF0 if byte < 0x90 => {
                    return Err(CheckpointError::Invalid("overlong UTF-8 prefix"));
                },
                0xF4 if byte > 0x8F => {
                    return Err(CheckpointError::Invalid("UTF-8 scalar range"));
                },
                _ => (),
            }
        }
    }
    // The explicit checks above are intentionally stricter than merely
    // accepting `from_utf8`'s incomplete error: a future parser revision must
    // not turn an already-complete scalar into continuation state.
    if str::from_utf8(bytes).is_ok() {
        return Err(CheckpointError::Invalid("completed UTF-8 scalar"));
    }
    Ok(())
}
