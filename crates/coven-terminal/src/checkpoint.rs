//! The stable, bounded outer checkpoint envelope.

use std::{
    error::Error,
    fmt,
    io::{self, Write},
};

use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::session::{Geometry, SessionCursor};

/// Schema identifier for the shared checkpoint envelope.
pub const CHECKPOINT_SCHEMA: &str = "coven.external-terminal";
/// Current envelope revision.
pub const CHECKPOINT_VERSION: u16 = 1;
/// Maximum encoded checkpoint accepted at the persistence boundary.
pub const MAX_CHECKPOINT_BYTES: usize = 16 * 1024 * 1024;
/// Maximum JSON nesting accepted before handing input to Serde. This bounds
/// parser recursion independently of the encoded byte limit.
pub const MAX_JSON_DEPTH: usize = 256;
/// Maximum aggregate array/object members accepted by the JSON front-end.
/// This prevents a small token stream from requesting an unbounded `Vec`
/// capacity before the fork-level semantic validators run.
pub const MAX_JSON_CONTAINER_ITEMS: usize = 4 * 1024 * 1024;
/// Initial output-buffer capacity for bounded checkpoint encoding. Encoding
/// grows on demand up to `MAX_CHECKPOINT_BYTES` rather than reserving the full
/// limit for every small checkpoint.
const INITIAL_WRITER_CAPACITY: usize = 8 * 1024;

/// The exact model/parser dependency pair and fork-owned codec revisions that
/// produced a checkpoint.
///
/// A decoder must reject a different fingerprint. Upstream package commits do
/// not identify changes to private checkpoint DTOs, so the codec schema and
/// version are part of the identity as well. A checkpoint is not an invitation
/// to best-effort migrate private terminal state.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EngineFingerprint {
    pub model_package: String,
    pub model_version: String,
    pub model_revision: String,
    /// Versioned semantic schema of the model checkpoint DTO. The upstream
    /// commit does not identify fork-owned serialization changes.
    pub model_checkpoint_schema: String,
    pub model_checkpoint_version: u16,
    pub parser_package: String,
    pub parser_version: String,
    pub parser_revision: String,
    /// Versioned semantic schema of the parser/processor checkpoint DTO.
    pub processor_checkpoint_schema: String,
    pub processor_checkpoint_version: u16,
}

impl EngineFingerprint {
    pub fn alacritty_checkpoint_v1() -> Self {
        Self {
            model_package: "alacritty_terminal".to_owned(),
            model_version: "0.26.0".to_owned(),
            model_revision: "94e7c8874e526b1e67b349d9ba30ddf81669119e".to_owned(),
            model_checkpoint_schema: "herdr.external-terminal".to_owned(),
            model_checkpoint_version: 1,
            parser_package: "vte".to_owned(),
            parser_version: "0.15.0".to_owned(),
            parser_revision: "3b3da71c34cc1256c7e20981cf03f8eb95e08ffc".to_owned(),
            processor_checkpoint_schema: "herdr.vte.processor".to_owned(),
            processor_checkpoint_version: 1,
        }
    }
}

/// Full terminal state at the exact beginning of the next stream cursor.
///
/// `model` must include both screen buffers, scrollback, cursor/saved cursor,
/// modes, attributes, palette, titles, and model configuration. `processor`
/// must include the VTE parser state, REP predecessor, synchronized-update
/// buffer, and timeout state. The outer owner adds the source cursor and
/// geometry. No field is reconstructed from an ANSI prefix.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[serde(bound(
    serialize = "M: Serialize, P: Serialize",
    deserialize = "M: DeserializeOwned, P: DeserializeOwned"
))]
pub struct FullTerminalCheckpoint<M, P> {
    pub schema: String,
    pub version: u16,
    pub engine: EngineFingerprint,
    pub geometry: Geometry,
    /// The next source sequence expected by the owner. The first sequence is
    /// one; zero is never a valid frame sequence.
    pub next_sequence: u64,
    /// The next source byte offset expected by the owner.
    pub next_offset: u64,
    /// Monotonic model revision, independent of source sequence.
    pub revision: u64,
    pub model: M,
    pub processor: P,
}

impl<M, P> FullTerminalCheckpoint<M, P> {
    pub const SCHEMA: &'static str = CHECKPOINT_SCHEMA;
    pub const VERSION: u16 = CHECKPOINT_VERSION;

    pub fn cursor(&self) -> SessionCursor {
        SessionCursor {
            sequence: self.next_sequence,
            offset: self.next_offset,
        }
    }

    /// Validate envelope fields before handing model or parser bytes to a
    /// fork. Fork-level validators run only after this inexpensive check.
    pub fn validate_envelope(
        &self,
        expected_engine: &EngineFingerprint,
    ) -> Result<(), CheckpointEnvelopeError> {
        if self.schema != CHECKPOINT_SCHEMA {
            return Err(CheckpointEnvelopeError::Invalid("checkpoint schema"));
        }
        if self.version != CHECKPOINT_VERSION {
            return Err(CheckpointEnvelopeError::Invalid("checkpoint version"));
        }
        if &self.engine != expected_engine {
            return Err(CheckpointEnvelopeError::EngineMismatch);
        }
        self.geometry
            .validate()
            .map_err(CheckpointEnvelopeError::InvalidOwned)?;
        if self.next_sequence == 0 {
            return Err(CheckpointEnvelopeError::Invalid("next sequence"));
        }
        if self.revision % crate::REVISION_STEP != 0 {
            return Err(CheckpointEnvelopeError::Invalid("revision"));
        }
        Ok(())
    }
}

/// A malformed, mismatched, or over-sized checkpoint envelope.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CheckpointEnvelopeError {
    Invalid(&'static str),
    InvalidOwned(String),
    EngineMismatch,
    TooLarge {
        actual: usize,
        maximum: usize,
    },
    BudgetExceeded {
        field: &'static str,
        actual: usize,
        maximum: usize,
    },
}

impl fmt::Display for CheckpointEnvelopeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(reason) => write!(f, "invalid terminal checkpoint: {reason}"),
            Self::InvalidOwned(reason) => write!(f, "invalid terminal checkpoint: {reason}"),
            Self::EngineMismatch => f.write_str("terminal checkpoint engine fingerprint mismatch"),
            Self::TooLarge { actual, maximum } => {
                write!(
                    f,
                    "terminal checkpoint is {actual} bytes, maximum is {maximum}"
                )
            }
            Self::BudgetExceeded {
                field,
                actual,
                maximum,
            } => write!(
                f,
                "terminal checkpoint field {field} has {actual} items; maximum is {maximum}"
            ),
        }
    }
}

impl Error for CheckpointEnvelopeError {}

/// Errors at the encoded bytes boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CheckpointDecodeError {
    Envelope(CheckpointEnvelopeError),
    Json(String),
    TrailingBytes,
}

impl fmt::Display for CheckpointDecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Envelope(error) => error.fmt(f),
            Self::Json(error) => write!(f, "terminal checkpoint decode failed: {error}"),
            Self::TrailingBytes => f.write_str("terminal checkpoint has trailing bytes"),
        }
    }
}

impl Error for CheckpointDecodeError {}

#[cfg(feature = "serde-json")]
impl<M, P> FullTerminalCheckpoint<M, P>
where
    M: Serialize + DeserializeOwned,
    P: Serialize + DeserializeOwned,
{
    /// Encode with a hard upper bound. This method is intentionally JSON for
    /// the draft runtime because it gives a bounded, inspectable evidence
    /// artifact; the outer protocol may replace it with a bounded binary codec
    /// without changing the typed envelope.
    pub fn encode_bounded(&self) -> Result<Vec<u8>, CheckpointDecodeError> {
        let mut writer = BoundedWriter::new(MAX_CHECKPOINT_BYTES);
        let result = {
            let mut serializer = serde_json::Serializer::new(&mut writer);
            self.serialize(&mut serializer)
        };
        if let Err(error) = result {
            if writer.overflowed {
                return Err(CheckpointDecodeError::Envelope(
                    CheckpointEnvelopeError::TooLarge {
                        actual: writer.attempted,
                        maximum: MAX_CHECKPOINT_BYTES,
                    },
                ));
            }
            return Err(CheckpointDecodeError::Json(error.to_string()));
        }
        Ok(writer.into_inner())
    }

    /// Decode one complete value after checking the encoded size. The parser
    /// rejects a second JSON value instead of silently accepting a suffix.
    pub fn decode_bounded(bytes: &[u8]) -> Result<Self, CheckpointDecodeError> {
        if bytes.len() > MAX_CHECKPOINT_BYTES {
            return Err(CheckpointDecodeError::Envelope(
                CheckpointEnvelopeError::TooLarge {
                    actual: bytes.len(),
                    maximum: MAX_CHECKPOINT_BYTES,
                },
            ));
        }
        preflight_json_budget(bytes)?;
        let mut deserializer = serde_json::Deserializer::from_slice(bytes);
        let value = Self::deserialize(&mut deserializer)
            .map_err(|error| CheckpointDecodeError::Json(error.to_string()))?;
        deserializer
            .end()
            .map_err(|_| CheckpointDecodeError::TrailingBytes)?;
        Ok(value)
    }
}

#[cfg(feature = "serde-json")]
struct BoundedWriter {
    bytes: Vec<u8>,
    maximum: usize,
    attempted: usize,
    overflowed: bool,
}

#[cfg(feature = "serde-json")]
impl BoundedWriter {
    fn new(maximum: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(maximum.min(INITIAL_WRITER_CAPACITY)),
            maximum,
            attempted: 0,
            overflowed: false,
        }
    }

    fn into_inner(self) -> Vec<u8> {
        self.bytes
    }
}

#[cfg(feature = "serde-json")]
impl Write for BoundedWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let attempted = self.bytes.len().saturating_add(bytes.len());
        if attempted > self.maximum {
            self.attempted = attempted;
            self.overflowed = true;
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "terminal checkpoint size limit exceeded",
            ));
        }
        self.bytes.extend_from_slice(bytes);
        self.attempted = attempted;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(feature = "serde-json")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum JsonBudgetError {
    Malformed,
    Depth,
    Elements,
}

#[cfg(feature = "serde-json")]
fn preflight_json_budget(bytes: &[u8]) -> Result<(), CheckpointDecodeError> {
    let mut scanner = JsonBudgetScanner {
        bytes,
        index: 0,
        elements: 0,
    };
    scanner.skip_whitespace();
    scanner.scan_value(0).map_err(|error| match error {
        JsonBudgetError::Malformed => {
            CheckpointDecodeError::Json("malformed terminal checkpoint JSON".to_owned())
        }
        JsonBudgetError::Depth => {
            CheckpointDecodeError::Envelope(CheckpointEnvelopeError::BudgetExceeded {
                field: "json.depth",
                actual: MAX_JSON_DEPTH + 1,
                maximum: MAX_JSON_DEPTH,
            })
        }
        JsonBudgetError::Elements => {
            CheckpointDecodeError::Envelope(CheckpointEnvelopeError::BudgetExceeded {
                field: "json.container_items",
                actual: MAX_JSON_CONTAINER_ITEMS + 1,
                maximum: MAX_JSON_CONTAINER_ITEMS,
            })
        }
    })?;
    scanner.skip_whitespace();
    if scanner.index != bytes.len() {
        return Err(CheckpointDecodeError::TrailingBytes);
    }
    Ok(())
}

#[cfg(feature = "serde-json")]
struct JsonBudgetScanner<'a> {
    bytes: &'a [u8],
    index: usize,
    elements: usize,
}

#[cfg(feature = "serde-json")]
impl<'a> JsonBudgetScanner<'a> {
    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.index).copied()
    }

    fn skip_whitespace(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\n' | b'\r' | b'\t')) {
            self.index += 1;
        }
    }

    fn scan_value(&mut self, depth: usize) -> Result<(), JsonBudgetError> {
        if depth > MAX_JSON_DEPTH {
            return Err(JsonBudgetError::Depth);
        }
        match self.peek() {
            Some(b'{') => self.scan_object(depth),
            Some(b'[') => self.scan_array(depth),
            Some(b'"') => self.scan_string(),
            Some(b't') => self.scan_literal(b"true"),
            Some(b'f') => self.scan_literal(b"false"),
            Some(b'n') => self.scan_literal(b"null"),
            Some(b'-' | b'0'..=b'9') => self.scan_number(),
            _ => Err(JsonBudgetError::Malformed),
        }
    }

    fn count_element(&mut self) -> Result<(), JsonBudgetError> {
        self.elements = self
            .elements
            .checked_add(1)
            .ok_or(JsonBudgetError::Elements)?;
        if self.elements > MAX_JSON_CONTAINER_ITEMS {
            return Err(JsonBudgetError::Elements);
        }
        Ok(())
    }

    fn scan_array(&mut self, depth: usize) -> Result<(), JsonBudgetError> {
        self.index += 1;
        self.skip_whitespace();
        if self.peek() == Some(b']') {
            self.index += 1;
            return Ok(());
        }
        loop {
            self.count_element()?;
            self.scan_value(depth + 1)?;
            self.skip_whitespace();
            match self.peek() {
                Some(b',') => {
                    self.index += 1;
                    self.skip_whitespace();
                }
                Some(b']') => {
                    self.index += 1;
                    return Ok(());
                }
                _ => return Err(JsonBudgetError::Malformed),
            }
        }
    }

    fn scan_object(&mut self, depth: usize) -> Result<(), JsonBudgetError> {
        self.index += 1;
        self.skip_whitespace();
        if self.peek() == Some(b'}') {
            self.index += 1;
            return Ok(());
        }
        loop {
            self.count_element()?;
            self.scan_string()?;
            self.skip_whitespace();
            if self.peek() != Some(b':') {
                return Err(JsonBudgetError::Malformed);
            }
            self.index += 1;
            self.skip_whitespace();
            self.scan_value(depth + 1)?;
            self.skip_whitespace();
            match self.peek() {
                Some(b',') => {
                    self.index += 1;
                    self.skip_whitespace();
                }
                Some(b'}') => {
                    self.index += 1;
                    return Ok(());
                }
                _ => return Err(JsonBudgetError::Malformed),
            }
        }
    }

    fn scan_string(&mut self) -> Result<(), JsonBudgetError> {
        if self.peek() != Some(b'"') {
            return Err(JsonBudgetError::Malformed);
        }
        self.index += 1;
        while let Some(byte) = self.peek() {
            match byte {
                b'"' => {
                    self.index += 1;
                    return Ok(());
                }
                b'\\' => {
                    self.index += 1;
                    let Some(escaped) = self.peek() else {
                        return Err(JsonBudgetError::Malformed);
                    };
                    self.index += 1;
                    if escaped == b'u' {
                        for _ in 0..4 {
                            let Some(hex) = self.peek() else {
                                return Err(JsonBudgetError::Malformed);
                            };
                            if !hex.is_ascii_hexdigit() {
                                return Err(JsonBudgetError::Malformed);
                            }
                            self.index += 1;
                        }
                    }
                }
                0..=0x1f => return Err(JsonBudgetError::Malformed),
                _ => self.index += 1,
            }
        }
        Err(JsonBudgetError::Malformed)
    }

    fn scan_literal(&mut self, literal: &[u8]) -> Result<(), JsonBudgetError> {
        let end = self
            .index
            .checked_add(literal.len())
            .ok_or(JsonBudgetError::Malformed)?;
        if self.bytes.get(self.index..end) != Some(literal) {
            return Err(JsonBudgetError::Malformed);
        }
        self.index = end;
        Ok(())
    }

    fn scan_number(&mut self) -> Result<(), JsonBudgetError> {
        let start = self.index;
        while let Some(byte) = self.peek() {
            if matches!(byte, b' ' | b'\n' | b'\r' | b'\t' | b',' | b']' | b'}') {
                break;
            }
            self.index += 1;
        }
        (self.index > start)
            .then_some(())
            .ok_or(JsonBudgetError::Malformed)
    }
}
