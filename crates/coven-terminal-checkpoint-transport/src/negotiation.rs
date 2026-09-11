//! Negotiation that adds checkpoint transport without changing RawBytesV1.

use std::{error::Error, fmt};

use serde::{Deserialize, Serialize};

/// Existing observer protocol. Its `CTS1` header and payload semantics remain
/// unchanged so an old observer can continue to attach to a raw stream.
pub const RAW_PROTOCOL_VERSION: u16 = 1;
/// Version selected only for the new checkpoint-aware codec.
pub const CHECKPOINT_PROTOCOL_VERSION: u16 = 2;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TerminalCodec {
    #[default]
    RawBytesV1,
    CheckpointV1,
}

impl TerminalCodec {
    pub const fn protocol_version(self) -> u16 {
        match self {
            Self::RawBytesV1 => RAW_PROTOCOL_VERSION,
            Self::CheckpointV1 => CHECKPOINT_PROTOCOL_VERSION,
        }
    }
}

/// Fields supplied by an attach request after the authenticated owner route
/// has established the stream binding.
///
/// `control` is a requested capability on the same attachment. Negotiation
/// only selects the byte codec; it does not grant a control lease or permit
/// writes. The authenticated owner route must authorize that capability and
/// apply its single-writer lease rules separately.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CodecNegotiation {
    pub protocol_version: u16,
    pub codec: TerminalCodec,
    pub observe: bool,
    pub control: bool,
}

impl CodecNegotiation {
    pub const fn raw_observer() -> Self {
        Self {
            protocol_version: RAW_PROTOCOL_VERSION,
            codec: TerminalCodec::RawBytesV1,
            observe: true,
            control: false,
        }
    }

    pub const fn checkpoint_observer() -> Self {
        Self {
            protocol_version: CHECKPOINT_PROTOCOL_VERSION,
            codec: TerminalCodec::CheckpointV1,
            observe: true,
            control: false,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NegotiatedCodec {
    pub protocol_version: u16,
    pub codec: TerminalCodec,
}

/// Validate the exact version/codec pair. Version 1 does not acquire any
/// checkpoint behavior, and version 2 cannot be silently downgraded to raw
/// frames when the caller requested a coherent model stream. A control
/// request is accepted as a capability negotiation signal; authorization and
/// lease ownership remain outside this codec selector.
pub fn negotiate(request: CodecNegotiation) -> Result<NegotiatedCodec, NegotiationError> {
    if !request.observe {
        return Err(NegotiationError::ObserverOnly);
    }
    if !matches!(
        request.protocol_version,
        RAW_PROTOCOL_VERSION | CHECKPOINT_PROTOCOL_VERSION
    ) {
        return Err(NegotiationError::UnsupportedVersion {
            version: request.protocol_version,
        });
    }
    if request.protocol_version != request.codec.protocol_version() {
        return Err(NegotiationError::VersionCodecMismatch {
            version: request.protocol_version,
            codec: request.codec,
        });
    }
    Ok(NegotiatedCodec {
        protocol_version: request.protocol_version,
        codec: request.codec,
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NegotiationError {
    ObserverOnly,
    UnsupportedVersion { version: u16 },
    VersionCodecMismatch { version: u16, codec: TerminalCodec },
}

impl fmt::Display for NegotiationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ObserverOnly => formatter.write_str("terminal checkpoint codec is observe-only"),
            Self::UnsupportedVersion { version } => {
                write!(
                    formatter,
                    "terminal protocol version {version} is unsupported"
                )
            }
            Self::VersionCodecMismatch { version, codec } => write!(
                formatter,
                "terminal protocol version {version} does not match codec {codec:?}"
            ),
        }
    }
}

impl Error for NegotiationError {}
