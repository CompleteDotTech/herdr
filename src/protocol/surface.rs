//! Negotiated shell surface encoding with explicit terminal cell widths.
//!
//! `shell.surface.v1` is immutable. Version 2 uses the existing two-string
//! `EndpointControl` envelope with kind `shell.surface.v2`. Its data is standard
//! base64 of `[u32LE core length][frozen v1 ServerMessage][u32LE width count]
//! [u16LE widths]`. Only PaneSurface and PaneSurfacePatch are allowed. Widths
//! follow the main frame then popup frame, or patch rows, in cell order. Zero
//! selects Unicode width; a positive value is the terminal model's width.
//!
//! The side channel is written without cloning cells and decoded directly into
//! the owned message. The existing endpoint presentation and patch validation
//! paths then consume that message unchanged.

use std::io::{self, Write};

use base64::{engine::general_purpose::STANDARD, Engine as _};

use super::{
    CellData, FrameData, FramingError, ServerMessage, MAX_FRAME_SIZE, MAX_GRAPHICS_FRAME_SIZE,
};

pub(crate) const SURFACE_CODEC_V2: &str = "shell.surface.v2";
const MAX_DIMENSION: u16 = 4096;
const MAX_CELLS: usize = 1_000_000;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum SurfaceCodec {
    #[default]
    V1,
    V2,
}

impl SurfaceCodec {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::V1 => super::endpoint::SURFACE_CODEC_V1,
            Self::V2 => SURFACE_CODEC_V2,
        }
    }

    pub(crate) fn from_name(name: &str) -> Option<Self> {
        match name {
            super::endpoint::SURFACE_CODEC_V1 => Some(Self::V1),
            SURFACE_CODEC_V2 => Some(Self::V2),
            _ => None,
        }
    }

    /// Serialize through the negotiated render lane, retaining its size cap.
    pub(crate) fn frame(
        self,
        message: &ServerMessage,
        max: usize,
    ) -> Result<Vec<u8>, FramingError> {
        let mut framed = Vec::new();
        if self == Self::V2 && is_surface(message) {
            let data = encode_surface(message, max)?;
            super::write_message(
                &mut framed,
                &ServerMessage::EndpointControl {
                    kind: SURFACE_CODEC_V2.into(),
                    data,
                },
            )?;
        } else {
            super::write_message(&mut framed, message)?;
        }
        let claimed = framed.len().saturating_sub(4);
        if claimed > max {
            return Err(FramingError::Oversized { claimed, max });
        }
        Ok(framed)
    }

    /// Normalize on the connection's reader before any presentation policy.
    pub(crate) fn decode(
        self,
        message: ServerMessage,
        max: usize,
    ) -> Result<ServerMessage, FramingError> {
        if let ServerMessage::EndpointControl { kind, data } = &message {
            if kind == SURFACE_CODEC_V2 {
                if self != Self::V2 {
                    return Err(invalid("surface codec was not negotiated"));
                }
                return decode_surface(data, max);
            }
        }
        if self == Self::V2 && is_surface(&message) {
            return Err(invalid("surface did not use the negotiated codec"));
        }
        Ok(message)
    }
}

fn invalid(message: &str) -> FramingError {
    FramingError::Bincode(message.into())
}

fn is_surface(message: &ServerMessage) -> bool {
    matches!(
        message,
        ServerMessage::PaneSurface(_) | ServerMessage::PaneSurfacePatch(_)
    )
}

fn frame_cell_count(frame: &FrameData) -> Result<usize, FramingError> {
    let expected = usize::from(frame.width) * usize::from(frame.height);
    if frame.width == 0
        || frame.height == 0
        || frame.width > MAX_DIMENSION
        || frame.height > MAX_DIMENSION
        || expected > MAX_CELLS
        || expected != frame.cells.len()
    {
        return Err(invalid("invalid surface frame geometry"));
    }
    Ok(expected)
}

fn cell_count(message: &ServerMessage) -> Result<usize, FramingError> {
    match message {
        ServerMessage::PaneSurface(surface) => {
            let main = frame_cell_count(&surface.frame)?;
            let popup = surface
                .popup
                .as_ref()
                .map(|popup| frame_cell_count(&popup.frame))
                .transpose()?
                .unwrap_or(0);
            Ok(main + popup)
        }
        ServerMessage::PaneSurfacePatch(patch) => {
            let mut count = 0_usize;
            for row in &patch.rows {
                if row.x >= MAX_DIMENSION
                    || row.y >= MAX_DIMENSION
                    || row.cells.len() > usize::from(MAX_DIMENSION - row.x)
                {
                    return Err(invalid("invalid surface patch geometry"));
                }
                count = count.saturating_add(row.cells.len());
                if count > MAX_CELLS {
                    return Err(invalid("surface patch exceeds cell limit"));
                }
            }
            Ok(count)
        }
        _ => Err(invalid("invalid message in surface codec")),
    }
}

fn visit_cells(message: &ServerMessage, mut visit: impl FnMut(&CellData)) {
    match message {
        ServerMessage::PaneSurface(surface) => {
            surface.frame.cells.iter().for_each(&mut visit);
            if let Some(popup) = &surface.popup {
                popup.frame.cells.iter().for_each(visit);
            }
        }
        ServerMessage::PaneSurfacePatch(patch) => {
            for row in &patch.rows {
                row.cells.iter().for_each(&mut visit);
            }
        }
        _ => {}
    }
}

fn visit_cells_mut(message: &mut ServerMessage, mut visit: impl FnMut(&mut CellData)) {
    match message {
        ServerMessage::PaneSurface(surface) => {
            surface.frame.cells.iter_mut().for_each(&mut visit);
            if let Some(popup) = &mut surface.popup {
                popup.frame.cells.iter_mut().for_each(visit);
            }
        }
        ServerMessage::PaneSurfacePatch(patch) => {
            for row in &mut patch.rows {
                row.cells.iter_mut().for_each(&mut visit);
            }
        }
        _ => {}
    }
}

struct LimitedBuffer {
    bytes: Vec<u8>,
    max: usize,
    exceeded: bool,
}

impl Write for LimitedBuffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.max.saturating_sub(self.bytes.len()) {
            self.exceeded = true;
            return Err(io::Error::other("surface exceeds frame limit"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn encode_surface(message: &ServerMessage, max: usize) -> Result<String, FramingError> {
    let count = cell_count(message)?;
    let binary_limit = max.min(MAX_GRAPHICS_FRAME_SIZE).saturating_mul(3) / 4;
    let width_bytes = count * 2;
    let Some(core_limit) = binary_limit.checked_sub(width_bytes + 8) else {
        return Err(FramingError::Oversized {
            claimed: width_bytes + 8,
            max,
        });
    };
    let mut buffer = LimitedBuffer {
        bytes: Vec::new(),
        max: core_limit,
        exceeded: false,
    };
    let encoded =
        bincode::serde::encode_into_std_write(message, &mut buffer, bincode::config::standard());
    if buffer.exceeded {
        return Err(FramingError::Oversized {
            claimed: max.saturating_add(1),
            max,
        });
    }
    encoded.map_err(|_| invalid("could not encode surface"))?;
    let mut bytes = Vec::with_capacity(buffer.bytes.len() + width_bytes + 8);
    bytes.extend_from_slice(&(buffer.bytes.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&buffer.bytes);
    bytes.extend_from_slice(&(count as u32).to_le_bytes());
    let mut valid = true;
    visit_cells(message, |cell| {
        valid &= cell.width <= MAX_DIMENSION;
        bytes.extend_from_slice(&cell.width.to_le_bytes());
    });
    if !valid {
        return Err(invalid("surface width exceeds geometry limit"));
    }
    Ok(STANDARD.encode(bytes))
}

fn decode_surface(data: &str, max: usize) -> Result<ServerMessage, FramingError> {
    if data.len() > max.min(MAX_GRAPHICS_FRAME_SIZE) {
        return Err(FramingError::Oversized {
            claimed: data.len(),
            max,
        });
    }
    let bytes = STANDARD
        .decode(data)
        .map_err(|_| invalid("invalid surface base64"))?;
    let core_length = read_u32(&bytes, 0)? as usize;
    let count_offset = core_length
        .checked_add(4)
        .ok_or_else(|| invalid("surface length overflow"))?;
    let count = read_u32(&bytes, count_offset)? as usize;
    if count > 2 * MAX_CELLS {
        return Err(invalid("surface width count exceeds cell limit"));
    }
    let widths_offset = count_offset
        .checked_add(4)
        .ok_or_else(|| invalid("surface length overflow"))?;
    if widths_offset.checked_add(count * 2) != Some(bytes.len()) {
        return Err(invalid("surface width payload length mismatch"));
    }
    // Bincode's limit is a const generic. Match the two supported connection
    // budgets instead of allowing the graphics allocation budget on every
    // ordinary connection. Byte length is also checked against `max` above.
    let decoded = if max <= MAX_FRAME_SIZE {
        bincode::serde::decode_from_slice(
            &bytes[4..count_offset],
            bincode::config::standard().with_limit::<MAX_FRAME_SIZE>(),
        )
    } else {
        bincode::serde::decode_from_slice(
            &bytes[4..count_offset],
            bincode::config::standard().with_limit::<MAX_GRAPHICS_FRAME_SIZE>(),
        )
    };
    let (mut message, used): (ServerMessage, _) = decoded.map_err(|error| {
        if matches!(error, bincode::error::DecodeError::LimitExceeded) {
            invalid("surface core exceeds connection allocation budget")
        } else {
            invalid("invalid surface core")
        }
    })?;
    if used != core_length || cell_count(&message)? != count {
        return Err(invalid("surface cell count mismatch"));
    }
    let mut widths = bytes[widths_offset..].chunks_exact(2);
    let mut valid = true;
    visit_cells_mut(&mut message, |cell| {
        if let Some(width) = widths.next() {
            cell.width = u16::from_le_bytes([width[0], width[1]]);
            valid &= cell.width <= MAX_DIMENSION;
        }
    });
    if !valid {
        return Err(invalid("surface width exceeds geometry limit"));
    }
    Ok(message)
}

fn read_u32(bytes: &[u8], offset: usize) -> Result<u32, FramingError> {
    let end = offset
        .checked_add(4)
        .ok_or_else(|| invalid("surface length overflow"))?;
    let value = bytes
        .get(offset..end)
        .ok_or_else(|| invalid("truncated surface header"))?;
    Ok(u32::from_le_bytes([value[0], value[1], value[2], value[3]]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{
        ClientShellPopupSurface, PaneSurfaceFrame, PaneSurfacePatch, PaneSurfacePatchRow,
        SurfaceGraphicsScene, MAX_FRAME_SIZE,
    };

    fn frame() -> FrameData {
        let mut buffer = ratatui::buffer::Buffer::empty(ratatui::layout::Rect::new(0, 0, 2, 1));
        buffer[(0, 0)].set_symbol("❤\u{fe0f}");
        buffer[(1, 0)].set_symbol("B");
        let mut frame = FrameData::from_ratatui_buffer_with_hyperlinks(&buffer, None, &[]);
        // The terminal's columns differ from the Unicode width table.
        frame.cells[0].width = 1;
        frame.cells[1].width = 1;
        frame
    }

    fn surface() -> ServerMessage {
        let mut popup = frame();
        popup.cells[0].symbol = "界".into();
        popup.cells[0].width = 2;
        popup.cells[1].symbol.clear();
        popup.cells[1].width = 0;
        popup.cells[1].skip = true;
        ServerMessage::PaneSurface(PaneSurfaceFrame {
            boot_id: "boot".into(),
            projection_revision: 4,
            surface_revision: 7,
            frame: frame(),
            panes: Vec::new(),
            splits: Vec::new(),
            popup: Some(Box::new(ClientShellPopupSurface {
                terminal_id: "popup".into(),
                title: "popup".into(),
                width: None,
                height: None,
                frame: popup,
                mouse_reporting: false,
                sgr_pixel_mouse: false,
                pixel_width: 0,
                pixel_height: 0,
            })),
            graphics: SurfaceGraphicsScene::default(),
        })
    }

    fn wire(message: &ServerMessage, codec: SurfaceCodec) -> ServerMessage {
        let bytes = codec.frame(message, MAX_FRAME_SIZE).unwrap();
        super::super::read_message(&mut bytes.as_slice(), MAX_FRAME_SIZE).unwrap()
    }

    #[test]
    fn full_surface_and_popup_widths_survive_real_framing() {
        let message = surface();
        let encoded = wire(&message, SurfaceCodec::V2);
        assert!(
            matches!(&encoded, ServerMessage::EndpointControl { kind, .. } if kind == SURFACE_CODEC_V2)
        );
        let restored = SurfaceCodec::V2.decode(encoded, MAX_FRAME_SIZE).unwrap();
        assert_eq!(restored, message);
        let ServerMessage::PaneSurface(surface) = restored else {
            panic!("surface expected")
        };
        let buffer = surface.frame.to_ratatui_buffer().unwrap();
        assert!(
            matches!(buffer[(0, 0)].diff_option, ratatui::buffer::CellDiffOption::ForcedWidth(width) if width.get() == 1)
        );
        assert_eq!(buffer[(1, 0)].symbol(), "B");
    }

    #[test]
    fn patch_widths_follow_rows_and_keep_revision_identity() {
        let message = ServerMessage::PaneSurfacePatch(PaneSurfacePatch {
            boot_id: "boot".into(),
            projection_revision: 4,
            base_surface_revision: 7,
            surface_revision: 8,
            rows: vec![
                PaneSurfacePatchRow {
                    x: 5,
                    y: 3,
                    cells: frame().cells,
                },
                PaneSurfacePatchRow {
                    x: 0,
                    y: 4,
                    cells: frame().cells,
                },
            ],
            panes: Vec::new(),
            cursor: None,
        });
        let restored = SurfaceCodec::V2
            .decode(wire(&message, SurfaceCodec::V2), MAX_FRAME_SIZE)
            .unwrap();
        assert_eq!(restored, message);
    }

    #[test]
    fn v1_bytes_remain_unchanged_and_wrong_codec_is_rejected() {
        let message = surface();
        let mut baseline = Vec::new();
        super::super::write_message(&mut baseline, &message).unwrap();
        assert_eq!(
            SurfaceCodec::V1.frame(&message, MAX_FRAME_SIZE).unwrap(),
            baseline
        );
        let legacy = wire(&message, SurfaceCodec::V1);
        let ServerMessage::PaneSurface(surface) = &legacy else {
            panic!("surface expected")
        };
        assert_eq!(surface.frame.cells[0].width, 0);
        assert!(SurfaceCodec::V1
            .decode(legacy.clone(), MAX_FRAME_SIZE)
            .is_ok());
        assert!(SurfaceCodec::V2.decode(legacy, MAX_FRAME_SIZE).is_err());
        assert!(SurfaceCodec::V1
            .decode(wire(&message, SurfaceCodec::V2), MAX_FRAME_SIZE)
            .is_err());
        let health = ServerMessage::EndpointControl {
            kind: super::super::endpoint::HEALTH_PONG_KIND.into(),
            data: "{}".into(),
        };
        assert_eq!(
            SurfaceCodec::V2
                .decode(health.clone(), MAX_FRAME_SIZE)
                .unwrap(),
            health
        );
    }

    #[test]
    fn malformed_width_count_trailing_bytes_and_widths_fail_closed() {
        let data = encode_surface(&surface(), MAX_FRAME_SIZE).unwrap();
        let bytes = STANDARD.decode(data).unwrap();
        let count_offset = read_u32(&bytes, 0).unwrap() as usize + 4;
        for count in [0, 3, 5, u32::MAX] {
            let mut broken = bytes.clone();
            broken[count_offset..count_offset + 4].copy_from_slice(&count.to_le_bytes());
            assert!(decode_surface(&STANDARD.encode(broken), MAX_FRAME_SIZE).is_err());
        }
        let mut trailing = bytes.clone();
        trailing.push(0);
        assert!(decode_surface(&STANDARD.encode(trailing), MAX_FRAME_SIZE).is_err());
        let mut width = bytes.clone();
        width[count_offset + 4..count_offset + 6].copy_from_slice(&u16::MAX.to_le_bytes());
        assert!(decode_surface(&STANDARD.encode(width), MAX_FRAME_SIZE).is_err());
        for end in 0..bytes.len() {
            assert!(decode_surface(&STANDARD.encode(&bytes[..end]), MAX_FRAME_SIZE).is_err());
        }
    }

    #[test]
    fn invalid_shapes_non_surface_messages_and_size_limits_are_rejected() {
        let mut message = surface();
        let ServerMessage::PaneSurface(value) = &mut message else {
            panic!("surface expected")
        };
        value.frame.width = 3;
        assert!(encode_surface(&message, MAX_FRAME_SIZE).is_err());
        let non_surface = ServerMessage::ServerShutdown { reason: None };
        let core =
            bincode::serde::encode_to_vec(&non_surface, bincode::config::standard()).unwrap();
        let mut bytes = (core.len() as u32).to_le_bytes().to_vec();
        bytes.extend(core);
        bytes.extend(0_u32.to_le_bytes());
        assert!(decode_surface(&STANDARD.encode(bytes), MAX_FRAME_SIZE).is_err());
        assert!(SurfaceCodec::V2.frame(&surface(), 16).is_err());
        let encoded = encode_surface(&surface(), MAX_FRAME_SIZE).unwrap();
        assert!(decode_surface(&encoded, encoded.len() - 1).is_err());
    }

    #[test]
    fn ordinary_surface_rejects_graphics_sized_inner_allocation_before_reading_it() {
        let core = bincode::serde::encode_to_vec(surface(), bincode::config::standard()).unwrap();
        assert!(
            core[0] < 251,
            "frozen surface enum tag is a one-byte varint"
        );
        let mut malicious_core = vec![core[0]];
        // The first field is boot_id. Advertise a large string but provide no
        // bytes: the allocation budget must fail before attempting that read.
        malicious_core.extend(
            bincode::serde::encode_to_vec(MAX_FRAME_SIZE + 1, bincode::config::standard()).unwrap(),
        );
        let mut bytes = (malicious_core.len() as u32).to_le_bytes().to_vec();
        bytes.extend(malicious_core);
        bytes.extend(0_u32.to_le_bytes());
        let error = decode_surface(&STANDARD.encode(bytes), MAX_FRAME_SIZE).unwrap_err();
        assert!(matches!(error, FramingError::Bincode(message)
            if message == "surface core exceeds connection allocation budget"));
    }
}
