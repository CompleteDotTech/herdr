//! Shared wire protocol and presentation encoding code.

pub mod endpoint;
pub(crate) mod render_ansi;
pub(crate) mod surface;
mod wire;

pub use wire::*;
