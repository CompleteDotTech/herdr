#![deny(unsafe_code)]

//! Standalone rendering bridge for a structured external terminal model.
//!
//! This crate deliberately accepts a borrowed, already-emulated cell view. It
//! does not parse bytes, replay ANSI, create a PTY, answer terminal queries,
//! or call Herdr's Ghostty runtime. The checkpoint/model owner supplies one
//! stable [`ExternalTerminalView`] under its own revision protocol, then this
//! module writes that view directly to a ratatui [`Buffer`].

mod external_renderer;

pub use external_renderer::*;
