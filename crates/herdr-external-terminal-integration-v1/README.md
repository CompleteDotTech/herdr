# Herdr external terminal integration v1 (private draft)

This directory is the private owner integration draft. It is built against
the in-tree `coven-terminal` runtime, the checkpointable Alacritty and VTE
forks under `vendor/`, and the standalone Herdr cell renderer.

`ExternalTerminalSession` owns one
`TerminalSession<AlacrittyEngine>` behind an `RwLock`. The provider poll owner
uses `ingest` for each accepted raw chunk. A render takes one cached, complete model projection, copies the bounded active
viewport into structured cells, and then calls the standalone renderer. `recent_lines`, `visible_hyperlinks`, and
`selection_to_string` read that same model; no normalized transcript is used
when the raw model is attached. A presentation scroll offset is kept outside
the model and therefore does not race parser or model mutation.

The integration crate is intentionally independent of Coven transport. It does
not create a PTY, start a child process, send input, answer terminal queries,
replay ANSI, or invoke Herdr Ghostty. The existing normalized transcript path
remains the explicit fallback for providers that only expose text.

## Owner patch

The owner source changes live alongside this crate under `src/`; the intended
integration behavior is:

1. Keep an `Arc<ExternalTerminalSession>` beside the provider handle in
   `TerminalRuntimeRegistry`. Native `TerminalRuntime` accessors continue to
   return only native PTY runtimes.
2. Let provider startup or the raw transport attach one model session after
   identity validation. Polling remains the only writer and is responsible for
   raw chunk cursor checks, `ingest`, timeout effects, checkpoint persistence,
   and render dirty notification.
3. Dispatch application-shell panes, direct terminal attach/observe frames,
   and retained surfaces through complete external rendering. Pane layout and
   metadata use the model's scroll, alternate-screen, mouse, and revision
   fields. The retained path falls back to a complete frame for every external
   revision until a proven damage protocol exists.
4. Dispatch `pane.read`, coordinate selection, and copy motion/search to the
   same model snapshot. The transcript projection is still used when no model
   session is attached. External ANSI export remains unavailable until a real
   model exporter is added.
5. Reject external key, mouse, paste, resize-as-input, and process-control
   operations with the existing structured unsupported-operation response.
   `scroll` is the only read-only presentation mutation in this draft.

The render helper converts external cursor shape values to Herdr's existing
DECSCUSR cursor parameter and converts renderer hyperlink records to the
existing `FrameData` hyperlink tuple. It writes a complete `Buffer` before
frame conversion, preserving wide-cell skip/forced-width behavior and all
structured styles supported by the renderer.

## API seam with the checkpoint fork

The shared runtime already exposes the necessary stable operations:

```rust
let session = coven_terminal::TerminalSession::<AlacrittyEngine>::new(
    Geometry::new(columns, rows),
    QueryReplyPolicy::Quiet,
    MAX_REPLAY_BYTES,
    MAX_REPLAY_FRAMES,
)?;
let report = session.ingest(RawChunk::new(cursor, raw_bytes))?;
let checkpoint = session.checkpoint()?;
```

The checkpoint model exposes `TermCheckpointV1`, both grids, explicit cells,
269 palette entries, cursor and mode fields, and
`AlacrittyEngine::term().selection_to_string()`. The integration adapter uses
the public `ProcessorCheckpointV1` to expose synchronized-output state in the
renderer metadata. No private field layout or unsafe cast is used.

## Validation

The crate was formatted and checked with its own target directory:

```text
CARGO_TARGET_DIR=target/herdr-external-terminal-integration-v1 \
  cargo clippy --manifest-path crates/herdr-external-terminal-integration-v1/Cargo.toml \
  --all-targets -- -D warnings

CARGO_TARGET_DIR=target/herdr-external-terminal-integration-v1 \
  cargo test --manifest-path crates/herdr-external-terminal-integration-v1/Cargo.toml
```

The focused crate tests exercise the adapter against the in-tree
checkpoint/runtime forks. They do not prove host deployment or live provider
settlement.
