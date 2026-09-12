# Herdr external terminal renderer v1

This draft is a small, standalone bridge from a checkpointed external terminal
model to a `ratatui::buffer::Buffer`. It is intentionally independent of the
Herdr owner checkout and of the provider transport. A future
`ExternalTerminalSession` supplies one stable `ExternalTerminalView` while
holding the model read lock; this crate validates that view and writes one
complete surface directly into the destination buffer.

The public entry points are:

```rust
use herdr_external_terminal_renderer_v1::{
    render_into_buffer, ExternalRenderOptions, ExternalTerminalView,
};

let output = render_into_buffer(&mut buffer, area, &view, ExternalRenderOptions::default())?;
```

An owner that wants to hide the view construction behind a session can
implement `ExternalRenderSource` and call `render_model_into_buffer`. The
renderer never creates a PTY, obtains a provider handle, parses bytes, replays
ANSI, writes input, answers terminal queries, or invokes Herdr's Ghostty
runtime.

## View contract

`ExternalTerminalView` is a borrowed, complete viewport snapshot:

- `content_revision` must be even and stable. Odd revisions represent an
  owner mutation in progress and are rejected before the destination buffer is
  touched.
- `columns`, `screen_lines`, and every `ExternalRowView` must agree exactly.
  Rows are already selected by the model's display offset and are in
  top-to-bottom order.
- Each `ExternalCellView` carries the base character, zero-width combining
  characters, foreground/background/underline colors, all pinned Alacritty
  flags, explicit narrow/wide/spacer width, and an optional hyperlink index.
- `ExternalPalette` keeps the 269 named color slots. `ExternalTheme` supplies
  defaults for named foreground, background, and cursor entries.
- `ExternalCursor`, `ExternalScrollMetrics`, and `ExternalModes` remain
  structured output metadata. The renderer does not infer them from text.

`ExternalTerminalView::validate` checks dimensions, row and cell shape,
revision stability, cursor bounds, scroll metrics, named-color bounds,
hyperlink references and size limits, and consistency between width tags and
wide-cell flags. Validation runs before any reset or cell write, so a malformed
or in-flight model cannot partially replace a prior frame.

## Render behavior

The complete requested area is first reset to the external theme. The viewport
is then copied cell by cell:

- Base plus zero-width characters becomes one ratatui symbol. Wide leading
  cells receive `CellDiffOption::ForcedWidth(2)`; wide and leading spacers keep
  their style but use `CellDiffOption::Skip`, so the second column is not
  emitted as a second glyph.
- Named colors resolve through the 269-entry palette, indexed colors stay
  indexed, and RGB colors become ratatui RGB. Inverse swaps resolved colors.
  Bold, italic, dim, hidden, crossed-out, underline variants, and underline
  color are retained in ratatui style data. The pinned Herdr high modifier
  bits preserve the five underline wire codes.
- Hyperlinks are returned as `ExternalRenderedHyperlink` metadata with absolute
  buffer coordinates, the full rendered symbol, ID, and URI. A URI is never
  opened or treated as a local path.
- Cursor metadata is returned separately because a ratatui `Buffer` has no
  cursor primitive. The default policy hides it. The read-only focused policy
  exposes it only when the model is at the bottom of its scrollback and the
  model cursor is visible.
- The output carries content revision, scroll metrics, mode bits, hyperlink
  metadata, and render statistics for the surrounding Herdr surface protocol.

The renderer deliberately emits a complete frame. Dirty-patch production,
scroll mutation, checkpointing, synchronized-output timeout handling, and
provider sequence validation belong to the single external session owner.

## Proposed Herdr integration diff

The owner integration in `../herdr-external-terminal-integration-v1` and the
surrounding `src/` modules use this renderer beside the provider handle. The
following sections document those seams and their ownership boundaries.

### 1. Keep the model beside the provider handle

The current registry at
`src/terminal/runtime_registry.rs:11-66` stores native `TerminalRuntime`
values in `runtimes` and provider `ProviderRuntimeHandle` values in `external`.
Add a separately owned external session map rather than wrapping a provider in
a fake native runtime:

```diff
 struct TerminalRuntimeRegistry {
     runtimes: HashMap<TerminalId, TerminalRuntime>,
     external: HashMap<TerminalId, crate::runtime_provider::ProviderRuntimeHandle>,
+    external_sessions: HashMap<TerminalId, ExternalTerminalSession>,
 }

+pub(crate) struct ExternalTerminalSession {
+    // The session owns the checkpointed Term + vte Processor and its source
+    // cursor. Its render_snapshot method returns a borrowed or copied view
+    // stamped with one even revision.
+}
```

The registry's native `get`, `insert`, and PTY handoff methods must continue to
see only `runtimes`. Add explicit `external_session` and shutdown/restore
methods. The provider poll owner is the only writer: it applies raw chunks,
resolves synchronized-output timeouts, resizes, checkpoints, advances the
source cursor, and emits the existing render dirty signal. Renderers and API
readers call `render_snapshot`, `recent_lines`, `selection_to_string`, or
`visible_hyperlinks` on a read snapshot.

The existing provider-only state confirms this separation. `TerminalState` has
`external_binding`, `external_observation`, and a normalized
`external_transcript` at `src/terminal/state.rs:120-127`; that transcript remains
the fallback for providers that do not expose the raw terminal contract.

### 2. Dispatch application-shell panes to the model renderer

The current branch at `src/ui/panes.rs:402-418` first renders a native runtime
and otherwise calls `transcript_view::render` when `external_binding` exists.
Once a raw session is attached, dispatch it through the renderer and preserve
the normalized fallback branch:

```diff
         if let Some(rt) = app.runtime_for_pane_in_workspace(...) {
             ... native Ghostty render ...
         } else if let Some(terminal) = ... {
-            if terminal.external_binding.is_some() {
+            if let Some(session) = terminal_runtimes.external_session(&terminal.id) {
+                let view = session.render_snapshot(info.inner_rect)?;
+                let rendered = render_into_buffer(
+                    frame.buffer_mut(),
+                    info.inner_rect,
+                    &view,
+                    ExternalRenderOptions::default(),
+                )?;
+                render_external_pane_scrollbar(app, frame, info, rendered.scroll);
+            } else if terminal.external_binding.is_some() {
                 crate::runtime_provider::transcript_view::render(
                     terminal, frame.buffer_mut(), info.inner_rect, &app.palette,
                 );
             }
         }
```

The concrete call should use the session's owned snapshot lifetime; the
pseudocode uses `?` only to show the error boundary. A render error should
leave the old frame or trigger a complete retry according to the existing
render-loop policy, while a permanently invalid checkpoint should mark the
attachment unavailable and request provider reset. The transcript fallback
must remain visibly labeled as read-only text and must never be described as an
emulated terminal.

The external scrollbar uses `ExternalScrollMetrics`. It must be suppressed for
alternate-screen mode and must not change the model's display offset from the
render path. If clients need independent scroll positions, the owner resolves
each requested logical viewport into a fresh read snapshot.

### 3. Add direct attach/observe dispatch

The direct path in `src/server/headless/render.rs:511-545` currently resolves a
native runtime, calls `render_terminal_virtual`, asks Ghostty for hyperlinks,
and converts the ratatui buffer to `FrameData`. Add the external branch beside
the external session:

```diff
         ClientConnectionMode::TerminalAttach { terminal_id }
         | ClientConnectionMode::TerminalObserve { terminal_id } => {
-            let Some(runtime) = self.runtime_for_terminal_id_string(&terminal_id) else {
+            let Some(session_or_runtime) =
+                self.runtime_for_terminal_id_or_external(&terminal_id)
+            else {
                 ... terminal not found ...
             };
-            let (buffer, cursor) = render_terminal_virtual(runtime, area);
-            let hyperlinks = runtime.visible_hyperlinks(area);
+            let (buffer, cursor, hyperlinks, modes, scroll) = match session_or_runtime {
+                Native(runtime) => {
+                    let (buffer, cursor) = render_terminal_virtual(runtime, area);
+                    let hyperlinks = runtime.visible_hyperlinks(area);
+                    (buffer, cursor, hyperlinks, None, None)
+                }
+                External(session) => render_external_terminal_virtual(session, area)?,
+            };
             let frame = FrameData::from_ratatui_buffer_with_hyperlinks(
                 &buffer, cursor, &hyperlinks,
             );
             ...
         }
```

`render_external_terminal_virtual` should call this crate's complete renderer,
translate `ExternalRenderedCursor` to the protocol `CursorState`, and copy
external mode/scroll/revision metadata into the frame and pane metadata. It
must not call `runtime_for_terminal_id_string` and must not create a local
process or PTY. A direct external attachment is read-only even when its model
reports mouse, focus, or bracketed-paste modes.

### 4. Keep input and process control read-only

`handle_terminal_attach_scroll` and `handle_terminal_attach_mouse` at
`src/server/headless.rs:1404-1475` currently resolve a native runtime and pass
events to native terminal input. External sessions need a separate scroll
operation that changes an owner-approved model display offset or client view
offset:

```diff
         let Some(runtime) = self.runtime_for_terminal_id_string(terminal_id) else {
-            return false;
+            if let Some(session) = self.external_session_for_terminal_id(terminal_id) {
+                return session.scroll(read_only_scroll_command(...)).is_ok();
+            }
+            return false;
         };
```

Do not route mouse bytes, `ClientInput`, key input, send-text, resize, or
process-control commands into the external model unless a later provider
contract explicitly authorizes and serializes that operation. For this draft,
mouse mode is model metadata only. Return the existing structured unsupported
operation error for external attachments instead of reporting a successful
native input write.

### 5. Force complete rendering through retained surfaces first

The retained path at
`src/server/headless/retained_surface.rs:212-340` collects a native dirty patch
from a `TerminalRuntime`. An external model revision must force the complete
external surface renderer until an external damage protocol is proven:

```diff
         for source in pty_sources {
             ... locate pane and runtime ...
+            if let Some(session) = self.external_session_for_pane(source) {
+                // A raw model revision, alternate-screen transition, scroll
+                // change, or hyperlink change invalidates native patch state.
+                external_full_render_targets.insert(session.terminal_id());
+                continue;
+            }
             let revision_before = runtime.content_seq();
             ... collect_dirty_patch ...
         }
```

The complete frame must update the retained pane's content revision, cursor,
mode, scroll, and hyperlink metadata together. A later `ExternalDirtyPatch`
may be added only after differential tests prove it agrees with complete
rendering across wide-cell transitions, scrollback movement, resize,
alternate-screen swaps, synchronized output, and hyperlink changes. Replaying
ANSI through Ghostty to obtain a patch is not a valid bridge.

### 6. Use the same model for API reads and copy

The provider read path at `src/app/api/panes.rs:1501-1555` currently handles an
external binding by reading normalized `TranscriptProjection` lines and
rejecting other sources. Add an external session branch before that fallback:

```diff
         if let Some(session) = self.external_session_for_pane(&params.pane_id) {
             return encode_external_read(
                 id,
                 session.recent_lines(params.lines.unwrap_or(80).min(1000)),
                 session.content_revision(),
             );
         }
         if terminal.external_binding.is_some() {
             ... current recent/recent_unwrapped transcript fallback ...
         }
```

`recent_lines` should use the fork's structured line export so wide cells,
spacers, wraps, tabs, combining characters, and the current model revision
have the same semantics as the renderer. A structured cell response is more
faithful than plain text. If ANSI output is offered later, it needs a real
exporter from the external model; it must not return `TranscriptProjection`
text while claiming style preservation. `selection_to_string` likewise reads
the model snapshot and does not synthesize text from a separately normalized
transcript.

### 7. Restore and poll at one owner seam

`src/app/runtime_providers.rs:161-220` currently drains provider updates,
applies `TranscriptReply` to `TranscriptProjection`, updates its source cursor,
and marks the app changed. For the raw contract, the same owner should:

1. validate binding, source identity, sequence/byte cursor, dimensions, and
   omission metadata;
2. mark the session revision odd;
3. feed the accepted raw bytes exactly once to the one checkpointed parser and
   terminal model;
4. resolve synchronized output according to the session timeout policy;
5. update the source cursor, checkpoint revision, and even content revision
   atomically;
6. request the existing render dirty signal and persist the binding plus
   checkpoint.

On restart, decode and validate the checkpoint before requesting bytes after
its cursor. A failed restore must not silently advance a provider cursor or
replay from normalized transcript text. It should expose an unavailable state
and request an explicit provider reset/full snapshot. The current
`ProviderRuntimeHandle` remains transport/control state; it is not the
checkpointed terminal model and must not be serialized into a frame.

## Feature coverage and boundaries

The bridge preserves the structured data represented by the view: SGR styles,
269-entry colors, inverse/hidden/dim/strike, underline variants and color,
wide and combining cells, hyperlinks, cursor shape metadata, primary or
alternate-screen mode, mouse/focus/bracketed-paste/synchronized-output mode
bits, and scroll metrics.

This draft does not implement the checkpoint engine, raw provider contract,
session owner, persistence, timeout handling, scroll mutation, API adapters,
direct attach dispatch, dirty patches, input, process control, or Kitty image
media. Those are integration work at the seams above. The existing normalized
transcript path remains the only correct path for a provider that exposes text
only.

## Validation

Run the standalone crate with its isolated target directory:

```text
CARGO_TARGET_DIR=target/herdr-external-terminal-renderer-v1 \
  cargo test --manifest-path crates/herdr-external-terminal-renderer-v1/Cargo.toml
```

The five unit tests cover direct style and hyperlink projection, combining and
wide/spacer cells, inverse and hidden colors, cursor suppression while
scrolled back, validation-before-mutation, mode/scroll metadata, and the
source adapter. `cargo fmt -- --check` is clean after formatting.
