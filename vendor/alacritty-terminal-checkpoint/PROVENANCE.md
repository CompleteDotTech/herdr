# alacritty_terminal checkpoint fork provenance

This standalone draft is copied from the Alacritty repository at commit
`94e7c8874e526b1e67b349d9ba30ddf81669119e` (tag `v0.17.0`).  The vendored
package version is `0.26.0` and its upstream license is Apache-2.0; the
original license texts are retained in `LICENSE-APACHE` and `LICENSE-MIT`.

The terminal model source remains the upstream implementation.  This fork
adds `term::checkpoint`, explicit version-one DTO conversions and validation,
and a small grid/color seam needed to rebuild semantic rows without importing
the private storage-ring layout.  The parser and processor checkpoint API is
owned by the separately vendored vte checkpoint fork at
`../vte-checkpoint-v1`, pinned to commit
`3b3da71c34cc1256c7e20981cf03f8eb95e08ffc`.

The source was copied from `/tmp/alacritty-v0.17-check/alacritty_terminal`.
This draft is not published and is not part of Herdr's dependency graph.
