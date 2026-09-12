# VTE checkpoint fork

Upstream: https://github.com/alacritty/vte

Pinned upstream commit: `3b3da71c34cc1256c7e20981cf03f8eb95e08ffc`. Package version: `0.15.0`.
Original MIT and Apache-2.0 license texts are retained.

This local fork adds typed parser and processor checkpoints, strict bounded
validation, and explicit overflow rejection. The shared terminal runtime must
use `try_checkpoint()` for untrusted runtime state; infallible `checkpoint()`
panics after OSC overflow and is not an owner error-handling API.

The independently reviewed source snapshot is `6ebfc46b5ef3e0d1c65bad53305476159a3d440d24fc58ded9d28f1150e20db8`.
The integration repository retains the detailed source manifest and review at
`run/evidence/vte-checkpoint-ckpt09-source-20260909.json` and
`run/reviews/vte-checkpoint-review-20260909.md`.

These files were copied byte-for-byte from the reviewed fork. No upstream
publication or release is implied.
