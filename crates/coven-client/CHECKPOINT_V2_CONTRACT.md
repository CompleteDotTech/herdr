# Checkpoint v2 attach contract

The client sends `POST /api/v1/terminal/attach` with a bounded JSON body. The
body is the authoritative negotiation; the headers are an explicit upgrade
marker and must not cause a fallback to RawBytesV1.

```json
{
  "protocolVersion": 2,
  "sessionId": "opaque owner session id",
  "attachmentId": "non-nil UUID",
  "authority": "ownerLocalIpc",
  "streamId": "optional UUID",
  "streamGeneration": "optional positive u64",
  "executionGeneration": "optional positive u64",
  "authorityEpoch": "optional positive u64",
  "codec": "checkpointV1",
  "observe": true,
  "control": false,
  "maxFrameBytes": 65536,
  "cursor": {"sequence": 1, "offset": 0},
  "revision": 0
}
```

`streamId` and `streamGeneration`, source incarnation fields, and
`cursor`/`revision` are each all-or-none. `control` requests the existing
single-writer capability; it does not permit writes through CTS2. The daemon
must authorize the request and keep the attachment UUID, stream identity,
execution generation, authority epoch, and control lease bound to this socket
for its entire lifetime.

The daemon returns HTTP 200 with these required headers before retaining the
same connection for CTS2 bytes:

```
X-Coven-Terminal-Protocol: 2
X-Coven-Terminal-Codec: checkpointV1
```

The JSON body is:

```json
{
  "protocolVersion": 2,
  "authority": "ownerLocalIpc",
  "sessionId": "same session id",
  "attachmentId": "same attachment UUID",
  "streamId": "resolved UUID",
  "streamGeneration": 3,
  "executionGeneration": 7,
  "authorityEpoch": 11,
  "backend": "ptyRaw",
  "codec": "checkpointV1",
  "permissions": {"observe": true, "control": false},
  "maxFrameBytes": 65536,
  "cursor": {"sequence": 1, "offset": 0},
  "revision": 0,
  "geometry": {"columns": 80, "rows": 24, "cellWidth": 0, "cellHeight": 0}
}
```

The response must echo matching requested identity fields and return all three
positive source generations. Every CTS2 frame repeats the resolved stream id,
stream generation, execution generation, and authority epoch; the client
rejects any mismatch. A `Close` frame is required for a clean reader finish;
EOF, malformed frames, or identity mismatch detach the socket fail-closed.
