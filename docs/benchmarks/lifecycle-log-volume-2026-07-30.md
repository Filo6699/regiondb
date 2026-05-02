# Lifecycle log volume comparison

This snapshot compares structured server logging before and after removing the
per-connection info event. It measures emitted line counts, not request
throughput, and is not a CI performance threshold.

## Environment

| Field | Value |
|---|---|
| Measurement date | 2026-07-30 |
| Architecture | x86-64 |
| OS | Arch Linux, Linux 6.18.9-arch1-2 |
| Go | go1.24.13 linux/amd64 |
| Before commit | `72ddf4c` |
| Workload | readiness probe plus 100 authenticated `PING` connections |

## Method

The server was built once for each tree and started with a loopback listener,
one-bit geometry, relaxed durability, and a fresh data directory. A TCP
readiness probe was followed by 100 connections that each sent `AUTH`, `PING`,
and `QUIT`. The process was then stopped with `SIGTERM`, and complete log lines
were counted by level.

Authentication tokens and data-directory paths are intentionally absent from
the captured output. The raw aggregate counts and event histogram are preserved
in
[`lifecycle-log-volume-2026-07-30.txt`](lifecycle-log-volume-2026-07-30.txt).

## Result

| Variant | Info | Warn | Error | Total |
|---|---:|---:|---:|---:|
| Per-connection info event | 107 | 0 | 0 | 107 |
| Lifecycle-only info events | 6 | 0 | 0 | 6 |

Removing the 101 connection-accept events reduced info volume by 94.4% for
this workload. The remaining six records describe process start, storage open
and close, listener readiness, and server start and stop. Request and
connection handling emits no info record, so steady-state volume does not grow
with successful traffic.

Warn and error remained zero in both successful runs. Error lifecycle events
remain available for TLS, geometry, storage, listener, protocol engine, and
serve failures; sensitive token and path fields are filtered by the logger.
