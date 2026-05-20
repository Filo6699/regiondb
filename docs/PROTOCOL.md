# regiondb text protocol version 1

This document specifies the text protocol implemented by regiondb. There is no
version negotiation on the wire.

## Transport and session

A server accepts TCP connections. Plaintext endpoints use
`region://token@host:port/`; TLS endpoints use
`regions://token@host:port/` and require TLS 1.2 or later.

Each connection owns an independent authentication session. A request is one
printable ASCII command line terminated by CRLF. Command names use uppercase
ASCII. Tokens are separated by exactly one space. Empty tokens, embedded line
breaks, other control bytes, and an unterminated final line are rejected as
frame errors.

`AUTH` and `QUIT` may be sent before authentication. Every other command
returns `NOAUTH` until authentication succeeds. A failed `AUTH` clears any
previously authenticated state.

## Responses

Simple success:

```text
+OK [detail]\r\n
```

Error:

```text
-ERR CODE [detail]\r\n
```

Bulk data:

```text
$<decimal-byte-count>\r\n<payload>\r\n
```

The byte count covers only `payload`. Protocol errors do not close the
connection. `QUIT` sends its response and then closes it.

## Commands

All integers use decimal ASCII input without a leading plus sign. Coordinates
are signed 64-bit integers. Block values are unsigned 64-bit integers and must
fit the configured block width.

`GET`, `SET`, and `EXISTS` take world block coordinates. `CHUNK`, `CHUNKBIN`,
and `CHUNKSET` take regular-chunk coordinates. These coordinate spaces are
distinct; the [project terminology](TERMINOLOGY.md) defines their names.

| Command | Response | Behavior |
|---|---|---|
| `AUTH token` | `+OK` or `-ERR AUTH ...` | Authenticate the connection. |
| `PING` | `+OK PONG` | Check an authenticated session. |
| `INFO` | Bulk runtime snapshot | Identify the server and report bounded runtime counters. |
| `GET x y` | Bulk decimal value | Read a block; an absent chunk reads as zero. |
| `SET x y value` | `+OK` | Persist one packed block value. |
| `EXISTS x y` | `+OK 0` or `+OK 1` | Report whether the current block value is nonzero. |
| `CHUNK x y` | Bulk lowercase hexadecimal payload | Read a packed regular chunk by chunk coordinate. |
| `CHUNKBIN x y` | Bulk binary payload | Read the exact packed regular-chunk bytes without hexadecimal encoding. |
| `CHUNKSET x y payload` | `+OK` | Persist a packed regular chunk from an exact-length hexadecimal payload. |
| `QUIT` | `+OK` | Close the session after the response. |

The `INFO` payload contains newline-terminated `key=value` records in this
fixed order: `regiondb_version`, `process_lock_mode`, `chunk_lock_mode`,
`cache_hits`, `cache_misses`, `loaded_chunks`, `dirty_chunks`, `evictions`,
`eviction_runs`, `wal_flushes`, `wal_foreground_flushes`,
`wal_group_flushes`, `wal_eviction_flushes`, `wal_checkpoint_flushes`,
`open_wal_handles`, and `checkpoints`. The version is `1`; counters and gauges
are unsigned decimal integers.

The fields have these meanings:

| Field | Kind | Meaning |
|---|---|---|
| `process_lock_mode` | value | Active data-directory writer lock: `flock`, `lock-file-ex`, or `none` for a read-only store. |
| `chunk_lock_mode` | value | In-process chunk access lock; `fs_split_v1` reports `shared-rwmutex`. |
| `cache_hits` | counter | Chunk lookups served by a resident cache entry since the store opened. |
| `cache_misses` | counter | Chunk lookups not resident when requested, including lookups whose file is absent. |
| `loaded_chunks` | gauge | Current resident chunk count; never exceeds `-max-loaded-chunks`. |
| `dirty_chunks` | gauge | Current dirty resident count; always zero for write-through `fs_split_v1`. |
| `evictions` | counter | Resident chunks removed by capacity-driven cache eviction. |
| `eviction_runs` | counter | High-watermark events that evicted one or more resident chunks. |
| `wal_flushes` | counter | Total successful WAL synchronization calls since the store opened; equals the sum of the four reason-specific flush counters. |
| `wal_foreground_flushes` | counter | WAL synchronizations performed directly for a foreground write when group commit is disabled. |
| `wal_group_flushes` | counter | WAL synchronizations performed at a group-commit boundary or for a pending group during clean shutdown. |
| `wal_eviction_flushes` | counter | WAL synchronizations forced by cache eviction; always zero for the write-through `fs_split_v1` cache. |
| `wal_checkpoint_flushes` | counter | WAL synchronizations that commit checkpoint or recovery truncation. |
| `open_wal_handles` | gauge | Current cached WAL append and scan handles; never above the storage limit, which defaults to two. |
| `checkpoints` | counter | Completed WAL checkpoints since the store opened. |

Counters are monotonic for one store lifetime and reset when the process
reopens the store. Gauges may increase or decrease. No unbounded or
backend-defined keys are included.

`CHUNK` and `CHUNKBIN` return `NOT_FOUND` when their chunk file is absent.
Storage failures are reported with the `STORAGE` code. The binary byte count
is the existing bulk response length; payload bytes may contain any value.
The packed payload layout is defined in the
[storage format specification](STORAGE_FORMAT.md).

The server rejects command lines larger than its configured `max_line_bytes`
limit, including CRLF, and keeps the connection available after a complete
oversized line. No binary write command, pipelining guarantee, protocol
upgrade, or durability negotiation is part of version 1. The CLI listens on
`127.0.0.1:4242` by default. Durability is a server startup setting and does
not change the wire format.
