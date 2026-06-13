# regiondb text protocol version 1

This document specifies the text protocol implemented by regiondb. There is no
version negotiation on the wire.

## Transport and session

A server accepts TCP connections. Plaintext endpoints use
`region://token@host:port/`; TLS endpoints use
`regions://token@host:port/` and require TLS 1.2 or later.

When the bounded server queue is full, a newly accepted connection may receive
`-ERR BUSY server overloaded` and close before processing a command. The
response is best effort and bounded by a short write deadline, so an overloaded
or non-reading peer may observe only the connection close.

Each connection owns an independent authentication session. A request is one
printable ASCII command line terminated by CRLF. Command names use uppercase
ASCII. Tokens are separated by exactly one space. Empty tokens, embedded line
breaks, other control bytes, and an unterminated final line are rejected as
frame errors. Idle reads, complete request frames, and response drains have
separate absolute deadlines. Sending request bytes incrementally does not
extend the request deadline.

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

`GET`, `SET`, `UNSET`, and `EXISTS` take world block coordinates. `CHUNK`,
`CHUNKBIN`, `CHUNKEXISTS`, and `CHUNKSET` take regular-chunk coordinates.
`CHUNK`, `CHUNKBIN`, and `CHUNKSET` also have the exact-state forms shown
below. These coordinate spaces are distinct; the
[project terminology](TERMINOLOGY.md) defines their names.

| Command | Response | Behavior |
|---|---|---|
| `AUTH token` | `+OK` or `-ERR AUTH ...` | Authenticate the connection. |
| `PING` | `+OK PONG` | Check an authenticated session. |
| `INFO` | Bulk runtime snapshot | Identify the server and report bounded runtime counters. |
| `GET x y` | Bulk decimal value | Read a block; an absent block or chunk reads as zero. |
| `SET x y value` | `+OK` | Persist one packed block value and mark the block present, including when the value is zero. |
| `UNSET x y` | `+OK` | Mark a block absent and clear its packed value; unsetting a block in a missing chunk is idempotent. |
| `EXISTS x y` | `+OK 0` or `+OK 1` | Report whether the block is present, independently of its value. |
| `CHUNK x y` | Bulk lowercase hexadecimal payload | Read a packed regular chunk by chunk coordinate. |
| `CHUNK x y STATE` | Bulk `payload\|presence` | Read the packed payload and exact packed presence bitmap as lowercase hexadecimal. |
| `CHUNKBIN x y` | Bulk binary payload | Read the exact packed regular-chunk bytes without hexadecimal encoding. |
| `CHUNKBIN x y STATE` | Bulk binary state | Read the payload bytes followed by the exact presence bitmap bytes. |
| `CHUNKEXISTS x y` | `+OK 0` or `+OK 1` | Report whether the chunk exists independently of block presence. |
| `CHUNKSET x y payload` | `+OK` | Persist a packed regular chunk from an exact-length hexadecimal payload. |
| `CHUNKSET x y STATE payload\|presence` | `+OK` | Atomically persist an exact-length hexadecimal payload and presence bitmap. |
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
| `open_wal_handles` | gauge | Current cached WAL append and scan handles; never above `-max-open-wal-streams` or the lower operating-system descriptor budget. |
| `checkpoints` | counter | Completed WAL checkpoints since the store opened. |

Counters are monotonic for one store lifetime and reset when the process
reopens the store. Gauges may increase or decrease. No unbounded or
backend-defined keys are included.

`CHUNK` and `CHUNKBIN`, including their `STATE` forms, return `NOT_FOUND` when
their chunk file is absent.
`CHUNKEXISTS` reports `1` for any persisted chunk, including one whose block
presence bitmap is empty, and `0` for a missing chunk. Storage failures are
reported with the `STORAGE` code. The binary byte count is the existing bulk
response length; payload bytes may contain any value.

The legacy forms retain their version 1 packed-value payload and do not include
the block presence bitmap. Legacy `CHUNKSET` therefore marks every imported
block present, including zero-valued blocks. Text `STATE` reads emit lowercase
hexadecimal. In the text `STATE` forms, `payload` has exactly
`2 * payload_bytes` hexadecimal characters and `presence` has exactly
`2 * presence_bytes`; one `|` separates them. In the binary read form the bulk
response contains exactly
`payload_bytes + presence_bytes`. Unused high bits in the final presence byte
and final payload byte must be zero. `CHUNKSET ... STATE` validates the
complete state before taking the write lock, clears payload values for absent
blocks, and publishes the chunk with one storage write. A validation or
storage error cannot apply a partial state. The packed payload layout and
presence bitmap are defined in the [storage format specification](STORAGE_FORMAT.md).

The server rejects command lines larger than its configured `max_line_bytes`
limit, including CRLF, and keeps the connection available after a complete
oversized line. No binary write command, pipelining guarantee, protocol
upgrade, or durability negotiation is part of version 1. The CLI listens on
`127.0.0.1:4242` by default. Durability is a server startup setting and does
not change the wire format.
