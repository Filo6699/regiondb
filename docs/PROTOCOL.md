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
previously authenticated state. Failed authentication responses are delayed
per source address; repeated failures from one source trigger a temporary ban
without delaying unrelated sources. When the server is started with explicit
`-no-auth`, sessions begin authenticated and `AUTH` remains a successful
no-op.

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

Array of bulk data:

```text
*<decimal-item-count>\r\n
$<decimal-byte-count>\r\n<payload>\r\n
...
```

The byte count covers only `payload`. Protocol errors do not close the
connection. `QUIT` sends its response and then closes it.

## Commands

All integers use decimal ASCII input without a leading plus sign. Coordinates
are signed 64-bit integers. Block values are unsigned 64-bit integers and must
fit the configured block width.

`GET`, `SET`, `UNSET`, and `EXISTS` take world block coordinates. `CHUNK`,
`CHUNKBIN`, `CHUNKBINC`, `CHUNKEXISTS`, `CHUNKSET`, `CHUNKVER`, `CHUNKCAS`,
`CHUNKBATCH`, `CHUNKSCAN`, `CHUNKRANGE`, and `CHUNKRADIUS` take or return
regular-chunk coordinates. `CHUNK`, `CHUNKBIN`, `CHUNKBINC`, and `CHUNKSET`
also have the exact-state forms shown below. These coordinate spaces are
distinct; the
[project terminology](TERMINOLOGY.md) defines their names.

| Command | Response | Behavior |
|---|---|---|
| `AUTH token` | `+OK` or `-ERR AUTH ...` | Authenticate the connection. |
| `PING` | `+OK PONG` | Check an authenticated session. |
| `INFO` | Bulk runtime snapshot | Identify the server and report bounded runtime counters. |
| `METRICS` | Bulk Prometheus text exposition | Report fixed-cardinality process and storage metrics. |
| `WALFLUSH` | `+OK` | Establish a global durability barrier for acknowledged writes. |
| `GET x y` | Bulk decimal value | Read a block; an absent block or chunk reads as zero. |
| `MGET x1 y1 [x2 y2 ...]` | Array of bulk decimal values | Read one or more blocks in argument order. |
| `SET x y value` | `+OK` | Persist one packed block value and mark the block present, including when the value is zero. |
| `MSET x1 y1 value1 [x2 y2 value2 ...]` | `+OK` | Persist one or more block values in argument order. |
| `UNSET x y` | `+OK` | Mark a block absent and clear its packed value; unsetting a block in a missing chunk is idempotent. |
| `EXISTS x y` | `+OK 0` or `+OK 1` | Report whether the block is present, independently of its value. |
| `CHUNK x y` | Bulk lowercase hexadecimal payload | Read a packed regular chunk by chunk coordinate. |
| `CHUNK x y STATE` | Bulk `payload\|presence` | Read the packed payload and exact packed presence bitmap as lowercase hexadecimal. |
| `CHUNKBIN x y` | Bulk binary payload | Read the exact packed regular-chunk bytes without hexadecimal encoding. |
| `CHUNKBIN x y STATE` | Bulk binary state | Read the payload bytes followed by the exact presence bitmap bytes. |
| `CHUNKBINC x y` | Bulk ZRLE data | Read the packed payload using the bounded ZRLE codec. |
| `CHUNKBINC x y STATE` | Bulk ZRLE data | Read payload plus presence bitmap using the bounded ZRLE codec. |
| `CHUNKEXISTS x y` | `+OK 0` or `+OK 1` | Report whether the chunk exists independently of block presence. |
| `CHUNKSET x y payload` | `+OK` | Persist a packed regular chunk from an exact-length hexadecimal payload. |
| `CHUNKSET x y STATE payload\|presence` | `+OK` | Atomically persist an exact-length hexadecimal payload and presence bitmap. |
| `CHUNKVER x y` | `+OK version` | Read the persisted unsigned chunk version; a never-written chunk has version zero. |
| `CHUNKCAS x y expected payload[\|presence]` | `+OK version` | Replace one chunk only when its current version equals `expected`. |
| `CHUNKBATCH x1 y1 expected1 state1 [x2 y2 expected2 state2 ...]` | Array of versions | Conditionally replace one or more distinct chunks as one commit decision. |
| `CHUNKSCAN limit [cursor_x cursor_y]` | Array headed by `END` or `CURSOR x y` | Enumerate populated chunks in deterministic coordinate order. |
| `CHUNKRANGE x0 y0 x1 y1` | Array of exact chunk states | Read populated chunks in an inclusive rectangular range. |
| `CHUNKRADIUS x y radius` | Array of exact chunk states | Read populated chunks in an inclusive Euclidean radius. |
| `QUIT` | `+OK` | Close the session after the response. |

`CHUNKSCAN` accepts a limit from 1 through 256 and orders coordinates by
ascending `x`, then ascending `y`. Its first response item is `CURSOR x y`
when another populated chunk exists after the page, or `END` otherwise. Each
remaining item is `x y`. A client continues with the returned coordinates as
the exclusive cursor. Persisted chunks with an empty presence bitmap are not
listed. Candidate accumulation is duplicate-free and bounded to at most one
coordinate beyond the requested page, so stale unrelated files or duplicate
artifacts cannot make scan memory grow with the world.

`CHUNKRANGE` takes inclusive corners satisfying `x0 <= x1` and `y0 <= y1`.
The rectangle may cover at most 256 coordinates. `CHUNKRADIUS` takes a
non-negative chunk radius and includes coordinates satisfying
`dx*dx + dy*dy <= radius*radius`; the resulting disc may likewise cover at
most 256 coordinates. Both commands return populated chunks in ascending
`x`, then `y`, and omit missing chunks and chunks with an empty presence
bitmap. Each array item is `x y payload|presence`, using the same lowercase
hexadecimal exact-state representation as `CHUNK x y STATE`.

Range and radius arithmetic is checked across the complete signed 64-bit
coordinate domain. Their complete encoded response, including array and bulk
framing, is capped at 64 MiB. A populated result that would cross the cap
returns `OUT_OF_RANGE` instead of allocating or returning a partial array.

`CHUNKBINC` uses zero-run/literal encoding. Each control byte stores a run
length minus one in its low seven bits, for a run length from 1 through 128.
A set high bit emits that many zero bytes; a clear high bit copies that many
following literal bytes. The decoded length is fixed by geometry:
`payload_bytes` without `STATE`, and `payload_bytes + presence_bytes` with
`STATE`. The response is compressed even when it is larger than the source.
Clients must reject truncated, trailing, or over-expanding streams.

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

`METRICS` uses Prometheus text exposition without labels derived from requests,
coordinates, tokens, or source addresses. It contains command totals and
errors, fixed command-duration buckets, authentication failures and bans,
current connections and tracked authentication sources, plus the cache, WAL,
and checkpoint values corresponding to `INFO`. Metric names are prefixed
`regiondb_`; the exact list emitted by protocol version 1 is:

- `regiondb_commands_total`, `regiondb_command_errors_total`, and
  `regiondb_command_duration_seconds`;
- `regiondb_auth_failures_total`, `regiondb_auth_bans_total`,
  `regiondb_connections`, and `regiondb_auth_sources`;
- `regiondb_cache_hits_total`, `regiondb_cache_misses_total`,
  `regiondb_loaded_chunks`, `regiondb_dirty_chunks`,
  `regiondb_evictions_total`, and `regiondb_eviction_runs_total`;
- `regiondb_wal_flushes_total`, `regiondb_wal_foreground_flushes_total`,
  `regiondb_wal_group_flushes_total`, `regiondb_wal_eviction_flushes_total`,
  `regiondb_wal_checkpoint_flushes_total`, `regiondb_open_wal_handles`, and
  `regiondb_checkpoints_total`.

`CHUNK` and `CHUNKBIN`, including their `STATE` forms, return `NOT_FOUND` when
their chunk file is absent.
`CHUNKEXISTS` reports `1` for any persisted chunk, including one whose block
presence bitmap is empty, and `0` for a missing chunk. Storage failures are
reported with the `STORAGE` code. The binary byte count is the existing bulk
response length; payload bytes may contain any value.

`MSET` and `MGET` require at least one item. A batch is an ordered sequence of
the corresponding single-item operation, not a transaction or snapshot.
`MSET` has applied-prefix semantics: it stops at the first invalid item or
storage error, every preceding item has already been applied, the failing item
is not reported as applied, and later items have not been attempted. Retrying
the complete request can therefore repeat writes from the already-applied
prefix. `MGET` stops and returns an error if any item fails, without returning
a partial array; concurrent writes may become visible between items.

`CHUNKVER`, `CHUNKCAS`, and `CHUNKBATCH` use persisted unsigned 64-bit versions.
Every successful ordinary or conditional chunk write receives a new nonzero
version from one data-directory-wide clock. A missing version file means
version zero for compatibility with older data; versions never wrap, and
exhaustion fails closed. `CHUNKCAS` returns `VERSION_MISMATCH` without changing
the chunk when `expected` differs from the current version. A conditional
payload without `|presence` uses the legacy import rule and marks every block
present; `payload|presence` carries exact state. Version reservations can have
gaps after a pre-commit storage failure, so clients compare tokens for equality
and ordering rather than treating them as a write count.

`CHUNKBATCH` requires at least one four-token mutation, rejects duplicate chunk
coordinates, and validates all expected versions before appending any batch
record. On success, the mutations receive consecutive versions in request
order and the response returns those versions in the same order. A validation,
version, or pre-commit storage failure applies none of the batch. Once the
single commit decision is durable, every mutation is committed even if later
publication or cleanup fails; recovery completes the committed batch. The
operation is atomic as a commit decision but does not provide a concurrent
multi-chunk read snapshot.

`WALFLUSH` is a store-wide barrier. A successful response means every write
acknowledged before the command is recoverable regardless of the configured
steady-state durability mode. It synchronizes the WAL, all pending chunk and
version publications, their directory entries, and the stable snapshot
generation. If the bounded pending-path set overflows, the implementation
falls back to a bounded-memory two-pass data-directory walk that synchronizes
regular files before directories. Any failure returns `STORAGE`; no success is
reported for a partial barrier.

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
