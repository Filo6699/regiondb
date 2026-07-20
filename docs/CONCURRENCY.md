# Concurrency model

This document describes regiondb's concurrency behavior.

## Server ownership

One `Serve` call owns one listener and a fixed-size worker pool. Accepted
connections wait in a bounded queue. When all workers and queue slots are
occupied, the accept loop sends a best-effort `BUSY` response under a short
write deadline and closes the extra connection. Each worker handles one
connection at a time, so a slow client cannot create an unbounded number of
goroutines. An idle connection has a fixed read deadline and releases its
worker when it sends no request. Once the first request byte arrives, a
separate absolute deadline bounds the complete frame; receiving additional
bytes does not extend it. Response draining has its own absolute deadline.
Connections retain independent authentication and close state.
Authentication failure accounting is shared by source address across
connections. IPv4 addresses are tracked individually and IPv6 addresses by
masked `/64` prefix. Its recency table is bounded to 4,096 sources, removes an
entry after successful authentication, and evicts the least-recent inactive
source when full. An active ban is not evicted; if every slot is actively
banned, a new source still receives the configured delay but is not retained.

Cancelling the server context closes the listener plus active and queued
connections, then waits for all workers to return. Workers, queue capacity,
the maximum command line size, I/O deadlines, and authentication delay/ban
controls are server startup settings.

Abnormal connection setup, read, and write termination emits a warning with a
stable `phase` and `reason`. Reason classes are `timeout`, `peer_close`,
`socket_error`, `tls_error`, `protocol_close`, and `server_shutdown`. Clean EOF,
`QUIT`, and server shutdown are classified but do not emit termination
warnings.

## In-process storage access

The protocol engine uses one read/write mutex for all commands sharing that
engine:

- `GET`, `EXISTS`, `CHUNK`, `CHUNKBIN`, `CHUNKBINC`, `CHUNKEXISTS`,
  `CHUNKVER`, `CHUNKSCAN`, `CHUNKRANGE`, and `CHUNKRADIUS`, including exact
  `STATE` reads, hold a shared read lock while loading their result;
- `SET`, `UNSET`, both `CHUNKSET` forms, `CHUNKCAS`, `CHUNKBATCH`, and
  `WALFLUSH` hold an exclusive write lock across their storage operation;
- `MGET` performs its shared-locked single-item reads in argument order, and
  `MSET` performs its exclusive-locked single-item writes in argument order.
  Neither holds one lock across the entire request.

The `fs_split_v1` store also protects its methods with a store-wide read/write
mutex. Reads may overlap other reads. A write excludes reads and other writes
for that store instance. Its LRU cache has a separate mutex and retains at
most `max_loaded_chunks`; chunks returned to callers are copies.

These locks prevent in-process races and lost updates through one engine.
`CHUNKBATCH` validates every distinct coordinate and expected version while
holding the exclusive store lock, then publishes one recoverable commit
decision. Its returned versions are consecutive in request order. Ordinary
commands and `MSET` do not inherit that multi-chunk atomicity.

The cache orders entries by recency and gives a touched entry one second
chance. Admission scans from the least-recent end. If concurrent reads touched
every resident between admissions, a complete scan clears their chances and
the least-recent entry is evicted as the rare contention fallback; eviction
therefore always progresses and never exceeds the configured resident bound.
Eviction maintenance has one worker and a queue of 16. A full queue runs the
task inline, and close drains the queue before returning. Maintenance errors
are reported but do not abort the write-through store.

## Process ownership

One writer owns a data directory at a time. A writer creates or reuses the
`.regiondb.lock` directory and takes a nonblocking operating-system lock on
`.regiondb.lock/guard`. A second writer is rejected while that lock is held.
The writer guard uses `flock` on supported Unix platforms and `LockFileEx` on
Windows. Other platforms fail writer startup rather than claim weaker
exclusion.

While it owns the guard, the writer publishes `.regiondb.lock/owner.json` with
these fields:

- `pid`: the writer process ID;
- `session_id`: a random 128-bit identifier encoded as 32 lowercase
  hexadecimal characters;
- `started_at`: the session start time in RFC 3339 format;
- `heartbeat_at`: the latest heartbeat time in RFC 3339 format.

The heartbeat is replaced atomically every five seconds. An orderly close
stops the heartbeat, removes metadata only when its session ID still matches,
and then releases the guard. Failure to refresh ownership prevents subsequent
writes and is returned again by close.

After an unclean exit, the operating-system guard is released but metadata can
remain. A new writer recovers it only after the heartbeat is older than 30
seconds and only while holding the guard. Metadata with a future heartbeat,
metadata not yet stale, or malformed metadata fails startup closed. The guard
therefore prevents takeover from a live but delayed writer, while the stale
window prevents immediate reuse of an owner record after an abnormal lifecycle.

Read-only store instances do not acquire writer ownership, create a missing
data directory, open the WAL, or replay recovery. Any number of them may run
beside the writer. Each read opens the current atomically published chunk file
rather than retaining a process-local chunk cache. It reads the persisted
snapshot generation before and after the file and accepts the result only when
both values are the same even generation. An odd generation reports active
publication; a changed generation reports read contention. This gives a
complete stable single-file load, but not a multi-chunk snapshot: separate
reads may observe different committed generations, and data present only in an
unreplayed WAL is not visible. Callers may retry these rare contention errors.
Writes through a read-only instance fail.

The data-directory-wide version clock and per-chunk version files participate
in the same generation protocol. A writer marks publication unstable before a
mutation or recovery sequence and restores an even generation only after
committed chunk/version publication. Overflow of either the version clock or
snapshot generation fails closed rather than wrapping.

The server and benchmark commands currently open writer instances;
`regiondb-verify` is read-only but is a diagnostic scan rather than a server
mode. There is no transaction, snapshot, or ordering guarantee across multiple
commands.
