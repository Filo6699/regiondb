# Concurrency model

This document describes regiondb's concurrency behavior.

## Server ownership

One `Serve` call owns one listener and a fixed-size worker pool. Accepted
connections wait in a bounded queue; the accept loop stops accepting while all
workers and queue slots are occupied. Each worker handles one connection at a
time, so a slow client cannot create an unbounded number of goroutines.
Connections retain independent authentication and close state.

Cancelling the server context closes the listener plus active and queued
connections, then waits for all workers to return. Workers, queue capacity, and
the maximum command line size are server startup settings.

## In-process storage access

The protocol engine uses one read/write mutex for all commands sharing that
engine:

- `GET`, `EXISTS`, and `CHUNK` hold a shared read lock;
- `SET` and `CHUNKSET` hold an exclusive write lock across read-modify-write or
  write operations.

The `fs_split_v1` store also protects its methods with a store-wide read/write
mutex. Reads may overlap other reads. A write excludes reads and other writes
for that store instance. Its LRU cache has a separate mutex and retains at
most `max_loaded_chunks`; chunks returned to callers are copies.

These locks prevent in-process races and lost updates through one engine.

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
rather than retaining a process-local chunk cache. This gives a complete
single-chunk image, but not a multi-chunk snapshot: separate reads may observe
different points in the writer's sequence, and data present only in an
unreplayed WAL is not visible. Writes through a read-only instance fail.

The server and benchmark commands currently open writer instances; there is no
read-only command-line mode. There is also no transaction, snapshot, or
ordering guarantee across multiple commands.
