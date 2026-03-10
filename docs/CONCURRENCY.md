# Concurrency model

This document describes the concurrency behavior of the current development
version.

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
Each open store also owns a nonblocking operating-system lock on
`.regiondb.lock`. A second writer for the same data directory is rejected
until the first store closes or exits. The lock file may remain after close;
ownership is represented by the OS lock, not by file existence.

The current writer lock is supported on Unix platforms that provide `flock`.
Other platforms fail store startup rather than claim weaker exclusion.
There is no read-only process role, transaction, snapshot, or ordering
guarantee across multiple commands.
