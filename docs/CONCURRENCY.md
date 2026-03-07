# Concurrency model

This document describes the concurrency behavior of the current development
version.

## Server ownership

One `Serve` call owns one listener. Each accepted connection is handled in its
own goroutine and has independent authentication and close state. Cancelling
the server context closes the listener and active connections, then waits for
their handlers to return.

The current runtime does not bound accepted connections or assign them to a
worker pool. Operators must impose connection limits outside the process when
needed.

## In-process storage access

The protocol engine uses one read/write mutex for all commands sharing that
engine:

- `GET`, `EXISTS`, and `CHUNK` hold a shared read lock;
- `SET` and `CHUNKSET` hold an exclusive write lock across read-modify-write or
  write operations.

The `fs_split_v1` store also protects its methods with a store-wide read/write
mutex. Reads may overlap other reads. A write excludes reads and other writes
for that store instance.

These locks prevent in-process races and lost updates through one engine.
There is no cross-process writer lock, SWMR ownership protocol, transaction,
snapshot, or ordering guarantee across multiple commands.
