# fs_split_v1 storage format

`fs_split_v1` stores each regular chunk in one file. Geometry is configured
when the store opens and is repeated in every file so mismatched data is
rejected.

This document uses regular chunk, large chunk, block coordinate, and chunk
coordinate as defined by the [project terminology](TERMINOLOGY.md).

## Coordinate mapping

For a chunk coordinate `(x, y)`, floor division by `large_chunk_edge` selects
the large-chunk directory. Negative coordinates therefore map toward negative
infinity.

Directories and files use sign-prefixed decimal components:

```text
l_<large-x>_<large-y>/c_<chunk-x>_<chunk-y>.rdb
```

Nonnegative components use `p`; negative components use `n` followed by their
absolute magnitude. For example, chunk `(-1, 2)` is stored in a file named
`c_n1_p2.rdb`.

## File layout

All multibyte integers are unsigned little-endian. Signed coordinates are
stored as their two's-complement `uint64` representation.

| Offset | Size | Field |
|---:|---:|---|
| 0 | 8 | ASCII magic `RGDBSPL1` |
| 8 | 4 | `chunk_edge` |
| 12 | 4 | `large_chunk_edge` |
| 16 | 1 | `block_bits` |
| 17 | 3 | Reserved, must be zero |
| 20 | 8 | Chunk X coordinate |
| 28 | 8 | Chunk Y coordinate |
| 36 | 8 | Payload byte length |
| 44 | variable | Packed regular-chunk payload |
| end - 4 | 4 | IEEE CRC-32 of every preceding byte |

The exact file size is `44 + payload_bytes + 4`. Readers reject a wrong magic,
size, checksum, coordinate, geometry, payload length, or nonzero reserved byte.

## Packed payload

Blocks are linearized in row-major order as
`index = offset_y * chunk_edge + offset_x`. Each value occupies exactly
`block_bits` contiguous bits. Bit index zero is the least-significant bit of
byte zero; values are encoded least-significant bit first and may cross byte
boundaries. Unused high bits in the final byte are zero in newly created
chunks.

`payload_bytes` is the checked ceiling of
`chunk_edge * chunk_edge * block_bits / 8`.

## Delta WAL

The data directory contains `.regiondb.wal`. It is a sequence of fixed-size
records for the configured geometry. Each record contains:

| Offset | Size | Field |
|---:|---:|---|
| 0 | 8 | ASCII magic `RGDBWAL1` |
| 8 | 4 | `chunk_edge` |
| 12 | 4 | `large_chunk_edge` |
| 16 | 1 | `block_bits` |
| 17 | 3 | Reserved, must be zero |
| 20 | 8 | Chunk X coordinate |
| 28 | 8 | Chunk Y coordinate |
| 36 | `payload_bytes` | Replacement packed chunk payload |
| end - 4 | 4 | IEEE CRC-32 of every preceding byte |

Opening a store validates and replays complete records in order. If multiple
records replace the same chunk, the last complete record determines its
contents. Replaying those replacement records again produces the same result.
A final partial record is treated as an interrupted append and discarded,
whether the interruption occurs in its header, payload, or checksum. Invalid
magic, geometry, reserved bytes, or checksum in a complete record fails closed
with no replay.

## Writes, checkpoints, and durability

A write creates a mode `0600` temporary file in the destination directory,
writes the complete encoded chunk, closes it, and atomically renames it over
the destination. Directories are created as needed with mode `0755`. Before
that replacement, the complete new payload is appended to the WAL.

The WAL is checkpointed when either the configured record or byte threshold
is reached. A durable checkpoint replays and synchronizes every WAL record
before truncating and synchronizing the WAL.

- `relaxed` acknowledges after the WAL append and atomic chunk replacement
  without calling `fsync`.
- `fsync-wal` with `wal_group_commit_updates=1` synchronizes each WAL record
  before replacing the chunk and acknowledging the write. With a value of
  `N > 1`, each write is acknowledged after append and chunk replacement, but
  the WAL is synchronized only every Nth update and during orderly close. The
  batching window can therefore contain up to `N-1` acknowledged updates
  beyond the last successful WAL sync. Recovery can reconstruct an
  unsynchronized chunk replacement when the appended WAL bytes persist, but a
  process exit is not equivalent to power-loss durability.
- `fsync-checkpoint` synchronizes the temporary chunk and the replacement
  before acknowledging the write. Unix systems synchronize the parent
  directory after the rename; Windows uses a write-through replacement. WAL
  truncation is also synchronized.

A synchronized write is acknowledged only when the new directory entry is
covered by one of those two paths. If a filesystem reports that a directory
handle cannot be flushed and the platform replacement does not commit the
entry itself, the write fails instead of acknowledging the missing guarantee.
A directory that cannot be opened or a failing flush stays an ordinary write
error and is not treated as a missing capability.

These modes describe single-host filesystem calls. Hardware and filesystem
behavior can impose weaker guarantees. There is no tombstone or alternate
backend compatibility contract. The separately versioned
[experimental region layout](EXPERIMENTAL_REGION_FORMAT.md) is opt-in, is not
selectable from the server, is not production-supported, and is part of no
contract described here.

Decoded chunks are retained in a bounded in-memory LRU cache. Eviction changes
only memory use: a later read reloads and validates the chunk file. Cache state
is neither stored in the data directory nor part of the on-disk format.

## Writer ownership metadata

`.regiondb.lock` is a mode `0700` control directory, not chunk data. Its
`guard` file carries the operating-system writer lock. Its atomically replaced
`owner.json` records the positive decimal process ID, a 32-character
hexadecimal session ID, and RFC 3339 `started_at` and `heartbeat_at`
timestamps. The session ID distinguishes separate ownership lifecycles even
when the operating system reuses a process ID.

Ownership metadata is operational state and does not change the
`fs_split_v1` chunk or WAL formats. A read-only store ignores this directory
and reads only published chunk files. A writer fails closed on malformed
metadata. It may replace valid abandoned metadata only after its heartbeat is
older than the documented stale interval and after acquiring the guard.

Earlier development versions created `.regiondb.lock` as a regular file.
Before opening such a data directory with this version, stop every older
writer and remove only that control file. Chunk files and `.regiondb.wal` do
not require conversion. Downgrading requires the same stopped-writer
procedure: remove the control directory before starting a version that expects
a regular lock file.
