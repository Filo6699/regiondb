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

## Chunk image generations

All multibyte integers are unsigned little-endian. Signed coordinates are
stored as their two's-complement `uint64` representation.

The current checkpoint image generation is `RGDBSPL3`:

| Offset | Size | Field |
|---:|---:|---|
| 0 | 8 | ASCII magic `RGDBSPL3` |
| 8 | 4 | `chunk_edge` |
| 12 | 4 | `large_chunk_edge` |
| 16 | 1 | `block_bits` |
| 17 | 1 | Codec: `0` for none, `1` for ZRLE |
| 18 | 2 | Reserved, must be zero |
| 20 | 8 | Chunk X coordinate |
| 28 | 8 | Chunk Y coordinate |
| 36 | 8 | Decoded packed payload byte length |
| 44 | variable | Encoded payload followed by presence state |
| end - 4 | 4 | IEEE CRC-32 of every preceding byte |

Codec zero stores exactly `payload_bytes + presence_bytes` bytes. Codec one is
ZRLE over that same complete state and may be shorter. ZRLE control bytes use
the high bit to distinguish zero runs from literal runs; the low seven bits
store a run length from 1 through 128 minus one. The decoder has the
geometry-derived output bound and rejects truncated, trailing, and
over-expanding input. A checkpoint uses ZRLE only when
`-checkpoint-compression=zrle` and the encoded state is smaller than the
uncompressed state; otherwise it writes codec zero.

Ordinary write-through publications continue to use `RGDBSPL2`. It has the
same header except that byte 17 is reserved and must be zero, and it always
stores the exact payload followed by the exact presence bitmap. Its exact size
is `44 + payload_bytes + presence_bytes + 4`.

Readers reject a wrong magic, bounded size, checksum, coordinate, geometry,
decoded payload length, codec, reserved byte, or nonzero unused high bit in
the decoded presence or payload. A complete CRC-32 covers each image, including
its header, but there is no separate header CRC. Consequently header and body
corruption are detected only after the bounded complete image is read, and
cannot be classified independently. Adding an independent header checksum
requires a new format generation; this is a known v1.1 limitation.

## Packed payload

Blocks are linearized in row-major order as
`index = offset_y * chunk_edge + offset_x`. Each value occupies exactly
`block_bits` contiguous bits. Bit index zero is the least-significant bit of
byte zero; values are encoded least-significant bit first and may cross byte
boundaries. Unused high bits in the final byte are zero in newly created
chunks.

`payload_bytes` is the checked ceiling of
`chunk_edge * chunk_edge * block_bits / 8`.

The presence bitmap uses block index `n` in bit `n % 8` of byte `n / 8`.
Presence is independent of the packed value, so a present zero differs from an
absent block. Clearing a block also zeroes its packed value. `CHUNKSET` imports
an existing packed payload and marks every block present.

The protocol maps this state without changing the on-disk format:

- `CHUNKBIN x y STATE` returns the payload bytes followed by the presence
  bitmap bytes;
- `CHUNK x y STATE` returns the same two byte sequences as lowercase
  hexadecimal separated by `|`;
- `CHUNKSET x y STATE payload|presence` validates both exact byte lengths,
  clears payload values whose presence bits are absent, and supplies the
  complete state to one atomic chunk write.

Readers also accept the previous `RGDBSPL1` layout, whose exact size is
`44 + payload_bytes + 4` and which has no presence bitmap. During migration,
each nonzero legacy block is present and each zero legacy block is absent. The
next ordinary write of that chunk publishes `RGDBSPL2`; a checkpoint may
publish `RGDBSPL3`. Downgrading after a newer chunk has been written requires
restoring a backup created by the older version.

## Delta WAL

The data directory contains `.regiondb.wal`. It is a sequence of fixed-size
records for the configured geometry. Each record contains:

| Offset | Size | Field |
|---:|---:|---|
| 0 | 8 | ASCII magic `RGDBWAL2` |
| 8 | 4 | `chunk_edge` |
| 12 | 4 | `large_chunk_edge` |
| 16 | 1 | `block_bits` |
| 17 | 3 | Reserved, must be zero |
| 20 | 8 | Chunk X coordinate |
| 28 | 8 | Chunk Y coordinate |
| 36 | `payload_bytes` | Replacement packed chunk payload |
| ... | `presence_bytes` | Replacement block presence bitmap |
| end - 4 | 4 | IEEE CRC-32 of every preceding byte |

Opening a store validates and replays complete records in order. If multiple
records replace the same chunk, the last complete record determines its
contents. Replaying those replacement records again produces the same result.
A final partial record is treated as an interrupted append and discarded,
whether the interruption occurs in its header, payload, presence bitmap, or
checksum. Invalid magic, geometry, reserved bytes, presence bits, or checksum
in a complete record fails closed with no replay.

Opening also accepts a WAL made entirely of fixed-size `RGDBWAL1` records.
Legacy record presence is inferred with the same nonzero rule as legacy chunk
files. Recovery validates and replays those records before replacing the WAL
with an empty file; new records therefore never mix versions in one WAL.

Like chunk images, WAL records have one whole-record CRC-32 and no independent
header CRC. Record size is fixed by the configured geometry, so recovery reads
at most that bounded size before validation. A final record cut anywhere in
the header, payload, presence bitmap, or checksum is discarded as an
interrupted append. An independently verifiable WAL header would require a new
WAL generation and is not part of v1.1.

## Version, generation, and intent metadata

Conditional operations use these checksummed control artifacts:

- `.regiondb.version` is `RGDBVER1`, followed by the data-directory-wide
  unsigned 64-bit version clock and CRC-32;
- `c_<x>_<y>.rdb.ver`, beside its chunk image, is `RGDBCVR1`, followed by the
  chunk coordinates, unsigned 64-bit version, and CRC-32;
- `.regiondb.snapshot` is `RGDBSNP1`, followed by an unsigned 64-bit generation
  and CRC-32;
- `.regiondb.intents/wal.rollback` is either `RGDBIRB1` or `RGDBICM1`,
  followed by the unsigned WAL byte boundary and CRC-32.

Missing per-chunk version metadata reads as version zero, which migrates
existing v1.0 data without rewriting every chunk. If the global clock is
missing, writer startup scans valid version files for the highest value and
publishes the clock. Corrupt metadata, a chunk version above the clock, or
clock exhaustion fails closed. Every successful write reserves a monotonically
increasing version; a successful conditional batch reserves consecutive
versions in mutation order. A failure after the clock file is published but
before the WAL commit can leave unused version gaps; versions are ordering
tokens, not a gap-free count of successful writes.

Snapshot generations are even while published files form a stable read point
and odd while a writer may be publishing a commit. The writer moves the
generation to odd before mutation/recovery and back to even only after all
committed publications are complete. A read-only load reads the generation
before and after one chunk or version file and accepts the result only when
both reads return the same even value. It reports contention for an odd or
changed generation rather than returning a potentially mixed result. This
protects one load from ABA-style replacement; it does not create a
multi-chunk snapshot.

The intent file makes the WAL byte boundary and commit decision recoverable.
A rollback intent means startup truncates and synchronizes only the rejected
WAL tail. A committed intent means every appended batch record remains
committed and recovery completes publication. Unexpected intent artifacts,
malformed records, or a boundary beyond the WAL fail closed.

## Writes, checkpoints, and durability

A write first durably publishes a rollback intent, appends the complete new
state to the WAL, establishes the mode-specific WAL commit point, and then
publishes a committed intent. Before that commit decision, a failure rolls the
WAL back to the recorded boundary and applies no write. After it, the write is
acknowledged as committed even if publishing the chunk image, version file,
checkpoint, or cleanup reports a later failure; those publications remain
pending and recovery or a later operation completes them. A repair failure
that makes safe continuation uncertain poisons the store, and subsequent
writes fail closed.

Chunk and version publication creates a mode `0600` temporary file in the
destination directory, writes the complete encoded artifact, closes it, and
atomically renames it over the destination. Directories are created as needed
with mode `0755`.

Writer startup removes stale `.regiondb-chunk-*` temporary files left by an
interrupted atomic replacement. This cleanup processes at most 100,000
data-directory entries per startup. Reaching the bound stops cleanup without
changing published chunk files and emits the structured `scan_capped` warning;
a later startup may resume cleanup of entries beyond the bound.

The configured record and byte thresholds are lower bounds for checkpoint
hysteresis. Values greater than one trigger a checkpoint at 150% of the
configured threshold, rounded down; a threshold of one remains immediate.
A checkpoint replays every WAL record, preserves its already-published version
metadata, writes either an uncompressed or optionally compressed chunk image,
and then truncates the WAL. A checkpoint removes a chunk image whose presence
bitmap is empty while retaining its version metadata, so `CHUNKSCAN`, range,
and radius reads omit the collected state without permitting an old expected
version to match.

- `relaxed` commits and acknowledges after the WAL append and committed-intent
  publication. Chunk and version files may still be pending, and ordinary
  data publications do not call `fsync`.
- `fsync-wal` with `wal_group_commit_updates=1` synchronizes each WAL record
  before committing and acknowledging the write. With a value of `N > 1`,
  ordinary writes are acknowledged after append and committed-intent
  publication, but the WAL is synchronized only every Nth update and during
  orderly close. A conditional batch forces the pending group to synchronize
  before its one commit decision. The batching window can contain up to
  `N-1` acknowledged ordinary updates beyond the last successful WAL sync.
- `fsync-checkpoint` synchronizes the WAL before the commit decision, then
  synchronizes each temporary chunk/version file and its replacement before
  completing publication. Unix systems synchronize parent directories;
  Windows uses write-through replacement where supported. WAL truncation is
  also synchronized.

A synchronized publication is complete only when the new directory entry is
covered by the selected platform path. If a filesystem reports that a
directory handle cannot be flushed and the platform replacement does not
commit the entry itself, the operation fails instead of claiming the missing
guarantee.
A directory that cannot be opened or a failing flush stays an ordinary write
error and is not treated as a missing capability.
On Windows, a file flush that reports `ERROR_INVALID_FUNCTION` or
`ERROR_NOT_SUPPORTED` fails with an unsupported-durability error; a
synchronized operation never falls back to unsynchronized success.

These modes describe single-host filesystem calls. Hardware and filesystem
behavior can impose weaker guarantees. There is no tombstone or alternate
backend compatibility contract. The separately versioned
[experimental region layout](EXPERIMENTAL_REGION_FORMAT.md) is opt-in, is not
selectable from the server, is not production-supported, and is part of no
contract described here.

`WALFLUSH` is a global durability barrier. After completing pending
publications it synchronizes the WAL, every tracked unsynchronized chunk and
version file, directories from deepest to root, and the final even snapshot
generation. Tracking is bounded at 4,096 paths. If that set overflows, the
barrier uses a bounded-memory two-pass tree walk: all regular files are
synchronized before any directory. A failed pass retains retry state and
never reports success.

Decoded chunks are retained in a bounded in-memory recency cache. Cache state
is neither stored in the data directory nor part of the on-disk format.
`-max-loaded-chunks` is a hard resident limit and defaults to 1,024. Hits move
entries to the most-recent position and grant one second chance. Admission at
the limit scans from the least-recent end, clears second chances, and evicts
the first unreferenced entry. In the rare read-contention case where the whole
resident set was touched between admissions, one complete scan clears all
second chances and the least-recent entry is selected as the fallback. This
guarantees progress while preserving the hard bound.

Eviction maintenance uses one worker and a queue of 16 tasks. A full queue
runs maintenance inline, so backpressure cannot grow memory or goroutines
without bound. Shutdown drains queued work. Maintenance failures are reported
but do not abort or poison the write-through store; evicted allocations are
reused after maintenance and a later read reloads and validates the file.

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

## Read-only verification

`regiondb-verify` scans an existing `fs_split_v1` data directory without
creating, removing, repairing, replaying, or locking files. It requires
`-data-dir`, `-chunk-edge`, `-large-chunk-edge`, and `-block-bits`. Newline
delimited JSON records are written to stdout: zero or more `issue` records
followed by one `summary`. Paths and details are JSON escaped.

Exit code 0 means the complete scan found no integrity issue, 1 means the scan
completed and found corruption, and 2 means usage, geometry, I/O, or another
condition prevented a complete scan. Verification covers chunk images, WAL
records, version files and clock, snapshot generation stability, intents,
coordinate placement, and unexpected artifacts. It is diagnostic and
read-only: it does not provide repair, recovery, or a writer-consistent
multi-file snapshot.

The verifier currently reports `image_missing` for any version file without a
chunk image. Checkpoint collection intentionally creates that shape for an
empty chunk so its version cannot revert to zero. Operators must correlate
that issue with expected empty-state collection; distinguishing an intentional
collected image from an accidentally missing image requires additional
versioned metadata and is a known v1.1 limitation.
