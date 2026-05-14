# Experimental fs_region_v1 layout

`fs_region_v1` is an experimental storage layout that keeps every regular chunk
of one large chunk in a single region image, addressed by a fixed slot and a
presence bitmap. It exists to measure region images against the production
[`fs_split_v1` format](STORAGE_FORMAT.md).

The layout is opt-in and versioned independently of `fs_split_v1`. It is not
selectable from the server or any command-line tool, it is covered by no
compatibility promise, and its images may change or disappear without a
migration path. Production data directories keep using `fs_split_v1`.
`fs_region_v1` is experimental-only and is not a production-supported backend
in this release line. Benchmark results for it do not establish support,
durability, migration, or compatibility guarantees.

This document uses regular chunk, large chunk, and chunk coordinate as defined
by the [project terminology](TERMINOLOGY.md).

## Coordinate mapping

For a chunk coordinate `(x, y)`, floor division by `large_chunk_edge` selects
the region, exactly as in `fs_split_v1`. The remainder selects the slot:

```text
slot = offset_y * large_chunk_edge + offset_x
```

One region image holds `large_chunk_edge * large_chunk_edge` slots and is named
with the same sign-prefixed components as the production layout:

```text
r_<region-x>_<region-y>.rdbregion
```

Nonnegative components use `p`; negative components use `n` followed by their
absolute magnitude.

## Image layout

All multibyte integers are unsigned little-endian. Signed coordinates are stored
as their two's-complement `uint64` representation.

| Offset | Size | Field |
|---:|---:|---|
| 0 | 8 | ASCII magic `RGDBRGN1` |
| 8 | 4 | `format_version`, currently `1` |
| 12 | 4 | `chunk_edge` |
| 16 | 4 | `large_chunk_edge` |
| 20 | 1 | `block_bits` |
| 21 | 3 | Reserved, must be zero |
| 24 | 8 | Region X coordinate |
| 32 | 8 | Region Y coordinate |
| 40 | 8 | `slot_count` |
| 48 | 8 | `slot_bytes`, the packed payload size of one chunk |
| 56 | 4 | IEEE CRC-32 of bytes 0 to 55 |
| 60 | 4 | IEEE CRC-32 of the remaining slot directory |
| 64 | `ceil(slot_count / 8)` | Presence bitmap, slot `n` in bit `n % 8` of byte `n / 8` |
| ... | `slot_count * 4` | IEEE CRC-32 per slot payload, zero while absent |
| ... | `slot_count * slot_bytes` | Slot payload area |

Every offset is derived from the store geometry, so a slot position never
depends on a value read back from the file. An image is sized to its full slot
area when it is created; on filesystems with sparse files the unwritten slots
occupy no space. A geometry whose image would exceed 1 GiB is rejected when the
store opens.

Slot payloads use the same packed encoding and the same `payload_bytes`
computation as `fs_split_v1`.

Readers reject a wrong magic, header checksum, format version, geometry, region
coordinate, slot count, slot size, image size, slot directory checksum, or slot
payload checksum. A chunk whose presence bit is clear, and a chunk in a region
without an image, are reported as a missing chunk.

## Writes and durability

Publishing a chunk writes the slot payload at its fixed offset, then rewrites
the slot directory, checksum first, in a single write. An interrupted first
publication therefore leaves the slot absent rather than present with
unverifiable bytes.

The layout has no write-ahead log, no checkpoint, no durability mode, and no
`fsync` call: an interrupted overwrite of a published slot is reported as a slot
checksum mismatch instead of being repaired. It also has no writer ownership
metadata, so an image directory must not be shared with another writer or with
an `fs_split_v1` store. Those boundaries are what the layout comparison is
measured against; they are not proposed guarantees.

The store keeps a bounded number of region images open and closes the least
recently used image when that bound is reached. Handle state is neither stored
in the image directory nor part of the format.
