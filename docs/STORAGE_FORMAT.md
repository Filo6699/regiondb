# fs_split_v1 storage format

`fs_split_v1` stores each regular chunk in one file. Geometry is configured
when the store opens and is repeated in every file so mismatched data is
rejected.

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

## Writes and durability

A write creates a mode `0600` temporary file in the destination directory,
writes the complete encoded chunk, closes it, and atomically renames it over
the destination. Directories are created as needed with mode `0755`.

This format currently has no WAL, checkpoint, fsync durability guarantee,
cache metadata, tombstone, or alternate backend compatibility contract.
