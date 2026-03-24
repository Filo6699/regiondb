# Terminology

regiondb uses chunk-native terms for its geometry, storage, protocol, and
benchmark contracts:

- A **block** is one logical value addressed by world block coordinates
  `(x, y)`. Its encoded width is `block_bits`.
- A **regular chunk** (or **chunk** where the meaning is unambiguous) is the
  square `chunk_edge` by `chunk_edge` collection of packed blocks that forms
  the unit of storage and chunk protocol operations.
- A **large chunk** is the `large_chunk_edge` by `large_chunk_edge` grouping
  of regular chunks used to partition `fs_split_v1` directories. It is not a
  stored payload or a protocol object.
- A **packed chunk payload** is the exact byte representation of one regular
  chunk. It contains block values only; file and WAL headers are not part of
  the payload.
- A **chunk coordinate** identifies a regular chunk. A **block coordinate**
  identifies a block in the world, and a **block offset** identifies a block
  within its regular chunk.

Terms such as page, sector, tile, and region are not synonyms for regular
chunk or large chunk in project contracts. The project name and the
`region://` and `regions://` URI schemes do not introduce another geometry
level.
