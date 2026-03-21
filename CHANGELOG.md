# Changelog

All notable changes to regiondb are documented in this file.

## [0.1.1-alpha] - 2026-03-22

### Added

- Cross-platform CI coverage for Linux, macOS, and the portable Windows
  packages, plus race, lint, and CodeQL checks.
- Repeatable hot-contention stress coverage for concurrent reads and writes
  under bounded-cache eviction.
- Focused protocol benchmarks for `PING`, `INFO`, text chunk, and binary chunk
  response paths.
- Recovery coverage for truncated WAL records, repeated replay, and long
  checkpoint/reopen cycles.
- SHA-256 verification data in the reproducible direct-storage benchmark
  bundle.

### Release status

This is an engineering alpha. Its wire protocol, on-disk format, command-line
interface, and Go API are not stable compatibility commitments.

## [0.1.0-alpha] - 2026-03-15

### Added

- Configurable chunk geometry and packed block payloads.
- Persistent `fs_split_v1` chunk files with checksums and a delta WAL.
- `relaxed`, `fsync-wal`, and `fsync-checkpoint` durability modes.
- An authenticated text protocol served over TCP or TLS.
- Bounded chunk caching, connection workers, accepted-connection queuing, and
  command-line input.
- Seeded direct-storage and TCP scenario benchmarks with JSON output.

### Changed

- The server now listens on `127.0.0.1:4242` by default.

### Release status

This is an engineering alpha. Its wire protocol, on-disk format, command-line
interface, and Go API are not stable compatibility commitments.

[0.1.1-alpha]: https://github.com/Filo6699/regiondb/releases/tag/v0.1.1-alpha
[0.1.0-alpha]: https://github.com/Filo6699/regiondb/releases/tag/v0.1.0-alpha
