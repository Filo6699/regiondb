# Changelog

All notable changes to regiondb are documented in this file.

## [1.0.0] - 2026-06-25

### Added

- Stable SemVer contracts for `fs_split_v1` readability, text protocol version
  1, durability acknowledgement boundaries, and the server CLI.
- Ordered `MSET` and `MGET` batch operations, exact chunk presence-state
  transfer, and frozen compatibility fixtures for every supported chunk and
  WAL generation.

### Changed

- The release channel is now stable and production-facing; experimental
  storage remains opt-in and outside compatibility guarantees.
- Runtime resource limits, protocol validation, storage recovery, and
  cross-platform descriptor behavior are hardened for the stable boundary.

### Validation

- The release gate covers Linux, macOS, and Windows builds and tests, race,
  lint, CodeQL, integration, repeated stress, durability crash/recovery,
  archive installation smoke, checksums, SBOMs, and format fixtures.

### Release status

This is the first stable release. The declared compatibility surfaces follow
SemVer; internal packages, the experimental backend, distributed features,
cross-chunk transactions, and unverified platform modes remain excluded.

## [0.1.1-preview] - 2026-05-08

### Added

- Portable single-writer ownership, read-only data-directory access, and
  bounded WAL group commit.
- Native TCP and TLS integration coverage, a Docker runtime image, structured
  lifecycle logging, and runtime counters.
- Opt-in `fs_region_v1` evaluation tooling with published correctness and
  benchmark evidence; `fs_split_v1` remains the default backend.
- GoReleaser archives for Linux, macOS, and Windows on AMD64 and ARM64, with
  SHA-256 checksums and per-archive SBOMs.

### Changed

- WAL, checkpoint, and atomic replacement boundaries now fail closed when the
  requested filesystem guarantee cannot be completed.
- The first WAL directory entry is committed before synchronized records can
  be acknowledged, so the first record follows the same recovery contract as
  later records.

### Validation

- Crash/restart coverage now exercises atomic replacement, synchronized WAL
  replay, checkpoint truncation, and first-WAL creation boundaries.
- CI includes native Linux, macOS, and Windows tests, race detection, repeated
  integration and stress checks, linting, and CodeQL.

### Release status

This is an evaluation preview. Its wire protocol, on-disk format, command-line
interface, and Go API are not stable compatibility commitments.

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

[1.0.0]: https://github.com/Filo6699/regiondb/releases/tag/v1.0.0
[0.1.1-preview]: https://github.com/Filo6699/regiondb/releases/tag/v0.1.1-preview
[0.1.1-alpha]: https://github.com/Filo6699/regiondb/releases/tag/v0.1.1-alpha
[0.1.0-alpha]: https://github.com/Filo6699/regiondb/releases/tag/v0.1.0-alpha
