# Changelog

All notable changes to regiondb are documented in this file.

## [1.1.0] - 2026-07-31

### Added

- Deterministic paginated `CHUNKSCAN` plus bounded `CHUNKRANGE` and
  `CHUNKRADIUS` world reads, with checked coordinates, 256-chunk limits, and a
  64 MiB encoded response cap.
- Persisted chunk versions, single-chunk compare-and-swap, and atomic
  conditional multi-chunk commit decisions with crash-recoverable intents.
- `WALFLUSH` as a global recoverability barrier across all steady-state
  durability modes, plus stable snapshot generations for concurrent read-only
  loads.
- Optional ZRLE for checkpoint images and `CHUNKBINC` responses, Prometheus
  metrics with fixed cardinality, and the read-only `regiondb-verify` integrity
  scanner.

### Changed

- Storage, durability, and server internals are split into focused source files
  without protocol, on-disk format, durability, locking, or public Go API
  changes.
- Write acknowledgement now follows the recoverable WAL commit decision;
  committed chunk/version publication failures are completed by retry or
  recovery instead of being reported as unapplied writes.
- The bounded chunk cache uses recency with second chances, guaranteed
  least-recent fallback under full read contention, a bounded maintenance
  queue, inline backpressure, and deterministic drain on shutdown.
- Authentication source tracking is bounded and groups IPv6 clients by `/64`;
  checkpoint collection removes empty chunk images while retaining their
  versions.

### Security

- No default token ships in any artifact or image; an auth-enabled server
  refuses to start with an empty token, the Compose sample requires
  `REGIONDB_TOKEN` and binds loopback by default, and token source precedence
  is logged without the value.
- Intent paths are validated for grammar and containment, slot writes are
  bounds-checked before mutation, and temporary files are created
  exclusive/no-follow where the platform supports it.

### Compatibility and limitations

- Existing `RGDBSPL1`, `RGDBSPL2`, `RGDBWAL1`, and `RGDBWAL2` data remains
  readable. `RGDBSPL3` checkpoint images and version/generation/intent metadata
  are forward additions; opening newer data with an older binary is not
  promised.
- `MSET` continues to apply its successful prefix before an error.
  `CHUNKBATCH` has one atomic commit decision, but separate reads and commands
  do not form a multi-chunk snapshot.
- Chunk images and WAL records retain one whole-artifact CRC-32 without an
  independently verifiable header CRC. CRC-32 detects corruption but is not
  cryptographic authentication.
- The verifier reports a retained version without its checkpoint-collected
  empty image as `image_missing`; v1.1 has no metadata that distinguishes that
  intentional state from accidental image loss.
- `fs_region_v1` remains experimental, opt-in, unavailable to the server and
  verifier, and outside durability, migration, and compatibility guarantees.

### Validation

- Release gate: `gofmt`, `go vet`, `golangci-lint`, race, security and
  hardened suites, integration, crash/recovery, repeated stress, plus
  archives, SHA-256 checksums, and SBOMs for Linux, macOS, and Windows on
  `amd64` and `arm64`.

### Release status

- Stable channel, marked `latest`.

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

[Unreleased]: https://github.com/Filo6699/regiondb/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/Filo6699/regiondb/releases/tag/v1.0.0
[0.1.1-preview]: https://github.com/Filo6699/regiondb/releases/tag/v0.1.1-preview
[0.1.1-alpha]: https://github.com/Filo6699/regiondb/releases/tag/v0.1.1-alpha
[0.1.0-alpha]: https://github.com/Filo6699/regiondb/releases/tag/v0.1.0-alpha
