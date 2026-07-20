# Stable compatibility policy

This policy applies to the stable `v1.x` release line. SemVer governs the
surfaces listed below. A patch release preserves them. A minor release may add
optional behavior, but existing valid data, requests, configuration, and
documented durability selections continue to work. Removing a surface or
changing its meaning incompatibly requires a new major version.

## Stable surfaces

### `fs_split_v1` readability

The production data-directory contract is the `fs_split_v1` layout documented
in [the storage format specification](STORAGE_FORMAT.md). Every `v1.x` reader
accepts valid `RGDBSPL2` chunk files and `RGDBWAL2` records written by `v1.0.0`.
The v1.1 reader additionally accepts `RGDBSPL3` checkpoint images with the
documented uncompressed or ZRLE codec, persisted chunk versions, the version
clock, snapshot generation, and conditional intent metadata. It retains the
documented upgrade path from the `RGDBSPL1` and `RGDBWAL1` fixtures produced by
the public prereleases. A write or checkpoint may publish the newest format
accepted by that release, so forward readability by an older binary is not
promised.

Geometry is part of a data directory's interpretation. Opening the same files
with different `chunk_edge`, `large_chunk_edge`, or `block_bits` values remains
an error, not a migration. Corrupt files and unsupported format versions fail
closed. Stable readability does not promise automatic repair, downgrade, or
conversion to another backend.

Frozen fixtures for every supported chunk and WAL generation are exercised by
the storage compatibility tests. Existing data without version metadata
migrates in place: a missing chunk version is zero, and writer startup derives
a missing global clock from valid version files. No rewrite of all chunk
images is required. A format change must retain those reads or introduce a
separately versioned format and an explicit migration contract.

### Wire protocol version 1

The request grammar, response framing, commands, error codes, and command
semantics in [the protocol specification](PROTOCOL.md) are stable for `v1.x`.
Existing valid requests keep their meaning and response type. TCP and TLS
connection URIs remain `region://` and `regions://`; TLS endpoints require TLS
1.2 or later.

Minor releases may add commands or optional command forms. Clients must not
send an addition to an older server and should treat an unknown command as
unsupported. Version 1 has no negotiation mechanism or cross-command snapshot.
`MSET` retains applied-prefix semantics rather than transaction semantics.
`CHUNKBATCH` is one atomic conditional commit decision, but does not turn
separate reads or commands into a multi-chunk snapshot. Human-readable error
detail and connection timing are diagnostic and are not compatibility
surfaces.

### Durability

The acknowledgement and recovery boundaries for `relaxed`, `fsync-wal`, and
`fsync-checkpoint` are stable and are defined by
[the storage format specification](STORAGE_FORMAT.md). A release does not
silently weaken the selected mode. When the operating system or filesystem
cannot provide a required synchronization primitive, the operation fails
instead of reporting a stronger guarantee.

The contract ends at successful operating-system file and directory
synchronization calls. Hardware, filesystem, mount, virtualization, and remote
storage behavior below that boundary is outside the guarantee. Group commit
retains its documented window of up to `N-1` acknowledged updates beyond the
last successful WAL sync. A successful `WALFLUSH` is the stable global barrier
that makes every write acknowledged before it recoverable in every durability
mode.

### Command-line interface

The stable executable name is `regiondb`. The following server flags, their
value types, and their documented meanings are stable:

- `-listen`, `-data-dir`, `-token`, `-token-file`, and `-no-auth`;
- `-tls-cert` and `-tls-key`;
- `-chunk-edge`, `-large-chunk-edge`, and `-block-bits`;
- `-durability`, `-checkpoint-compression`, `-checkpoint-records`, and
  `-checkpoint-bytes`;
- `-max-loaded-chunks`, `-max-open-wal-streams`, and
  `-wal-group-commit-updates`;
- `-workers`, `-accept-queue`, and `-max-line-bytes`;
- `-idle-timeout`, `-request-timeout`, and `-response-timeout`;
- `-auth-failure-delay`, `-auth-failure-limit`, and `-auth-ban-duration`;
- `-version`.

The defaults listed in the README remain the stable-line defaults unless a
release explicitly documents an operational change. A stable update does not
reinterpret an explicit existing value. New optional flags may be added in a
minor release. Removing a flag, changing its type, or changing an accepted
value incompatibly requires a new major version.

`-version` exits successfully; invalid configuration and runtime failure exit
unsuccessfully. Exact help formatting, diagnostic text, structured log fields,
and log ordering are not stable APIs.

`regiondb-verify` is shipped as a read-only operational tool. Its exit classes
are stable: 0 for a clean completed scan, 1 for a completed scan with integrity
issues, and 2 when the scan cannot complete. New issue codes may be added as
validation expands. Exact issue detail text and repair behavior are not
compatibility surfaces; the tool does not repair data.

## Explicit exclusions

The stable contract does not include:

- packages below `internal/`, or the currently export-free placeholder at
  `pkg/regiondb`;
- the opt-in `fs_region_v1` experimental backend, its build tag, file format,
  benchmarks, or migration;
- replication, clustering, consensus, sharding, or any other distributed
  feature;
- atomicity, isolation, or rollback across ordinary commands or `MSET` items,
  beyond the explicit `CHUNKBATCH` commit-decision contract;
- platform, architecture, filesystem, or durability-mode combinations not
  covered by the published release artifacts and platform documentation;
- throughput, latency, cache residency, descriptor counts, log output, or
  benchmark results as performance guarantees.

The v1.1 line also does not promise an independent header checksum for
`fs_split_v1` chunk images or WAL records. Their complete CRC-32 covers the
header and body, but classification before the bounded full artifact is read
would require a new format generation. CRC-32 is corruption detection, not
cryptographic authentication.

Checkpoint collection intentionally retains a version file after removing an
empty chunk image. The v1.1 verifier reports any such version-only state as
`image_missing` because no metadata distinguishes intentional collection from
accidental loss. Operators must interpret that issue with workload context; a
future distinction requires new versioned metadata.

The Linux, macOS, and Windows archives identify the tested release targets.
Building on another target is permitted by the Go toolchain but does not create
a support or durability commitment.
