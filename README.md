# regiondb

`regiondb` is a specialized chunk/grid storage engine for games and grid-based
simulations.

regiondb provides a packed chunk store backed by the `fs_split_v1` on-disk
format and an authenticated text protocol over TCP or TLS.

## Install

Download a `v1.1.0` archive for Linux, macOS, or Windows from the
[GitHub release](https://github.com/Filo6699/regiondb/releases/tag/v1.1.0),
verify it against `SHA256SUMS`, and place the `regiondb` binary on your
`PATH`.

To build the stable source instead:

```sh
go install github.com/Filo6699/regiondb/cmd/regiondb@v1.1.0
```

## Quick start

All geometry and authentication settings are explicit:

```sh
regiondb \
  -data-dir ./data \
  -token region-token \
  -chunk-edge 16 \
  -large-chunk-edge 8 \
  -block-bits 5 \
  -durability fsync-wal
```

The server listens on `127.0.0.1:4242` by default. Use `-listen` to select a
different interface or port. Storage defaults to `relaxed`; select
`fsync-wal` or `fsync-checkpoint` according to the acknowledgement guarantees
in the [storage format](docs/STORAGE_FORMAT.md). The authentication token is
resolved in `-token`, `REGIONDB_TOKEN`, then `-token-file` precedence. Use
`-no-auth` only when unauthenticated access is intentional; it overrides an
ambient `REGIONDB_TOKEN` and cannot be combined with token flags. Data
directory and geometry have no defaults and must be provided explicitly.
Run `regiondb -help` for the complete option list. A non-loopback listener
emits a warning without logging token or token-file contents.

Checkpoint images are uncompressed by default. Use
`-checkpoint-compression=zrle` to compress zero-heavy payload and presence
state when the encoded image is smaller. This does not change protocol payloads
or the steady-state durability mode.

To enable TLS, provide both files from a PEM certificate/key pair:

```sh
regiondb \
  -data-dir ./data \
  -token region-token \
  -chunk-edge 16 \
  -large-chunk-edge 8 \
  -block-bits 5 \
  -tls-cert ./server.crt \
  -tls-key ./server.key
```

The server rejects an incomplete or invalid TLS configuration before opening
the data directory or listener. TLS listeners require TLS 1.2 or later.
Connection URIs use `region://token@host:port/` for plaintext TCP and
`regions://token@host:port/` for TLS.

`-workers` and `-accept-queue` bound active and pending connections.
`-idle-timeout`, `-request-timeout`, and `-response-timeout` bound socket I/O.
Failed authentication is delayed per source address and temporarily banned
according to `-auth-failure-delay`, `-auth-failure-limit`, and
`-auth-ban-duration`.

For container deployment, resource limits, platform-specific behavior, and
benchmark operation, use the focused guides below.

The current contracts are documented in:

- [Terminology](docs/TERMINOLOGY.md)
- [Protocol specification](docs/PROTOCOL.md)
- [Storage format](docs/STORAGE_FORMAT.md)
- [Stable compatibility policy](docs/COMPATIBILITY.md)
- [Concurrency model](docs/CONCURRENCY.md)
- [Runtime hardening controls](docs/RUNTIME_HARDENING.md)
- [Scenario benchmarks](docs/BENCHMARKS.md)
- [Docker](docs/DOCKER.md)
- [Windows guide](docs/WINDOWS.md)
- [Release notes](docs/releases/v1.1.0.md)

`v1.1.0` is the current stable release. Its declared on-disk readability, wire
protocol, durability, and CLI surfaces follow SemVer. The v1.1 contract adds
bounded world reads, persisted chunk versions and conditional commits, an
explicit global durability barrier, compressed checkpoint images, bounded
metrics and maintenance, and read-only verification without weakening the
v1.0 contracts.

## World and conditional operations

Protocol version 1 includes deterministic paginated `CHUNKSCAN`, bounded
`CHUNKRANGE` and `CHUNKRADIUS`, compressed `CHUNKBINC` reads, and persisted
`CHUNKVER` values. `CHUNKCAS` updates one expected version; `CHUNKBATCH`
validates distinct chunks and commits the complete conditional batch or none
of it. `MSET` remains an ordered sequence with applied-prefix semantics, not a
transaction. `WALFLUSH` provides an explicit recoverability barrier for every
write acknowledged before it.

To inspect an offline `fs_split_v1` directory without modifying it:

```sh
regiondb-verify \
  -data-dir ./data \
  -chunk-edge 16 \
  -large-chunk-edge 8 \
  -block-bits 5
```

The verifier emits newline-delimited JSON. Exit code 0 means clean, 1 means
integrity issues were found, and 2 means the scan could not complete. It does
not repair or replay data.

## Scenario benchmarks

The repository provides direct `fs_split_v1` and TCP scenario benchmarks with
seeded read, write, and mixed workloads. Both emit JSON latency percentiles and
operation counts. Their results characterize the implemented regular-chunk
paths on a particular configuration and machine; they are not general database
performance claims or CI thresholds.

With the server example above running, a short TCP benchmark can use its
connection URI directly:

```sh
go run ./cmd/regiondb-bench \
  -uri region://region-token@127.0.0.1:4242/ \
  -ops 1000
```

## Development

The project requires Go 1.24 or later.

See [Contributing](CONTRIBUTING.md) for commit, scope, experimental-feature,
and validation policy.

```sh
scripts/test/quick.sh
```

The quick gate runs vet, tests, and builds and is used by the pull-request CI
matrix. `scripts/test/full.sh` adds the race detector, uncached tests, and the
existing integration, crash, and repeated stress suites; release automation
runs that full gate. The reproducible benchmark workflow runs the benchmark
smoke tests before collecting artifacts; full stress and crash suites stay in
their dedicated gates. None of these gates applies a benchmark throughput
threshold.

## License

regiondb is available under the MIT License.
