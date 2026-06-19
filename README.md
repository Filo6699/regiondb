# regiondb

`regiondb` is a specialized chunk/grid storage engine for games and grid-based
simulations.

regiondb provides a packed chunk store backed by the `fs_split_v1` on-disk
format and an authenticated text protocol over TCP or TLS.

## Running the server

All geometry and authentication settings are explicit:

```sh
go run ./cmd/regiondb \
  -data-dir ./data \
  -token region-token \
  -chunk-edge 16 \
  -large-chunk-edge 8 \
  -block-bits 5 \
  -durability fsync-wal
```

Storage defaults to `relaxed` durability. Operators can select `fsync-wal` or
`fsync-checkpoint` and tune WAL checkpoint thresholds with
`-checkpoint-records` and `-checkpoint-bytes`. `-max-loaded-chunks` bounds the
in-memory LRU cache. `-max-open-wal-streams` bounds cached WAL file streams and
is lowered automatically when the operating-system descriptor budget is
smaller. On Unix, that budget reserves descriptors for the listener, active
and queued sockets, logs, control files, directory scans, and atomic file
replacement. `-workers`, `-accept-queue`, and `-max-line-bytes` bound
connection processing, queued accepted connections, and command lines. The
exact acknowledgement boundaries are defined in the storage format
specification.

The server listens on `127.0.0.1:4242` by default. Use `-listen` to select a
different interface or port.

The server defaults are:

| Setting | Default |
|---|---:|
| `-listen` | `127.0.0.1:4242` |
| `-durability` | `relaxed` |
| `-checkpoint-records` | `1024` |
| `-checkpoint-bytes` | `67108864` (64 MiB) |
| `-max-loaded-chunks` | `1024` |
| `-max-open-wal-streams` | `2` |
| `-wal-group-commit-updates` | `1` |
| `-workers` | current `GOMAXPROCS` |
| `-accept-queue` | `128` |
| `-max-line-bytes` | `1048576` |

The cache maximum is its high watermark. Eviction lowers the default
1,024-entry cache to 768 entries before admissions resume. Authentication,
data directory, and geometry have no defaults and must be provided explicitly.
TLS is disabled unless both certificate and key paths are provided.

To enable TLS, provide both files from a PEM certificate/key pair:

```sh
go run ./cmd/regiondb \
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

For a non-root container with persistent storage, an authenticated healthcheck,
and an opt-in smoke-test profile, see [Docker](docs/DOCKER.md).

The current contracts are documented in:

- [Terminology](docs/TERMINOLOGY.md)
- [Protocol specification](docs/PROTOCOL.md)
- [Storage format](docs/STORAGE_FORMAT.md)
- [Concurrency model](docs/CONCURRENCY.md)
- [Scenario benchmarks](docs/BENCHMARKS.md)
- [Docker](docs/DOCKER.md)
- [Windows guide](docs/WINDOWS.md)
- [Release notes](docs/releases/v0.1.1-preview.md)

The `preview` label identifies the current prerelease channel. The
implementation remains at engineering-alpha maturity.

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
