# regiondb

`regiondb` is a specialized chunk/grid storage engine for games and grid-based
simulations.

The current development version provides a packed chunk store backed by the
`fs_split_v1` on-disk format and an authenticated text protocol over TCP or
TLS.

## Running the server

All geometry and authentication settings are explicit:

```sh
go run ./cmd/regiondb \
  -data-dir ./data \
  -token development-secret \
  -chunk-edge 16 \
  -large-chunk-edge 8 \
  -block-bits 5 \
  -durability fsync-wal
```

Storage defaults to `relaxed` durability. Operators can select `fsync-wal` or
`fsync-checkpoint` and tune WAL checkpoint thresholds with
`-checkpoint-records` and `-checkpoint-bytes`. `-max-loaded-chunks` bounds the
in-memory LRU cache. `-workers`, `-accept-queue`, and `-max-line-bytes` bound
connection processing, queued accepted connections, and command lines. The
exact acknowledgement boundaries are defined in the storage format
specification.

The server listens on `127.0.0.1:4242` by default. Use `-listen` to select a
different interface or port.

To enable TLS, provide both files from a PEM certificate/key pair:

```sh
go run ./cmd/regiondb \
  -data-dir ./data \
  -token development-secret \
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
- [Release notes](docs/releases/v0.1.1-alpha.md)

## Scenario benchmarks

The repository provides direct `fs_split_v1` and TCP scenario benchmarks with
seeded read, write, and mixed workloads. Both emit JSON latency percentiles and
operation counts. Their results characterize the implemented regular-chunk
paths on a particular configuration and machine; they are not general database
performance claims or CI thresholds.

## Development

The project requires Go 1.24 or later.

```sh
scripts/test/quick.sh
```

The quick gate runs vet, tests, and builds and is used by the pull-request CI
matrix. `scripts/test/full.sh` adds the race detector, uncached tests, and the
existing integration, crash, and repeated stress suites; release automation
runs that full gate. Neither gate applies a benchmark throughput threshold.

## License

regiondb is available under the MIT License.
