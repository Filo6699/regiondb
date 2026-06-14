# Scenario benchmarks

regiondb includes two scenario benchmark programs:

- `regiondb-direct-bench` measures calls to the `fs_split_v1` chunk store in
  the benchmark process.
- `regiondb-bench` measures complete `CHUNKSET` and `CHUNKBIN` round trips over
  an existing plaintext TCP server connection.

The protocol package also provides focused server command benchmarks for
`PING`, `INFO`, hexadecimal `CHUNK`, and binary `CHUNKBIN` response paths:

```sh
go test -run '^$' -bench BenchmarkSessionCommands ./internal/protocol
```

Both programs accept `-seed`, `-ops`, and `-workload`. The operation count must
be positive and is bounded at 10,000,000 so exact percentile samples remain
bounded. The supported workloads are `read`, `write`, and `mixed`; `mixed`
selects approximately 80% reads and 20% writes. A seed determines the operation
types, coordinates, and payload values. Read and mixed runs prepare a bounded
working set of regular chunks before timing so their measured reads address
existing chunks.

The direct benchmark requires its own `-data-dir`. Its `-durability` setting
selects one of the existing storage durability modes. The TCP benchmark uses
an external server at `127.0.0.1:4242` by default and requires `-token`.
Alternatively, `-uri` accepts a complete `region://token@host:port/` or
`regions://token@host:port/` endpoint; it cannot be combined with `-address`
or `-token`. A `regions://` endpoint uses TLS 1.2 or later and the operating
system trust store. Its geometry flags should match the server configuration;
`chunk-edge` and `block-bits` determine the packed chunk payload length.
`-clients` requests multiple authenticated connections; measured operations
are distributed round-robin across the connections that were established
successfully.

For example:

```sh
go run ./cmd/regiondb-direct-bench \
  -data-dir ./bench-data \
  -seed 42 \
  -ops 1000 \
  -workload mixed
```

With a server listening on the default address:

```sh
go run ./cmd/regiondb-bench \
  -uri region://region-token@127.0.0.1:4242/ \
  -seed 42 \
  -ops 1000 \
  -workload mixed
```

For an isolated local run, build the server and explicitly select spawn mode.
The benchmark creates and removes a temporary data directory and stops the
spawned process after the run:

```sh
go build -o ./regiondb ./cmd/regiondb
go run ./cmd/regiondb-bench \
  -server-mode spawn \
  -server-binary ./regiondb \
  -token region-token \
  -seed 42 \
  -ops 1000 \
  -workload mixed
```

## Output

Each successful run writes one JSON object to standard output. It records the
backend, workload, seed, measured operation and read/write counts, total
duration, working-set size, operations per second, and latency minimum, p50,
p95, p99, and maximum. It also records the geometry; direct results include
durability and TCP results include the server mode and address without the
authentication token. Both results record the active process and chunk lock
modes. TCP results also distinguish requested and active clients and count
connection failures. Durations and latencies use nanoseconds, and percentiles
use the nearest-rank method.

The reproducible direct benchmark bundle copies `process_lock_mode` and
`chunk_lock_mode` from the raw result into its manifest. This makes the
operating-system writer guard and in-process chunk access mode visible without
inferring them from the runner name.

The measured loop excludes working-set preparation. The TCP result includes
client command encoding and one complete request/response round trip. The
direct result includes the store method call but not chunk construction.

Benchmark values depend on geometry, durability, cache state, filesystem,
hardware, operating system, and server configuration. Quick benchmark tests
only prove that the scenarios complete and emit valid output; they do not
enforce performance thresholds. The reproducible benchmark workflow runs those
quick tests before collecting its bundle. Full integration, stress, and crash
suites remain in their dedicated validation gates. Benchmark executables,
working data, and generated artifacts are staged under the runner temporary
directory, and the workflow fails if a run changes the source checkout.

The [measured baseline](benchmarks/measured-baseline-2026-07-30.md) records a
specific local environment, command, commit, and raw artifact. It is suitable
only as a reproducible point of reference under comparable conditions.
The [command-path optimization results](benchmarks/command-path-optimization-2026-07-30.md)
compare the same focused benchmark before and after buffer reuse and preserve
all samples in a raw artifact.
The [experimental region layout comparison](benchmarks/experimental-region-layout-2026-07-30.md)
records the measured A/B result and the decision to keep `fs_region_v1` out of
the production default and compatibility guarantees.
The [Windows-native validation snapshot](benchmarks/windows-native-evidence-2026-07-30.md)
records functional benchmark smoke, storage concurrency, and writer-lock
evidence from a hosted Windows run. It does not provide throughput results.
The [post-fix Windows cleanup snapshot](benchmarks/windows-post-fix-snapshot-2026-07-30.md)
records the lock-mode metadata contract, deterministic benchmark teardown
regression, and its explicit native-execution evidence boundary.
The [sparse low-cache snapshot](benchmarks/sparse-low-cache-cross-platform-2026-07-30.md)
preserves every repeated TCP result and reports the median together with the
complete spread instead of selecting the best run.

regiondb is specialized chunk/grid storage for games and grid-based
simulations. These scenarios characterize only the implemented regular-chunk
paths; they are not general database benchmarks and do not support broad
database performance comparisons. Terms used here follow the
[project terminology](TERMINOLOGY.md).
