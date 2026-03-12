# Scenario benchmarks

regiondb includes two scenario benchmark programs:

- `regiondb-direct-bench` measures calls to the `fs_split_v1` chunk store in
  the benchmark process.
- `regiondb-bench` measures complete `CHUNKSET` and `CHUNKBIN` round trips over
  an existing plaintext TCP server connection.

Both programs accept `-seed`, `-ops`, and `-workload`. The operation count must
be positive and is bounded at 10,000,000 so exact percentile samples remain
bounded. The supported workloads are `read`, `write`, and `mixed`; `mixed`
selects approximately 80% reads and 20% writes. A seed determines the operation
types, coordinates, and payload values. Read and mixed runs prepare a bounded
working set before timing so their measured reads address existing chunks.

The direct benchmark requires its own `-data-dir`. Its `-durability` setting
selects one of the existing storage durability modes. The TCP benchmark
requires `-address` and `-token`. Its geometry flags should match the server
configuration; `chunk-edge` and `block-bits` determine the packed chunk payload
length.

For example:

```sh
go run ./cmd/regiondb-direct-bench \
  -data-dir ./bench-data \
  -seed 42 \
  -ops 1000 \
  -workload mixed
```

With a server already listening on `127.0.0.1:8123`:

```sh
go run ./cmd/regiondb-bench \
  -address 127.0.0.1:8123 \
  -token development-secret \
  -seed 42 \
  -ops 1000 \
  -workload mixed
```

## Output

Each successful run writes one JSON object to standard output. It records the
backend, workload, seed, measured operation and read/write counts, total
duration, working-set size, operations per second, and latency minimum, p50,
p95, p99, and maximum. It also records the geometry; direct results include
durability and TCP results include the server address without the
authentication token. Durations and latencies use nanoseconds, and percentiles
use the nearest-rank method.

The measured loop excludes working-set preparation. The TCP result includes
client command encoding and one complete request/response round trip. The
direct result includes the store method call but not chunk construction.

Benchmark values depend on geometry, durability, cache state, filesystem,
hardware, operating system, and server configuration. Quick benchmark tests
only prove that the scenarios complete and emit valid output; they do not
enforce performance thresholds.

regiondb is specialized chunk/grid storage for games and grid-based
simulations. These scenarios characterize only the implemented chunk paths;
they are not general database benchmarks and do not support broad database
performance comparisons.
