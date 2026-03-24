# Measured direct-storage baseline

This snapshot records one local measurement of the current Go implementation.
It is evidence for later comparisons on the same environment, not a
cross-machine performance target or a CI threshold.

## Environment

| Field | Value |
|---|---|
| Measurement date | 2026-07-30 |
| Hardware | 11th Gen Intel Core i5-11300H, 4 cores / 8 logical CPUs |
| Architecture | x86-64 |
| OS | Arch Linux, Linux 6.18.9-arch1-2 |
| Go | go1.25.12 linux/amd64 |
| Commit | `53ffd6b471a94b367ab3829412ba0e826f026037` |

## Command

The data directory was newly created and empty before the command:

```sh
go run ./cmd/regiondb-direct-bench \
  -data-dir /tmp/regiondb-t016-baseline.zputl7 \
  -seed 42 \
  -ops 10000 \
  -workload mixed \
  -chunk-edge 16 \
  -large-chunk-edge 8 \
  -block-bits 5 \
  -durability fsync-wal
```

The exact program output is preserved in
[`direct-fsync-wal-mixed-2026-07-30.json`](direct-fsync-wal-mixed-2026-07-30.json).

## Observed result

The run completed 10,000 operations, including 7,962 reads and 2,038 writes,
in 58,708,719 ns. It reported 170,332.45 operations per second, with latency
p50 393 ns, p95 15,279 ns, and p99 22,486 ns.

These values include the direct store operation but exclude working-set
preparation and chunk construction, as defined in the benchmark contract.
Filesystem cache state, system load, CPU frequency scaling, and the storage
stack can change subsequent results. Historical benchmark values from other
implementations are intentionally not represented as regiondb Go results.
