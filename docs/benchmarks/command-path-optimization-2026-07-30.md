# Command-path optimization results

This snapshot compares the existing focused command benchmark immediately
before and after parser-buffer reuse. It is evidence from one local
environment, not a cross-machine target or a CI performance threshold.

## Environment

| Field | Value |
|---|---|
| Measurement date | 2026-07-30 |
| Hardware | 11th Gen Intel Core i5-11300H, 4 cores / 8 logical CPUs |
| Architecture | x86-64 |
| OS | Arch Linux, Linux 6.18.9-arch1-2 |
| Go | go1.24.13 linux/amd64 |
| Before commit | `6f1e62ed0c7be359e9ca0a335e9819d70c00e138` |
| After commit | `4b3681e90281fe39306347cc4ab468480749d5eb` |

## Command

The same command was run from each commit:

```sh
go test -run '^$' \
  -bench '^BenchmarkSessionCommands$' \
  -benchmem \
  -count=10 \
  ./internal/protocol
```

The after commit also exposes focused benchmarks for the reusable parser and
connection-frame buffers:

```sh
go test -run '^$' -bench '^BenchmarkParseFrameReuse$' -benchmem -count=10 ./internal/protocol
go test -run '^$' -bench '^BenchmarkReadFrameReuse$' -benchmem -count=10 ./internal/server
```

All command output is preserved in
[`command-path-optimization-2026-07-30.txt`](command-path-optimization-2026-07-30.txt).

## Observed result

The table reports the median elapsed time across ten samples. Allocation
columns were stable across every sample.

| Command | Before ns/op | After ns/op | Before B/op | After B/op | Before allocs/op | After allocs/op |
|---|---:|---:|---:|---:|---:|---:|
| `PING` | 104.9 | 79.5 | 48 | 32 | 4 | 3 |
| `INFO` | 113.1 | 81.8 | 48 | 32 | 4 | 3 |
| text `CHUNK` | 365.4 | 302.9 | 184 | 136 | 9 | 8 |
| binary `CHUNKBIN` | 342.5 | 279.4 | 168 | 120 | 8 | 7 |

The focused after-only measurements reported 16 B/op and 1 alloc/op for
reusable frame parsing, and 0 B/op and 0 allocs/op for reusable connection
frame reads. These microbenchmarks isolate the paths changed by buffer reuse;
they do not represent full TCP throughput.

Elapsed time remains sensitive to system load, CPU frequency, and toolchain
changes. The deterministic allocation reductions are the primary evidence;
the timing values characterize only this measurement environment.

## WAL batching trade-off

In `fsync-wal` mode, `wal_group_commit_updates=1` retains the original
per-update WAL synchronization boundary. A value of `N > 1` acknowledges each
write after its WAL append and chunk replacement, but synchronizes the WAL only
after every Nth update or during an orderly close. This amortizes sync cost at
the expense of allowing up to `N-1` acknowledged updates after the last
successful WAL sync to be lost or depend on filesystem persistence after a
power or kernel failure.

Batching does not change WAL encoding, checkpoint thresholds, or recovery
validation. Process termination alone may still leave appended WAL bytes for
recovery, but that is not equivalent to an `fsync` durability guarantee.
