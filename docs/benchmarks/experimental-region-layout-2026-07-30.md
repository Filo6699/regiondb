# Experimental region layout comparison

This snapshot compares the experimental `fs_region_v1` layout with the
production `fs_split_v1` layout using the same seeded operation sequence. It is
evidence from one local environment, not a cross-machine target or a CI
performance threshold.

## Environment

| Field | Value |
|---|---|
| Measurement date | 2026-07-30 |
| Hardware | 11th Gen Intel Core i5-11300H, 4 cores / 8 logical CPUs |
| Architecture | x86-64 |
| OS | Arch Linux, Linux 6.18.9-arch1-2 |
| Go | go1.24.13 linux/amd64 |
| Commit | `6c955c817256eeaa4a43b63228c47bebe562ca88` |

## Command

The benchmark used a fixed seed and a 1,024-chunk working set. Read and mixed
workloads prepared both stores before measurement. Ten samples were collected
for each workload and layout:

```sh
go test -run '^$' \
  -tags=regiondb_experimental \
  -bench '^BenchmarkLayout$' \
  -benchmem \
  -count=10 \
  ./internal/storage/fs_region
```

All command output is preserved in
[`experimental-region-layout-2026-07-30.txt`](experimental-region-layout-2026-07-30.txt).

## Observed result

The table reports the median elapsed time across ten samples. Allocation
columns were stable except for the production mixed workload, which varied
between 5 and 6 allocations per operation; its table entry reports the lower
observed value.

| Workload | Layout | Median ns/op | B/op | allocs/op |
|---|---|---:|---:|---:|
| read | `fs_region_v1` | 530.8 | 384 | 3 |
| read | `fs_split_v1` | 196.4 | 224 | 2 |
| write | `fs_region_v1` | 845.8 | 160 | 1 |
| write | `fs_split_v1` | 13,959.0 | 1,789 | 22 |
| mixed | `fs_region_v1` | 619.3 | 339 | 2 |
| mixed | `fs_split_v1` | 3,130.5 | 535 | 5 |

On this machine, the experimental layout's median existing-chunk read was
2.70 times slower than the production layout. Its in-place write path was
16.50 times faster, and the read-heavy mixed workload was 5.06 times faster.
Those write results compare different persistence designs: `fs_region_v1`
writes image slots in place and has no WAL, checkpoint, crash-recovery, or
durability-mode contract.

## Decision

`fs_region_v1` is a no-go for the production default and for compatibility or
durability guarantees. The measured write advantage does not establish an
equivalent durable write path, while the equivalent existing-chunk read path
regressed materially. The experiment therefore remains opt-in evidence only;
`fs_split_v1` remains the production layout.

This decision does not establish a permanent performance ranking. A future
comparison would need equivalent durability boundaries, crash/recovery
coverage, multiple operating systems and storage devices, and representative
sparse data before reconsidering the layout.
