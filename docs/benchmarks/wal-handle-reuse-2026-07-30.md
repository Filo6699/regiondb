# WAL handle reuse benchmark

The append benchmark compares the existing per-record seek with an append-mode
handle on the same Linux host. Both variants keep the WAL open for the Store
lifetime and preserve the existing durability boundaries. Windows retains the
same read/write handle but positions it before appends because Go append-mode
handles cannot also perform the checkpoint truncation required by the WAL.

Command:

```text
go test ./internal/storage/fs_split -run '^$' -bench '^BenchmarkAppendWAL$' -benchmem -count=5
```

The median improved from 513.3 ns/op to 323.8 ns/op (36.9%) while remaining at
zero allocations per operation. The accompanying cache benchmark exercises
continuous LRU eviction with 1, 1,024, and 65,536 residents; every case remains
at zero allocations per eviction and inspects only the selected LRU entry.

The raw benchmark output, Go target, CPU, and package are recorded in
`wal-handle-reuse-2026-07-30.txt`. Results are evidence for this host, not a
portable throughput guarantee or CI threshold.
