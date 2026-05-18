# Cache eviction hysteresis benchmark

The cache benchmark compares per-admission LRU eviction with a high/low
watermark. The configured maximum remains the hard high watermark. A run
reclaims one quarter of the resident entries, with a minimum of one, and later
admissions reuse those entries and payload buffers.

```text
go test ./internal/storage/fs_split -run '^$' -bench '^BenchmarkChunkCacheEviction$' -benchmem -count=5
```

At the default 1,024-entry limit, eviction-run frequency fell from one run per
admission to one per 256 admissions. Median time changed from 79.28 ns/op to
75.80 ns/op. The one-entry control cannot create a hysteresis window and
changed from 53.57 ns/op to 57.97 ns/op. The 65,536-entry case changed from
125.0 ns/op to 140.2 ns/op while reducing run frequency to approximately one
per 16,384 admissions. All cases remained at zero allocations per operation.

The raw samples and host details are recorded in
`eviction-hysteresis-2026-07-30.txt`. These local results demonstrate reduced
eviction-run frequency; they are not a portable throughput guarantee or CI
threshold.
