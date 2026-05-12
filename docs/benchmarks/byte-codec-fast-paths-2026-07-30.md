# Byte codec fast-path results

This snapshot compares the bit-by-bit codec with byte-aligned get/set paths on
the same Linux host. The benchmark performs one Set and one Get per operation.

```text
go test ./internal/bitcodec -run '^$' -bench '^BenchmarkCodecSetGet$' -benchmem -count=5
```

| Width/order | Before median ns/op | After median ns/op |
|---|---:|---:|
| 8/LSB | 38.22 | 10.40 |
| 8/MSB | 40.16 | 10.42 |
| 16/LSB | 81.63 | 10.27 |
| 16/MSB | 82.84 | 10.32 |
| 24/LSB | 113.8 | 15.24 |
| 24/MSB | 119.1 | 14.00 |
| 32/LSB | 147.3 | 10.28 |
| 32/MSB | 153.5 | 10.97 |
| 64/LSB | 269.8 | 10.44 |
| 64/MSB | 287.7 | 10.17 |

All samples remained at zero allocations per operation. The non-byte-aligned
5-bit control retained the existing bit path and stayed within benchmark noise.
The complete sample values and environment are recorded in
`byte-codec-fast-paths-2026-07-30.txt`. These results are local evidence, not a
portable throughput promise or CI threshold.
