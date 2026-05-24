# Sparse low-cache TCP benchmark

This snapshot records repeated mixed-workload TCP measurements with the
production `fs_split_v1` backend limited to one resident chunk. Each platform
uses five independent server processes, data directories, ports, and raw
result artifacts. The summary reports the median and the complete minimum to
maximum spread; no run was selected or discarded based on throughput.

## Shared scenario

| Setting | Value |
|---|---|
| Benchmark implementation | `7ae725264abafa6ceeb823fd5bdcd34103a69e87` |
| Seed | `42` |
| Operations per run | `10,000` |
| Workload | mixed: 7,962 reads and 2,038 writes |
| Working set | 1,024 chunks |
| Resident chunk limit | `1` |
| Geometry | chunk edge 16, large-chunk edge 8, 5 bits per block |
| Durability | relaxed |
| Client connections | 1 |

The server is built before measurement and started with:

```sh
regiondb \
  -listen 127.0.0.1:PORT \
  -data-dir DATA_DIR \
  -token TOKEN \
  -max-loaded-chunks 1 \
  -chunk-edge 16 \
  -large-chunk-edge 8 \
  -block-bits 5
```

Readiness is established with a bounded authenticated `PING` loop. The
benchmark then runs:

```sh
regiondb-bench \
  -address 127.0.0.1:PORT \
  -token TOKEN \
  -seed 42 \
  -ops 10000 \
  -workload mixed \
  -chunk-edge 16 \
  -large-chunk-edge 8 \
  -block-bits 5
```

## Linux measurement

| Field | Value |
|---|---|
| Execution environment | Debian 12 container on Arch Linux |
| Kernel | Linux 6.18.9-arch1-2 x86-64 |
| CPU | 11th Gen Intel Core i5-11300H |
| Go | go1.24.13 linux/amd64 |
| Process lock | `flock` |
| Chunk lock | `shared-rwmutex` |

| Metric | Median | Full spread |
|---|---:|---:|
| Operations per second | 24,056.80 | 20,247.92–29,133.44 |
| Duration | 415.68 ms | 343.25–493.88 ms |
| Latency p50 | 26.10 us | 21.68–29.97 us |
| Latency p95 | 112.79 us | 85.84–132.57 us |
| Latency p99 | 150.32 us | 96.39–185.64 us |

All five JSON results are preserved in
[`sparse-low-cache-linux-2026-07-30.jsonl`](sparse-low-cache-linux-2026-07-30.jsonl).
The throughput spread is 8,885.52 operations per second, or 36.94% of the
median. This variability is part of the evidence and is not filtered.

## Windows measurement

The five matrix jobs ran independently on GitHub-hosted Windows workers. The
[workflow run](https://github.com/Filo6699/regiondb/actions/runs/30556105338)
used commit `577764bcbab32d1b0667b55c49afc80c82a48ab3`; that commit only aligns the
workflow's explicit geometry with the already measured benchmark
implementation.

| Field | Value |
|---|---|
| Operating system | Microsoft Windows Server 2025 Datacenter, 10.0.26100 |
| Runner image | `windows-2025-vs2026`, version `20260714.173.1` |
| Go | go1.24.13 windows/amd64 |
| Process lock | `lock-file-ex` |
| Chunk lock | `shared-rwmutex` |

| Metric | Median | Full spread |
|---|---:|---:|
| Operations per second | 4,712.40 | 4,019.36–6,638.14 |
| Duration | 2,122.06 ms | 1,506.45–2,487.96 ms |
| Latency p50 | 0 us | 0–0 us |
| Latency p95 | 683.40 us | 660.50–699.60 us |
| Latency p99 | 809.00 us | 791.70–877.60 us |

All five JSON results are preserved in
[`sparse-low-cache-windows-2026-07-30.jsonl`](sparse-low-cache-windows-2026-07-30.jsonl).
The throughput spread is 2,618.78 operations per second, or 55.57% of the
median. Each job succeeded on its first attempt. The Windows clock reported
zero-duration samples through p50 in every run, so those p50 values expose the
available timer resolution rather than a claim of zero-cost operations.

## Interpretation boundary

These results describe this exact sparse, low-cache scenario. They are not a
cross-machine ranking, a performance guarantee, or a CI threshold. Container
overhead, filesystem cache state, host load, CPU frequency scaling, storage,
and operating-system behavior can materially change the result.
