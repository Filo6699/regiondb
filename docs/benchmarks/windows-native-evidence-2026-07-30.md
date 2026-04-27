# Windows-native validation evidence

This snapshot records Windows-native correctness evidence for commit
`ee1f3bf40d89e4a59e6f8850653be901d040e35d`. The source is the
[completed Windows CI job](https://github.com/Filo6699/regiondb/actions/runs/30538663563/job/90857971395)
from 2026-07-30.

## Environment and gate

The GitHub-hosted runner reported:

- `GOOS=windows`
- `GOARCH=amd64`
- `GOVERSION=go1.24.13`

The job completed `go vet ./...`, `go test ./...`, and `go build -v ./...`.
Its JSON test log identifies the individual cases summarized below.

| Case | Result | Elapsed |
|---|---:|---:|
| Direct benchmark functional smoke | pass | 0.09 s |
| TCP benchmark functional smoke | pass | 0.09 s |
| Concurrent store reads and writes | pass | 0.46 s |
| Second writer rejection | pass | 0.02 s |
| Shared-coordinate contention | pass | 1.34 s |
| Hot-contention eviction cycles | pass | 1.69 s |
| Complete `fs_split_v1` package | pass | 15.52 s |

The direct and TCP smoke cases each execute a deterministic 12-operation
workload and validate completion plus JSON output. The storage cases exercise
Go read/write mutexes and the Windows `LockFileEx` writer guard. This is the
relevant portability boundary: Go's `sync.RWMutex` does not require a
platform-specific fallback, while data-directory exclusion still depends on
the operating-system file lock.

The corresponding
[Linux race job](https://github.com/Filo6699/regiondb/actions/runs/30538663563/job/90857971288)
completed `go test -race ./...`. A follow-up child-process writer-lock
regression was also run locally on Linux amd64 with Go 1.24.13, including the
full race gate and 20 repeated targeted runs.

## Caveats

- The 12-operation benchmark smokes validate behavior, not steady-state
  performance. Their elapsed times are not throughput measurements.
- GitHub-hosted runner hardware and load are not controlled, so timings from
  this job must not be compared with dedicated benchmark snapshots.
- The race detector evidence is from Linux. The Windows job exercises the same
  Go synchronization path without the race detector and separately covers the
  Windows file-lock implementation.
- The writer-lock result applies to the hosted runner's local filesystem. It
  does not extend support claims to network shares, synchronized directories,
  filter drivers, or every Windows filesystem.
- This snapshot predates the child-process writer-lock regression. Results for
  later revisions should be taken from their own CI runs rather than inferred
  from this baseline.
