# Windows benchmark cleanup post-fix snapshot

This snapshot records the validation state for commit
`775f588147feb06cdbba4260a3673749276a19a2`, which makes benchmark cleanup
ordering explicit and publishes active lock modes. It contains no throughput
comparison.

## Lock-mode contract

On a native Windows writer, the implemented values are:

```text
process_lock_mode=lock-file-ex
chunk_lock_mode=shared-rwmutex
```

`INFO` publishes those fields before the bounded runtime counters. Direct and
TCP benchmark JSON copy both values into `lock_modes`; the reproducible direct
benchmark manifest copies them again beside its environment metadata. This
distinguishes the Windows `LockFileEx` process guard from Go's portable
`sync.RWMutex` chunk access path.

## Cleanup regression

The TCP benchmark smoke now performs teardown in this order:

1. cancel the server context and wait for `Serve` to return;
2. close the store, including its WAL and writer-guard handles;
3. remove the benchmark data directory;
4. verify that the directory no longer exists.

This checks the equivalent Go risk behind a Windows benchmark leaving its
writer-lock handle live during temporary-directory cleanup. The direct
benchmark continues to preserve its caller-provided data directory by
contract.

## Validation

The post-fix commit passed the following local gate on Linux amd64 with Go
1.24.13:

```text
go vet ./...
go test ./...
go build ./...
golangci-lint run
go test -race ./...
go test -count=1 ./...
go test -run Integration -count=1 ./...
go test -run Crash -count=1 ./...
go test -run Stress -count=20 ./...
go test -run '^TestRunQuick(TCP|Direct)Benchmark$' -count=20 \
  ./cmd/regiondb-bench ./cmd/regiondb-direct-bench
```

The affected commands and packages also passed a Windows amd64 compile-only
check:

```text
GOOS=windows GOARCH=amd64 go test -run '^$' -exec /usr/bin/true \
  ./cmd/regiondb-bench ./cmd/regiondb-direct-bench \
  ./internal/protocol ./internal/server ./internal/storage/fs_split
```

The earlier [Windows-native validation snapshot](windows-native-evidence-2026-07-30.md)
provides native execution evidence for both benchmark smokes and the
`LockFileEx` writer guard on the parent line.

## Evidence boundary

The compile-only command does not execute Windows binaries. This snapshot
therefore does not claim a post-fix native Windows runtime result before the
pull request's Windows job runs, and it does not infer throughput from smoke
test elapsed time. Its claims are limited to the implemented lock-mode
contract, deterministic teardown regression, green local gate, Windows
compilation, and the separately identified native baseline.
