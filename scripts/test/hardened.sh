#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
readonly repo_root
cd "$repo_root"

go vet ./...
go test -run '^$' -fuzz '^FuzzParseFrame$' -fuzztime=10s ./internal/protocol
go test -run '^$' -fuzz '^FuzzCodecRoundTrip$' -fuzztime=10s ./internal/bitcodec
go test -gcflags=all=-d=checkptr=2 ./internal/bitcodec ./internal/protocol ./internal/storage/...
