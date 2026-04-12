#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
readonly repo_root
cd "$repo_root"

go vet ./...

if [[ -n "${REGIONDB_TEST_JSON_OUTPUT:-}" ]]; then
  go test -json ./... | tee "$REGIONDB_TEST_JSON_OUTPUT"
else
  go test ./...
fi

if [[ "${REGIONDB_BUILD_VERBOSE:-0}" == "1" ]]; then
  go build -v ./...
else
  go build ./...
fi
