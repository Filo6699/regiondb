#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
readonly repo_root
cd "$repo_root"

run_test() {
  local test_name="$1"
  shift

  local output
  output="$(go test -json -count=1 -run "^${test_name}$" "$@")"
  printf '%s\n' "$output"
  if ! grep -Eq \
    "\"Action\":\"pass\".*\"Test\":\"${test_name}\"" \
    <<<"$output"; then
    echo "security regression did not run to completion: ${test_name}" >&2
    return 1
  fi
}

run_test TestIntentControlPathRejectsTraversalAndUnsafeGrammar \
  ./internal/storage/fs_split
run_test TestParseChunkFileNameCoversInt64Domain ./internal/storage/fs_split
run_test TestRegionImageRejectsOutOfBoundsSlotBeforeWrite \
  -tags regiondb_experimental ./internal/storage/fs_region
run_test TestStoreReopenRoundTrip \
  -tags regiondb_experimental ./internal/storage/fs_region
run_test TestNewEngineValidation ./internal/protocol
run_test TestParseConfigAuthenticationPrecedence ./cmd/regiondb

case "$(go env GOOS)" in
  aix|darwin|dragonfly|freebsd|illumos|linux|netbsd|openbsd|solaris)
    run_test TestIntegrationHighFileDescriptorBusyResponse \
      ./internal/server
    ;;
esac
