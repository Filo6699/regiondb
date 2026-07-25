#!/usr/bin/env bash
set -euo pipefail

readonly image="${1:-regiondb:security-regression}"

if docker image inspect --format '{{range .Config.Env}}{{println .}}{{end}}' \
  "$image" | grep -q '^REGIONDB_TOKEN='; then
  echo "image config contains REGIONDB_TOKEN" >&2
  exit 1
fi

if docker history --no-trunc --format '{{.CreatedBy}}' "$image" |
  grep -Eq 'REGIONDB_TOKEN=[^[:space:]$"]+'; then
  echo "image history contains an embedded REGIONDB_TOKEN value" >&2
  exit 1
fi

set +e
timeout 10s docker run --rm "$image"
status="$?"
set -e
if [[ "$status" == "0" ]]; then
  echo "image started without explicit authentication configuration" >&2
  exit 1
fi
if [[ "$status" == "124" ]]; then
  echo "image remained running without explicit authentication configuration" >&2
  exit 1
fi
