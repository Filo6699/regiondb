#!/bin/sh
set -eu

if [ -z "${REGIONDB_TOKEN:-}" ]; then
    echo "REGIONDB_TOKEN must be set to a non-empty token" >&2
    exit 1
fi

exec /usr/local/bin/regiondb -token "$REGIONDB_TOKEN" "$@"
