#!/bin/sh
set -eu

if [ -n "${REGIONDB_TOKEN:-}" ]; then
    set -- -token "$REGIONDB_TOKEN" "$@"
else
    set -- -no-auth "$@"
fi

exec /usr/local/bin/regiondb "$@"
