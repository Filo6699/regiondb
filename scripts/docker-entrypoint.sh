#!/bin/sh
set -eu

if [ "${1:-}" = "healthcheck" ]; then
    [ -n "${REGIONDB_TOKEN:-}" ] || exit 1
    response="$(
        printf 'AUTH %s\r\nINFO\r\n' "$REGIONDB_TOKEN" |
            nc -w 2 127.0.0.1 4242
    )"
    printf '%s\n' "$response" | grep -q '^+OK'
    printf '%s\n' "$response" | grep -q 'regiondb'
    exit 0
fi

if [ -n "${REGIONDB_TOKEN:-}" ]; then
    set -- -token "$REGIONDB_TOKEN" "$@"
fi

exec /usr/local/bin/regiondb "$@"
