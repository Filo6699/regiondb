# syntax=docker/dockerfile:1

FROM golang:1.24-alpine AS build

WORKDIR /src
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal
COPY pkg ./pkg
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/regiondb ./cmd/regiondb

FROM alpine:3.22

RUN apk add --no-cache netcat-openbsd \
    && addgroup -S -g 10001 regiondb \
    && adduser -S -D -H -u 10001 -G regiondb regiondb \
    && install -d -o regiondb -g regiondb /var/lib/regiondb

COPY --from=build /out/regiondb /usr/local/bin/regiondb
COPY scripts/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh

USER regiondb
EXPOSE 4242
VOLUME ["/var/lib/regiondb"]

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD response="$(printf 'AUTH %s\r\nINFO\r\n' "$REGIONDB_TOKEN" | nc -w 2 127.0.0.1 4242)" \
        && printf '%s\n' "$response" | grep -q '^+OK' \
        && printf '%s\n' "$response" | grep -q 'regiondb'

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD ["-listen", "0.0.0.0:4242", "-data-dir", "/var/lib/regiondb", "-chunk-edge", "16", "-large-chunk-edge", "8", "-block-bits", "5"]
