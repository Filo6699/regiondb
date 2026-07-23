# Docker

The repository provides a multi-stage image and a Compose configuration for
running `regiondb` as an unprivileged user. Chunk data is stored in the
`regiondb-data` volume.

## Compose

Set the authentication token explicitly before starting the service:

```sh
export REGIONDB_TOKEN='replace-with-a-random-token'
docker compose up --build -d regiondb
docker compose ps
```

The Compose configuration publishes TCP port `4242` on host loopback only.
Set `REGIONDB_PORT` to publish a different loopback port:

```sh
REGIONDB_PORT=14242 docker compose up --build -d regiondb
```

The image healthcheck authenticates over the container loopback TCP listener
and issues `INFO`. It reports healthy only when both commands return valid
responses, so an open socket that cannot serve authenticated requests is not
ready.

Stop the service without deleting its data:

```sh
docker compose down
```

Deleting the `regiondb-data` volume permanently removes the stored chunks. Back
up the volume before using `docker compose down --volumes`.

## Test profile

The opt-in `test` profile starts the server, waits for its healthcheck, and runs
an authenticated protocol smoke test:

```sh
export REGIONDB_TOKEN='test-only-random-token'
docker compose --profile test up --build \
  --abort-on-container-exit --exit-code-from test
docker compose --profile test down
```

The test profile uses the same persistent volume as the normal service.

## Runtime sizing and performance boundaries

The image and Compose file do not set CPU or memory limits and do not
automatically tune regiondb from container limits. Set container resource
limits in the deployment environment, then size the existing server options
for that budget. In particular:

- `-max-loaded-chunks` bounds resident chunk payloads, but the process also
  needs memory for cache metadata, WAL buffers, queued connections, command
  buffers, and the Go runtime.
- `-workers` bounds concurrently served connections. `-accept-queue` bounds
  accepted connections waiting for a worker, and each active or queued
  connection consumes socket and process resources.
- `-max-line-bytes` bounds text command input, but binary chunk payload size is
  determined by the configured geometry.
- `-idle-timeout`, `-request-timeout`, and `-response-timeout` bound socket
  occupancy. Authentication failures are delayed and temporarily banned per
  source according to the `-auth-failure-*` and `-auth-ban-duration` controls.

Pass option overrides after the image name with `docker run`, or replace the
Compose service `command`. Keep geometry identical when reopening an existing
data volume.

Container storage is part of the measured system. Named volumes and bind
mounts can have materially different latency and durability behavior across
Docker Desktop, native Linux, storage drivers, and host filesystems. The
selected `-durability` mode defines which sync operations regiondb requests;
Docker and the underlying storage stack determine how those requests reach
persistent media. Benchmark the intended volume, durability mode, geometry,
cache size, and resource limits on the target host.

The healthcheck and test profile prove authenticated startup and a basic
request path only. They are not load tests, readiness guarantees under
sustained traffic, or performance thresholds. Scenario benchmark results are
comparable only when the recorded runtime and storage conditions also match.

## Docker CLI

Build and run the image without Compose:

```sh
docker build -t regiondb:local .
docker volume create regiondb-data
docker run --rm \
  --name regiondb \
  --publish 4242:4242 \
  --volume regiondb-data:/var/lib/regiondb \
  --env REGIONDB_TOKEN \
  regiondb:local
```

`REGIONDB_TOKEN` has no default and is never stored in the image. The container
exits before opening the data directory or listener when it is absent or empty.
Pass `-no-auth` explicitly only for an intentionally unauthenticated container.
Avoid putting real tokens in image build arguments, Dockerfiles, Compose files,
or shell history.
