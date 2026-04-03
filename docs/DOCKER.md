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

The Compose configuration publishes TCP port `4242`. Set `REGIONDB_PORT` to
publish a different host port:

```sh
REGIONDB_PORT=14242 docker compose up --build -d regiondb
```

The image healthcheck authenticates over the loopback TCP listener and issues
`INFO`. It reports healthy only when both commands return valid responses.

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
Avoid putting real tokens in image build arguments, Dockerfiles, Compose files,
or shell history.
