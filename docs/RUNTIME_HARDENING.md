# Runtime hardening controls

regiondb exposes bounded server controls for deployments where clients may be
slow, faulty, or untrusted. These controls complement operating-system and
container limits; they do not replace them.

## Connection capacity

`-workers` limits the number of connections served concurrently and defaults
to `GOMAXPROCS`. `-accept-queue` limits accepted connections waiting for a
worker and defaults to 128. When both limits are occupied, the server sends a
best-effort `BUSY` response under a short deadline and closes the excess
connection.

`-max-line-bytes` limits a text command, including its CRLF terminator, and
defaults to 1 MiB. Binary chunk payload size is instead fixed by the configured
geometry. Memory planning must include active command buffers, queued sockets,
the chunk cache, WAL buffers, and the Go runtime.

## I/O deadlines

The server applies three independent deadlines:

- `-idle-timeout` limits the wait for the first request byte and defaults to
  30 seconds.
- `-request-timeout` limits receipt of the complete request after its first
  byte and defaults to 10 seconds. Additional bytes do not extend this
  deadline.
- `-response-timeout` limits response writes and defaults to 10 seconds.

All three values must be positive. A deadline closes the affected connection;
it does not change whether an earlier command was applied. Clients must use the
protocol response, not a later socket error, to determine command success.

## Authentication controls

Authentication is required unless `-no-auth` is explicit. Token precedence is
`-token`, `REGIONDB_TOKEN`, then `-token-file`; `-no-auth` overrides an ambient
environment token and cannot be combined with token flags. A non-loopback
listener logs a warning without logging token values or token-file contents.

Failed authentication is tracked by source address. Each failure is delayed by
`-auth-failure-delay` (250 milliseconds by default). After
`-auth-failure-limit` failures (five by default), that source is rejected for
`-auth-ban-duration` (one minute by default). Successful authentication clears
the source state. These controls slow repeated attempts but are not a firewall
or a distributed rate limiter; deploy network-level controls where those
properties are required.

## Deployment guidance

Start with the defaults, measure representative peak traffic, and set explicit
values when the deployment needs a reproducible resource envelope. Keep
`-workers` and `-accept-queue` within the process file-descriptor budget and
leave memory for the configured `-max-loaded-chunks` cache. Tight timeouts can
reject legitimate slow clients, while large queues and timeouts retain
resources longer during overload.

Shutdown is deterministic: cancellation closes the listener plus active and
queued connections, waits for workers, and then closes storage. Storage
durability remains controlled separately by `-durability`; connection limits
and deadlines do not strengthen its acknowledgement guarantees.

See the [concurrency model](CONCURRENCY.md) for ownership and shutdown details,
the [protocol specification](PROTOCOL.md) for authentication behavior, and the
[Docker guide](DOCKER.md) for container sizing.
