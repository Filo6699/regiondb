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

World reads are independently bounded. `CHUNKSCAN`, `CHUNKRANGE`, and
`CHUNKRADIUS` process at most 256 populated/candidate chunk coordinates per
request, and range/radius responses have a 64 MiB encoded-wire cap. Arithmetic
is checked over the signed 64-bit coordinate domain before allocation.
`CHUNKSCAN` retains at most one candidate beyond its page. `CHUNKBINC` decoding
has an exact geometry-derived output bound.

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
Startup logs the selected token source (`command_line`, `environment`, `file`,
or `disabled`) without logging the credential value.

Failed authentication is tracked by source address. Each failure is delayed by
`-auth-failure-delay` (250 milliseconds by default). After
`-auth-failure-limit` failures (five by default), that source is rejected for
`-auth-ban-duration` (one minute by default). Successful authentication clears
the source state. IPv6 clients are grouped by `/64`. At most 4,096 source
entries are retained; least-recent inactive entries are evicted, active bans
are preserved, and a new source is delayed but not retained when all slots are
actively banned. These controls slow repeated attempts but are not a firewall
or a distributed rate limiter; deploy network-level controls where those
properties are required.

## Storage and observability bounds

`-max-loaded-chunks` is a hard resident-cache limit and defaults to 1,024.
Recency plus one second chance favors recently read entries. If every entry was
touched during contention, one complete scan clears the chances and evicts the
least-recent entry, guaranteeing progress. Eviction maintenance has one worker
and 16 queued tasks; a full queue applies inline backpressure, and shutdown
drains it.

`-max-open-wal-streams` bounds cached WAL handles before the operating-system
descriptor clamp. At startup, regiondb reserves descriptors for the listener,
workers, and accepted queue, then reduces the queue or WAL handle limit when
necessary rather than exceeding the detected budget.

Socket readiness is delegated to Go's network poller. regiondb does not keep a
descriptor-indexed fixed bitmap such as `FD_SET`; the integration suite forces
the listener and overload-response socket above descriptor 1,100 and verifies
that the bounded `BUSY` reply remains functional.

The unsynchronized publication tracker retains at most 4,096 paths. On
overflow, `WALFLUSH` switches to a bounded-memory full data-tree walk. Files
are synchronized before directories, and any failure preserves retry state.
`-checkpoint-compression=zrle` bounds decompression by geometry and uses
compression only when a checkpoint image becomes smaller.

`METRICS` exposes fixed-cardinality Prometheus text. It has no
coordinate-, command-argument-, token-, or source-derived labels. The command
duration histogram has a fixed bucket set; counters reset at process start.

## Deployment guidance

Start with the defaults, measure representative peak traffic, and set explicit
values when the deployment needs a reproducible resource envelope. Keep
`-workers` and `-accept-queue` within the process file-descriptor budget and
leave memory for the configured `-max-loaded-chunks` cache. Tight timeouts can
reject legitimate slow clients, while large queues and timeouts retain
resources longer during overload.

Use `WALFLUSH` when an application needs an explicit recoverability barrier
across writes previously acknowledged under any steady-state durability mode.
The barrier can perform substantial I/O after the 4,096-path tracker
overflows, so size client response deadlines for the intended data directory
and storage medium.

Shutdown is deterministic: cancellation closes the listener plus active and
queued connections, waits for workers, and then closes storage. Storage
durability remains controlled separately by `-durability`; connection limits
and deadlines do not strengthen its acknowledgement guarantees.

See the [concurrency model](CONCURRENCY.md) for ownership and shutdown details,
the [protocol specification](PROTOCOL.md) for authentication behavior, and the
[Docker guide](DOCKER.md) for container sizing.
