# regiondb text protocol version 1

This document specifies the text protocol implemented by the current
development version. There is no version negotiation on the wire.

## Transport and session

A server accepts TCP connections. Plaintext endpoints use
`region://token@host:port/`; TLS endpoints use
`regions://token@host:port/` and require TLS 1.2 or later.

Each connection owns an independent authentication session. A request is one
printable ASCII command line terminated by CRLF. Command names use uppercase
ASCII. Tokens are separated by exactly one space. Empty tokens, embedded line
breaks, other control bytes, and an unterminated final line are rejected as
frame errors.

`AUTH` and `QUIT` may be sent before authentication. Every other command
returns `NOAUTH` until authentication succeeds. A failed `AUTH` clears any
previously authenticated state.

## Responses

Simple success:

```text
+OK [detail]\r\n
```

Error:

```text
-ERR CODE [detail]\r\n
```

Bulk data:

```text
$<decimal-byte-count>\r\n<payload>\r\n
```

The byte count covers only `payload`. Protocol errors do not close the
connection. `QUIT` sends its response and then closes it.

## Commands

All integers use decimal ASCII input without a leading plus sign. Coordinates
are signed 64-bit integers. Block values are unsigned 64-bit integers and must
fit the configured block width.

| Command | Response | Behavior |
|---|---|---|
| `AUTH token` | `+OK` or `-ERR AUTH ...` | Authenticate the connection. |
| `PING` | `+OK PONG` | Check an authenticated session. |
| `INFO` | Bulk `regiondb` identifier | Identify the server implementation. |
| `GET x y` | Bulk decimal value | Read a block; an absent chunk reads as zero. |
| `SET x y value` | `+OK` | Persist one packed block value. |
| `EXISTS x y` | `+OK 0` or `+OK 1` | Report whether the current block value is nonzero. |
| `CHUNK x y` | Bulk lowercase hexadecimal payload | Read a packed regular chunk by chunk coordinate. |
| `CHUNKBIN x y` | Bulk binary payload | Read the exact packed regular-chunk bytes without hexadecimal encoding. |
| `CHUNKSET x y payload` | `+OK` | Persist a packed regular chunk from an exact-length hexadecimal payload. |
| `QUIT` | `+OK` | Close the session after the response. |

`CHUNK` and `CHUNKBIN` return `NOT_FOUND` when their chunk file is absent.
Storage failures are reported with the `STORAGE` code. The binary byte count
is the existing bulk response length; payload bytes may contain any value.
The packed payload layout is defined in the
[storage format specification](STORAGE_FORMAT.md).

The server rejects command lines larger than its configured `max_line_bytes`
limit, including CRLF, and keeps the connection available after a complete
oversized line. No binary write command, pipelining guarantee, protocol
upgrade, or durability negotiation is part of version 1. The CLI listens on
`127.0.0.1:4242` by default. Durability is a server startup setting and does
not change the wire format.
