# Native Windows guide

regiondb runs natively on Windows and stores data in an ordinary Windows
directory. The commands below use PowerShell and Go 1.24.13, the version used
by the Windows CI job.

## Prerequisites

Install Go 1.24 or later and Git, then confirm the selected toolchain:

```powershell
go version
```

Clone the repository and run the same vet, test, and build gate used by the
cross-platform CI matrix:

```powershell
git clone https://github.com/Filo6699/regiondb.git
Set-Location regiondb
go vet ./...
go test ./...
go build ./...
```

Build a standalone executable:

```powershell
New-Item -ItemType Directory -Force .\bin | Out-Null
go build -o .\bin\regiondb.exe .\cmd\regiondb
.\bin\regiondb.exe -version
```

## Run the server

Choose a data directory and a non-empty authentication token:

```powershell
New-Item -ItemType Directory -Force .\data | Out-Null
.\bin\regiondb.exe `
  -data-dir .\data `
  -token development-secret `
  -chunk-edge 16 `
  -large-chunk-edge 8 `
  -block-bits 5 `
  -durability fsync-wal
```

The server listens on `127.0.0.1:4242` by default. Windows Defender Firewall
does not need an inbound rule for loopback-only use. If `-listen` exposes the
server on another interface, restrict the firewall rule to the intended
network and use TLS for untrusted networks. The authentication token is shared
by every client and must be treated as a secret.

Stop the process with Ctrl+C. An orderly stop closes the listener, drains
owned workers, flushes pending WAL data when required, and releases the
data-directory writer lock.

## Windows storage behavior

Only one writing process may open a data directory. Windows writer ownership
uses an exclusive `LockFileEx` lock on `.regiondb.lock\guard`; a second writer
fails instead of opening the directory concurrently. Stale owner metadata is
handled according to the [concurrency model](CONCURRENCY.md).

The `fsync-wal` and `fsync-checkpoint` modes flush file data before reporting
their documented commit points. Atomic file replacement uses
`MoveFileEx` with write-through behavior when a synchronized replacement is
required. Windows does not expose the same parent-directory synchronization
operation as Unix, so the precise guarantees and limitations are defined in
the [storage format](STORAGE_FORMAT.md).

Do not place an active data directory on a network share or in a directory
synchronized by another program. Filesystems and filter drivers can provide
weaker locking, replacement, or persistence behavior than a local NTFS
volume. Stop regiondb before copying a data directory for backup.

## Support matrix

| Surface | Linux | macOS | Windows |
|---|---|---|---|
| Server and client protocol | CI tested | CI tested | CI tested |
| `fs_split_v1` writer ownership | `flock` | `flock` | `LockFileEx` |
| WAL replay and checkpoint tests | CI tested | CI tested | CI tested |
| Race detector | CI tested | Not in the required gate | Not in the required gate |
| Native build | CI tested | CI tested | CI tested on x86-64 |
| Docker image | Native Linux container | Linux container through Docker Desktop | Linux container through Docker Desktop |

The matrix describes repository CI coverage, not every operating-system,
filesystem, architecture, or storage-driver combination. The experimental
`fs_region_v1` layout is opt-in and is not part of the supported storage
contract.

## Troubleshooting

- If startup reports that the writer is locked, stop the other regiondb
  process using that data directory. Do not delete lock files while a writer
  may still be running.
- If Windows reports an access or sharing violation, exclude the active data
  directory from tools that open files without delete sharing, then retry.
- If binding a non-loopback address fails, check that the address exists and
  that local firewall policy permits the listener.
- If the server cannot load a TLS key pair, verify that both `-tls-cert` and
  `-tls-key` point to readable PEM files.
