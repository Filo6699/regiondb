//go:build darwin || dragonfly || freebsd || illumos || linux || netbsd || openbsd

package fs_split

import (
	"errors"
	"syscall"
)

// isUnsupportedDirectorySyncError separates "this filesystem cannot flush a
// directory handle" from a real I/O failure. Only the capability answers below
// are treated as unsupported; EIO, ENOSPC, EACCES and every other errno stay
// write failures and reach the caller unchanged.
func isUnsupportedDirectorySyncError(err error) bool {
	return errors.Is(err, syscall.EINVAL) ||
		errors.Is(err, syscall.ENOTSUP) ||
		errors.Is(err, syscall.ENOSYS)
}
