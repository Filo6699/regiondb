package fs_split

import (
	"errors"
	"syscall"
)

const (
	windowsErrorSharingViolation syscall.Errno = 32
	windowsErrorLockViolation    syscall.Errno = 33
)

func isTransientReplaceError(err error) bool {
	// MoveFileEx reports these errors while another process temporarily denies
	// delete access or holds a byte-range lock. Other failures can describe a
	// permanent path or permission problem and must reach the caller at once.
	return errors.Is(err, windowsErrorSharingViolation) ||
		errors.Is(err, windowsErrorLockViolation)
}
