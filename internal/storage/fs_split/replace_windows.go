package fs_split

import (
	"errors"

	"golang.org/x/sys/windows"
)

const (
	windowsErrorSharingViolation = windows.ERROR_SHARING_VIOLATION
	windowsErrorLockViolation    = windows.ERROR_LOCK_VIOLATION
)

func replacePath(oldPath, newPath string, writeThrough bool) error {
	oldPathPointer, err := windows.UTF16PtrFromString(oldPath)
	if err != nil {
		return err
	}
	newPathPointer, err := windows.UTF16PtrFromString(newPath)
	if err != nil {
		return err
	}
	flags := uint32(windows.MOVEFILE_REPLACE_EXISTING)
	if writeThrough {
		flags |= windows.MOVEFILE_WRITE_THROUGH
	}
	return windows.MoveFileEx(oldPathPointer, newPathPointer, flags)
}

func isTransientReplaceError(err error) bool {
	// MoveFileEx reports these errors while another process temporarily denies
	// delete access or holds a byte-range lock. Other failures can describe a
	// permanent path or permission problem and must reach the caller at once.
	return errors.Is(err, windowsErrorSharingViolation) ||
		errors.Is(err, windowsErrorLockViolation)
}
