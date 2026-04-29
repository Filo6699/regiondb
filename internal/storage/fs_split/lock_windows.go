//go:build windows

package fs_split

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/windows"
)

func writerGuardMode() string {
	return "lock-file-ex"
}

func openWriterGuard(path string) (*os.File, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open writer lock: %w", err)
	}
	overlapped := new(windows.Overlapped)
	err = windows.LockFileEx(
		windows.Handle(file.Fd()),
		windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY,
		0,
		1,
		0,
		overlapped,
	)
	if err != nil {
		closeErr := file.Close()
		if errors.Is(err, windows.ERROR_LOCK_VIOLATION) {
			return nil, errors.Join(ErrWriterLocked, closeErr)
		}
		return nil, errors.Join(fmt.Errorf("acquire writer lock: %w", err), closeErr)
	}
	return file, nil
}

func closeWriterGuard(file *os.File) error {
	overlapped := new(windows.Overlapped)
	unlockErr := windows.UnlockFileEx(windows.Handle(file.Fd()), 0, 1, 0, overlapped)
	closeErr := file.Close()
	return errors.Join(unlockErr, closeErr)
}
