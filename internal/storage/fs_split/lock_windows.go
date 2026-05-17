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
	return lockWriterGuardFile(file)
}

func openExistingWriterGuard(path string) (*os.File, error) {
	pathPointer, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return nil, fmt.Errorf("open writer lock: %w", err)
	}
	handle, err := windows.CreateFile(
		pathPointer,
		windows.GENERIC_READ|windows.GENERIC_WRITE,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_ATTRIBUTE_NORMAL,
		0,
	)
	if err != nil {
		return nil, fmt.Errorf("open writer lock: %w", err)
	}
	return lockWriterGuardFile(os.NewFile(uintptr(handle), path))
}

func lockWriterGuardFile(file *os.File) (*os.File, error) {
	overlapped := new(windows.Overlapped)
	err := windows.LockFileEx(
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
