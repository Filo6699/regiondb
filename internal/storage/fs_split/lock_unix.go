//go:build darwin || dragonfly || freebsd || illumos || linux || netbsd || openbsd

package fs_split

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

func writerGuardMode() string {
	return "flock"
}

func openWriterGuard(path string) (*os.File, error) {
	return openWriterGuardWithFlags(path, os.O_RDWR|os.O_CREATE)
}

func openExistingWriterGuard(path string) (*os.File, error) {
	return openWriterGuardWithFlags(path, os.O_RDWR)
}

func openWriterGuardWithFlags(path string, flags int) (*os.File, error) {
	file, err := os.OpenFile(path, flags, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open writer lock: %w", err)
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		closeErr := file.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, errors.Join(ErrWriterLocked, closeErr)
		}
		return nil, errors.Join(fmt.Errorf("acquire writer lock: %w", err), closeErr)
	}
	return file, nil
}

func closeWriterGuard(file *os.File) error {
	unlockErr := syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
	closeErr := file.Close()
	return errors.Join(unlockErr, closeErr)
}
