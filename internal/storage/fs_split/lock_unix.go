//go:build darwin || dragonfly || freebsd || illumos || linux || netbsd || openbsd

package fs_split

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

const lockName = ".regiondb.lock"

var ErrWriterLocked = errors.New("data directory already has a writer")

type writerLock struct {
	file *os.File
}

func acquireWriterLock(path string) (*writerLock, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
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
	return &writerLock{file: file}, nil
}

func (lock *writerLock) release() error {
	unlockErr := syscall.Flock(int(lock.file.Fd()), syscall.LOCK_UN)
	closeErr := lock.file.Close()
	return errors.Join(unlockErr, closeErr)
}
