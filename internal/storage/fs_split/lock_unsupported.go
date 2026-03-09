//go:build !darwin && !dragonfly && !freebsd && !illumos && !linux && !netbsd && !openbsd

package fs_split

import "errors"

const lockName = ".regiondb.lock"

var ErrWriterLocked = errors.New("data directory already has a writer")

type writerLock struct{}

func acquireWriterLock(string) (*writerLock, error) {
	return nil, errors.New("exclusive data-directory locking is unsupported on this platform")
}

func (lock *writerLock) release() error {
	return nil
}
