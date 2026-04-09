package fs_split

import (
	"errors"
	"time"
)

const writerLockReleaseTimeout = 2 * time.Second

func acquireWriterLockAfterRelease(path string, config lockConfig) (*writerLock, error) {
	deadline := time.Now().Add(writerLockReleaseTimeout)
	for {
		lock, err := acquireWriterLockWithConfig(path, config)
		if err == nil || !errors.Is(err, ErrWriterLocked) || time.Now().After(deadline) {
			return lock, err
		}
		time.Sleep(10 * time.Millisecond)
	}
}
