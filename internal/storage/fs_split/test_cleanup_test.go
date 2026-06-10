package fs_split

import (
	"time"
)

const (
	testCleanupRetryAttempts = 80
	testCleanupRetryDelay    = 25 * time.Millisecond
)

func removeTestDirectoryWithRetry(
	remove func() error,
	retryable func(error) bool,
	wait func(time.Duration),
) error {
	for range testCleanupRetryAttempts {
		err := remove()
		if err == nil || !retryable(err) {
			return err
		}
		wait(testCleanupRetryDelay)
	}
	return remove()
}
