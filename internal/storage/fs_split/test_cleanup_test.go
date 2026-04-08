package fs_split

import (
	"time"
)

var testCleanupRetryDelays = [...]time.Duration{
	10 * time.Millisecond,
	25 * time.Millisecond,
	50 * time.Millisecond,
	100 * time.Millisecond,
	200 * time.Millisecond,
}

func removeTestDirectoryWithRetry(
	remove func() error,
	retryable func(error) bool,
	wait func(time.Duration),
) error {
	for _, delay := range testCleanupRetryDelays {
		err := remove()
		if err == nil || !retryable(err) {
			return err
		}
		wait(delay)
	}
	return remove()
}
