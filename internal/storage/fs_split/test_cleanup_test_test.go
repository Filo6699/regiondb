package fs_split

import (
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestRemoveTestDirectoryRetriesHandleReleaseErrors(t *testing.T) {
	t.Parallel()

	handleBusy := errors.New("handle is still open")
	attempts := 0
	var waits []time.Duration
	err := removeTestDirectoryWithRetry(
		func() error {
			attempts++
			if attempts <= 2 {
				return handleBusy
			}
			return nil
		},
		func(err error) bool { return errors.Is(err, handleBusy) },
		func(delay time.Duration) { waits = append(waits, delay) },
	)
	if err != nil {
		t.Fatalf("removeTestDirectoryWithRetry() error = %v", err)
	}
	if attempts != 3 {
		t.Fatalf("remove attempts = %d, want 3", attempts)
	}
	if want := []time.Duration{testCleanupRetryDelay, testCleanupRetryDelay}; !reflect.DeepEqual(waits, want) {
		t.Fatalf("retry waits = %v, want %v", waits, want)
	}
}

func TestRemoveTestDirectoryDoesNotRetryOtherErrors(t *testing.T) {
	t.Parallel()

	permanent := errors.New("permanent cleanup error")
	attempts := 0
	waits := 0
	err := removeTestDirectoryWithRetry(
		func() error {
			attempts++
			return permanent
		},
		func(error) bool { return false },
		func(time.Duration) { waits++ },
	)
	if !errors.Is(err, permanent) {
		t.Fatalf("removeTestDirectoryWithRetry() error = %v, want %v", err, permanent)
	}
	if attempts != 1 || waits != 0 {
		t.Fatalf("remove attempts = %d, waits = %d, want 1 and 0", attempts, waits)
	}
}

func TestRemoveTestDirectoryBoundsRetryWindow(t *testing.T) {
	t.Parallel()

	handleBusy := errors.New("handle is still open")
	attempts := 0
	waits := 0
	err := removeTestDirectoryWithRetry(
		func() error {
			attempts++
			return handleBusy
		},
		func(err error) bool { return errors.Is(err, handleBusy) },
		func(delay time.Duration) {
			if delay != testCleanupRetryDelay {
				t.Fatalf("retry delay = %v, want %v", delay, testCleanupRetryDelay)
			}
			waits++
		},
	)
	if !errors.Is(err, handleBusy) {
		t.Fatalf("removeTestDirectoryWithRetry() error = %v, want %v", err, handleBusy)
	}
	if attempts != testCleanupRetryAttempts+1 || waits != testCleanupRetryAttempts {
		t.Fatalf(
			"remove attempts = %d, waits = %d, want %d and %d",
			attempts,
			waits,
			testCleanupRetryAttempts+1,
			testCleanupRetryAttempts,
		)
	}
}
