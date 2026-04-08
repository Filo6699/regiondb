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
	if want := testCleanupRetryDelays[:2]; !reflect.DeepEqual(waits, want) {
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
