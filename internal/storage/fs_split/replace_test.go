package fs_split

import (
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestReplaceFileRetriesTransientErrors(t *testing.T) {
	t.Parallel()

	transientErr := errors.New("transient replace")
	calls := 0
	var waits []time.Duration
	err := replaceFileWithRetry(
		"temporary",
		"destination",
		func(oldPath, newPath string) error {
			calls++
			if oldPath != "temporary" || newPath != "destination" {
				t.Fatalf("replace paths = (%q, %q)", oldPath, newPath)
			}
			if calls <= 2 {
				return transientErr
			}
			return nil
		},
		func(err error) bool { return errors.Is(err, transientErr) },
		func(delay time.Duration) { waits = append(waits, delay) },
	)
	if err != nil {
		t.Fatalf("replaceFileWithRetry() error = %v", err)
	}
	if calls != 3 {
		t.Fatalf("replace calls = %d, want 3", calls)
	}
	if want := replaceRetryDelays[:2]; !reflect.DeepEqual(waits, want) {
		t.Fatalf("retry waits = %v, want %v", waits, want)
	}
}

func TestReplaceFileReturnsPermanentErrorImmediately(t *testing.T) {
	t.Parallel()

	permanentErr := errors.New("permanent replace")
	calls := 0
	waits := 0
	err := replaceFileWithRetry(
		"temporary",
		"destination",
		func(string, string) error {
			calls++
			return permanentErr
		},
		func(error) bool { return false },
		func(time.Duration) { waits++ },
	)
	if !errors.Is(err, permanentErr) {
		t.Fatalf("replaceFileWithRetry() error = %v, want permanent error", err)
	}
	if calls != 1 || waits != 0 {
		t.Fatalf("replace calls = %d, waits = %d, want 1 and 0", calls, waits)
	}
}

func TestReplaceFileStopsAfterBoundedRetries(t *testing.T) {
	t.Parallel()

	transientErr := errors.New("transient replace")
	calls := 0
	var waits []time.Duration
	err := replaceFileWithRetry(
		"temporary",
		"destination",
		func(string, string) error {
			calls++
			return transientErr
		},
		func(error) bool { return true },
		func(delay time.Duration) { waits = append(waits, delay) },
	)
	if !errors.Is(err, transientErr) {
		t.Fatalf("replaceFileWithRetry() error = %v, want transient error", err)
	}
	if want := len(replaceRetryDelays) + 1; calls != want {
		t.Fatalf("replace calls = %d, want %d", calls, want)
	}
	if !reflect.DeepEqual(waits, replaceRetryDelays[:]) {
		t.Fatalf("retry waits = %v, want %v", waits, replaceRetryDelays)
	}
}
