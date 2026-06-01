//go:build windows

package fs_split

import (
	"errors"
	"fmt"
	"testing"

	"golang.org/x/sys/windows"
)

func TestWindowsUnsupportedFileSyncFailsClosed(t *testing.T) {
	t.Parallel()

	for _, syncErr := range []error{
		windows.ERROR_INVALID_FUNCTION,
		fmt.Errorf("flush file buffers: %w", windows.ERROR_NOT_SUPPORTED),
	} {
		if !isUnsupportedFileSyncError(syncErr) {
			t.Fatalf("isUnsupportedFileSyncError(%v) = false, want true", syncErr)
		}
		classified := classifyFileSyncError(syncErr)
		if !errors.Is(classified, ErrDurabilityUnsupported) {
			t.Fatalf("classifyFileSyncError(%v) = %v, want ErrDurabilityUnsupported", syncErr, classified)
		}
	}
}

func TestWindowsFileSyncFailureIsNotMisclassified(t *testing.T) {
	t.Parallel()

	syncErr := windows.ERROR_ACCESS_DENIED
	if isUnsupportedFileSyncError(syncErr) {
		t.Fatalf("isUnsupportedFileSyncError(%v) = true, want false", syncErr)
	}
	if classified := classifyFileSyncError(syncErr); !errors.Is(classified, syncErr) ||
		errors.Is(classified, ErrDurabilityUnsupported) {
		t.Fatalf("classifyFileSyncError(%v) = %v, want original error", syncErr, classified)
	}
}
