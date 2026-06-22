//go:build windows

package fs_split

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWindowsWALHandleBudgetIsConservative(t *testing.T) {
	if got := walDescriptorBudget(128); got != windowsWALHandleBudget {
		t.Fatalf("Windows WAL handle budget = %d, want %d", got, windowsWALHandleBudget)
	}
	if windowsWALHandleBudget < DefaultMaxOpenWALHandles {
		t.Fatalf(
			"Windows WAL handle budget = %d, below safe default %d",
			windowsWALHandleBudget,
			DefaultMaxOpenWALHandles,
		)
	}
	if got, err := EffectiveWALHandleLimit(1024, 128); err != nil {
		t.Fatalf("EffectiveWALHandleLimit() error = %v", err)
	} else if got != windowsWALHandleBudget {
		t.Fatalf("effective Windows WAL handle limit = %d, want %d", got, windowsWALHandleBudget)
	}
}

func TestWindowsNativeFileHandlesExceedCRTStdioLimit(t *testing.T) {
	const handleCount = 600

	path := filepath.Join(t.TempDir(), "handle-probe")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatalf("create handle probe: %v", err)
	}

	handles := make([]*os.File, 0, handleCount)
	t.Cleanup(func() {
		for _, handle := range handles {
			if err := handle.Close(); err != nil {
				t.Errorf("close handle probe: %v", err)
			}
		}
	})
	for range handleCount {
		handle, err := os.Open(path)
		if err != nil {
			t.Fatalf("open native Windows handle %d of %d: %v", len(handles)+1, handleCount, err)
		}
		handles = append(handles, handle)
	}
}
