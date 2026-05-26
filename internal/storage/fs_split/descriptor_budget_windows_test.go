//go:build windows

package fs_split

import "testing"

func TestWindowsWALStreamBudgetIsConservative(t *testing.T) {
	if got := walDescriptorBudget(); got != windowsWALStreamBudget {
		t.Fatalf("Windows WAL stream budget = %d, want %d", got, windowsWALStreamBudget)
	}
	if windowsWALStreamBudget < DefaultMaxOpenWALHandles {
		t.Fatalf(
			"Windows WAL stream budget = %d, below safe default %d",
			windowsWALStreamBudget,
			DefaultMaxOpenWALHandles,
		)
	}
}
