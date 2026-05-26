//go:build windows

package fs_split

// Windows has no RLIMIT_NOFILE equivalent. Keep the WAL pool within a
// conservative measured process-handle budget while leaving headroom for
// non-WAL and runtime-owned handles.
const windowsWALStreamBudget = 64

func walDescriptorBudget() int {
	return windowsWALStreamBudget
}
