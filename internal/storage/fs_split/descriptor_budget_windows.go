//go:build windows

package fs_split

// Go opens files as native Windows handles, so the C runtime stdio stream
// limit does not apply. Keep the WAL pool within a conservative application
// budget instead; native Windows tests exercise the relevant handle behavior.
const windowsWALHandleBudget = 64

func walDescriptorBudget(_ int) int {
	return windowsWALHandleBudget
}
