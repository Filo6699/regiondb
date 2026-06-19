//go:build !darwin && !linux && !windows

package fs_split

func walDescriptorBudget(_ int) int {
	return DefaultMaxOpenWALHandles
}
