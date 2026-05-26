//go:build !darwin && !linux && !windows

package fs_split

func walDescriptorBudget() int {
	return DefaultMaxOpenWALHandles
}
