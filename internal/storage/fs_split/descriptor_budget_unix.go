//go:build darwin || linux

package fs_split

import "golang.org/x/sys/unix"

// descriptorReserve leaves headroom for non-WAL files and runtime-owned
// descriptors.
const descriptorReserve = uint64(32)

func walDescriptorBudget() int {
	var limit unix.Rlimit
	if err := unix.Getrlimit(unix.RLIMIT_NOFILE, &limit); err != nil {
		return DefaultMaxOpenWALHandles
	}
	if limit.Cur <= descriptorReserve {
		return 1
	}
	available := limit.Cur - descriptorReserve
	maxInt := uint64(^uint(0) >> 1)
	if available > maxInt {
		return int(maxInt)
	}
	return int(available)
}
