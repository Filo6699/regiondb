//go:build darwin || linux

package fs_split

import "golang.org/x/sys/unix"

// descriptorReserve leaves headroom for standard streams, logs, control files,
// directory scans, atomic replacements, and short-lived runtime descriptors.
// Server-owned listeners and connection sockets are added by the caller.
const descriptorReserve = uint64(32)

func walDescriptorBudget(additionalReserve int) int {
	var limit unix.Rlimit
	if err := unix.Getrlimit(unix.RLIMIT_NOFILE, &limit); err != nil {
		return DefaultMaxOpenWALHandles
	}
	reserve := descriptorReserve + uint64(additionalReserve)
	if reserve < descriptorReserve || limit.Cur <= reserve {
		return 1
	}
	available := limit.Cur - reserve
	maxInt := uint64(^uint(0) >> 1)
	if available > maxInt {
		return int(maxInt)
	}
	return int(available)
}
