package fs_split

import (
	"errors"
	"fmt"
)

var (
	// ErrDurabilityUnsupported reports that the requested durability mode cannot
	// be honored on this platform or filesystem. A synchronized write fails
	// closed with this error instead of acknowledging a guarantee that was never
	// applied.
	ErrDurabilityUnsupported = errors.New("requested durability guarantee is unsupported")

	// errDirectorySyncUnsupported reports that a directory entry cannot be
	// committed by flushing a directory handle. It is a capability answer, not a
	// write failure: the caller decides whether the platform replacement already
	// commits the entry.
	errDirectorySyncUnsupported = errors.New("directory sync is unsupported")
)

// commitDirectoryEntry resolves the strict-mode contract after a synchronized
// replacement. A directory handle flush is the primary path; when the platform
// or filesystem cannot provide it, the entry is durable only if the replacement
// itself was write-through. Otherwise the guarantee is missing and has to be
// reported rather than assumed.
func commitDirectoryEntry(syncErr error, replaceCommitsEntry bool) error {
	switch {
	case syncErr == nil:
		return nil
	case !errors.Is(syncErr, errDirectorySyncUnsupported):
		return fmt.Errorf("sync parent directory: %w", syncErr)
	case replaceCommitsEntry:
		return nil
	default:
		return fmt.Errorf(
			"%w: %v and the platform replacement does not commit the directory entry",
			ErrDurabilityUnsupported,
			syncErr,
		)
	}
}
