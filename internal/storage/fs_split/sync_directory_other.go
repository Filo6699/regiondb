//go:build !windows

package fs_split

import (
	"fmt"
	"os"
)

// replaceCommitsDirectoryEntry is false because a POSIX rename gives no
// guarantee that the new directory entry reached stable storage, so a
// synchronized write has to flush the parent directory handle itself.
const replaceCommitsDirectoryEntry = false

func syncParentDirectory(path string) (returnErr error) {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() {
		if err := directory.Close(); err != nil && returnErr == nil {
			returnErr = err
		}
	}()
	if err := directory.Sync(); err != nil {
		if isUnsupportedDirectorySyncError(err) {
			return fmt.Errorf("%w: %v", errDirectorySyncUnsupported, err)
		}
		return err
	}
	return nil
}
