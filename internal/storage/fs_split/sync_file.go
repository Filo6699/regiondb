package fs_split

import (
	"fmt"
	"os"
)

func syncFile(file *os.File) error {
	return classifyFileSyncError(file.Sync())
}

func classifyFileSyncError(err error) error {
	if err == nil {
		return nil
	}
	if isUnsupportedFileSyncError(err) {
		return fmt.Errorf("%w: file sync: %v", ErrDurabilityUnsupported, err)
	}
	return err
}
