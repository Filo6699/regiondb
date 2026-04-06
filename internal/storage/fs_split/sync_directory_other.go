//go:build !windows

package fs_split

import "os"

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
	return directory.Sync()
}
