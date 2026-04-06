//go:build !windows

package fs_split

import "os"

func replacePath(oldPath, newPath string, _ bool) error {
	return os.Rename(oldPath, newPath)
}

func isTransientReplaceError(error) bool {
	return false
}
