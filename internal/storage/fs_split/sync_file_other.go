//go:build !windows

package fs_split

func isUnsupportedFileSyncError(error) bool {
	return false
}
