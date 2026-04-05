//go:build !windows

package fs_split

func isTransientReplaceError(error) bool {
	return false
}
