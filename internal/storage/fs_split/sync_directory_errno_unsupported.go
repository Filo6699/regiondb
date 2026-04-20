//go:build !darwin && !dragonfly && !freebsd && !illumos && !linux && !netbsd && !openbsd && !windows

package fs_split

// isUnsupportedDirectorySyncError cannot classify errno values on a platform
// this store does not support: openWriterGuard already refuses to lock a data
// directory there, so no synchronized write reaches a directory sync. Every
// failure stays a write failure.
func isUnsupportedDirectorySyncError(error) bool {
	return false
}
