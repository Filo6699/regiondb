//go:build windows

package fs_split

// replaceCommitsDirectoryEntry is true because writeAtomic asks MoveFileEx for
// MOVEFILE_WRITE_THROUGH on a synchronized write, which flushes the replacement
// itself. That is the only directory-level guarantee Windows offers here, so
// strict mode depends on it rather than on a directory handle flush.
const replaceCommitsDirectoryEntry = true

// syncParentDirectory always reports the missing capability: os.Open cannot
// return a directory handle FlushFileBuffers accepts, so Windows has no way to
// commit a directory entry through the directory itself.
func syncParentDirectory(string) error {
	return errDirectorySyncUnsupported
}
