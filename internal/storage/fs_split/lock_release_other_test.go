//go:build !windows

package fs_split

func acquireWriterLockAfterRelease(path string, config lockConfig) (*writerLock, error) {
	return acquireWriterLockWithConfig(path, config)
}
