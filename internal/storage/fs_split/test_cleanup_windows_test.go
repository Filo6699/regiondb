package fs_split

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/windows"
)

func testTempDir(t *testing.T) string {
	t.Helper()

	directory := t.TempDir()
	t.Cleanup(func() {
		err := removeTestDirectory(directory)
		if err == nil {
			return
		}
		matches, globErr := filepath.Glob(filepath.Join(directory, "*"))
		if globErr != nil {
			t.Errorf("remove temporary directory %q: %v (inspect contents: %v)", directory, err, globErr)
			return
		}
		t.Errorf("remove temporary directory %q: %v (remaining entries: %q)", directory, err, matches)
	})
	return directory
}

func removeTestDirectory(directory string) error {
	// Windows may retain a closed file handle while antivirus or indexing
	// completes. Retry that OS variance for a bounded two seconds, but preserve
	// the invariant that successful cleanup means the directory is gone.
	return removeTestDirectoryWithRetry(
		func() error { return os.RemoveAll(directory) },
		isRetryableTestCleanupError,
		time.Sleep,
	)
}

func isRetryableTestCleanupError(err error) bool {
	return errors.Is(err, windows.ERROR_SHARING_VIOLATION) ||
		errors.Is(err, windows.ERROR_LOCK_VIOLATION) ||
		errors.Is(err, windows.ERROR_ACCESS_DENIED)
}
