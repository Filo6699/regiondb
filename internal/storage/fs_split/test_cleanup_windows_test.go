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
		err := removeTestDirectoryWithRetry(
			func() error { return os.RemoveAll(directory) },
			isRetryableTestCleanupError,
			time.Sleep,
		)
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

func isRetryableTestCleanupError(err error) bool {
	return errors.Is(err, windows.ERROR_SHARING_VIOLATION) ||
		errors.Is(err, windows.ERROR_LOCK_VIOLATION) ||
		errors.Is(err, windows.ERROR_ACCESS_DENIED)
}
