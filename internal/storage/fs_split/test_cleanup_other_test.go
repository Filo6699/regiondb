//go:build !windows

package fs_split

import (
	"os"
	"testing"
)

func testTempDir(t *testing.T) string {
	t.Helper()
	return t.TempDir()
}

func removeTestDirectory(directory string) error {
	return os.RemoveAll(directory)
}
