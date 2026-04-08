//go:build !windows

package fs_split

import "testing"

func testTempDir(t *testing.T) string {
	t.Helper()
	return t.TempDir()
}
