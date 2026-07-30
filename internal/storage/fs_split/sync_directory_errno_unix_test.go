//go:build darwin || dragonfly || freebsd || illumos || linux || netbsd || openbsd

package fs_split

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestUnsupportedDirectorySyncClassification(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "invalid argument", err: syscall.EINVAL, want: true},
		{name: "not supported", err: syscall.ENOTSUP, want: true},
		{name: "not implemented", err: syscall.ENOSYS, want: true},
		{name: "wrapped invalid argument", err: &os.PathError{
			Op: "sync", Path: "directory", Err: syscall.EINVAL,
		}, want: true},
		{name: "input output error", err: syscall.EIO, want: false},
		{name: "no space", err: syscall.ENOSPC, want: false},
		{name: "permission denied", err: syscall.EACCES, want: false},
		{name: "bad file descriptor", err: syscall.EBADF, want: false},
		{name: "unrelated", err: errors.New("unrelated"), want: false},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := isUnsupportedDirectorySyncError(test.err); got != test.want {
				t.Fatalf("isUnsupportedDirectorySyncError(%v) = %t, want %t", test.err, got, test.want)
			}
		})
	}
}

func TestSyncParentDirectorySucceedsOnRealDirectory(t *testing.T) {
	t.Parallel()

	if err := syncParentDirectory(testTempDir(t)); err != nil {
		t.Fatalf("syncParentDirectory() = %v, want nil", err)
	}
}

// A directory that cannot be opened is not a capability answer: it has to reach
// the caller as a write failure so a missing or unreadable path is never mistaken
// for an unsupported filesystem.
func TestSyncParentDirectoryReportsOpenFailure(t *testing.T) {
	t.Parallel()

	missing := filepath.Join(testTempDir(t), "missing")
	err := syncParentDirectory(missing)
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("syncParentDirectory(%q) = %v, want os.ErrNotExist", missing, err)
	}
	if errors.Is(err, errDirectorySyncUnsupported) {
		t.Fatal("a missing directory was classified as an unsupported filesystem")
	}
	if commitErr := commitDirectoryEntry(err, replaceCommitsDirectoryEntry); !errors.Is(commitErr, os.ErrNotExist) {
		t.Fatalf("commitDirectoryEntry() = %v, want os.ErrNotExist", commitErr)
	}
}
