package fs_split

import (
	"errors"
	"os"
	"syscall"
	"testing"
)

func TestTransientReplaceErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "sharing violation", err: windowsErrorSharingViolation, want: true},
		{name: "wrapped lock violation", err: &os.LinkError{
			Op: "rename", Old: "old", New: "new", Err: windowsErrorLockViolation,
		}, want: true},
		{name: "access denied", err: syscall.ERROR_ACCESS_DENIED, want: false},
		{name: "missing path", err: syscall.ERROR_PATH_NOT_FOUND, want: false},
		{name: "unrelated", err: errors.New("unrelated"), want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := isTransientReplaceError(test.err); got != test.want {
				t.Fatalf("isTransientReplaceError(%v) = %t, want %t", test.err, got, test.want)
			}
		})
	}
}
