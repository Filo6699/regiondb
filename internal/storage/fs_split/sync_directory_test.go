package fs_split

import (
	"errors"
	"fmt"
	"testing"
)

// A synchronized write may only be acknowledged when the directory entry it
// created is durable. Exactly one of the two paths has to answer for it: the
// directory handle flush, or a write-through replacement. When neither does, the
// write fails with ErrDurabilityUnsupported instead of reporting success.
func TestCommitDirectoryEntryContract(t *testing.T) {
	t.Parallel()

	ioErr := errors.New("input/output error")
	tests := []struct {
		name                string
		syncErr             error
		replaceCommitsEntry bool
		want                error
	}{
		{
			name:                "directory handle flushed",
			syncErr:             nil,
			replaceCommitsEntry: false,
			want:                nil,
		},
		{
			name:                "unsupported flush covered by write-through replace",
			syncErr:             fmt.Errorf("%w: invalid argument", errDirectorySyncUnsupported),
			replaceCommitsEntry: true,
			want:                nil,
		},
		{
			name:                "unsupported flush with no compensating guarantee",
			syncErr:             fmt.Errorf("%w: invalid argument", errDirectorySyncUnsupported),
			replaceCommitsEntry: false,
			want:                ErrDurabilityUnsupported,
		},
		{
			name:                "flush failure is not a capability answer",
			syncErr:             ioErr,
			replaceCommitsEntry: true,
			want:                ioErr,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := commitDirectoryEntry(test.syncErr, test.replaceCommitsEntry)
			if test.want == nil {
				if err != nil {
					t.Fatalf("commitDirectoryEntry() = %v, want nil", err)
				}
				return
			}
			if !errors.Is(err, test.want) {
				t.Fatalf("commitDirectoryEntry() = %v, want %v", err, test.want)
			}
			if errors.Is(test.want, ErrDurabilityUnsupported) &&
				errors.Is(err, errDirectorySyncUnsupported) {
				t.Fatal("missing guarantee was reported as a plain capability answer")
			}
		})
	}
}

// Strict mode is only offered where one of the two paths really answers for the
// directory entry, so the platform running the suite has to provide one of them
// for the directory a synchronized write would use.
func TestDirectoryEntryGuaranteeExistsOnThisPlatform(t *testing.T) {
	t.Parallel()

	root := testTempDir(t)
	syncErr := syncParentDirectory(root)
	switch {
	case syncErr == nil:
	case errors.Is(syncErr, errDirectorySyncUnsupported):
		if !replaceCommitsDirectoryEntry {
			t.Fatalf(
				"directory sync is unsupported (%v) and the replacement does not commit the entry",
				syncErr,
			)
		}
	default:
		t.Fatalf("syncParentDirectory(%q) = %v", root, syncErr)
	}
	if err := commitDirectoryEntry(syncErr, replaceCommitsDirectoryEntry); err != nil {
		t.Fatalf("commitDirectoryEntry(%v, %t) = %v, want nil", syncErr, replaceCommitsDirectoryEntry, err)
	}
}
