package fs_split

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/Filo6699/regiondb/internal/geometry"
)

const writerLockHelperRootEnv = "REGIONDB_TEST_WRITER_LOCK_ROOT"

func TestWriterOwnershipRejectsOtherProcess(t *testing.T) {
	if root := os.Getenv(writerLockHelperRootEnv); root != "" {
		g := mustGeometry(t, geometry.Config{ChunkEdge: 1, LargeChunkEdge: 1, BlockBits: 1})
		if _, err := Open(root, g); !errors.Is(err, ErrWriterLocked) {
			t.Fatalf("Open() error = %v, want ErrWriterLocked", err)
		}
		return
	}

	root := testTempDir(t)
	g := mustGeometry(t, geometry.Config{ChunkEdge: 1, LargeChunkEdge: 1, BlockBits: 1})
	store := mustOpenWithoutWriterHeartbeat(t, root, g)
	t.Cleanup(func() {
		closeStore(t, store)
	})

	command := exec.Command(os.Args[0], "-test.run=^TestWriterOwnershipRejectsOtherProcess$")
	command.Env = append(os.Environ(), writerLockHelperRootEnv+"="+root)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("writer lock helper failed: %v\n%s", err, output)
	}
}

func TestWriterOwnershipDoesNotMigrateOtherProcessLegacyLock(t *testing.T) {
	root := testTempDir(t)
	path := filepath.Join(root, lockName)
	if err := os.WriteFile(path, []byte("live legacy writer\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	foreign, err := openExistingWriterGuard(path)
	if err != nil {
		t.Fatalf("lock legacy writer fixture: %v", err)
	}
	t.Cleanup(func() {
		if err := closeWriterGuard(foreign); err != nil {
			t.Errorf("release legacy writer fixture: %v", err)
		}
	})

	command := exec.Command(os.Args[0], "-test.run=^TestWriterOwnershipRejectsOtherProcess$")
	command.Env = append(os.Environ(), writerLockHelperRootEnv+"="+root)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("legacy writer lock helper failed: %v\n%s", err, output)
	}

	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !info.Mode().IsRegular() {
		t.Fatalf("live legacy writer path mode = %v, want regular file", info.Mode())
	}
	matches, err := filepath.Glob(path + legacyLockMarker + "*")
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 0 {
		t.Fatalf("live legacy writer was migrated to %v", matches)
	}
}
