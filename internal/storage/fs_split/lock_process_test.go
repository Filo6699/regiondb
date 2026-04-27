package fs_split

import (
	"errors"
	"os"
	"os/exec"
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
