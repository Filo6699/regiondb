package fs_split

import (
	"bytes"
	"errors"
	"os"
	"testing"

	"github.com/Filo6699/regiondb/internal/geometry"
)

// Windows cannot flush a directory entry through a directory handle, so the
// capability has to be reported instead of silently returning success. Strict
// mode is honored only because writeAtomic asks MoveFileEx for
// MOVEFILE_WRITE_THROUGH; without that compensating guarantee the same
// capability answer must fail the write.
func TestWindowsDirectorySyncIsExplicitlyUnsupported(t *testing.T) {
	t.Parallel()

	root := testTempDir(t)
	syncErr := syncParentDirectory(root)
	if !errors.Is(syncErr, errDirectorySyncUnsupported) {
		t.Fatalf("syncParentDirectory(%q) = %v, want errDirectorySyncUnsupported", root, syncErr)
	}
	if !replaceCommitsDirectoryEntry {
		t.Fatal("replaceCommitsDirectoryEntry = false, want true for the write-through replacement")
	}
	if err := commitDirectoryEntry(syncErr, replaceCommitsDirectoryEntry); err != nil {
		t.Fatalf("commitDirectoryEntry() with the write-through replacement = %v, want nil", err)
	}
	err := commitDirectoryEntry(syncErr, false)
	if !errors.Is(err, ErrDurabilityUnsupported) {
		t.Fatalf("commitDirectoryEntry() without a compensating guarantee = %v, want ErrDurabilityUnsupported", err)
	}
}

// The reported capability must not turn strict mode into a failing mode on
// Windows: a synchronized write still completes and publishes the new chunk.
func TestWindowsStrictWriteCompletesThroughWriteThroughReplace(t *testing.T) {
	t.Parallel()

	root := testTempDir(t)
	g := mustGeometry(t, geometry.Config{ChunkEdge: 1, LargeChunkEdge: 1, BlockBits: 8})
	store := mustOpenWithOptions(t, root, g, Options{
		Durability:        DurabilityFsyncCheckpoint,
		CheckpointRecords: 8,
		CheckpointBytes:   1 << 20,
	})
	coord := geometry.Coord{X: 4, Y: -5}
	chunk := mustChunk(t, g)
	if err := chunk.Set(geometry.Offset{}, 7); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteChunk(coord, chunk); err != nil {
		t.Fatalf("WriteChunk() in fsync-checkpoint mode: %v", err)
	}

	got, err := os.ReadFile(store.chunkPath(coord))
	if err != nil {
		t.Fatalf("read published chunk: %v", err)
	}
	if want := store.encode(coord, chunk.Bytes()); !bytes.Equal(got, want) {
		t.Fatal("published chunk file does not hold the synchronized write")
	}
	assertNoChunkTemporaryFiles(t, root)
}
