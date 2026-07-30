package fs_split

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/windows"

	"github.com/Filo6699/regiondb/internal/geometry"
)

// Windows commits a file through FlushFileBuffers on a handle that was opened
// for writing. Every sync the store issues has to stay on that path: the WAL
// append handle it keeps open, the truncation it writes back at checkpoint
// time, and the temporary chunk file replaced by writeAtomic.
func TestWindowsFileSyncPathsAreSupported(t *testing.T) {
	t.Parallel()

	root := testTempDir(t)
	g := mustGeometry(t, geometry.Config{ChunkEdge: 1, LargeChunkEdge: 1, BlockBits: 8})
	store := mustOpenWithOptions(t, root, g, Options{
		Durability:        DurabilityFsyncWAL,
		CheckpointRecords: 1,
		CheckpointBytes:   1 << 20,
	})
	chunk := mustChunk(t, g)
	if err := chunk.Set(geometry.Offset{}, 3); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteChunk(geometry.Coord{X: 1, Y: -1}, chunk); err != nil {
		t.Fatalf("WriteChunk(): %v", err)
	}

	stats := store.RuntimeStats()
	if stats.WALFlushes != 2 {
		t.Fatalf("WAL flushes = %d, want 2 (append handle and truncation)", stats.WALFlushes)
	}
	if stats.Checkpoints != 1 {
		t.Fatalf("checkpoints = %d, want 1", stats.Checkpoints)
	}
	info, err := os.Stat(filepath.Join(root, walName))
	if err != nil {
		t.Fatalf("stat WAL: %v", err)
	}
	if info.Size() != 0 {
		t.Fatalf("WAL size after the synchronized checkpoint = %d, want 0", info.Size())
	}
}

// Go opens files with FILE_SHARE_READ|FILE_SHARE_WRITE and no
// FILE_SHARE_DELETE, so MoveFileEx cannot take the delete access it needs while
// another handle is alive: it reports "Access is denied." rather than a sharing
// violation. Replacing a path is therefore only defined once every handle to it
// is closed, which is why writeAtomic closes the temporary file before it
// replaces the destination instead of leaving the ordering to the retry loop.
func TestWindowsReplaceRequiresClosedDestinationHandle(t *testing.T) {
	t.Parallel()

	root := testTempDir(t)
	source := filepath.Join(root, "source")
	destination := filepath.Join(root, "destination")
	if err := os.WriteFile(source, []byte("new"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(destination, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}

	reader, err := os.Open(destination)
	if err != nil {
		t.Fatalf("open destination: %v", err)
	}
	defer func() {
		_ = reader.Close()
	}()
	blocked := replacePath(source, destination, true)
	if blocked == nil {
		t.Fatal("replace succeeded while another handle still held the destination open")
	}
	if !errors.Is(blocked, windows.ERROR_ACCESS_DENIED) &&
		!errors.Is(blocked, windows.ERROR_SHARING_VIOLATION) {
		t.Fatalf("replace with an open destination = %v, want a Windows denial", blocked)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("close destination: %v", err)
	}

	if err := replacePath(source, destination, true); err != nil {
		t.Fatalf("replace after the destination handle closed: %v", err)
	}
	got, err := os.ReadFile(destination)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "new" {
		t.Fatalf("destination contents = %q, want %q", got, "new")
	}
}
