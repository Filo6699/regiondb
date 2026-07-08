package fs_split

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/Filo6699/regiondb/internal/geometry"
	"github.com/Filo6699/regiondb/internal/storage"
)

func TestChunkVersionsPersistAndMigrateExistingStore(t *testing.T) {
	root := t.TempDir()
	g := testGeometry(t)
	coord := geometry.Coord{X: -3, Y: 4}

	store := openTestStore(t, root, g)
	chunk := testChunk(t, g, 1)
	if err := store.persistChunk(coord, chunk.Bytes(), chunk.PresenceBytes(), false); err != nil {
		t.Fatalf("persist legacy chunk: %v", err)
	}
	if version, err := store.ChunkVersion(coord); err != nil || version != 0 {
		t.Fatalf("legacy ChunkVersion() = %d, %v; want 0, nil", version, err)
	}
	version, err := store.CompareAndSwapChunk(coord, 0, testChunk(t, g, 2))
	if err != nil || version != 1 {
		t.Fatalf("CompareAndSwapChunk() = %d, %v; want 1, nil", version, err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := os.Remove(filepath.Join(root, versionClockName)); err != nil {
		t.Fatalf("remove version clock: %v", err)
	}

	reopened := openTestStore(t, root, g)
	t.Cleanup(func() { _ = reopened.Close() })
	if version, err := reopened.ChunkVersion(coord); err != nil || version != 1 {
		t.Fatalf("reopened ChunkVersion() = %d, %v; want 1, nil", version, err)
	}
	if reopened.versionClock != 1 {
		t.Fatalf("reopened version clock = %d, want 1", reopened.versionClock)
	}
}

func TestConditionalBatchMismatchHasNoSideEffects(t *testing.T) {
	root := t.TempDir()
	g := testGeometry(t)
	store := openTestStore(t, root, g)
	t.Cleanup(func() { _ = store.Close() })
	first := geometry.Coord{X: 1, Y: 1}
	second := geometry.Coord{X: 2, Y: 2}
	if err := store.WriteChunk(first, testChunk(t, g, 1)); err != nil {
		t.Fatalf("WriteChunk(first) error = %v", err)
	}
	if err := store.WriteChunk(second, testChunk(t, g, 2)); err != nil {
		t.Fatalf("WriteChunk(second) error = %v", err)
	}
	walBefore, err := os.Stat(filepath.Join(root, walName))
	if err != nil {
		t.Fatalf("stat WAL: %v", err)
	}
	statsBefore := store.RuntimeStats()
	clockBefore := store.versionClock

	_, err = store.ConditionalWriteChunks([]storage.ConditionalMutation{
		{Coord: first, ExpectedVersion: 1, Chunk: testChunk(t, g, 3)},
		{Coord: second, ExpectedVersion: 1, Chunk: testChunk(t, g, 4)},
	})
	if !errors.Is(err, ErrVersionMismatch) {
		t.Fatalf("ConditionalWriteChunks() error = %v, want ErrVersionMismatch", err)
	}
	walAfter, err := os.Stat(filepath.Join(root, walName))
	if err != nil {
		t.Fatalf("stat WAL after rejection: %v", err)
	}
	if walAfter.Size() != walBefore.Size() || store.walRecords != 2 ||
		store.versionClock != clockBefore {
		t.Fatalf(
			"rejected batch changed WAL/counters: bytes %d -> %d, records %d, clock %d -> %d",
			walBefore.Size(), walAfter.Size(), store.walRecords, clockBefore, store.versionClock,
		)
	}
	if statsAfter := store.RuntimeStats(); statsAfter != statsBefore {
		t.Fatalf("rejected batch changed runtime stats: before %+v, after %+v", statsBefore, statsAfter)
	}
	assertChunkValue(t, store, first, 1)
	assertChunkValue(t, store, second, 2)
}

func TestVersionMetadataCorruptionFailsClosed(t *testing.T) {
	root := t.TempDir()
	g := testGeometry(t)
	store := openTestStore(t, root, g)
	if err := store.WriteChunk(geometry.Coord{}, testChunk(t, g, 1)); err != nil {
		t.Fatalf("WriteChunk() error = %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	clockPath := filepath.Join(root, versionClockName)
	encoded, err := os.ReadFile(clockPath)
	if err != nil {
		t.Fatalf("ReadFile(clock) error = %v", err)
	}
	encoded[len(encoded)-1] ^= 0xff
	if err := os.WriteFile(clockPath, encoded, 0o600); err != nil {
		t.Fatalf("WriteFile(clock) error = %v", err)
	}
	if _, err := Open(root, g); !errors.Is(err, ErrCorruptVersion) {
		t.Fatalf("Open(corrupt clock) error = %v, want ErrCorruptVersion", err)
	}
}

func testGeometry(t *testing.T) geometry.Geometry {
	t.Helper()
	g, err := geometry.New(geometry.Config{ChunkEdge: 1, LargeChunkEdge: 1, BlockBits: 3})
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func openTestStore(t *testing.T, root string, g geometry.Geometry) *Store {
	t.Helper()
	store, err := OpenWithOptions(root, g, Options{
		CheckpointRecords: 100,
		CheckpointBytes:   1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func testChunk(t *testing.T, g geometry.Geometry, value uint64) *storage.Chunk {
	t.Helper()
	chunk, err := storage.NewChunk(g)
	if err != nil {
		t.Fatal(err)
	}
	if err := chunk.Set(geometry.Offset{}, value); err != nil {
		t.Fatal(err)
	}
	return chunk
}

func assertChunkValue(t *testing.T, store *Store, coord geometry.Coord, want uint64) {
	t.Helper()
	chunk, err := store.ReadChunk(coord)
	if err != nil {
		t.Fatal(err)
	}
	got, err := chunk.Get(geometry.Offset{})
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("chunk %v value = %d, want %d", coord, got, want)
	}
}
