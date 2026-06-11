//go:build regiondb_experimental

package fs_region

import (
	"errors"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/Filo6699/regiondb/internal/geometry"
	"github.com/Filo6699/regiondb/internal/storage"
)

func TestStoreReopenRoundTrip(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	g := mustGeometry(t, geometry.Config{ChunkEdge: 3, LargeChunkEdge: 4, BlockBits: 5})
	store := mustOpen(t, root, g)
	coord := geometry.Coord{X: -5, Y: 8}
	values := map[geometry.Offset]uint64{
		{X: 0, Y: 0}: 1,
		{X: 2, Y: 0}: 31,
		{X: 1, Y: 2}: 18,
		{X: 2, Y: 2}: 0,
	}
	chunk := mustChunk(t, g)
	for offset, value := range values {
		if err := chunk.Set(offset, value); err != nil {
			t.Fatalf("Set(%v): %v", offset, err)
		}
	}
	if err := store.WriteChunk(coord, chunk); err != nil {
		t.Fatalf("WriteChunk(): %v", err)
	}
	closeStore(t, store)

	reopened := mustOpen(t, root, g)
	defer closeStore(t, reopened)
	gotChunk, err := reopened.ReadChunk(coord)
	if err != nil {
		t.Fatalf("ReadChunk(): %v", err)
	}
	for offset, want := range values {
		got, err := gotChunk.Get(offset)
		if err != nil {
			t.Fatalf("Get(%v): %v", offset, err)
		}
		if got != want {
			t.Fatalf("Get(%v) = %d, want %d", offset, got, want)
		}
		if exists, err := gotChunk.Exists(offset); err != nil || !exists {
			t.Fatalf("Exists(%v) = %t, %v, want true", offset, exists, err)
		}
	}
	if exists, err := gotChunk.Exists(geometry.Offset{X: 1, Y: 1}); err != nil || exists {
		t.Fatalf("Exists(absent) = %t, %v, want false", exists, err)
	}

	// Chunk (-5,8) belongs to large chunk (-2,2) for a four-chunk region edge.
	path := filepath.Join(root, "r_n2_p2.rdbregion")
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("expected region image %q: %v", path, err)
	}
	layout, err := newImageLayout(g)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != layout.imageBytes {
		t.Fatalf("region image size = %d, want %d", info.Size(), layout.imageBytes)
	}
}

func TestStoreKeepsRegionSlotsIndependent(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	g := mustGeometry(t, geometry.Config{ChunkEdge: 2, LargeChunkEdge: 4, BlockBits: 6})
	store := mustOpen(t, root, g)
	defer closeStore(t, store)

	// Every coordinate below maps to large chunk (0,0), so all four chunks share
	// one region image and only their slots may differ.
	coords := []geometry.Coord{{X: 0, Y: 0}, {X: 3, Y: 0}, {X: 0, Y: 3}, {X: 3, Y: 3}}
	for index, coord := range coords {
		chunk := mustChunk(t, g)
		if err := chunk.Set(geometry.Offset{X: 1, Y: 1}, uint64(index+1)); err != nil {
			t.Fatal(err)
		}
		if err := store.WriteChunk(coord, chunk); err != nil {
			t.Fatalf("WriteChunk(%v): %v", coord, err)
		}
	}
	for index, coord := range coords {
		chunk, err := store.ReadChunk(coord)
		if err != nil {
			t.Fatalf("ReadChunk(%v): %v", coord, err)
		}
		got, err := chunk.Get(geometry.Offset{X: 1, Y: 1})
		if err != nil {
			t.Fatal(err)
		}
		if want := uint64(index + 1); got != want {
			t.Fatalf("ReadChunk(%v) value = %d, want %d", coord, got, want)
		}
	}

	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "r_p0_p0.rdbregion" {
		t.Fatalf("region directory entries = %v, want a single region image", entries)
	}
}

func TestStoreOverwritesPublishedSlot(t *testing.T) {
	t.Parallel()

	g := mustGeometry(t, geometry.Config{ChunkEdge: 2, LargeChunkEdge: 2, BlockBits: 4})
	store := mustOpen(t, t.TempDir(), g)
	defer closeStore(t, store)

	coord := geometry.Coord{X: -3, Y: 5}
	offset := geometry.Offset{X: 0, Y: 1}
	for _, want := range []uint64{3, 11} {
		chunk := mustChunk(t, g)
		if err := chunk.Set(offset, want); err != nil {
			t.Fatal(err)
		}
		if err := store.WriteChunk(coord, chunk); err != nil {
			t.Fatalf("WriteChunk(%d): %v", want, err)
		}
		stored, err := store.ReadChunk(coord)
		if err != nil {
			t.Fatalf("ReadChunk(): %v", err)
		}
		got, err := stored.Get(offset)
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("Get() = %d, want %d", got, want)
		}
	}
}

func TestStoreReportsAbsentChunk(t *testing.T) {
	t.Parallel()

	g := mustGeometry(t, geometry.Config{ChunkEdge: 2, LargeChunkEdge: 2, BlockBits: 3})
	store := mustOpen(t, t.TempDir(), g)
	defer closeStore(t, store)

	// A region without an image and a published region with a clear presence bit
	// have to report the same missing-chunk class.
	if _, err := store.ReadChunk(geometry.Coord{X: 40, Y: 40}); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("ReadChunk(missing image) error = %v, want fs.ErrNotExist", err)
	}
	if err := store.WriteChunk(geometry.Coord{X: 0, Y: 0}, mustChunk(t, g)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReadChunk(geometry.Coord{X: 1, Y: 1}); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("ReadChunk(absent slot) error = %v, want fs.ErrNotExist", err)
	}
}

func TestStoreBoundsOpenRegionImages(t *testing.T) {
	t.Parallel()

	g := mustGeometry(t, geometry.Config{ChunkEdge: 2, LargeChunkEdge: 2, BlockBits: 5})
	store, err := OpenWithOptions(t.TempDir(), g, Options{MaxOpenRegions: 1})
	if err != nil {
		t.Fatalf("OpenWithOptions(): %v", err)
	}
	defer closeStore(t, store)

	coords := []geometry.Coord{{X: 0, Y: 0}, {X: 4, Y: 0}, {X: -4, Y: -4}}
	for index, coord := range coords {
		chunk := mustChunk(t, g)
		if err := chunk.Set(geometry.Offset{X: 1, Y: 0}, uint64(index+1)); err != nil {
			t.Fatal(err)
		}
		if err := store.WriteChunk(coord, chunk); err != nil {
			t.Fatalf("WriteChunk(%v): %v", coord, err)
		}
	}
	// Reading in the same order forces an eviction before every access.
	for index, coord := range coords {
		chunk, err := store.ReadChunk(coord)
		if err != nil {
			t.Fatalf("ReadChunk(%v): %v", coord, err)
		}
		got, err := chunk.Get(geometry.Offset{X: 1, Y: 0})
		if err != nil {
			t.Fatal(err)
		}
		if want := uint64(index + 1); got != want {
			t.Fatalf("ReadChunk(%v) value = %d, want %d", coord, got, want)
		}
		if open := len(store.images); open > 1 {
			t.Fatalf("open region images = %d, want at most 1", open)
		}
	}
}

func TestStoreDiscardsImageAfterFailedPublication(t *testing.T) {
	t.Parallel()

	g := mustGeometry(t, geometry.Config{ChunkEdge: 2, LargeChunkEdge: 2, BlockBits: 5})
	store := mustOpen(t, t.TempDir(), g)
	defer closeStore(t, store)

	coord := geometry.Coord{X: 0, Y: 1}
	if err := store.WriteChunk(coord, mustChunk(t, g)); err != nil {
		t.Fatal(err)
	}
	region, _ := locate(g, coord)
	element, found := store.images[region]
	if !found {
		t.Fatalf("region %v is not open after a write", region)
	}
	// Releasing the handle underneath the store makes the next publication fail
	// after its in-memory slot directory was already updated.
	if err := element.Value.(*regionImage).file.Close(); err != nil {
		t.Fatal(err)
	}

	chunk := mustChunk(t, g)
	if err := chunk.Set(geometry.Offset{X: 1, Y: 1}, 21); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteChunk(coord, chunk); err == nil {
		t.Fatal("WriteChunk(closed handle) error = nil, want an error")
	}
	if open := len(store.images); open != 0 {
		t.Fatalf("open region images = %d, want 0 after a failed publication", open)
	}
	if err := store.WriteChunk(coord, chunk); err != nil {
		t.Fatalf("WriteChunk() after recovery: %v", err)
	}
	stored, err := store.ReadChunk(coord)
	if err != nil {
		t.Fatalf("ReadChunk(): %v", err)
	}
	got, err := stored.Get(geometry.Offset{X: 1, Y: 1})
	if err != nil {
		t.Fatal(err)
	}
	if got != 21 {
		t.Fatalf("Get() = %d, want 21", got)
	}
}

func TestStoreRejectsInvalidWrites(t *testing.T) {
	t.Parallel()

	g := mustGeometry(t, geometry.Config{ChunkEdge: 2, LargeChunkEdge: 2, BlockBits: 5})
	other := mustGeometry(t, geometry.Config{ChunkEdge: 4, LargeChunkEdge: 2, BlockBits: 5})
	store := mustOpen(t, t.TempDir(), g)
	coord := geometry.Coord{X: 1, Y: 1}

	if err := store.WriteChunk(coord, nil); err == nil {
		t.Fatal("WriteChunk(nil) error = nil, want an error")
	}
	if err := store.WriteChunk(coord, mustChunk(t, other)); !errors.Is(err, ErrGeometryMismatch) {
		t.Fatalf("WriteChunk(other geometry) error = %v, want ErrGeometryMismatch", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("second Close(): %v", err)
	}
	if err := store.WriteChunk(coord, mustChunk(t, g)); err == nil {
		t.Fatal("WriteChunk(closed) error = nil, want an error")
	}
	if _, err := store.ReadChunk(coord); err == nil {
		t.Fatal("ReadChunk(closed) error = nil, want an error")
	}
}

func TestOpenRejectsInvalidConfiguration(t *testing.T) {
	t.Parallel()

	g := mustGeometry(t, geometry.Config{ChunkEdge: 2, LargeChunkEdge: 2, BlockBits: 5})
	if _, err := Open("", g); err == nil {
		t.Fatal("Open(empty root) error = nil, want an error")
	}
	if _, err := Open(t.TempDir(), geometry.Geometry{}); !errors.Is(err, geometry.ErrInvalid) {
		t.Fatalf("Open(zero geometry) error = %v, want geometry.ErrInvalid", err)
	}
	if _, err := OpenWithOptions(t.TempDir(), g, Options{MaxOpenRegions: -1}); err == nil {
		t.Fatal("OpenWithOptions(negative bound) error = nil, want an error")
	}
}

func TestOpenRejectsUnaddressableImageGeometry(t *testing.T) {
	t.Parallel()

	tests := map[string]geometry.Config{
		"slot area beyond the image limit": {ChunkEdge: 64, LargeChunkEdge: 4096, BlockBits: 8},
		"slot count overflow":              {ChunkEdge: 2, LargeChunkEdge: math.MaxUint32, BlockBits: 1},
	}
	for name, config := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			g := mustGeometry(t, config)
			if _, err := Open(t.TempDir(), g); !errors.Is(err, ErrImageTooLarge) {
				t.Fatalf("Open(%v) error = %v, want ErrImageTooLarge", config, err)
			}
		})
	}
}

func mustGeometry(t testing.TB, config geometry.Config) geometry.Geometry {
	t.Helper()

	g, err := geometry.New(config)
	if err != nil {
		t.Fatalf("geometry.New(%v): %v", config, err)
	}
	return g
}

func mustChunk(t testing.TB, g geometry.Geometry) *storage.Chunk {
	t.Helper()

	chunk, err := storage.NewChunk(g)
	if err != nil {
		t.Fatalf("storage.NewChunk(): %v", err)
	}
	return chunk
}

func mustOpen(t testing.TB, root string, g geometry.Geometry) *Store {
	t.Helper()

	store, err := Open(root, g)
	if err != nil {
		t.Fatalf("Open(%q): %v", root, err)
	}
	// Close is idempotent, so this backstop keeps a test that forgets a store from
	// holding a region image open. Windows refuses to remove the temporary
	// directory while any image handle is still open.
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("Close(): %v", err)
		}
	})
	return store
}

func closeStore(t testing.TB, store *Store) {
	t.Helper()

	if err := store.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}
}
