package fs_split

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
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
	chunk := mustChunk(t, g)
	values := map[geometry.Offset]uint64{
		{X: 0, Y: 0}: 1,
		{X: 2, Y: 0}: 31,
		{X: 1, Y: 2}: 18,
	}
	for offset, value := range values {
		if err := chunk.Set(offset, value); err != nil {
			t.Fatalf("Set(%v): %v", offset, err)
		}
	}
	if err := store.WriteChunk(coord, chunk); err != nil {
		t.Fatalf("WriteChunk(): %v", err)
	}

	reopened := mustOpen(t, root, g)
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
	}

	path := filepath.Join(root, "l_n2_p2", "c_n5_p8.rdb")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected split chunk path %q: %v", path, err)
	}
	matches, err := filepath.Glob(filepath.Join(filepath.Dir(path), ".regiondb-chunk-*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 0 {
		t.Fatalf("temporary files remain after atomic write: %v", matches)
	}
}

func TestStoreRejectsCorruption(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	g := mustGeometry(t, geometry.Config{ChunkEdge: 2, LargeChunkEdge: 2, BlockBits: 7})
	store := mustOpen(t, root, g)
	coord := geometry.Coord{X: -1, Y: -1}
	if err := store.WriteChunk(coord, mustChunk(t, g)); err != nil {
		t.Fatal(err)
	}

	path := store.chunkPath(coord)
	encoded, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	encoded[headerBytes] ^= 0x80
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReadChunk(coord); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("ReadChunk(corrupt) error = %v, want ErrCorrupt", err)
	}
}

func TestStoreRejectsTruncatedFile(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	g := mustGeometry(t, geometry.Config{ChunkEdge: 2, LargeChunkEdge: 2, BlockBits: 1})
	store := mustOpen(t, root, g)
	coord := geometry.Coord{X: 1, Y: 2}
	if err := store.WriteChunk(coord, mustChunk(t, g)); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(store.chunkPath(coord), headerBytes); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReadChunk(coord); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("ReadChunk(truncated) error = %v, want ErrCorrupt", err)
	}
}

func TestStoreRejectsGeometryMismatch(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	firstGeometry := mustGeometry(t, geometry.Config{ChunkEdge: 2, LargeChunkEdge: 2, BlockBits: 3})
	firstStore := mustOpen(t, root, firstGeometry)
	coord := geometry.Coord{X: 3, Y: 4}
	if err := firstStore.WriteChunk(coord, mustChunk(t, firstGeometry)); err != nil {
		t.Fatal(err)
	}

	secondGeometry := mustGeometry(t, geometry.Config{ChunkEdge: 2, LargeChunkEdge: 2, BlockBits: 4})
	secondStore := mustOpen(t, root, secondGeometry)
	if _, err := secondStore.ReadChunk(coord); !errors.Is(err, ErrCorrupt) ||
		!errors.Is(err, ErrGeometryMismatch) {
		t.Fatalf("ReadChunk(wrong geometry) error = %v", err)
	}
	if err := firstStore.WriteChunk(coord, mustChunk(t, secondGeometry)); !errors.Is(err, ErrGeometryMismatch) {
		t.Fatalf("WriteChunk(wrong geometry) error = %v", err)
	}
}

func TestOpenRejectsInvalidGeometry(t *testing.T) {
	t.Parallel()

	if _, err := Open(t.TempDir(), geometry.Geometry{}); !errors.Is(err, geometry.ErrInvalid) {
		t.Fatalf("Open(invalid geometry) error = %v", err)
	}
	if _, err := Open("", mustGeometry(t, geometry.Config{
		ChunkEdge: 1, LargeChunkEdge: 1, BlockBits: 1,
	})); err == nil {
		t.Fatal("Open(empty root) succeeded")
	}
}

func TestStoreConcurrentReadsAndWrites(t *testing.T) {
	t.Parallel()

	g := mustGeometry(t, geometry.Config{ChunkEdge: 2, LargeChunkEdge: 2, BlockBits: 2})
	store := mustOpen(t, t.TempDir(), g)
	coord := geometry.Coord{X: -3, Y: 5}
	chunks := []*storage.Chunk{mustChunk(t, g), mustChunk(t, g)}
	for index, chunk := range chunks {
		if err := chunk.Set(geometry.Offset{}, uint64(index+1)); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.WriteChunk(coord, chunks[0]); err != nil {
		t.Fatal(err)
	}

	const (
		writerCount = 4
		readerCount = 6
		iterations  = 50
	)
	start := make(chan struct{})
	results := make(chan error, writerCount+readerCount)
	var workers sync.WaitGroup
	workers.Add(writerCount + readerCount)

	for writer := 0; writer < writerCount; writer++ {
		go func(writer int) {
			defer workers.Done()
			<-start
			for iteration := 0; iteration < iterations; iteration++ {
				chunk := chunks[(writer+iteration)%len(chunks)]
				if err := store.WriteChunk(coord, chunk); err != nil {
					results <- fmt.Errorf("writer %d: %w", writer, err)
					return
				}
			}
			results <- nil
		}(writer)
	}
	for reader := 0; reader < readerCount; reader++ {
		go func(reader int) {
			defer workers.Done()
			<-start
			for iteration := 0; iteration < iterations; iteration++ {
				chunk, err := store.ReadChunk(coord)
				if err != nil {
					results <- fmt.Errorf("reader %d: %w", reader, err)
					return
				}
				value, err := chunk.Get(geometry.Offset{})
				if err != nil {
					results <- fmt.Errorf("reader %d: read value: %w", reader, err)
					return
				}
				if value != 1 && value != 2 {
					results <- fmt.Errorf("reader %d: value = %d, want 1 or 2", reader, value)
					return
				}
			}
			results <- nil
		}(reader)
	}

	close(start)
	workers.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Error(err)
		}
	}
}

func mustGeometry(t *testing.T, config geometry.Config) geometry.Geometry {
	t.Helper()
	g, err := geometry.New(config)
	if err != nil {
		t.Fatalf("geometry.New(%+v): %v", config, err)
	}
	return g
}

func mustOpen(t *testing.T, root string, g geometry.Geometry) *Store {
	t.Helper()
	store, err := Open(root, g)
	if err != nil {
		t.Fatalf("Open(): %v", err)
	}
	return store
}

func mustChunk(t *testing.T, g geometry.Geometry) *storage.Chunk {
	t.Helper()
	chunk, err := storage.NewChunk(g)
	if err != nil {
		t.Fatalf("storage.NewChunk(): %v", err)
	}
	return chunk
}
