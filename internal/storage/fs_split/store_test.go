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
	closeStore(t, store)

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
	store := mustOpenWithOptions(t, root, g, Options{MaxLoadedChunks: 1})
	coord := geometry.Coord{X: -1, Y: -1}
	if err := store.WriteChunk(coord, mustChunk(t, g)); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteChunk(geometry.Coord{X: 8, Y: 8}, mustChunk(t, g)); err != nil {
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
	store := mustOpenWithOptions(t, root, g, Options{MaxLoadedChunks: 1})
	coord := geometry.Coord{X: 1, Y: 2}
	if err := store.WriteChunk(coord, mustChunk(t, g)); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteChunk(geometry.Coord{X: 8, Y: 8}, mustChunk(t, g)); err != nil {
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
	firstStore := mustOpenWithOptions(t, root, firstGeometry, Options{
		CheckpointRecords: 1,
		CheckpointBytes:   1 << 20,
	})
	coord := geometry.Coord{X: 3, Y: 4}
	if err := firstStore.WriteChunk(coord, mustChunk(t, firstGeometry)); err != nil {
		t.Fatal(err)
	}
	if err := firstStore.WriteChunk(coord, mustChunk(t, mustGeometry(t, geometry.Config{
		ChunkEdge: 2, LargeChunkEdge: 2, BlockBits: 4,
	}))); !errors.Is(err, ErrGeometryMismatch) {
		t.Fatalf("WriteChunk(wrong geometry) error = %v", err)
	}
	closeStore(t, firstStore)

	secondGeometry := mustGeometry(t, geometry.Config{ChunkEdge: 2, LargeChunkEdge: 2, BlockBits: 4})
	secondStore := mustOpen(t, root, secondGeometry)
	defer closeStore(t, secondStore)
	if _, err := secondStore.ReadChunk(coord); !errors.Is(err, ErrCorrupt) ||
		!errors.Is(err, ErrGeometryMismatch) {
		t.Fatalf("ReadChunk(wrong geometry) error = %v", err)
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

func TestStressHotContentionEvictionCycles(t *testing.T) {
	t.Parallel()

	const (
		workerCount = 8
		cycles      = 100
	)
	g := mustGeometry(t, geometry.Config{ChunkEdge: 1, LargeChunkEdge: 2, BlockBits: 8})
	store := mustOpenWithOptions(t, t.TempDir(), g, Options{MaxLoadedChunks: 2})
	coords := make([]geometry.Coord, workerCount)
	for worker := range workerCount {
		coords[worker] = geometry.Coord{X: int64(worker), Y: int64(-worker)}
		if err := store.WriteChunk(coords[worker], mustChunk(t, g)); err != nil {
			t.Fatalf("initialize chunk %v: %v", coords[worker], err)
		}
	}

	start := make(chan struct{})
	results := make(chan error, workerCount)
	var workers sync.WaitGroup
	workers.Add(workerCount)
	for worker := range workerCount {
		go func(worker int) {
			defer workers.Done()
			<-start
			for cycle := range cycles {
				value := byte(cycle + worker)
				chunk, err := storage.NewChunk(g)
				if err != nil {
					results <- fmt.Errorf("worker %d cycle %d: create chunk: %w", worker, cycle, err)
					return
				}
				if err := chunk.Set(geometry.Offset{}, uint64(value)); err != nil {
					results <- fmt.Errorf("worker %d cycle %d: set value: %w", worker, cycle, err)
					return
				}
				if err := store.WriteChunk(coords[worker], chunk); err != nil {
					results <- fmt.Errorf("worker %d cycle %d: write chunk: %w", worker, cycle, err)
					return
				}

				// Sweep the shared working set so every cycle reloads chunks
				// that other workers have displaced from the small cache.
				for offset := range workerCount {
					coord := coords[(worker+offset)%workerCount]
					if _, err := store.ReadChunk(coord); err != nil {
						results <- fmt.Errorf(
							"worker %d cycle %d: read chunk %v: %w",
							worker,
							cycle,
							coord,
							err,
						)
						return
					}
				}
			}
			results <- nil
		}(worker)
	}

	close(start)
	workers.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatal(err)
		}
	}

	for worker, coord := range coords {
		chunk, err := store.ReadChunk(coord)
		if err != nil {
			t.Fatalf("final ReadChunk(%v): %v", coord, err)
		}
		value, err := chunk.Get(geometry.Offset{})
		if err != nil {
			t.Fatalf("final chunk %v value: %v", coord, err)
		}
		want := uint64(byte(cycles - 1 + worker))
		if value != want {
			t.Fatalf("final chunk %v value = %d, want %d", coord, value, want)
		}
	}
}

func TestStoreWALReopenDurabilityModes(t *testing.T) {
	t.Parallel()

	for _, mode := range []DurabilityMode{
		DurabilityRelaxed,
		DurabilityFsyncWAL,
		DurabilityFsyncCheckpoint,
	} {
		mode := mode
		t.Run(string(mode), func(t *testing.T) {
			t.Parallel()

			root := t.TempDir()
			g := mustGeometry(t, geometry.Config{ChunkEdge: 2, LargeChunkEdge: 2, BlockBits: 3})
			options := Options{
				Durability:        mode,
				CheckpointRecords: 8,
				CheckpointBytes:   1 << 20,
			}
			store := mustOpenWithOptions(t, root, g, options)
			coord := geometry.Coord{X: 4, Y: -7}
			chunk := mustChunk(t, g)
			if err := chunk.Set(geometry.Offset{X: 1, Y: 1}, 7); err != nil {
				t.Fatal(err)
			}
			if err := store.WriteChunk(coord, chunk); err != nil {
				t.Fatalf("WriteChunk(): %v", err)
			}
			walInfo, err := os.Stat(filepath.Join(root, walName))
			if err != nil {
				t.Fatal(err)
			}
			if walInfo.Size() == 0 {
				t.Fatal("WAL was checkpointed before either threshold")
			}
			if err := os.Remove(store.chunkPath(coord)); err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}

			reopened := mustOpenWithOptions(t, root, g, options)
			defer closeStore(t, reopened)
			got, err := reopened.ReadChunk(coord)
			if err != nil {
				t.Fatalf("ReadChunk() after replay: %v", err)
			}
			value, err := got.Get(geometry.Offset{X: 1, Y: 1})
			if err != nil {
				t.Fatal(err)
			}
			if value != 7 {
				t.Fatalf("replayed value = %d, want 7", value)
			}
			walInfo, err = os.Stat(filepath.Join(root, walName))
			if err != nil {
				t.Fatal(err)
			}
			if walInfo.Size() != 0 {
				t.Fatalf("replayed WAL size = %d, want 0", walInfo.Size())
			}
		})
	}
}

func TestStoreCrashRecoveryDiscardsTruncatedWALTail(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	g := mustGeometry(t, geometry.Config{ChunkEdge: 2, LargeChunkEdge: 2, BlockBits: 2})
	options := Options{CheckpointRecords: 8, CheckpointBytes: 1 << 20}
	store := mustOpenWithOptions(t, root, g, options)
	coord := geometry.Coord{X: -2, Y: 3}
	chunk := mustChunk(t, g)
	if err := chunk.Set(geometry.Offset{}, 3); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteChunk(coord, chunk); err != nil {
		t.Fatal(err)
	}
	partial := store.encodeWALRecord(geometry.Coord{X: 9, Y: 9}, mustChunk(t, g).Bytes())
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	wal, err := os.OpenFile(filepath.Join(root, walName), os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wal.Write(partial[:len(partial)/2]); err != nil {
		_ = wal.Close()
		t.Fatal(err)
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(root, "l_n1_p1", "c_n2_p3.rdb")); err != nil {
		t.Fatal(err)
	}

	reopened := mustOpenWithOptions(t, root, g, options)
	defer closeStore(t, reopened)
	got, err := reopened.ReadChunk(coord)
	if err != nil {
		t.Fatalf("ReadChunk() after truncated tail recovery: %v", err)
	}
	value, err := got.Get(geometry.Offset{})
	if err != nil {
		t.Fatal(err)
	}
	if value != 3 {
		t.Fatalf("replayed value = %d, want 3", value)
	}
	if _, err := reopened.ReadChunk(geometry.Coord{X: 9, Y: 9}); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("partial record created a chunk: %v", err)
	}
}

func TestStoreRejectsCorruptWALRecord(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	g := mustGeometry(t, geometry.Config{ChunkEdge: 1, LargeChunkEdge: 1, BlockBits: 1})
	store := mustOpenWithOptions(t, root, g, Options{CheckpointRecords: 8, CheckpointBytes: 1 << 20})
	if err := store.WriteChunk(geometry.Coord{}, mustChunk(t, g)); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	walPath := filepath.Join(root, walName)
	encoded, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatal(err)
	}
	encoded[walHeaderBytes] ^= 1
	if err := os.WriteFile(walPath, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenWithOptions(root, g, Options{}); !errors.Is(err, ErrCorruptWAL) {
		t.Fatalf("OpenWithOptions(corrupt WAL) error = %v, want ErrCorruptWAL", err)
	}
}

func TestStoreCheckpointsWALThresholds(t *testing.T) {
	t.Parallel()

	tests := []Options{
		{CheckpointRecords: 1, CheckpointBytes: 1 << 20},
		{CheckpointRecords: 8, CheckpointBytes: 1},
	}
	for _, options := range tests {
		root := t.TempDir()
		g := mustGeometry(t, geometry.Config{ChunkEdge: 1, LargeChunkEdge: 1, BlockBits: 1})
		store := mustOpenWithOptions(t, root, g, options)
		if err := store.WriteChunk(geometry.Coord{}, mustChunk(t, g)); err != nil {
			t.Fatal(err)
		}
		info, err := os.Stat(filepath.Join(root, walName))
		if err != nil {
			t.Fatal(err)
		}
		if info.Size() != 0 {
			t.Fatalf("WAL size after threshold = %d, want 0", info.Size())
		}
		closeStore(t, store)
	}
}

func TestOpenRejectsInvalidOptions(t *testing.T) {
	t.Parallel()

	g := mustGeometry(t, geometry.Config{ChunkEdge: 1, LargeChunkEdge: 1, BlockBits: 1})
	for _, options := range []Options{
		{Durability: "unknown"},
		{CheckpointBytes: -1},
		{MaxLoadedChunks: -1},
	} {
		if _, err := OpenWithOptions(t.TempDir(), g, options); err == nil {
			t.Fatalf("OpenWithOptions(%+v) succeeded", options)
		}
	}
}

func TestStoreEvictsAndReloadsChunks(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	g := mustGeometry(t, geometry.Config{ChunkEdge: 1, LargeChunkEdge: 2, BlockBits: 3})
	store := mustOpenWithOptions(t, root, g, Options{MaxLoadedChunks: 1})
	coords := []geometry.Coord{{X: 1, Y: 1}, {X: 2, Y: 2}}
	for index, coord := range coords {
		chunk := mustChunk(t, g)
		if err := chunk.Set(geometry.Offset{}, uint64(index+1)); err != nil {
			t.Fatal(err)
		}
		if err := store.WriteChunk(coord, chunk); err != nil {
			t.Fatalf("WriteChunk(%v): %v", coord, err)
		}
	}

	store.cache.mu.Lock()
	_, firstLoaded := store.cache.entries[coords[0]]
	loaded := len(store.cache.entries)
	store.cache.mu.Unlock()
	if firstLoaded || loaded != 1 {
		t.Fatalf("cache after eviction: first loaded = %t, size = %d", firstLoaded, loaded)
	}

	reloaded, err := store.ReadChunk(coords[0])
	if err != nil {
		t.Fatalf("ReadChunk(evicted): %v", err)
	}
	value, err := reloaded.Get(geometry.Offset{})
	if err != nil {
		t.Fatal(err)
	}
	if value != 1 {
		t.Fatalf("reloaded value = %d, want 1", value)
	}
	store.cache.mu.Lock()
	_, firstLoaded = store.cache.entries[coords[0]]
	_, secondLoaded := store.cache.entries[coords[1]]
	loaded = len(store.cache.entries)
	store.cache.mu.Unlock()
	if !firstLoaded || secondLoaded || loaded != 1 {
		t.Fatalf("cache after reload: first = %t, second = %t, size = %d", firstLoaded, secondLoaded, loaded)
	}
}

func TestStoreCacheReturnsIndependentChunks(t *testing.T) {
	t.Parallel()

	g := mustGeometry(t, geometry.Config{ChunkEdge: 1, LargeChunkEdge: 1, BlockBits: 2})
	store := mustOpenWithOptions(t, t.TempDir(), g, Options{MaxLoadedChunks: 1})
	coord := geometry.Coord{}
	chunk := mustChunk(t, g)
	if err := chunk.Set(geometry.Offset{}, 1); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteChunk(coord, chunk); err != nil {
		t.Fatal(err)
	}
	first, err := store.ReadChunk(coord)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Set(geometry.Offset{}, 2); err != nil {
		t.Fatal(err)
	}
	second, err := store.ReadChunk(coord)
	if err != nil {
		t.Fatal(err)
	}
	value, err := second.Get(geometry.Offset{})
	if err != nil {
		t.Fatal(err)
	}
	if value != 1 {
		t.Fatalf("cached value changed through returned chunk: got %d, want 1", value)
	}
}

func TestStoreRejectsSecondWriter(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	g := mustGeometry(t, geometry.Config{ChunkEdge: 1, LargeChunkEdge: 1, BlockBits: 1})
	first := mustOpen(t, root, g)
	if _, err := Open(root, g); !errors.Is(err, ErrWriterLocked) {
		t.Fatalf("second Open() error = %v, want ErrWriterLocked", err)
	}
	closeStore(t, first)

	second := mustOpen(t, root, g)
	closeStore(t, second)
}

func TestClosedStoreRejectsOperations(t *testing.T) {
	t.Parallel()

	g := mustGeometry(t, geometry.Config{ChunkEdge: 1, LargeChunkEdge: 1, BlockBits: 1})
	store := mustOpen(t, t.TempDir(), g)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReadChunk(geometry.Coord{}); err == nil {
		t.Fatal("ReadChunk() succeeded after Close()")
	}
	if err := store.WriteChunk(geometry.Coord{}, mustChunk(t, g)); err == nil {
		t.Fatal("WriteChunk() succeeded after Close()")
	}
	if err := store.Close(); err != nil {
		t.Fatalf("second Close(): %v", err)
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

func mustOpenWithOptions(t *testing.T, root string, g geometry.Geometry, options Options) *Store {
	t.Helper()
	store, err := OpenWithOptions(root, g, options)
	if err != nil {
		t.Fatalf("OpenWithOptions(): %v", err)
	}
	t.Cleanup(func() {
		closeStore(t, store)
	})
	return store
}

func closeStore(t *testing.T, store *Store) {
	t.Helper()
	if err := store.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}
}

func mustOpen(t *testing.T, root string, g geometry.Geometry) *Store {
	t.Helper()
	store, err := Open(root, g)
	if err != nil {
		t.Fatalf("Open(): %v", err)
	}
	t.Cleanup(func() {
		closeStore(t, store)
	})
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
