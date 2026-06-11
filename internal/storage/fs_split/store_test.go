package fs_split

import (
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Filo6699/regiondb/internal/bitcodec"
	"github.com/Filo6699/regiondb/internal/geometry"
	"github.com/Filo6699/regiondb/internal/storage"
)

func TestStoreReopenRoundTrip(t *testing.T) {
	t.Parallel()

	root := testTempDir(t)
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

func TestStorePersistsExplicitZeroAndAbsentBlocks(t *testing.T) {
	t.Parallel()

	root := testTempDir(t)
	g := mustGeometry(t, geometry.Config{ChunkEdge: 2, LargeChunkEdge: 2, BlockBits: 3})
	store := mustOpen(t, root, g)
	coord := geometry.Coord{X: -2, Y: 4}
	chunk := mustChunk(t, g)
	explicitZero := geometry.Offset{X: 0, Y: 0}
	nonzero := geometry.Offset{X: 1, Y: 0}
	absent := geometry.Offset{X: 0, Y: 1}
	if err := chunk.Set(explicitZero, 0); err != nil {
		t.Fatal(err)
	}
	if err := chunk.Set(nonzero, 5); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteChunk(coord, chunk); err != nil {
		t.Fatalf("WriteChunk(): %v", err)
	}
	closeStore(t, store)

	reopened := mustOpen(t, root, g)
	defer closeStore(t, reopened)
	got, err := reopened.ReadChunk(coord)
	if err != nil {
		t.Fatalf("ReadChunk(): %v", err)
	}
	for _, test := range []struct {
		offset geometry.Offset
		want   bool
	}{
		{offset: explicitZero, want: true},
		{offset: nonzero, want: true},
		{offset: absent, want: false},
	} {
		exists, err := got.Exists(test.offset)
		if err != nil {
			t.Fatal(err)
		}
		if exists != test.want {
			t.Fatalf("Exists(%v) = %t, want %t", test.offset, exists, test.want)
		}
	}
}

func TestStoreReadsLegacyChunkPresence(t *testing.T) {
	t.Parallel()

	root := testTempDir(t)
	g := mustGeometry(t, geometry.Config{ChunkEdge: 2, LargeChunkEdge: 2, BlockBits: 3})
	store := mustOpen(t, root, g)
	defer closeStore(t, store)
	coord := geometry.Coord{X: 3, Y: -2}
	payload := []byte{0x28, 0x00}
	current := store.encode(coord, payload)
	legacyEnd := headerBytes + len(payload)
	legacy := append([]byte(nil), current[:legacyEnd]...)
	copy(legacy[:len(legacyFileMagic)], legacyFileMagic)
	legacy = bitcodec.AppendUint32(legacy, crc32.ChecksumIEEE(legacy))
	path := store.chunkPath(coord)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, legacy, 0o600); err != nil {
		t.Fatal(err)
	}

	got, err := store.ReadChunk(coord)
	if err != nil {
		t.Fatalf("ReadChunk(legacy): %v", err)
	}
	if exists, err := got.Exists(geometry.Offset{}); err != nil || exists {
		t.Fatalf("legacy zero Exists() = %t, %v, want false", exists, err)
	}
	if exists, err := got.Exists(geometry.Offset{X: 1}); err != nil || !exists {
		t.Fatalf("legacy nonzero Exists() = %t, %v, want true", exists, err)
	}
}

func TestStoreReplaysLegacyWALPresence(t *testing.T) {
	t.Parallel()

	root := testTempDir(t)
	g := mustGeometry(t, geometry.Config{ChunkEdge: 2, LargeChunkEdge: 2, BlockBits: 3})
	coord := geometry.Coord{X: -7, Y: 6}
	payload := []byte{0x28, 0x00}
	encoder := &Store{geometry: g}
	current := encoder.encodeWALRecord(coord, payload)
	legacyEnd := walHeaderBytes + len(payload)
	legacy := append([]byte(nil), current[:legacyEnd]...)
	copy(legacy[:len(legacyWALMagic)], legacyWALMagic)
	legacy = bitcodec.AppendUint32(legacy, crc32.ChecksumIEEE(legacy))
	if err := os.WriteFile(filepath.Join(root, walName), legacy, 0o600); err != nil {
		t.Fatal(err)
	}

	store := mustOpen(t, root, g)
	defer closeStore(t, store)
	got, err := store.ReadChunk(coord)
	if err != nil {
		t.Fatalf("ReadChunk(replayed legacy WAL): %v", err)
	}
	if exists, err := got.Exists(geometry.Offset{}); err != nil || exists {
		t.Fatalf("legacy WAL zero Exists() = %t, %v, want false", exists, err)
	}
	if exists, err := got.Exists(geometry.Offset{X: 1}); err != nil || !exists {
		t.Fatalf("legacy WAL nonzero Exists() = %t, %v, want true", exists, err)
	}
}

func TestStoreRejectsCorruption(t *testing.T) {
	t.Parallel()

	root := testTempDir(t)
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

	root := testTempDir(t)
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

	root := testTempDir(t)
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

	if _, err := Open(testTempDir(t), geometry.Geometry{}); !errors.Is(err, geometry.ErrInvalid) {
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
	store := mustOpen(t, testTempDir(t), g)
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
	store := mustOpenWithOptions(t, testTempDir(t), g, Options{MaxLoadedChunks: 2})
	coords := make([]geometry.Coord, workerCount)
	expected := make([]*expectedState, workerCount)
	for worker := range workerCount {
		coords[worker] = geometry.Coord{X: int64(worker), Y: int64(-worker)}
		if err := store.WriteChunk(coords[worker], mustChunk(t, g)); err != nil {
			t.Fatalf("initialize chunk %v: %v", coords[worker], err)
		}
		expected[worker] = newExpectedState(0)
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
				// Every value is unique per coordinate and still fits the
				// eight-bit blocks, so an observation identifies exactly one
				// write of this worker.
				value := uint64(cycle + worker + 1)
				chunk, err := storage.NewChunk(g)
				if err != nil {
					results <- fmt.Errorf("worker %d cycle %d: create chunk: %w", worker, cycle, err)
					return
				}
				if err := chunk.Set(geometry.Offset{}, value); err != nil {
					results <- fmt.Errorf("worker %d cycle %d: set value: %w", worker, cycle, err)
					return
				}
				err = expected[worker].write(value, func() error {
					return store.WriteChunk(coords[worker], chunk)
				})
				if err != nil {
					results <- fmt.Errorf("worker %d cycle %d: write chunk: %w", worker, cycle, err)
					return
				}

				// Sweep the shared working set so every cycle reloads chunks
				// that other workers have displaced from the small cache.
				for offset := range workerCount {
					owner := (worker + offset) % workerCount
					coord := coords[owner]
					window := expected[owner].beginRead()
					chunk, err := store.ReadChunk(coord)
					if err != nil {
						results <- fmt.Errorf(
							"worker %d cycle %d: read chunk %v: %w",
							worker,
							cycle,
							coord,
							err,
						)
						return
					}
					value, err := chunk.Get(geometry.Offset{})
					if err != nil {
						results <- fmt.Errorf(
							"worker %d cycle %d: read value of chunk %v: %w",
							worker,
							cycle,
							coord,
							err,
						)
						return
					}
					if err := expected[owner].observe(value, window); err != nil {
						results <- fmt.Errorf(
							"worker %d cycle %d: chunk %v: %w",
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

	// The scheduler decides which successful reads are most recent, so the
	// identity of the final residents is intentionally unspecified. Eviction
	// must still leave a full, bounded, internally consistent cache whose
	// payloads agree with the completed writes.
	store.cache.mu.Lock()
	residents := len(store.cache.entries)
	candidates := store.cache.recent.length
	free := len(store.cache.free)
	allocated := candidates + free
	for coord, entry := range store.cache.entries {
		if entry.coord != coord {
			store.cache.mu.Unlock()
			t.Fatalf("cache key %v contains entry for %v", coord, entry.coord)
		}
		worker := int(coord.X)
		if worker < 0 || worker >= workerCount || coords[worker] != coord {
			store.cache.mu.Unlock()
			t.Fatalf("cache contains unexpected coordinate %v", coord)
		}
		chunk, err := storage.ChunkFromBytes(g, entry.payload)
		if err != nil {
			store.cache.mu.Unlock()
			t.Fatalf("cached chunk %v: %v", coord, err)
		}
		value, err := chunk.Get(geometry.Offset{})
		if err != nil {
			store.cache.mu.Unlock()
			t.Fatalf("cached chunk %v value: %v", coord, err)
		}
		want := uint64(cycles + worker)
		if value != want {
			store.cache.mu.Unlock()
			t.Fatalf("cached chunk %v value = %d, want %d", coord, value, want)
		}
	}
	store.cache.mu.Unlock()
	if residents > 2 || candidates != residents || allocated != residents+free {
		t.Fatalf(
			"cache after stress: residents = %d, candidates = %d, free = %d, allocated entries = %d, want at most 2 residents, one candidate per resident, and allocated = residents + free",
			residents,
			candidates,
			free,
			allocated,
		)
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
		want := uint64(cycles + worker)
		if value != want {
			t.Fatalf("final chunk %v value = %d, want %d", coord, value, want)
		}
	}
}

func TestStressSharedCoordExpectedState(t *testing.T) {
	t.Parallel()

	const (
		writerCount  = 4
		readerCount  = 4
		iterations   = 150
		initialValue = 1
	)
	g := mustGeometry(t, geometry.Config{ChunkEdge: 1, LargeChunkEdge: 2, BlockBits: 16})
	store := mustOpenWithOptions(t, testTempDir(t), g, Options{MaxLoadedChunks: 1})
	coord := geometry.Coord{X: 6, Y: -9}
	initial := mustChunk(t, g)
	if err := initial.Set(geometry.Offset{}, initialValue); err != nil {
		t.Fatalf("set initial value: %v", err)
	}
	if err := store.WriteChunk(coord, initial); err != nil {
		t.Fatalf("initialize chunk %v: %v", coord, err)
	}
	expected := newExpectedState(initialValue)

	start := make(chan struct{})
	results := make(chan error, writerCount+readerCount)
	var workers sync.WaitGroup
	workers.Add(writerCount + readerCount)

	for writer := range writerCount {
		go func(writer int) {
			defer workers.Done()
			<-start
			for iteration := range iterations {
				// Distinct values keep every observation attributable to a
				// single write, even though all writers share the coordinate.
				value := uint64(initialValue + 1 + writer*iterations + iteration)
				chunk, err := storage.NewChunk(g)
				if err != nil {
					results <- fmt.Errorf("writer %d iteration %d: create chunk: %w", writer, iteration, err)
					return
				}
				if err := chunk.Set(geometry.Offset{}, value); err != nil {
					results <- fmt.Errorf("writer %d iteration %d: set value: %w", writer, iteration, err)
					return
				}
				err = expected.write(value, func() error {
					return store.WriteChunk(coord, chunk)
				})
				if err != nil {
					results <- fmt.Errorf("writer %d iteration %d: write chunk: %w", writer, iteration, err)
					return
				}
			}
			results <- nil
		}(writer)
	}
	for reader := range readerCount {
		go func(reader int) {
			defer workers.Done()
			<-start
			for iteration := range iterations {
				window := expected.beginRead()
				chunk, err := store.ReadChunk(coord)
				if err != nil {
					results <- fmt.Errorf("reader %d iteration %d: read chunk: %w", reader, iteration, err)
					return
				}
				value, err := chunk.Get(geometry.Offset{})
				if err != nil {
					results <- fmt.Errorf("reader %d iteration %d: read value: %w", reader, iteration, err)
					return
				}
				if err := expected.observe(value, window); err != nil {
					results <- fmt.Errorf("reader %d iteration %d: %w", reader, iteration, err)
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
			t.Fatal(err)
		}
	}

	chunk, err := store.ReadChunk(coord)
	if err != nil {
		t.Fatalf("final ReadChunk(%v): %v", coord, err)
	}
	value, err := chunk.Get(geometry.Offset{})
	if err != nil {
		t.Fatalf("final chunk %v value: %v", coord, err)
	}
	if err := expected.observe(value, expected.beginRead()); err != nil {
		t.Fatalf("final chunk %v: %v", coord, err)
	}
}

// expectedState tracks the chunk values a concurrent reader is allowed to
// observe for one coordinate. Ordering is the whole point. A value becomes
// visible to readers while WriteChunk is still running, so it is published as in
// flight before the write starts, and it is recorded as committed inside the
// same critical section that performed the write, so the recorded order matches
// the order the store accepted the writes. Recording a commit after releasing
// that section lets two writers append in the opposite order, which makes the
// tracker report healthy reads as stale and turns the stress suite into a coin
// flip.
type expectedState struct {
	order     sync.Mutex
	mu        sync.Mutex
	pending   map[uint64]int
	committed []uint64
}

func newExpectedState(initial uint64) *expectedState {
	return &expectedState{
		pending:   make(map[uint64]int),
		committed: []uint64{initial},
	}
}

// write publishes value, runs the write, and records the outcome without ever
// leaving the tracker's write order.
func (e *expectedState) write(value uint64, apply func() error) error {
	e.order.Lock()
	defer e.order.Unlock()

	e.beginWrite(value)
	if err := apply(); err != nil {
		e.retire(value, false)
		return err
	}
	e.retire(value, true)
	return nil
}

// beginWrite publishes a value as observable before the write reaches the store.
func (e *expectedState) beginWrite(value uint64) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.pending[value]++
}

// retire drops a value from the in-flight set, appending it to the committed
// history when the write succeeded.
func (e *expectedState) retire(value uint64, committed bool) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.retirePending(value)
	if committed {
		e.committed = append(e.committed, value)
	}
}

func (e *expectedState) retirePending(value uint64) {
	if e.pending[value] <= 1 {
		delete(e.pending, value)
		return
	}
	e.pending[value]--
}

// beginRead snapshots how many writes had already committed, opening the window
// an observation is later checked against.
func (e *expectedState) beginRead() int {
	e.mu.Lock()
	defer e.mu.Unlock()

	return len(e.committed)
}

// observe reports whether value could be produced by a read that started when
// committed writes numbered window. The write committed right before the window
// opened is still visible, as is anything committed since, plus every write that
// is in flight when the observation is checked. Reading an older committed value
// means the store lost a write that was already durable when the read started.
func (e *expectedState) observe(value uint64, window int) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.pending[value] > 0 {
		return nil
	}
	first := max(window-1, 0)
	visible := e.committed[first:]
	for _, candidate := range visible {
		if candidate == value {
			return nil
		}
	}
	return fmt.Errorf(
		"value = %d, want a value visible during the read (in flight %v, committed %v)",
		value,
		e.pending,
		visible,
	)
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

			root := testTempDir(t)
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

func TestStoreCrashRecoveryDiscardsTruncatedWALRecord(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		cutoff func([]byte) int
	}{
		{
			name: "header",
			cutoff: func([]byte) int {
				return len(walMagic) / 2
			},
		},
		{
			name: "tail",
			cutoff: func(record []byte) int {
				return len(record) - 1
			},
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			root := testTempDir(t)
			g := mustGeometry(t, geometry.Config{ChunkEdge: 2, LargeChunkEdge: 2, BlockBits: 2})
			options := Options{CheckpointRecords: 8, CheckpointBytes: 1 << 20}
			store := mustOpenWithOptions(t, root, g, options)
			completeCoord := geometry.Coord{X: -2, Y: 3}
			chunk := mustChunk(t, g)
			if err := chunk.Set(geometry.Offset{}, 3); err != nil {
				t.Fatal(err)
			}
			if err := store.WriteChunk(completeCoord, chunk); err != nil {
				t.Fatal(err)
			}
			partialCoord := geometry.Coord{X: 9, Y: 9}
			partial := store.encodeWALRecord(partialCoord, mustChunk(t, g).Bytes())
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			wal, err := os.OpenFile(filepath.Join(root, walName), os.O_WRONLY|os.O_APPEND, 0)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := wal.Write(partial[:test.cutoff(partial)]); err != nil {
				_ = wal.Close()
				t.Fatal(err)
			}
			if err := wal.Close(); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(store.chunkPath(completeCoord)); err != nil {
				t.Fatal(err)
			}

			reopened := mustOpenWithOptions(t, root, g, options)
			got, err := reopened.ReadChunk(completeCoord)
			if err != nil {
				t.Fatalf("ReadChunk() after truncated %s recovery: %v", test.name, err)
			}
			value, err := got.Get(geometry.Offset{})
			if err != nil {
				t.Fatal(err)
			}
			if value != 3 {
				t.Fatalf("replayed value = %d, want 3", value)
			}
			if _, err := reopened.ReadChunk(partialCoord); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("partial record created a chunk: %v", err)
			}
			info, err := os.Stat(filepath.Join(root, walName))
			if err != nil {
				t.Fatal(err)
			}
			if info.Size() != 0 {
				t.Fatalf("recovered WAL size = %d, want 0", info.Size())
			}
		})
	}
}

func TestStoreRecoveryRepeatedWALReplay(t *testing.T) {
	t.Parallel()

	root := testTempDir(t)
	g := mustGeometry(t, geometry.Config{ChunkEdge: 1, LargeChunkEdge: 2, BlockBits: 4})
	options := Options{CheckpointRecords: 8, CheckpointBytes: 1 << 20}
	coord := geometry.Coord{X: -4, Y: 6}
	store := mustOpenWithOptions(t, root, g, options)
	for _, value := range []uint64{5, 11} {
		chunk := mustChunk(t, g)
		if err := chunk.Set(geometry.Offset{}, value); err != nil {
			t.Fatal(err)
		}
		if err := store.WriteChunk(coord, chunk); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	walPath := filepath.Join(root, walName)
	records, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatal(err)
	}

	const replayCount = 3
	for replay := 1; replay <= replayCount; replay++ {
		if err := os.Remove(store.chunkPath(coord)); err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		if err := os.WriteFile(walPath, records, 0o600); err != nil {
			t.Fatal(err)
		}
		reopened := mustOpenWithOptions(t, root, g, options)
		got, err := reopened.ReadChunk(coord)
		if err != nil {
			t.Fatalf("replay %d ReadChunk(): %v", replay, err)
		}
		value, err := got.Get(geometry.Offset{})
		if err != nil {
			t.Fatal(err)
		}
		if value != 11 {
			t.Fatalf("replay %d value = %d, want 11", replay, value)
		}
		info, err := os.Stat(walPath)
		if err != nil {
			t.Fatal(err)
		}
		if info.Size() != 0 {
			t.Fatalf("replay %d WAL size = %d, want 0", replay, info.Size())
		}
		if err := reopened.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestStoreWALReplayInvalidatesReloadedChunk(t *testing.T) {
	t.Parallel()

	root := testTempDir(t)
	g := mustGeometry(t, geometry.Config{ChunkEdge: 1, LargeChunkEdge: 2, BlockBits: 4})
	store := mustOpenWithOptions(t, root, g, Options{
		CheckpointRecords: 1,
		CheckpointBytes:   1 << 20,
		MaxLoadedChunks:   1,
	})
	replayedCoord := geometry.Coord{X: 3, Y: -4}
	evictingCoord := geometry.Coord{X: 8, Y: 9}

	for _, write := range []struct {
		coord geometry.Coord
		value uint64
	}{
		{coord: replayedCoord, value: 3},
		{coord: evictingCoord, value: 7},
	} {
		chunk := mustChunk(t, g)
		if err := chunk.Set(geometry.Offset{}, write.value); err != nil {
			t.Fatal(err)
		}
		if err := store.WriteChunk(write.coord, chunk); err != nil {
			t.Fatalf("WriteChunk(%v): %v", write.coord, err)
		}
	}
	if _, err := store.ReadChunk(replayedCoord); err != nil {
		t.Fatalf("reload evicted chunk: %v", err)
	}

	replayed := mustChunk(t, g)
	if err := replayed.Set(geometry.Offset{}, 11); err != nil {
		t.Fatal(err)
	}
	if err := store.appendWAL(store.encodeWALRecord(replayedCoord, replayed.Bytes())); err != nil {
		t.Fatalf("append replay record: %v", err)
	}
	if err := store.recoverWAL(); err != nil {
		t.Fatalf("recoverWAL(): %v", err)
	}

	got, err := store.ReadChunk(replayedCoord)
	if err != nil {
		t.Fatalf("ReadChunk() after replay: %v", err)
	}
	value, err := got.Get(geometry.Offset{})
	if err != nil {
		t.Fatal(err)
	}
	if value != 11 {
		t.Fatalf("replayed value = %d, want 11", value)
	}
}

func TestStoreLongWALCheckpointReopenCycles(t *testing.T) {
	t.Parallel()

	const (
		checkpointRecords = 257
		cycles            = 4
	)
	checkpointTrigger := int(checkpointRecordTrigger(checkpointRecords))
	root := testTempDir(t)
	g := mustGeometry(t, geometry.Config{ChunkEdge: 1, LargeChunkEdge: 2, BlockBits: 16})
	options := Options{
		Durability:        DurabilityFsyncWAL,
		CheckpointRecords: checkpointRecords,
		CheckpointBytes:   1 << 20,
	}
	coord := geometry.Coord{X: 12, Y: -7}
	recordBytes := int64(walHeaderBytes + g.PayloadBytes() + g.PresenceBytes() + checksumSize)

	for cycle := range cycles {
		store := mustOpenWithOptions(t, root, g, options)
		for write := range checkpointTrigger {
			chunk := mustChunk(t, g)
			value := uint64(cycle*checkpointTrigger + write + 1)
			if err := chunk.Set(geometry.Offset{}, value); err != nil {
				t.Fatal(err)
			}
			if err := store.WriteChunk(coord, chunk); err != nil {
				t.Fatalf("cycle %d write %d: %v", cycle, write, err)
			}
			if write == checkpointTrigger-2 {
				info, err := os.Stat(filepath.Join(root, walName))
				if err != nil {
					t.Fatal(err)
				}
				want := int64(checkpointTrigger-1) * recordBytes
				if info.Size() != want {
					t.Fatalf("cycle %d WAL size before checkpoint = %d, want %d", cycle, info.Size(), want)
				}
			}
		}
		info, err := os.Stat(filepath.Join(root, walName))
		if err != nil {
			t.Fatal(err)
		}
		if info.Size() != 0 {
			t.Fatalf("cycle %d WAL size after checkpoint = %d, want 0", cycle, info.Size())
		}
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}

		reopened := mustOpenWithOptions(t, root, g, options)
		got, err := reopened.ReadChunk(coord)
		if err != nil {
			t.Fatalf("cycle %d ReadChunk() after reopen: %v", cycle, err)
		}
		value, err := got.Get(geometry.Offset{})
		if err != nil {
			t.Fatal(err)
		}
		want := uint64((cycle + 1) * checkpointTrigger)
		if value != want {
			t.Fatalf("cycle %d value after reopen = %d, want %d", cycle, value, want)
		}
		if err := reopened.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestStoreRejectsCorruptWALRecord(t *testing.T) {
	t.Parallel()

	root := testTempDir(t)
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
		root := testTempDir(t)
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

func TestStoreCheckpointHysteresis(t *testing.T) {
	t.Parallel()

	const checkpointRecords = 4
	g := mustGeometry(t, geometry.Config{ChunkEdge: 1, LargeChunkEdge: 1, BlockBits: 1})
	store := mustOpenWithOptions(t, testTempDir(t), g, Options{
		CheckpointRecords: checkpointRecords,
		CheckpointBytes:   1 << 20,
	})
	defer closeStore(t, store)

	trigger := checkpointRecordTrigger(checkpointRecords)
	for update := uint64(1); update < trigger; update++ {
		if err := store.WriteChunk(geometry.Coord{}, mustChunk(t, g)); err != nil {
			t.Fatalf("WriteChunk(%d): %v", update, err)
		}
		if got := store.RuntimeStats().Checkpoints; got != 0 {
			t.Fatalf("update %d checkpoints = %d, want 0 before trigger %d", update, got, trigger)
		}
	}
	if err := store.WriteChunk(geometry.Coord{}, mustChunk(t, g)); err != nil {
		t.Fatalf("WriteChunk(trigger): %v", err)
	}
	if got := store.RuntimeStats().Checkpoints; got != 1 {
		t.Fatalf("checkpoints at trigger %d = %d, want 1", trigger, got)
	}
}

func TestStoreCheckpointByteHysteresis(t *testing.T) {
	t.Parallel()

	g := mustGeometry(t, geometry.Config{ChunkEdge: 1, LargeChunkEdge: 1, BlockBits: 1})
	recordBytes := int64(walHeaderBytes + g.PayloadBytes() + g.PresenceBytes() + checksumSize)
	lowerBound := 2 * recordBytes
	store := mustOpenWithOptions(t, testTempDir(t), g, Options{
		CheckpointRecords: math.MaxUint64,
		CheckpointBytes:   lowerBound,
	})
	defer closeStore(t, store)

	if err := store.WriteChunk(geometry.Coord{}, mustChunk(t, g)); err != nil {
		t.Fatalf("WriteChunk(1): %v", err)
	}
	if err := store.WriteChunk(geometry.Coord{}, mustChunk(t, g)); err != nil {
		t.Fatalf("WriteChunk(2): %v", err)
	}
	if got := store.RuntimeStats().Checkpoints; got != 0 {
		t.Fatalf("checkpoints at lower byte bound %d = %d, want 0", lowerBound, got)
	}

	if err := store.WriteChunk(geometry.Coord{}, mustChunk(t, g)); err != nil {
		t.Fatalf("WriteChunk(3): %v", err)
	}
	if got := store.RuntimeStats().Checkpoints; got != 1 {
		t.Fatalf(
			"checkpoints at byte trigger %d = %d, want 1",
			checkpointByteTrigger(lowerBound),
			got,
		)
	}
}

func TestCheckpointHysteresisTriggersSaturate(t *testing.T) {
	t.Parallel()

	recordTests := []struct {
		lower uint64
		want  uint64
	}{
		{lower: 1, want: 1},
		{lower: 2, want: 3},
		{lower: 3, want: 4},
		{lower: math.MaxUint64, want: math.MaxUint64},
	}
	for _, test := range recordTests {
		if got := checkpointRecordTrigger(test.lower); got != test.want {
			t.Fatalf("checkpointRecordTrigger(%d) = %d, want %d", test.lower, got, test.want)
		}
	}

	byteTests := []struct {
		lower int64
		want  int64
	}{
		{lower: 1, want: 1},
		{lower: 2, want: 3},
		{lower: 3, want: 4},
		{lower: math.MaxInt64, want: math.MaxInt64},
	}
	for _, test := range byteTests {
		if got := checkpointByteTrigger(test.lower); got != test.want {
			t.Fatalf("checkpointByteTrigger(%d) = %d, want %d", test.lower, got, test.want)
		}
	}
}

func TestStoreWALGroupCommitUsesUpdateBoundaries(t *testing.T) {
	t.Parallel()

	g := mustGeometry(t, geometry.Config{ChunkEdge: 1, LargeChunkEdge: 1, BlockBits: 4})
	for _, groupSize := range []uint64{1, 2, 3, 5} {
		groupSize := groupSize
		t.Run(fmt.Sprintf("group_%d", groupSize), func(t *testing.T) {
			t.Parallel()

			store := mustOpenWithOptions(t, testTempDir(t), g, Options{
				Durability:            DurabilityFsyncWAL,
				CheckpointRecords:     16,
				CheckpointBytes:       1 << 20,
				WALGroupCommitUpdates: groupSize,
			})
			t.Cleanup(func() {
				closeStore(t, store)
			})

			for update := uint64(1); update <= groupSize*2+1; update++ {
				chunk := mustChunk(t, g)
				if err := chunk.Set(geometry.Offset{}, update); err != nil {
					t.Fatal(err)
				}
				if err := store.WriteChunk(geometry.Coord{}, chunk); err != nil {
					t.Fatalf("WriteChunk(%d): %v", update, err)
				}
				wantPending := update % groupSize
				if store.walUnsyncedUpdates != wantPending {
					t.Fatalf(
						"update %d pending WAL sync count = %d, want %d",
						update,
						store.walUnsyncedUpdates,
						wantPending,
					)
				}
			}
			stats := store.RuntimeStats()
			if groupSize == 1 {
				if stats.WALForegroundFlushes != 3 || stats.WALGroupFlushes != 0 {
					t.Fatalf("single-update flush stats = %+v, want three foreground flushes", stats)
				}
			} else if stats.WALForegroundFlushes != 0 || stats.WALGroupFlushes != 2 {
				t.Fatalf("group size %d flush stats = %+v, want two group flushes", groupSize, stats)
			}
			if stats.WALEvictionFlushes != 0 || stats.WALCheckpointFlushes != 0 {
				t.Fatalf("unrelated flush stats for group size %d = %+v", groupSize, stats)
			}
		})
	}
}

func TestStoreWALGroupCommitFlushesOnClose(t *testing.T) {
	t.Parallel()

	g := mustGeometry(t, geometry.Config{ChunkEdge: 1, LargeChunkEdge: 1, BlockBits: 4})
	store := mustOpenWithOptions(t, testTempDir(t), g, Options{
		Durability:            DurabilityFsyncWAL,
		CheckpointRecords:     8,
		CheckpointBytes:       1 << 20,
		WALGroupCommitUpdates: 3,
	})
	coord := geometry.Coord{}
	chunk := mustChunk(t, g)
	if err := chunk.Set(geometry.Offset{}, 4); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteChunk(coord, chunk); err != nil {
		t.Fatalf("WriteChunk(): %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}
	if store.walUnsyncedUpdates != 0 {
		t.Fatalf("pending WAL sync count after close = %d, want 0", store.walUnsyncedUpdates)
	}
	if err := os.Remove(store.chunkPath(coord)); err != nil {
		t.Fatal(err)
	}

	reopened := mustOpenWithOptions(t, store.root, g, Options{
		Durability:            DurabilityFsyncWAL,
		CheckpointRecords:     8,
		CheckpointBytes:       1 << 20,
		WALGroupCommitUpdates: 3,
	})
	got, err := reopened.ReadChunk(coord)
	if err != nil {
		t.Fatalf("ReadChunk() after grouped WAL replay: %v", err)
	}
	value, err := got.Get(geometry.Offset{})
	if err != nil {
		t.Fatal(err)
	}
	if value != 4 {
		t.Fatalf("replayed grouped value = %d, want 4", value)
	}
}

func TestOpenRejectsInvalidOptions(t *testing.T) {
	t.Parallel()

	g := mustGeometry(t, geometry.Config{ChunkEdge: 1, LargeChunkEdge: 1, BlockBits: 1})
	for _, options := range []Options{
		{Durability: "unknown"},
		{CheckpointBytes: -1},
		{MaxLoadedChunks: -1},
		{MaxOpenWALHandles: -1},
	} {
		if _, err := OpenWithOptions(testTempDir(t), g, options); err == nil {
			t.Fatalf("OpenWithOptions(%+v) succeeded", options)
		}
	}
}

func TestStoreEvictsAndReloadsChunks(t *testing.T) {
	t.Parallel()

	root := testTempDir(t)
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
	store.cache.mu.Unlock()
	loaded := store.cache.loadedChunks()
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
	store.cache.mu.Unlock()
	loaded = store.cache.loadedChunks()
	if !firstLoaded || secondLoaded || loaded != 1 {
		t.Fatalf("cache after reload: first = %t, second = %t, size = %d", firstLoaded, secondLoaded, loaded)
	}

	closeStore(t, store)
	if err := removeTestDirectory(root); err != nil {
		t.Fatalf("remove cache eviction data directory: %v", err)
	}
	if _, err := os.Stat(root); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("cache eviction data directory remains after cleanup: %v", err)
	}
}

func TestStoreCacheEvictionSkipsLowValueWALCheckpoint(t *testing.T) {
	t.Parallel()

	root := testTempDir(t)
	g := mustGeometry(t, geometry.Config{ChunkEdge: 1, LargeChunkEdge: 1, BlockBits: 3})
	const checkpointRecords = 3
	store := mustOpenWithOptions(t, root, g, Options{
		CheckpointRecords: checkpointRecords,
		CheckpointBytes:   1 << 20,
		MaxLoadedChunks:   1,
	})
	defer closeStore(t, store)

	for index := range checkpointRecords - 1 {
		coord := geometry.Coord{X: int64(index)}
		if err := store.WriteChunk(coord, mustChunk(t, g)); err != nil {
			t.Fatalf("WriteChunk(%v): %v", coord, err)
		}
	}

	stats := store.RuntimeStats()
	if stats.Evictions == 0 {
		t.Fatal("writes did not exercise cache eviction")
	}
	if stats.Checkpoints != 0 {
		t.Fatalf("checkpoints after low-value eviction = %d, want 0", stats.Checkpoints)
	}
	info, err := os.Stat(filepath.Join(root, walName))
	if err != nil {
		t.Fatalf("stat WAL after low-value eviction: %v", err)
	}
	recordBytes := int64(walHeaderBytes + g.PayloadBytes() + g.PresenceBytes() + checksumSize)
	if want := int64(checkpointRecords-1) * recordBytes; info.Size() != want {
		t.Fatalf("WAL size after low-value eviction = %d, want %d", info.Size(), want)
	}
}

func TestStoreRuntimeStats(t *testing.T) {
	t.Parallel()

	g := mustGeometry(t, geometry.Config{ChunkEdge: 1, LargeChunkEdge: 1, BlockBits: 2})
	store := mustOpenWithOptions(t, testTempDir(t), g, Options{
		Durability:        DurabilityFsyncWAL,
		CheckpointRecords: 2,
		CheckpointBytes:   1 << 20,
		MaxLoadedChunks:   1,
	})
	first := geometry.Coord{X: 1}
	second := geometry.Coord{X: 2}
	missing := geometry.Coord{X: 3}

	if err := store.WriteChunk(first, mustChunk(t, g)); err != nil {
		t.Fatalf("WriteChunk(%v): %v", first, err)
	}
	if _, err := store.ReadChunk(first); err != nil {
		t.Fatalf("ReadChunk(%v): %v", first, err)
	}
	if _, err := store.ReadChunk(missing); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("ReadChunk(%v) error = %v, want os.ErrNotExist", missing, err)
	}
	if err := store.WriteChunk(second, mustChunk(t, g)); err != nil {
		t.Fatalf("WriteChunk(%v): %v", second, err)
	}
	if err := store.WriteChunk(first, mustChunk(t, g)); err != nil {
		t.Fatalf("WriteChunk(%v): %v", first, err)
	}

	want := storage.RuntimeStats{
		ProcessLockMode:      writerGuardMode(),
		ChunkLockMode:        "shared-rwmutex",
		CacheHits:            1,
		CacheMisses:          1,
		LoadedChunks:         1,
		Evictions:            2,
		EvictionRuns:         2,
		WALFlushes:           4,
		WALForegroundFlushes: 3,
		WALCheckpointFlushes: 1,
		OpenWALHandles:       2,
		Checkpoints:          1,
	}
	got := store.RuntimeStats()
	if got != want {
		t.Fatalf("RuntimeStats() = %+v, want %+v", got, want)
	}
	if got.WALEvictionFlushes != 0 {
		t.Fatal("write-through cache eviction unexpectedly requires a WAL flush")
	}
}

func TestStoreCacheReturnsIndependentChunks(t *testing.T) {
	t.Parallel()

	g := mustGeometry(t, geometry.Config{ChunkEdge: 1, LargeChunkEdge: 1, BlockBits: 2})
	store := mustOpenWithOptions(t, testTempDir(t), g, Options{MaxLoadedChunks: 1})
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

// Rejecting a second writer is a deterministic outcome, so nothing in this case
// may depend on a background timer. The heartbeat rewrites owner.json through an
// atomic replacement on a five-second tick, which on Windows means an unrelated
// process is deleting and recreating the very file the second writer reads. Both
// stores are therefore opened without a heartbeat, and the case pins the
// ownership metadata it asserts on.
func TestStoreRejectsSecondWriter(t *testing.T) {
	t.Parallel()

	root := testTempDir(t)
	g := mustGeometry(t, geometry.Config{ChunkEdge: 1, LargeChunkEdge: 1, BlockBits: 1})
	first := mustOpenWithoutWriterHeartbeat(t, root, g)
	assertNoWriterHeartbeat(t, first)

	ownerPath := filepath.Join(root, lockName, lockOwnerName)
	owner, found, err := readOwnerMetadata(ownerPath)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("writer owner metadata is missing")
	}
	if !owner.HeartbeatAt.Equal(owner.StartedAt) {
		t.Fatalf("owner metadata was rewritten before the assertion: %+v", owner)
	}

	if _, err := openStoreWithoutWriterHeartbeat(root, g, Options{}); !errors.Is(err, ErrWriterLocked) {
		t.Fatalf("second open error = %v, want ErrWriterLocked", err)
	}
	observed, found, err := readOwnerMetadata(ownerPath)
	if err != nil {
		t.Fatal(err)
	}
	if !found || observed != owner {
		t.Fatalf("owner metadata changed during the rejection: %+v, want %+v", observed, owner)
	}
	closeStore(t, first)
	if _, err := os.Stat(ownerPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("owner metadata remains after release: %v", err)
	}

	// The bounded retry covers operating-system lock release, not the heartbeat:
	// Windows can still report the guard as locked for a moment after the handle
	// is closed.
	second := mustOpenWithoutWriterHeartbeatAfterRelease(t, root, g)
	assertNoWriterHeartbeat(t, second)
	if second.writerLock.owner.SessionID == owner.SessionID {
		t.Fatal("released writer session id was reused")
	}
	closeStore(t, second)
}

func TestConcurrentStoresUseIsolatedDataDirectories(t *testing.T) {
	t.Parallel()

	const storeCount = 8
	g := mustGeometry(t, geometry.Config{ChunkEdge: 1, LargeChunkEdge: 1, BlockBits: 1})
	roots := make([]string, storeCount)
	for index := range roots {
		roots[index] = testTempDir(t)
	}

	type openResult struct {
		index int
		store *Store
		err   error
	}
	start := make(chan struct{})
	results := make(chan openResult, storeCount)
	for index, root := range roots {
		go func(index int, root string) {
			<-start
			// Heartbeat-free, because this case reads the ownership metadata each
			// store recorded at acquisition; a tick would rewrite it concurrently.
			store, err := openStoreWithoutWriterHeartbeat(root, g, Options{})
			results <- openResult{index: index, store: store, err: err}
		}(index, root)
	}
	close(start)

	stores := make([]*Store, storeCount)
	for range storeCount {
		result := <-results
		if result.err != nil {
			t.Errorf("Open(store %d): %v", result.index, result.err)
			continue
		}
		stores[result.index] = result.store
	}

	sessions := make(map[string]int, storeCount)
	for index, store := range stores {
		if store == nil {
			continue
		}
		if store.root != roots[index] {
			t.Errorf("store %d root = %q, want %q", index, store.root, roots[index])
		}
		if store.writerLock.owner.PID != os.Getpid() {
			t.Errorf("store %d owner PID = %d, want %d", index, store.writerLock.owner.PID, os.Getpid())
		}
		sessionID := store.writerLock.owner.SessionID
		if previous, exists := sessions[sessionID]; exists {
			t.Errorf("stores %d and %d share writer session %q", previous, index, sessionID)
		}
		sessions[sessionID] = index
		assertNoWriterHeartbeat(t, store)
		closeStore(t, store)
	}
}

func TestStoreAllowsReadOnlyProcessesWithWriter(t *testing.T) {
	t.Parallel()

	root := testTempDir(t)
	g := mustGeometry(t, geometry.Config{ChunkEdge: 1, LargeChunkEdge: 1, BlockBits: 2})
	coord := geometry.Coord{X: 2, Y: -3}
	chunk := mustChunk(t, g)
	if err := chunk.Set(geometry.Offset{}, 3); err != nil {
		t.Fatal(err)
	}
	writer := mustOpen(t, root, g)
	if err := writer.WriteChunk(coord, chunk); err != nil {
		t.Fatal(err)
	}

	firstReader := mustOpenWithOptions(t, root, g, Options{ReadOnly: true})
	secondReader := mustOpenWithOptions(t, root, g, Options{ReadOnly: true})
	for index, reader := range []*Store{firstReader, secondReader} {
		got, err := reader.ReadChunk(coord)
		if err != nil {
			t.Fatalf("reader %d ReadChunk(): %v", index, err)
		}
		value, err := got.Get(geometry.Offset{})
		if err != nil {
			t.Fatal(err)
		}
		if value != 3 {
			t.Fatalf("reader %d value = %d, want 3", index, value)
		}
		if err := reader.WriteChunk(coord, chunk); !errors.Is(err, ErrReadOnly) {
			t.Fatalf("reader %d WriteChunk() error = %v, want ErrReadOnly", index, err)
		}
	}

	updated := mustChunk(t, g)
	if err := updated.Set(geometry.Offset{}, 1); err != nil {
		t.Fatal(err)
	}
	if err := writer.WriteChunk(coord, updated); err != nil {
		t.Fatal(err)
	}
	got, err := firstReader.ReadChunk(coord)
	if err != nil {
		t.Fatal(err)
	}
	value, err := got.Get(geometry.Offset{})
	if err != nil {
		t.Fatal(err)
	}
	if value != 1 {
		t.Fatalf("read-only process retained stale chunk: got %d, want 1", value)
	}
}

func TestReadOnlyStoreDoesNotCreateDataDirectory(t *testing.T) {
	t.Parallel()

	root := filepath.Join(testTempDir(t), "missing")
	g := mustGeometry(t, geometry.Config{ChunkEdge: 1, LargeChunkEdge: 1, BlockBits: 1})
	if _, err := OpenWithOptions(root, g, Options{ReadOnly: true}); err == nil {
		t.Fatal("read-only OpenWithOptions() created a missing data directory")
	}
	if _, err := os.Stat(root); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing read-only data directory was changed: %v", err)
	}
}

func TestClosedStoreRejectsOperations(t *testing.T) {
	t.Parallel()

	g := mustGeometry(t, geometry.Config{ChunkEdge: 1, LargeChunkEdge: 1, BlockBits: 1})
	store := mustOpen(t, testTempDir(t), g)
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

// acquireWriterLockWithoutHeartbeat owns a data directory exactly like the
// production path, minus the background tick: acquireWriterLockWithConfig starts
// no goroutine when no heartbeat channel is supplied, so ownership metadata is
// written once at acquisition and never rewritten.
func acquireWriterLockWithoutHeartbeat(path string) (*writerLock, error) {
	return acquireWriterLockWithConfig(path, lockConfig{now: time.Now})
}

func openStoreWithoutWriterHeartbeat(root string, g geometry.Geometry, options Options) (*Store, error) {
	return openStore(root, g, options, acquireWriterLockWithoutHeartbeat)
}

func mustOpenWithoutWriterHeartbeat(t *testing.T, root string, g geometry.Geometry) *Store {
	t.Helper()

	store, err := openStoreWithoutWriterHeartbeat(root, g, Options{})
	if err != nil {
		t.Fatalf("open store without a writer heartbeat: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("Close(): %v", err)
		}
	})
	return store
}

func assertNoWriterHeartbeat(t *testing.T, store *Store) {
	t.Helper()

	select {
	case <-store.writerLock.done:
	default:
		t.Fatal("writer lock is running a background heartbeat")
	}
}

// mustOpenWithoutWriterHeartbeatAfterRelease keeps the bounded retry a released
// writer lock still needs on Windows, where the operating system can report the
// guard as locked for a moment after its handle is closed. Only ErrWriterLocked
// is retried, the deadline bounds the wait, and the last error is reported.
func mustOpenWithoutWriterHeartbeatAfterRelease(t *testing.T, root string, g geometry.Geometry) *Store {
	t.Helper()

	const (
		retryTimeout = 2 * time.Second
		retryDelay   = 10 * time.Millisecond
	)
	deadline := time.NewTimer(retryTimeout)
	defer deadline.Stop()
	retry := time.NewTicker(retryDelay)
	defer retry.Stop()

	var lastErr error
	for {
		store, err := openStoreWithoutWriterHeartbeat(root, g, Options{})
		if err == nil {
			t.Cleanup(func() {
				closeStore(t, store)
			})
			return store
		}
		if !errors.Is(err, ErrWriterLocked) {
			t.Fatalf("open after writer release: %v", err)
		}
		lastErr = err
		select {
		case <-retry.C:
		case <-deadline.C:
			t.Fatalf("open after writer release timed out; last error: %v", lastErr)
		}
	}
}

func mustChunk(t *testing.T, g geometry.Geometry) *storage.Chunk {
	t.Helper()
	chunk, err := storage.NewChunk(g)
	if err != nil {
		t.Fatalf("storage.NewChunk(): %v", err)
	}
	return chunk
}
