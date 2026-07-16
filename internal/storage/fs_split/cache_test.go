package fs_split

import (
	"bytes"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Filo6699/regiondb/internal/geometry"
	"github.com/Filo6699/regiondb/internal/storage"
)

func TestChunkCacheConcurrentEvictionKeepsRingConsistent(t *testing.T) {
	t.Parallel()

	const (
		max     = 8
		workers = 16
		writes  = 64
	)
	g := mustGeometry(t, geometry.Config{ChunkEdge: 1, LargeChunkEdge: 1, BlockBits: 8})
	cache := newChunkCache(g, max)
	payload := cachePayload(t, g, 1)
	start := make(chan struct{})
	var group sync.WaitGroup
	group.Add(workers)
	for worker := range workers {
		go func() {
			defer group.Done()
			<-start
			for write := range writes {
				coord := geometry.Coord{X: int64(worker*writes + write)}
				if err := cache.put(coord, payload); err != nil {
					t.Errorf("put(%v): %v", coord, err)
					return
				}
				if _, _, err := cache.get(coord); err != nil {
					t.Errorf("get(%v): %v", coord, err)
					return
				}
				if write%2 == 0 {
					cache.remove(coord)
				}
			}
		}()
	}
	close(start)
	group.Wait()

	cache.mu.Lock()
	defer cache.mu.Unlock()
	if residents := len(cache.entries); residents > max {
		t.Fatalf("resident entries = %d, want at most %d", residents, max)
	}
	if cache.recent.length != len(cache.entries) {
		t.Fatalf(
			"eviction ring length = %d, want resident entries %d",
			cache.recent.length,
			len(cache.entries),
		)
	}

	seen := make(map[*cacheEntry]struct{}, len(cache.entries))
	var previous *cacheEntry
	for entry := cache.recent.front; entry != nil; entry = entry.next {
		if entry.previous != previous {
			t.Fatalf("entry %v has previous %p, want %p", entry.coord, entry.previous, previous)
		}
		if _, duplicate := seen[entry]; duplicate {
			t.Fatalf("entry %v appears more than once in the eviction ring", entry.coord)
		}
		seen[entry] = struct{}{}
		if cached := cache.entries[entry.coord]; cached != entry {
			t.Fatalf("entry %v is not owned by the resident map", entry.coord)
		}
		previous = entry
	}
	if previous != cache.recent.back {
		t.Fatalf("eviction ring back = %p, want %p", cache.recent.back, previous)
	}
	if len(seen) != len(cache.entries) {
		t.Fatalf("reachable eviction entries = %d, want %d", len(seen), len(cache.entries))
	}
}

func TestChunkCacheSparseEvictionKeepsConstantFootprint(t *testing.T) {
	t.Parallel()

	const (
		max    = 4
		writes = 512
	)
	g := mustGeometry(t, geometry.Config{ChunkEdge: 2, LargeChunkEdge: 2, BlockBits: 8})
	cache := newChunkCache(g, max)

	// Coordinates never repeat and spread across many large chunks, so this is
	// the sparse working set that evicts on nearly every admission.
	buffers := make(map[*byte]struct{})
	entries := make(map[*cacheEntry]struct{})
	coords := make([]geometry.Coord, 0, writes)
	for write := range writes {
		coord := geometry.Coord{X: int64(write) * 3, Y: int64(-write) * 7}
		coords = append(coords, coord)
		if err := cache.put(coord, cachePayload(t, g, write)); err != nil {
			t.Fatalf("put(%v): %v", coord, err)
		}

		residents := cache.loadedChunks()
		cache.mu.Lock()
		allocated := cache.recent.length + len(cache.free)
		free := len(cache.free)
		for _, entry := range cache.entries {
			buffers[&entry.payload[0]] = struct{}{}
			entries[entry] = struct{}{}
		}
		cache.mu.Unlock()

		if residents > max || allocated != residents+free {
			t.Fatalf(
				"write %d: residents = %d, free = %d, allocated entries = %d, want at most %d residents and allocated = residents + free",
				write,
				residents,
				free,
				allocated,
				max,
			)
		}
	}

	// Every admission beyond the first max writes takes over both the element
	// and buffer it evicts, so a sparse sweep keeps a bounded resident set
	// instead of replacing either resource per eviction.
	if len(buffers) != max {
		t.Fatalf("distinct cached payload buffers = %d, want %d", len(buffers), max)
	}
	if len(entries) != max {
		t.Fatalf("distinct cache entries = %d, want %d", len(entries), max)
	}

	for index, coord := range coords {
		chunk, found, err := cache.get(coord)
		if err != nil {
			t.Fatalf("get(%v): %v", coord, err)
		}
		wantResident := index >= writes-max
		if found != wantResident {
			t.Fatalf("get(%v) found = %t, want %t", coord, found, wantResident)
		}
		if !found {
			continue
		}
		if want := cachePayload(t, g, index); !bytes.Equal(chunk.Bytes(), want) {
			t.Fatalf("get(%v) payload = %x, want %x", coord, chunk.Bytes(), want)
		}
	}
}

func TestChunkCacheEvictionWorkIsBoundedPerAdmission(t *testing.T) {
	t.Parallel()

	const (
		max       = 256
		evictions = 64
	)
	g := mustGeometry(t, geometry.Config{ChunkEdge: 1, LargeChunkEdge: 1, BlockBits: 8})
	cache := newChunkCache(g, max)
	payload := cachePayload(t, g, 1)

	for write := range max {
		if err := cache.put(geometry.Coord{X: int64(write)}, payload); err != nil {
			t.Fatalf("put(%d): %v", write, err)
		}
	}

	for write := range evictions {
		before := cache.runtimeStats()
		coord := geometry.Coord{X: int64(max + write)}
		if err := cache.put(coord, payload); err != nil {
			t.Fatalf("evicting put(%v): %v", coord, err)
		}
		after := cache.runtimeStats()
		if after.Evictions-before.Evictions != 1 ||
			after.EvictionRuns-before.EvictionRuns != 1 {
			t.Fatalf(
				"write %d eviction delta = (%d, %d), want (1, 1)",
				write,
				after.Evictions-before.Evictions,
				after.EvictionRuns-before.EvictionRuns,
			)
		}
	}
	if got := cache.loadedChunks(); got != max {
		t.Fatalf("loaded chunks = %d, want %d", got, max)
	}
}

func TestChunkCachePrunesStaleEvictionCandidates(t *testing.T) {
	t.Parallel()

	const high = 64
	g := mustGeometry(t, geometry.Config{ChunkEdge: 1, LargeChunkEdge: 1, BlockBits: 8})
	cache := newChunkCache(g, high)
	payload := cachePayload(t, g, 1)
	for write := range high {
		if err := cache.put(geometry.Coord{X: int64(write)}, payload); err != nil {
			t.Fatalf("fill put(%d): %v", write, err)
		}
	}

	for cycle := range 32 {
		for index := range high / 2 {
			cache.remove(geometry.Coord{X: int64(cycle*high + index)})
		}
		cache.mu.Lock()
		candidates := cache.recent.length
		residents := len(cache.entries)
		cache.mu.Unlock()
		if candidates != residents {
			t.Fatalf(
				"cycle %d: eviction candidates = %d, want resident count %d",
				cycle,
				candidates,
				residents,
			)
		}
		for index := range high / 2 {
			coord := geometry.Coord{X: int64((cycle+1)*high + index)}
			if err := cache.put(coord, payload); err != nil {
				t.Fatalf("cycle %d refill put(%v): %v", cycle, coord, err)
			}
		}
	}
}

func TestChunkCacheEvictsLeastRecentlyUsedEntry(t *testing.T) {
	t.Parallel()

	g := mustGeometry(t, geometry.Config{ChunkEdge: 2, LargeChunkEdge: 2, BlockBits: 8})
	cache := newChunkCache(g, 2)
	first := geometry.Coord{X: 1, Y: 1}
	second := geometry.Coord{X: -9, Y: 40}
	third := geometry.Coord{X: 512, Y: -512}
	for index, coord := range []geometry.Coord{first, second} {
		if err := cache.put(coord, cachePayload(t, g, index)); err != nil {
			t.Fatalf("put(%v): %v", coord, err)
		}
	}

	// Reading the older entry makes the newer one the eviction candidate.
	if _, found, err := cache.get(first); err != nil || !found {
		t.Fatalf("get(%v) = (found %t, %v)", first, found, err)
	}
	if err := cache.put(third, cachePayload(t, g, 2)); err != nil {
		t.Fatalf("put(%v): %v", third, err)
	}

	for coord, wantResident := range map[geometry.Coord]bool{first: true, second: false, third: true} {
		_, found, err := cache.get(coord)
		if err != nil {
			t.Fatalf("get(%v): %v", coord, err)
		}
		if found != wantResident {
			t.Fatalf("get(%v) found = %t, want %t", coord, found, wantResident)
		}
	}
}

func TestChunkCacheSecondChanceProtectsRecentlyUsedCandidate(t *testing.T) {
	t.Parallel()

	g := mustGeometry(t, geometry.Config{ChunkEdge: 1, LargeChunkEdge: 1, BlockBits: 8})
	cache := newChunkCache(g, 3)
	coords := []geometry.Coord{{X: 1}, {X: 2}, {X: 3}, {X: 4}}
	for index := range 2 {
		if err := cache.put(coords[index], cachePayload(t, g, index)); err != nil {
			t.Fatalf("put(%v): %v", coords[index], err)
		}
	}
	if _, found, err := cache.get(coords[0]); err != nil || !found {
		t.Fatalf("get(%v) = (found %t, %v)", coords[0], found, err)
	}
	if err := cache.put(coords[2], cachePayload(t, g, 2)); err != nil {
		t.Fatalf("put(%v): %v", coords[2], err)
	}
	if _, found, err := cache.get(coords[1]); err != nil || !found {
		t.Fatalf("get(%v) = (found %t, %v)", coords[1], found, err)
	}

	// The first coordinate is now the LRU candidate, but its reference bit
	// gives it one second chance. The next unreferenced candidate is evicted.
	if err := cache.put(coords[3], cachePayload(t, g, 3)); err != nil {
		t.Fatalf("put(%v): %v", coords[3], err)
	}
	for coord, wantResident := range map[geometry.Coord]bool{
		coords[0]: true,
		coords[1]: true,
		coords[2]: false,
		coords[3]: true,
	} {
		if _, found, err := cache.get(coord); err != nil || found != wantResident {
			t.Fatalf("get(%v) = (found %t, %v), want found %t", coord, found, err, wantResident)
		}
	}
}

func TestChunkCacheMaintenanceQueueFallsBackInlineAndDrains(t *testing.T) {
	t.Parallel()

	g := mustGeometry(t, geometry.Config{ChunkEdge: 1, LargeChunkEdge: 1, BlockBits: 8})
	cache := newChunkCache(g, 1)
	firstTask := make(chan struct{})
	releaseFirstTask := make(chan struct{})
	var calls atomic.Int64
	cache.maintain = func(geometry.Coord) error {
		if calls.Add(1) == 1 {
			close(firstTask)
			<-releaseFirstTask
		}
		return nil
	}
	cache.startMaintenance()

	if err := cache.put(geometry.Coord{}, cachePayload(t, g, 0)); err != nil {
		t.Fatal(err)
	}
	if err := cache.put(geometry.Coord{X: 1}, cachePayload(t, g, 1)); err != nil {
		t.Fatal(err)
	}
	<-firstTask
	for index := 2; index <= cacheMaintenanceQueueSize+2; index++ {
		if err := cache.put(
			geometry.Coord{X: int64(index)},
			cachePayload(t, g, index),
		); err != nil {
			t.Fatalf("put(%d): %v", index, err)
		}
	}
	if queued := len(cache.maintenance); queued != cacheMaintenanceQueueSize {
		t.Fatalf("queued maintenance tasks = %d, want %d", queued, cacheMaintenanceQueueSize)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("started maintenance calls = %d, want worker plus inline fallback", got)
	}

	closed := make(chan struct{})
	go func() {
		cache.close()
		close(closed)
	}()
	for {
		cache.mu.Lock()
		closing := cache.closed
		cache.mu.Unlock()
		if closing {
			break
		}
		runtime.Gosched()
	}
	select {
	case <-closed:
		t.Fatal("cache close returned before queued maintenance drained")
	default:
	}
	close(releaseFirstTask)
	<-closed

	const wantCalls = cacheMaintenanceQueueSize + 2
	if got := calls.Load(); got != wantCalls {
		t.Fatalf("maintenance calls after close = %d, want %d", got, wantCalls)
	}
}

func TestChunkCacheRecycledBufferDoesNotAliasReturnedChunks(t *testing.T) {
	t.Parallel()

	g := mustGeometry(t, geometry.Config{ChunkEdge: 2, LargeChunkEdge: 2, BlockBits: 8})
	cache := newChunkCache(g, 1)
	resident := geometry.Coord{X: 4, Y: -4}
	admitted := geometry.Coord{X: -4, Y: 4}
	residentPayload := cachePayload(t, g, 1)
	if err := cache.put(resident, residentPayload); err != nil {
		t.Fatalf("put(%v): %v", resident, err)
	}
	chunk, found, err := cache.get(resident)
	if err != nil || !found {
		t.Fatalf("get(%v) = (found %t, %v)", resident, found, err)
	}
	evictedBuffer := cacheBuffer(t, cache, resident)

	if err := cache.put(admitted, cachePayload(t, g, 2)); err != nil {
		t.Fatalf("put(%v): %v", admitted, err)
	}
	if reused := cacheBuffer(t, cache, admitted); reused != evictedBuffer {
		t.Fatal("admission allocated a new buffer instead of reusing the evicted one")
	}
	if got := chunk.Bytes(); !bytes.Equal(got, residentPayload) {
		t.Fatalf("chunk returned before eviction changed to %x, want %x", got, residentPayload)
	}
	got, found, err := cache.get(admitted)
	if err != nil || !found {
		t.Fatalf("get(%v) = (found %t, %v)", admitted, found, err)
	}
	if want := cachePayload(t, g, 2); !bytes.Equal(got.Bytes(), want) {
		t.Fatalf("get(%v) payload = %x, want %x", admitted, got.Bytes(), want)
	}
}

func TestChunkCacheUpdatesResidentEntryInPlace(t *testing.T) {
	t.Parallel()

	g := mustGeometry(t, geometry.Config{ChunkEdge: 2, LargeChunkEdge: 2, BlockBits: 8})
	cache := newChunkCache(g, 2)
	coord := geometry.Coord{X: 7, Y: 7}
	if err := cache.put(coord, cachePayload(t, g, 1)); err != nil {
		t.Fatalf("put(%v): %v", coord, err)
	}
	buffer := cacheBuffer(t, cache, coord)
	updated := cachePayload(t, g, 2)
	if err := cache.put(coord, updated); err != nil {
		t.Fatalf("put(%v) update: %v", coord, err)
	}
	if reused := cacheBuffer(t, cache, coord); reused != buffer {
		t.Fatal("updating a resident entry replaced its buffer")
	}
	chunk, found, err := cache.get(coord)
	if err != nil || !found {
		t.Fatalf("get(%v) = (found %t, %v)", coord, found, err)
	}
	if got := chunk.Bytes(); !bytes.Equal(got, updated) {
		t.Fatalf("updated payload = %x, want %x", got, updated)
	}
}

func TestChunkCacheRejectsMismatchedPayloadSize(t *testing.T) {
	t.Parallel()

	g := mustGeometry(t, geometry.Config{ChunkEdge: 2, LargeChunkEdge: 2, BlockBits: 8})
	cache := newChunkCache(g, 1)
	coord := geometry.Coord{}
	for _, size := range []int{0, g.PayloadBytes() - 1, g.PayloadBytes() + 1} {
		if err := cache.put(coord, make([]byte, size)); !errors.Is(err, storage.ErrPayloadSize) {
			t.Fatalf("put(payload of %d bytes) error = %v, want ErrPayloadSize", size, err)
		}
	}
	residents := cache.loadedChunks()
	if residents != 0 {
		t.Fatalf("rejected payloads left %d cached entries", residents)
	}
}

func TestChunkCacheLoadedObservationTracksResidents(t *testing.T) {
	t.Parallel()

	g := mustGeometry(t, geometry.Config{ChunkEdge: 1, LargeChunkEdge: 1, BlockBits: 1})
	cache := newChunkCache(g, 2)
	coords := []geometry.Coord{{X: 1}, {X: 2}, {X: 3}}
	for index, coord := range coords {
		if err := cache.put(coord, cachePayload(t, g, index)); err != nil {
			t.Fatalf("put(%v): %v", coord, err)
		}
		want := min(index+1, 2)
		if got := cache.loadedChunks(); got != want {
			t.Fatalf("loaded chunks after put(%v) = %d, want %d", coord, got, want)
		}
	}

	cache.remove(coords[0])
	if got := cache.loadedChunks(); got != 2 {
		t.Fatalf("loaded chunks after removing evicted entry = %d, want 2", got)
	}
	cache.remove(coords[1])
	if got := cache.loadedChunks(); got != 1 {
		t.Fatalf("loaded chunks after removing resident entry = %d, want 1", got)
	}
}

func cachePayload(t *testing.T, g geometry.Geometry, seed int) []byte {
	t.Helper()
	payload := make([]byte, g.PayloadBytes())
	if len(payload) == 0 {
		t.Fatal("geometry has an empty payload")
	}
	for index := range payload {
		payload[index] = byte(seed + index + 1)
	}
	return payload
}

func cacheBuffer(t *testing.T, cache *chunkCache, coord geometry.Coord) *byte {
	t.Helper()
	cache.mu.Lock()
	defer cache.mu.Unlock()
	element, found := cache.entries[coord]
	if !found {
		t.Fatalf("coordinate %v is not cached", coord)
	}
	return &element.payload[0]
}
