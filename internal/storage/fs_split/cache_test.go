package fs_split

import (
	"bytes"
	"container/list"
	"errors"
	"testing"

	"github.com/Filo6699/regiondb/internal/geometry"
	"github.com/Filo6699/regiondb/internal/storage"
)

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
	elements := make(map[*list.Element]struct{})
	coords := make([]geometry.Coord, 0, writes)
	for write := range writes {
		coord := geometry.Coord{X: int64(write) * 3, Y: int64(-write) * 7}
		coords = append(coords, coord)
		if err := cache.put(coord, cachePayload(t, g, write)); err != nil {
			t.Fatalf("put(%v): %v", coord, err)
		}

		residents := cache.loadedChunks()
		cache.mu.Lock()
		allocated := cache.recent.Len()
		free := len(cache.free)
		for _, element := range cache.entries {
			buffers[&element.Value.(*cacheEntry).payload[0]] = struct{}{}
			elements[element] = struct{}{}
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
	if len(elements) != max {
		t.Fatalf("distinct cache elements = %d, want %d", len(elements), max)
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

func TestChunkCacheEvictionHysteresisInvariants(t *testing.T) {
	t.Parallel()

	const high = 8
	g := mustGeometry(t, geometry.Config{ChunkEdge: 1, LargeChunkEdge: 1, BlockBits: 8})
	cache := newChunkCache(g, high)
	payload := cachePayload(t, g, 1)

	for write := range high + 1 {
		if err := cache.put(geometry.Coord{X: int64(write)}, payload); err != nil {
			t.Fatalf("put(%d): %v", write, err)
		}
	}

	stats := cache.runtimeStats()
	if cache.low >= cache.high {
		t.Fatalf("watermarks: low = %d, high = %d", cache.low, cache.high)
	}
	if got, want := cache.loadedChunks(), cache.low+1; got != want {
		t.Fatalf("loaded chunks after high-water eviction = %d, want %d", got, want)
	}
	if stats.Evictions != uint64(cache.high-cache.low) {
		t.Fatalf("evictions = %d, want high-low = %d", stats.Evictions, cache.high-cache.low)
	}
	if stats.EvictionRuns != 1 {
		t.Fatalf("eviction runs = %d, want 1", stats.EvictionRuns)
	}

	for write := high + 1; cache.loadedChunks() < cache.high; write++ {
		if err := cache.put(geometry.Coord{X: int64(write)}, payload); err != nil {
			t.Fatalf("refill put(%d): %v", write, err)
		}
	}
	refilled := cache.runtimeStats()
	if refilled.Evictions != stats.Evictions || refilled.EvictionRuns != stats.EvictionRuns {
		t.Fatalf("refill changed eviction counters: before=%+v after=%+v", stats, refilled)
	}
	if got := cache.loadedChunks(); got > cache.high {
		t.Fatalf("loaded chunks = %d, want at most high watermark %d", got, cache.high)
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
	return &element.Value.(*cacheEntry).payload[0]
}
