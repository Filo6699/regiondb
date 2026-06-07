package fs_split

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/Filo6699/regiondb/internal/geometry"
	"github.com/Filo6699/regiondb/internal/storage"
)

type cacheEntry struct {
	coord    geometry.Coord
	payload  []byte
	previous *cacheEntry
	next     *cacheEntry
}

type evictionRing struct {
	front  *cacheEntry
	back   *cacheEntry
	length int
}

func (ring *evictionRing) pushFront(entry *cacheEntry) {
	entry.previous = nil
	entry.next = ring.front
	if ring.front == nil {
		ring.back = entry
	} else {
		ring.front.previous = entry
	}
	ring.front = entry
	ring.length++
}

func (ring *evictionRing) moveToFront(entry *cacheEntry) {
	if ring.front == entry {
		return
	}
	ring.remove(entry)
	ring.pushFront(entry)
}

func (ring *evictionRing) remove(entry *cacheEntry) {
	if entry.previous == nil {
		ring.front = entry.next
	} else {
		entry.previous.next = entry.next
	}
	if entry.next == nil {
		ring.back = entry.previous
	} else {
		entry.next.previous = entry.previous
	}
	entry.previous = nil
	entry.next = nil
	ring.length--
}

type chunkCache struct {
	geometry     geometry.Geometry
	max          int
	mu           sync.Mutex
	entries      map[geometry.Coord]*cacheEntry
	recent       evictionRing
	free         []*cacheEntry
	loaded       atomic.Int64
	hits         atomic.Uint64
	misses       atomic.Uint64
	evictions    atomic.Uint64
	evictionRuns atomic.Uint64
}

func newChunkCache(g geometry.Geometry, max int) *chunkCache {
	return &chunkCache{
		geometry: g,
		max:      max,
		entries:  make(map[geometry.Coord]*cacheEntry),
	}
}

func (cache *chunkCache) get(coord geometry.Coord) (*storage.Chunk, bool, error) {
	cache.mu.Lock()
	defer cache.mu.Unlock()

	element, found := cache.entries[coord]
	if !found {
		cache.misses.Add(1)
		return nil, false, nil
	}
	cache.hits.Add(1)
	cache.recent.moveToFront(element)
	entry := element
	chunk, err := storage.ChunkFromBytes(cache.geometry, entry.payload)
	if err != nil {
		return nil, false, err
	}
	return chunk, true, nil
}

func (cache *chunkCache) put(coord geometry.Coord, payload []byte) error {
	if len(payload) != cache.geometry.PayloadBytes() {
		return fmt.Errorf(
			"%w: got %d bytes, want %d",
			storage.ErrPayloadSize,
			len(payload),
			cache.geometry.PayloadBytes(),
		)
	}

	cache.mu.Lock()
	defer cache.mu.Unlock()

	if element, found := cache.entries[coord]; found {
		// The buffer of a resident entry is owned by the cache alone, so an
		// update overwrites it in place instead of replacing the entry.
		copy(element.payload, payload)
		cache.recent.moveToFront(element)
		return nil
	}
	if len(cache.entries) == cache.max {
		oldest := cache.recent.back
		delete(cache.entries, oldest.coord)
		oldest.coord = coord
		copy(oldest.payload, payload)
		cache.recent.moveToFront(oldest)
		cache.entries[coord] = oldest
		cache.evictions.Add(1)
		cache.evictionRuns.Add(1)
		return nil
	}

	var element *cacheEntry
	if last := len(cache.free) - 1; last >= 0 {
		element = cache.free[last]
		cache.free = cache.free[:last]
		cache.recent.pushFront(element)
	} else {
		element = &cacheEntry{
			payload: make([]byte, cache.geometry.PayloadBytes()),
		}
		cache.recent.pushFront(element)
	}
	entry := element
	entry.coord = coord
	copy(entry.payload, payload)
	cache.entries[coord] = element
	cache.loaded.Add(1)
	return nil
}

func (cache *chunkCache) remove(coord geometry.Coord) {
	cache.mu.Lock()
	defer cache.mu.Unlock()

	element, found := cache.entries[coord]
	if !found {
		return
	}
	delete(cache.entries, coord)
	cache.recent.remove(element)
	cache.free = append(cache.free, element)
	cache.loaded.Add(-1)
}

func (cache *chunkCache) loadedChunks() int {
	return int(cache.loaded.Load())
}

func (cache *chunkCache) runtimeStats() storage.RuntimeStats {
	return storage.RuntimeStats{
		CacheHits:    cache.hits.Load(),
		CacheMisses:  cache.misses.Load(),
		LoadedChunks: uint64(cache.loaded.Load()),
		Evictions:    cache.evictions.Load(),
		EvictionRuns: cache.evictionRuns.Load(),
	}
}
