package fs_split

import (
	"container/list"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/Filo6699/regiondb/internal/geometry"
	"github.com/Filo6699/regiondb/internal/storage"
)

type cacheEntry struct {
	coord   geometry.Coord
	payload []byte
}

type chunkCache struct {
	geometry  geometry.Geometry
	max       int
	mu        sync.Mutex
	entries   map[geometry.Coord]*list.Element
	recent    *list.List
	loaded    atomic.Int64
	hits      atomic.Uint64
	misses    atomic.Uint64
	evictions atomic.Uint64
}

func newChunkCache(g geometry.Geometry, max int) *chunkCache {
	return &chunkCache{
		geometry: g,
		max:      max,
		entries:  make(map[geometry.Coord]*list.Element),
		recent:   list.New(),
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
	cache.recent.MoveToFront(element)
	entry := element.Value.(*cacheEntry)
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
		copy(element.Value.(*cacheEntry).payload, payload)
		cache.recent.MoveToFront(element)
		return nil
	}
	if cache.recent.Len() < cache.max {
		buffer := append([]byte(nil), payload...)
		cache.entries[coord] = cache.recent.PushFront(&cacheEntry{coord: coord, payload: buffer})
		cache.loaded.Add(1)
		return nil
	}

	// Sparse workloads evict on nearly every admission. Reusing both the
	// least-recently-used element and its payload keeps that path constant:
	// it examines one resident and allocates neither resource instead of
	// scanning the world.
	oldest := cache.recent.Back()
	entry := oldest.Value.(*cacheEntry)
	delete(cache.entries, entry.coord)
	entry.coord = coord
	copy(entry.payload, payload)
	cache.recent.MoveToFront(oldest)
	cache.entries[coord] = oldest
	cache.evictions.Add(1)
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
	cache.recent.Remove(element)
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
	}
}
