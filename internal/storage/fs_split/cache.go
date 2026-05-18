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
	geometry     geometry.Geometry
	high         int
	low          int
	mu           sync.Mutex
	entries      map[geometry.Coord]*list.Element
	recent       *list.List
	free         []*list.Element
	loaded       atomic.Int64
	hits         atomic.Uint64
	misses       atomic.Uint64
	evictions    atomic.Uint64
	evictionRuns atomic.Uint64
}

func newChunkCache(g geometry.Geometry, max int) *chunkCache {
	// Keep the configured maximum as a hard high watermark. Reclaiming one
	// quarter of the residents gives subsequent admissions room before another
	// eviction run; very small caches still reclaim at least one entry.
	reclaim := max / 4
	if reclaim == 0 {
		reclaim = 1
	}
	return &chunkCache{
		geometry: g,
		high:     max,
		low:      max - reclaim,
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
	if len(cache.entries) == cache.high {
		if cache.high-cache.low == 1 {
			oldest := cache.recent.Back()
			entry := oldest.Value.(*cacheEntry)
			delete(cache.entries, entry.coord)
			entry.coord = coord
			copy(entry.payload, payload)
			cache.recent.MoveToFront(oldest)
			cache.entries[coord] = oldest
			cache.evictions.Add(1)
			cache.evictionRuns.Add(1)
			return nil
		}
		cache.evictToLow()
	}

	var element *list.Element
	if last := len(cache.free) - 1; last >= 0 {
		element = cache.free[last]
		cache.free = cache.free[:last]
		cache.recent.MoveToFront(element)
	} else {
		element = cache.recent.PushFront(&cacheEntry{
			payload: make([]byte, cache.geometry.PayloadBytes()),
		})
	}
	entry := element.Value.(*cacheEntry)
	entry.coord = coord
	copy(entry.payload, payload)
	cache.entries[coord] = element
	cache.loaded.Add(1)
	return nil
}

func (cache *chunkCache) evictToLow() {
	evicted := len(cache.entries) - cache.low
	oldest := cache.recent.Back()
	for range evicted {
		previous := oldest.Prev()
		entry := oldest.Value.(*cacheEntry)
		delete(cache.entries, entry.coord)
		cache.free = append(cache.free, oldest)
		oldest = previous
	}
	cache.loaded.Add(-int64(evicted))
	cache.evictions.Add(uint64(evicted))
	cache.evictionRuns.Add(1)
}

func (cache *chunkCache) remove(coord geometry.Coord) {
	cache.mu.Lock()
	defer cache.mu.Unlock()

	element, found := cache.entries[coord]
	if !found {
		return
	}
	delete(cache.entries, coord)
	cache.recent.MoveToBack(element)
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
