package fs_split

import (
	"container/list"
	"sync"

	"github.com/Filo6699/regiondb/internal/geometry"
	"github.com/Filo6699/regiondb/internal/storage"
)

type cacheEntry struct {
	coord geometry.Coord
	chunk *storage.Chunk
}

type chunkCache struct {
	geometry geometry.Geometry
	max      int
	mu       sync.Mutex
	entries  map[geometry.Coord]*list.Element
	recent   *list.List
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
		return nil, false, nil
	}
	cache.recent.MoveToFront(element)
	entry := element.Value.(cacheEntry)
	chunk, err := storage.ChunkFromBytes(cache.geometry, entry.chunk.Bytes())
	if err != nil {
		return nil, false, err
	}
	return chunk, true, nil
}

func (cache *chunkCache) put(coord geometry.Coord, payload []byte) error {
	chunk, err := storage.ChunkFromBytes(cache.geometry, payload)
	if err != nil {
		return err
	}

	cache.mu.Lock()
	defer cache.mu.Unlock()

	if element, found := cache.entries[coord]; found {
		element.Value = cacheEntry{coord: coord, chunk: chunk}
		cache.recent.MoveToFront(element)
		return nil
	}
	element := cache.recent.PushFront(cacheEntry{coord: coord, chunk: chunk})
	cache.entries[coord] = element
	if cache.recent.Len() > cache.max {
		oldest := cache.recent.Back()
		entry := oldest.Value.(cacheEntry)
		delete(cache.entries, entry.coord)
		cache.recent.Remove(oldest)
	}
	return nil
}
