package fs_split

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/Filo6699/regiondb/internal/geometry"
	"github.com/Filo6699/regiondb/internal/storage"
)

type cacheEntry struct {
	coord        geometry.Coord
	payload      []byte
	presence     []byte
	recentlyUsed bool
	previous     *cacheEntry
	next         *cacheEntry
}

type evictionTask struct {
	entry    *cacheEntry
	coord    geometry.Coord
	maintain func(geometry.Coord) error
	report   func(error)
}

type chunkCache struct {
	geometry     geometry.Geometry
	max          int
	mu           sync.Mutex
	entries      map[geometry.Coord]*cacheEntry
	recent       evictionRing
	free         []*cacheEntry
	maintenance  chan evictionTask
	worker       sync.WaitGroup
	started      bool
	closed       bool
	maintain     func(geometry.Coord) error
	report       func(error)
	loaded       atomic.Int64
	hits         atomic.Uint64
	misses       atomic.Uint64
	evictions    atomic.Uint64
	evictionRuns atomic.Uint64
}

func newChunkCache(g geometry.Geometry, max int) *chunkCache {
	return &chunkCache{
		geometry:    g,
		max:         max,
		entries:     make(map[geometry.Coord]*cacheEntry),
		maintenance: make(chan evictionTask, cacheMaintenanceQueueSize),
		report: func(err error) {
			slog.Warn("cache eviction maintenance failed",
				slog.String("component", "storage"),
				slog.Any("error", err),
			)
		},
	}
}

func (cache *chunkCache) startMaintenance() {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if cache.started || cache.closed {
		return
	}
	cache.started = true
	cache.worker.Add(1)
	go cache.runMaintenance()
}

func (cache *chunkCache) runMaintenance() {
	defer cache.worker.Done()
	for task := range cache.maintenance {
		cache.finishEviction(task)
	}
}

func (cache *chunkCache) close() {
	cache.mu.Lock()
	if cache.closed {
		cache.mu.Unlock()
		return
	}
	cache.closed = true
	if cache.started {
		close(cache.maintenance)
	}
	cache.mu.Unlock()
	cache.worker.Wait()
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
	element.recentlyUsed = true
	cache.recent.moveToFront(element)
	entry := element
	chunk, err := storage.ChunkFromState(cache.geometry, entry.payload, entry.presence)
	if err != nil {
		return nil, false, err
	}
	return chunk, true, nil
}

func (cache *chunkCache) put(coord geometry.Coord, payload []byte) error {
	presence := make([]byte, cache.geometry.PresenceBytes())
	for index := uint64(0); index < cache.geometry.BlockCount(); index++ {
		presence[index/8] |= byte(1 << (index % 8))
	}
	return cache.putState(coord, payload, presence)
}

func (cache *chunkCache) putState(coord geometry.Coord, payload, presence []byte) error {
	if len(payload) != cache.geometry.PayloadBytes() {
		return fmt.Errorf(
			"%w: got %d bytes, want %d",
			storage.ErrPayloadSize,
			len(payload),
			cache.geometry.PayloadBytes(),
		)
	}
	if len(presence) != cache.geometry.PresenceBytes() {
		return fmt.Errorf(
			"%w: got %d bytes, want %d",
			storage.ErrPresenceSize,
			len(presence),
			cache.geometry.PresenceBytes(),
		)
	}

	cache.mu.Lock()
	if cache.closed {
		cache.mu.Unlock()
		return errors.New("chunk cache is closed")
	}

	if element, found := cache.entries[coord]; found {
		// The buffer of a resident entry is owned by the cache alone, so an
		// update overwrites it in place instead of replacing the entry.
		copy(element.payload, payload)
		copy(element.presence, presence)
		element.recentlyUsed = true
		cache.recent.moveToFront(element)
		cache.mu.Unlock()
		return nil
	}
	var inline *evictionTask
	if len(cache.entries) == cache.max {
		candidate := cache.evictionCandidate()
		delete(cache.entries, candidate.coord)
		cache.recent.remove(candidate)
		task := cache.newEvictionTask(candidate)
		if cache.scheduleMaintenance(task) {
			candidate = cache.takeFreeEntry()
		} else {
			task.entry = nil
			inline = &task
		}
		cache.admit(candidate, coord, payload, presence)
		cache.evictions.Add(1)
		cache.evictionRuns.Add(1)
		cache.mu.Unlock()
		if inline != nil {
			cache.runInlineMaintenance(*inline)
		}
		return nil
	}

	cache.admit(cache.takeFreeEntry(), coord, payload, presence)
	cache.loaded.Add(1)
	cache.mu.Unlock()
	return nil
}

func (cache *chunkCache) remove(coord geometry.Coord) {
	cache.mu.Lock()

	element, found := cache.entries[coord]
	if !found {
		cache.mu.Unlock()
		return
	}
	delete(cache.entries, coord)
	cache.recent.remove(element)
	task := cache.newEvictionTask(element)
	inline := !cache.scheduleMaintenance(task)
	cache.loaded.Add(-1)
	cache.mu.Unlock()
	if inline {
		cache.runInlineMaintenance(task)
	}
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

func (cache *chunkCache) evictionCandidate() *cacheEntry {
	limit := cache.recent.length
	for range limit {
		candidate := cache.recent.back
		if !candidate.recentlyUsed {
			return candidate
		}
		candidate.recentlyUsed = false
		cache.recent.moveToFront(candidate)
	}
	// A full pass clears every second chance. Selecting the least-recent entry
	// after that pass guarantees progress even when the entire working set was
	// touched between admissions.
	return cache.recent.back
}

func (cache *chunkCache) takeFreeEntry() *cacheEntry {
	last := len(cache.free) - 1
	if last >= 0 {
		entry := cache.free[last]
		cache.free = cache.free[:last]
		return entry
	}
	return &cacheEntry{
		payload:  make([]byte, cache.geometry.PayloadBytes()),
		presence: make([]byte, cache.geometry.PresenceBytes()),
	}
}

func (cache *chunkCache) admit(
	entry *cacheEntry,
	coord geometry.Coord,
	payload []byte,
	presence []byte,
) {
	entry.coord = coord
	entry.recentlyUsed = false
	copy(entry.payload, payload)
	copy(entry.presence, presence)
	cache.recent.pushFront(entry)
	cache.entries[coord] = entry
}

func (cache *chunkCache) newEvictionTask(entry *cacheEntry) evictionTask {
	return evictionTask{
		entry:    entry,
		coord:    entry.coord,
		maintain: cache.maintain,
		report:   cache.report,
	}
}

func (cache *chunkCache) scheduleMaintenance(task evictionTask) bool {
	if !cache.started || cache.closed {
		return false
	}
	select {
	case cache.maintenance <- task:
		return true
	default:
		return false
	}
}

func (cache *chunkCache) runInlineMaintenance(task evictionTask) {
	err := runEvictionMaintenance(task)
	if task.entry != nil {
		cache.mu.Lock()
		cache.free = append(cache.free, task.entry)
		cache.mu.Unlock()
	}
	if err != nil && task.report != nil {
		task.report(err)
	}
}

func (cache *chunkCache) finishEviction(task evictionTask) {
	err := runEvictionMaintenance(task)
	cache.mu.Lock()
	cache.free = append(cache.free, task.entry)
	cache.mu.Unlock()
	if err != nil && task.report != nil {
		task.report(err)
	}
}

func runEvictionMaintenance(task evictionTask) error {
	if task.maintain == nil {
		return nil
	}
	if err := task.maintain(task.coord); err != nil {
		return fmt.Errorf("maintain evicted chunk (%d,%d): %w", task.coord.X, task.coord.Y, err)
	}
	return nil
}
