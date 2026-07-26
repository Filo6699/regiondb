package fs_split

import "fmt"

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
