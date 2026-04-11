package storage

// RuntimeStats is a bounded snapshot of storage activity since the store was
// opened. LoadedChunks and DirtyChunks are gauges; the other fields are
// monotonic counters.
type RuntimeStats struct {
	CacheHits    uint64
	CacheMisses  uint64
	LoadedChunks uint64
	DirtyChunks  uint64
	Evictions    uint64
	WALFlushes   uint64
	Checkpoints  uint64
}
