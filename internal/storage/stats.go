package storage

// RuntimeStats is a bounded snapshot of the store's active lock modes and
// activity since it was opened. LoadedChunks and DirtyChunks are gauges; the
// remaining numeric fields are monotonic counters.
type RuntimeStats struct {
	ProcessLockMode string
	ChunkLockMode   string
	CacheHits       uint64
	CacheMisses     uint64
	LoadedChunks    uint64
	DirtyChunks     uint64
	Evictions       uint64
	WALFlushes      uint64
	OpenWALHandles  uint64
	Checkpoints     uint64
}
