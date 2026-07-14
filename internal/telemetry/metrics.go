package telemetry

import (
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Filo6699/regiondb/internal/storage"
)

var commandDurationBuckets = [...]float64{
	0.0001,
	0.0005,
	0.001,
	0.005,
	0.01,
	0.05,
	0.1,
	0.5,
	1,
	5,
}

// Metrics stores only fixed-cardinality process metrics.
type Metrics struct {
	commands       atomic.Uint64
	commandErrors  atomic.Uint64
	authFailures   atomic.Uint64
	authBans       atomic.Uint64
	connections    atomic.Int64
	authSources    atomic.Int64
	commandSeconds histogram
}

type histogram struct {
	mu      sync.Mutex
	buckets [len(commandDurationBuckets)]uint64
	count   uint64
	sum     float64
}

func (m *Metrics) ObserveCommand(duration time.Duration, failed bool) {
	m.commands.Add(1)
	if failed {
		m.commandErrors.Add(1)
	}
	m.commandSeconds.observe(duration.Seconds())
}

func (m *Metrics) AuthFailure() {
	m.authFailures.Add(1)
}

func (m *Metrics) AuthBan() {
	m.authBans.Add(1)
}

func (m *Metrics) ConnectionOpened() {
	m.connections.Add(1)
}

func (m *Metrics) ConnectionClosed() {
	m.connections.Add(-1)
}

func (m *Metrics) SetAuthSources(count int) {
	m.authSources.Store(int64(count))
}

func (m *Metrics) AppendPrometheus(dst []byte, stats storage.RuntimeStats) []byte {
	dst = appendCounter(dst, "regiondb_commands_total", m.commands.Load())
	dst = appendCounter(dst, "regiondb_command_errors_total", m.commandErrors.Load())
	dst = m.commandSeconds.appendPrometheus(dst, "regiondb_command_duration_seconds")
	dst = appendCounter(dst, "regiondb_auth_failures_total", m.authFailures.Load())
	dst = appendCounter(dst, "regiondb_auth_bans_total", m.authBans.Load())
	dst = appendGauge(dst, "regiondb_connections", m.connections.Load())
	dst = appendGauge(dst, "regiondb_auth_sources", m.authSources.Load())
	dst = appendCounter(dst, "regiondb_cache_hits_total", stats.CacheHits)
	dst = appendCounter(dst, "regiondb_cache_misses_total", stats.CacheMisses)
	dst = appendUnsignedGauge(dst, "regiondb_loaded_chunks", stats.LoadedChunks)
	dst = appendUnsignedGauge(dst, "regiondb_dirty_chunks", stats.DirtyChunks)
	dst = appendCounter(dst, "regiondb_evictions_total", stats.Evictions)
	dst = appendCounter(dst, "regiondb_eviction_runs_total", stats.EvictionRuns)
	dst = appendCounter(dst, "regiondb_wal_flushes_total", stats.WALFlushes)
	dst = appendCounter(dst, "regiondb_wal_foreground_flushes_total", stats.WALForegroundFlushes)
	dst = appendCounter(dst, "regiondb_wal_group_flushes_total", stats.WALGroupFlushes)
	dst = appendCounter(dst, "regiondb_wal_eviction_flushes_total", stats.WALEvictionFlushes)
	dst = appendCounter(dst, "regiondb_wal_checkpoint_flushes_total", stats.WALCheckpointFlushes)
	dst = appendUnsignedGauge(dst, "regiondb_open_wal_handles", stats.OpenWALHandles)
	return appendCounter(dst, "regiondb_checkpoints_total", stats.Checkpoints)
}

func (h *histogram) observe(value float64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for index, upperBound := range commandDurationBuckets {
		if value <= upperBound {
			h.buckets[index]++
		}
	}
	h.count++
	h.sum += value
}

func (h *histogram) appendPrometheus(dst []byte, name string) []byte {
	h.mu.Lock()
	defer h.mu.Unlock()
	for index, upperBound := range commandDurationBuckets {
		dst = append(dst, name...)
		dst = append(dst, "_bucket{le=\""...)
		dst = strconv.AppendFloat(dst, upperBound, 'g', -1, 64)
		dst = append(dst, "\"} "...)
		dst = strconv.AppendUint(dst, h.buckets[index], 10)
		dst = append(dst, '\n')
	}
	dst = append(dst, name...)
	dst = append(dst, "_bucket{le=\"+Inf\"} "...)
	dst = strconv.AppendUint(dst, h.count, 10)
	dst = append(dst, '\n')
	dst = append(dst, name...)
	dst = append(dst, "_sum "...)
	dst = strconv.AppendFloat(dst, h.sum, 'g', -1, 64)
	dst = append(dst, '\n')
	dst = append(dst, name...)
	dst = append(dst, "_count "...)
	dst = strconv.AppendUint(dst, h.count, 10)
	return append(dst, '\n')
}

func appendCounter(dst []byte, name string, value uint64) []byte {
	dst = append(dst, name...)
	dst = append(dst, ' ')
	dst = strconv.AppendUint(dst, value, 10)
	return append(dst, '\n')
}

func appendGauge(dst []byte, name string, value int64) []byte {
	dst = append(dst, name...)
	dst = append(dst, ' ')
	dst = strconv.AppendInt(dst, value, 10)
	return append(dst, '\n')
}

func appendUnsignedGauge(dst []byte, name string, value uint64) []byte {
	dst = append(dst, name...)
	dst = append(dst, ' ')
	dst = strconv.AppendUint(dst, value, 10)
	return append(dst, '\n')
}
