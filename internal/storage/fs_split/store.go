package fs_split

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/Filo6699/regiondb/internal/geometry"
	"github.com/Filo6699/regiondb/internal/storage"
)

const (
	chunkTemporaryPrefix = ".regiondb-chunk-"
	// StartupScanEntryLimit bounds the entries processed by stale
	// temporary-file cleanup during one startup.
	StartupScanEntryLimit = 100_000
)

var ErrReadOnly = errors.New("store is read-only")

type Store struct {
	root                 string
	geometry             geometry.Geometry
	options              Options
	walHandles           *walHandlePool
	writerLock           *writerLock
	cache                *chunkCache
	walRecords           uint64
	walBytes             int64
	walUnsyncedUpdates   uint64
	walRecordBuffer      []byte
	versionClock         uint64
	pendingPublications  map[geometry.Coord]pendingPublication
	pendingSync          durabilitySyncSet
	snapshotGeneration   uint64
	durabilityPoisoned   error
	walForegroundFlushes atomic.Uint64
	walGroupFlushes      atomic.Uint64
	walCheckpointFlushes atomic.Uint64
	checkpointCount      atomic.Uint64
	startupScanCapped    bool
	closed               bool
	atomicWriteFailpoint func(atomicWriteBoundary) error
	walFailpoint         func(walBoundary) error
	intentFailpoint      func(intentBoundary) error
	walRollbackFailpoint func() error
	durabilityFailpoint  func(string, bool) error
	snapshotReadpoint    func(snapshotReadBoundary) error
	mu                   sync.RWMutex
}

type pendingPublication struct {
	payload  []byte
	presence []byte
	version  uint64
}

type atomicWriteBoundary string

const (
	atomicWriteTemporaryCreated    atomicWriteBoundary = "temporary-created"
	atomicWriteDataWritten         atomicWriteBoundary = "data-written"
	atomicWriteDataSynced          atomicWriteBoundary = "data-synced"
	atomicWriteTemporaryClosed     atomicWriteBoundary = "temporary-closed"
	atomicWriteDestinationReplaced atomicWriteBoundary = "destination-replaced"
	atomicWriteDirectorySynced     atomicWriteBoundary = "directory-synced"
)

func (s *Store) RuntimeStats() storage.RuntimeStats {
	stats := s.cache.runtimeStats()
	if s.options.ReadOnly {
		stats.ProcessLockMode = "none"
	} else {
		stats.ProcessLockMode = writerGuardMode()
	}
	stats.ChunkLockMode = "shared-rwmutex"
	// fs_split writes each chunk before admitting it to the cache, so it has
	// no dirty resident state to report.
	stats.WALForegroundFlushes = s.walForegroundFlushes.Load()
	stats.WALGroupFlushes = s.walGroupFlushes.Load()
	// Cache entries are write-through and carry no pending WAL batch, so
	// eviction never forces synchronization.
	stats.WALEvictionFlushes = 0
	stats.WALCheckpointFlushes = s.walCheckpointFlushes.Load()
	stats.WALFlushes = stats.WALForegroundFlushes +
		stats.WALGroupFlushes +
		stats.WALEvictionFlushes +
		stats.WALCheckpointFlushes
	if s.walHandles != nil {
		stats.OpenWALHandles = s.walHandles.stats().open
	}
	stats.Checkpoints = s.checkpointCount.Load()
	return stats
}

func (s *Store) StartupScanCapped() bool {
	return s.startupScanCapped
}

func Open(root string, g geometry.Geometry) (*Store, error) {
	return OpenWithOptions(root, g, Options{})
}

func OpenWithOptions(root string, g geometry.Geometry, options Options) (*Store, error) {
	return openStore(root, g, options, acquireWriterLock)
}

// openStore takes writer-lock acquisition as a parameter so a deterministic test
// can own a data directory without the background heartbeat rewriting ownership
// metadata underneath its assertions. Production callers pass acquireWriterLock.
func openStore(
	root string,
	g geometry.Geometry,
	options Options,
	acquireLock func(path string) (*writerLock, error),
) (_ *Store, returnErr error) {
	if root == "" {
		return nil, errors.New("data directory must not be empty")
	}
	validated, err := geometry.New(g.Config())
	if err != nil || validated != g {
		return nil, fmt.Errorf("open fs_split_v1: %w", geometry.ErrInvalid)
	}
	absoluteRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve data directory: %w", err)
	}
	options, err = options.validated()
	if err != nil {
		return nil, fmt.Errorf("open fs_split_v1: %w", err)
	}
	if options.ReadOnly {
		info, err := os.Stat(absoluteRoot)
		if err != nil {
			return nil, fmt.Errorf("open read-only data directory: %w", err)
		}
		if !info.IsDir() {
			return nil, errors.New("open read-only data directory: path is not a directory")
		}
		store := &Store{
			root:        absoluteRoot,
			geometry:    g,
			options:     options,
			cache:       newChunkCache(g, options.MaxLoadedChunks),
			pendingSync: newDurabilitySyncSet(),
		}
		store.cache.startMaintenance()
		return store, nil
	}
	if err := os.MkdirAll(absoluteRoot, 0o755); err != nil {
		return nil, fmt.Errorf("create data directory: %w", err)
	}
	lock, err := acquireLock(filepath.Join(absoluteRoot, lockName))
	if err != nil {
		return nil, fmt.Errorf("open fs_split_v1: %w", err)
	}
	defer func() {
		if returnErr != nil {
			if err := lock.release(); err != nil {
				returnErr = errors.Join(returnErr, fmt.Errorf("release writer lock: %w", err))
			}
		}
	}()

	scan, err := reclaimStaleChunkTemporaryFiles(absoluteRoot, StartupScanEntryLimit)
	if err != nil {
		return nil, fmt.Errorf("reclaim stale chunk temporary files: %w", err)
	}

	wal, err := openWAL(absoluteRoot, nil)
	if err != nil {
		return nil, fmt.Errorf("open WAL: %w", err)
	}
	defer func() {
		if returnErr != nil && wal != nil {
			if err := wal.Close(); err != nil {
				returnErr = errors.Join(returnErr, fmt.Errorf("close WAL: %w", err))
			}
		}
	}()

	store := &Store{
		root:     absoluteRoot,
		geometry: g,
		options:  options,
		walHandles: newWALHandlePool(
			filepath.Join(absoluteRoot, walName),
			options.MaxOpenWALHandles,
			wal,
		),
		writerLock:          lock,
		cache:               newChunkCache(g, options.MaxLoadedChunks),
		pendingPublications: make(map[geometry.Coord]pendingPublication),
		pendingSync:         newDurabilitySyncSet(),
		startupScanCapped:   scan.capped,
	}
	wal = nil
	if err := store.beginWriterSnapshot(); err != nil {
		if closeErr := store.walHandles.close(); closeErr != nil {
			return nil, errors.Join(err, closeErr)
		}
		return nil, fmt.Errorf("open snapshot generation: %w", err)
	}
	if err := store.loadVersionClock(); err != nil {
		if closeErr := store.walHandles.close(); closeErr != nil {
			return nil, errors.Join(err, closeErr)
		}
		return nil, fmt.Errorf("open version clock: %w", err)
	}
	if err := store.recoverConditionalIntent(); err != nil {
		if closeErr := store.walHandles.close(); closeErr != nil {
			return nil, errors.Join(err, closeErr)
		}
		return nil, fmt.Errorf("recover conditional intent: %w", err)
	}
	if err := store.recoverWAL(); err != nil {
		if closeErr := store.walHandles.close(); closeErr != nil {
			return nil, errors.Join(err, closeErr)
		}
		return nil, err
	}
	if err := store.finishWriterSnapshot(true); err != nil {
		if closeErr := store.walHandles.close(); closeErr != nil {
			return nil, errors.Join(err, closeErr)
		}
		return nil, fmt.Errorf("finish recovered snapshot generation: %w", err)
	}
	store.cache.startMaintenance()
	return store, nil
}

func openWAL(
	root string,
	failpoint func(atomicWriteBoundary) error,
) (*os.File, error) {
	path := filepath.Join(root, walName)
	wal, err := openWALHandle(path)
	if err == nil {
		return wal, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	// A synced WAL record is not recoverable after a crash if the WAL's first
	// directory entry was never committed. Use the same durable create boundary
	// as synchronized chunk replacement before opening the append handle.
	if err := writeAtomic(path, nil, true, failpoint); err != nil {
		return nil, fmt.Errorf("create durable WAL: %w", err)
	}
	return openWALHandle(path)
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}
	s.closed = true
	s.cache.close()
	var result error
	if s.walHandles != nil {
		// The pending tail has to be flushed before the handle is released:
		// os.File.Sync rejects a closed file, so no platform offers a
		// close-then-flush ordering.
		if s.options.Durability == DurabilityFsyncWAL && s.walUnsyncedUpdates != 0 {
			if err := s.syncWAL(walGroupFlush); err != nil {
				result = errors.Join(result, err)
			}
		}
		if err := s.walHandles.close(); err != nil {
			result = errors.Join(result, err)
		}
		s.walHandles = nil
	}
	if s.writerLock != nil {
		if err := s.writerLock.release(); err != nil {
			result = errors.Join(result, fmt.Errorf("release writer lock: %w", err))
		}
		s.writerLock = nil
	}
	return result
}

func (s *Store) WriteChunk(coord geometry.Coord, chunk *storage.Chunk) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if chunk == nil {
		return errors.New("write chunk: nil chunk")
	}
	if chunk.Geometry() != s.geometry {
		return ErrGeometryMismatch
	}
	if s.closed {
		return errors.New("write chunk: store is closed")
	}
	if s.options.ReadOnly {
		return fmt.Errorf("write chunk: %w", ErrReadOnly)
	}
	if err := s.writerLock.checkHealthy(); err != nil {
		return err
	}
	if err := s.checkDurabilityHealthy(); err != nil {
		return err
	}
	_ = s.retryPendingPublications()
	if err := s.beginWriteSnapshot(); err != nil {
		return err
	}
	if s.versionClock == math.MaxUint64 {
		s.finishRejectedSnapshot()
		return ErrVersionOverflow
	}
	version := s.versionClock + 1
	if err := s.persistVersionClock(version); err != nil {
		s.finishRejectedSnapshot()
		return fmt.Errorf("persist version clock: %w", err)
	}
	s.versionClock = version
	if err := s.commitOrdinaryChunk(coord, chunk, version); err != nil {
		s.finishRejectedSnapshot()
		return err
	}
	s.finishCommittedSnapshot()
	return nil
}

func (s *Store) commitOrdinaryChunk(coord geometry.Coord, chunk *storage.Chunk, version uint64) error {
	payload := chunk.Bytes()
	presence := chunk.PresenceBytes()
	record := s.appendWALRecord(s.walRecordBuffer[:0], coord, payload, presence)
	boundary, err := s.walSize()
	if err != nil {
		return err
	}
	if err := s.publishIntent(intentRollback, boundary); err != nil {
		if exists, _ := s.intentExists(); exists {
			if clearErr := s.clearIntent(); clearErr != nil {
				s.poisonDurability(fmt.Errorf("clean failed rollback intent publication: %w", clearErr))
				return errors.Join(err, s.checkDurabilityHealthy())
			}
		}
		return err
	}
	unsyncedBefore := s.walUnsyncedUpdates
	if err := s.appendWAL(record); err != nil {
		return s.rollbackRejectedWrite(boundary, unsyncedBefore, err)
	}
	if err := s.ensureWALCommit(false); err != nil {
		return s.rollbackRejectedWrite(boundary, unsyncedBefore, err)
	}
	commitErr := s.publishIntent(intentCommitted, boundary)
	if commitErr != nil {
		state, _, inspectErr := s.readIntent()
		if inspectErr != nil {
			s.poisonDurability(fmt.Errorf("inspect commit decision: %w", inspectErr))
			return errors.Join(commitErr, s.checkDurabilityHealthy())
		}
		if state != intentCommitted {
			return s.rollbackRejectedWrite(
				boundary,
				unsyncedBefore,
				commitErr,
			)
		}
		s.reportPostCommitFailure("committed_intent_sync_failed")
	}
	s.walRecordBuffer = record[:0]
	s.walRecords++
	s.walBytes += int64(len(record))
	s.pendingPublications[coord] = pendingPublication{
		payload: payload, presence: presence, version: version,
	}
	if err := s.cache.putState(coord, payload, presence); err != nil {
		s.poisonDurability(fmt.Errorf("cache committed chunk: %w", err))
		s.reportPostCommitFailure("committed_write_publication_failed")
	}
	if err := s.clearIntent(); err != nil {
		s.reportPostCommitFailure("committed_intent_cleanup_failed")
		if syncErr := s.syncIntentDirectory(); syncErr != nil {
			s.poisonDurability(fmt.Errorf("finish committed intent cleanup: %w", syncErr))
		}
	}
	_ = s.retryPendingPublications()
	return nil
}

func (s *Store) retryPendingPublications() error {
	syncCheckpoint := s.options.Durability == DurabilityFsyncCheckpoint
	for coord, pending := range s.pendingPublications {
		if err := s.persistChunkVersion(coord, pending.version, syncCheckpoint); err != nil {
			s.reportPostCommitFailure("committed_version_publication_failed")
			return fmt.Errorf("publish committed chunk version: %w", err)
		}
		if err := s.persistChunk(coord, pending.payload, pending.presence, syncCheckpoint); err != nil {
			s.reportPostCommitFailure("committed_chunk_publication_failed")
			return fmt.Errorf("publish committed chunk: %w", err)
		}
		delete(s.pendingPublications, coord)
	}
	if s.checkpointDue() {
		if err := s.checkpointWAL(); err != nil {
			s.reportPostCommitFailure("committed_checkpoint_failed")
			return fmt.Errorf("checkpoint committed WAL: %w", err)
		}
	}
	return nil
}

func (s *Store) ensureWALCommit(forceGrouped bool) error {
	switch s.options.Durability {
	case DurabilityFsyncCheckpoint:
		return s.syncWAL(walForegroundFlush)
	case DurabilityFsyncWAL:
		if forceGrouped && s.walUnsyncedUpdates != 0 {
			return s.syncWAL(walForegroundFlush)
		}
	}
	return nil
}

func (s *Store) reportPostCommitFailure(event string) {
	if s.options.PostCommitFailure == nil {
		return
	}
	// Observability is post-commit bookkeeping too. A broken callback must not
	// turn an already committed write into a panic observed as failure.
	defer func() {
		_ = recover()
	}()
	s.options.PostCommitFailure(event)
}

func (s *Store) poisonDurability(err error) {
	if s.durabilityPoisoned == nil {
		s.durabilityPoisoned = err
	}
}

func (s *Store) checkDurabilityHealthy() error {
	if s.durabilityPoisoned == nil {
		return nil
	}
	return fmt.Errorf("store is fail-closed after durability repair failure: %w", s.durabilityPoisoned)
}

func (s *Store) ReadChunk(coord geometry.Coord) (chunk *storage.Chunk, returnErr error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, errors.New("read chunk: store is closed")
	}
	if pending, found := s.pendingPublications[coord]; found {
		chunk, err := storage.ChunkFromState(s.geometry, pending.payload, pending.presence)
		if err != nil {
			return nil, fmt.Errorf("read pending committed chunk: %w", err)
		}
		return chunk, nil
	}
	if !s.options.ReadOnly {
		if chunk, found, err := s.cache.get(coord); found || err != nil {
			if err != nil {
				return nil, fmt.Errorf("read cached chunk: %w", err)
			}
			return chunk, nil
		}
	}
	generation, err := s.beginReadOnlySnapshot()
	if err != nil {
		return nil, err
	}
	chunk, err = s.readChunkFile(coord)
	if validationErr := s.finishReadOnlySnapshot(generation); validationErr != nil {
		return nil, validationErr
	}
	return chunk, err
}

type startupScanResult struct {
	entries int
	capped  bool
}

func reclaimStaleChunkTemporaryFiles(root string, limit int) (startupScanResult, error) {
	var result startupScanResult
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == root {
			return nil
		}
		if result.entries == limit {
			result.capped = true
			return filepath.SkipAll
		}
		result.entries++
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), chunkTemporaryPrefix) {
			return nil
		}
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove %q: %w", path, err)
		}
		return nil
	})
	return result, err
}

func writeAtomic(
	path string,
	data []byte,
	syncData bool,
	failpoint func(atomicWriteBoundary) error,
) (returnErr error) {
	temporary, err := createTemporaryFile(filepath.Dir(path), chunkTemporaryPrefix)
	if err != nil {
		return fmt.Errorf("create temporary file: %w", err)
	}
	temporaryPath := temporary.Name()
	closed := false
	renamed := false
	defer func() {
		if !closed {
			if err := temporary.Close(); err != nil && returnErr == nil {
				returnErr = fmt.Errorf("close temporary file: %w", err)
			}
		}
		if !renamed {
			if err := os.Remove(temporaryPath); err != nil && !errors.Is(err, os.ErrNotExist) && returnErr == nil {
				returnErr = fmt.Errorf("remove temporary file: %w", err)
			}
		}
	}()

	if err := temporary.Chmod(0o600); err != nil {
		return fmt.Errorf("set temporary file permissions: %w", err)
	}
	if err := runAtomicWriteFailpoint(failpoint, atomicWriteTemporaryCreated); err != nil {
		return err
	}
	if _, err := io.Copy(temporary, bytes.NewReader(data)); err != nil {
		return fmt.Errorf("write temporary file: %w", err)
	}
	if err := runAtomicWriteFailpoint(failpoint, atomicWriteDataWritten); err != nil {
		return err
	}
	if syncData {
		if err := syncFile(temporary); err != nil {
			return fmt.Errorf("sync temporary file: %w", err)
		}
		if err := runAtomicWriteFailpoint(failpoint, atomicWriteDataSynced); err != nil {
			return err
		}
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close temporary file: %w", err)
	}
	closed = true
	if err := runAtomicWriteFailpoint(failpoint, atomicWriteTemporaryClosed); err != nil {
		return err
	}
	if err := replaceFile(temporaryPath, path, syncData); err != nil {
		return fmt.Errorf("replace destination: %w", err)
	}
	renamed = true
	if err := runAtomicWriteFailpoint(failpoint, atomicWriteDestinationReplaced); err != nil {
		return err
	}
	if syncData {
		if err := commitDirectoryEntry(
			syncParentDirectory(filepath.Dir(path)),
			replaceCommitsDirectoryEntry,
		); err != nil {
			return err
		}
		if err := runAtomicWriteFailpoint(failpoint, atomicWriteDirectorySynced); err != nil {
			return err
		}
	}
	return nil
}

func runAtomicWriteFailpoint(
	failpoint func(atomicWriteBoundary) error,
	boundary atomicWriteBoundary,
) error {
	if failpoint == nil {
		return nil
	}
	if err := failpoint(boundary); err != nil {
		return fmt.Errorf("atomic write failpoint %q: %w", boundary, err)
	}
	return nil
}
