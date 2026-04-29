package fs_split

import (
	"bytes"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/Filo6699/regiondb/internal/bitcodec"
	"github.com/Filo6699/regiondb/internal/geometry"
	"github.com/Filo6699/regiondb/internal/storage"
)

const (
	fileMagic            = "RGDBSPL1"
	headerBytes          = 44
	checksumSize         = 4
	chunkTemporaryPrefix = ".regiondb-chunk-"
)

var (
	ErrCorrupt          = errors.New("corrupt fs_split_v1 chunk")
	ErrGeometryMismatch = errors.New("chunk geometry does not match store")
	ErrReadOnly         = errors.New("store is read-only")
)

type Store struct {
	root                 string
	geometry             geometry.Geometry
	options              Options
	wal                  *os.File
	writerLock           *writerLock
	cache                *chunkCache
	walRecords           uint64
	walBytes             int64
	walUnsyncedUpdates   uint64
	walRecordBuffer      []byte
	walFlushCount        atomic.Uint64
	checkpointCount      atomic.Uint64
	closed               bool
	atomicWriteFailpoint func(atomicWriteBoundary) error
	mu                   sync.RWMutex
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
	stats.WALFlushes = s.walFlushCount.Load()
	stats.Checkpoints = s.checkpointCount.Load()
	return stats
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
		return &Store{
			root:     absoluteRoot,
			geometry: g,
			options:  options,
			cache:    newChunkCache(g, options.MaxLoadedChunks),
		}, nil
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

	if err := reclaimStaleChunkTemporaryFiles(absoluteRoot); err != nil {
		return nil, fmt.Errorf("reclaim stale chunk temporary files: %w", err)
	}

	wal, err := os.OpenFile(filepath.Join(absoluteRoot, walName), os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open WAL: %w", err)
	}
	defer func() {
		if returnErr != nil {
			if err := wal.Close(); err != nil {
				returnErr = errors.Join(returnErr, fmt.Errorf("close WAL: %w", err))
			}
		}
	}()

	store := &Store{
		root:       absoluteRoot,
		geometry:   g,
		options:    options,
		wal:        wal,
		writerLock: lock,
		cache:      newChunkCache(g, options.MaxLoadedChunks),
	}
	if err := store.recoverWAL(); err != nil {
		return nil, err
	}
	return store, nil
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}
	s.closed = true
	var result error
	if s.wal != nil {
		// The pending tail has to be flushed before the handle is released:
		// os.File.Sync rejects a closed file, so no platform offers a
		// close-then-flush ordering.
		if s.options.Durability == DurabilityFsyncWAL && s.walUnsyncedUpdates != 0 {
			if err := s.syncWAL(); err != nil {
				result = errors.Join(result, err)
			}
		}
		if err := s.wal.Close(); err != nil {
			result = errors.Join(result, fmt.Errorf("close WAL: %w", err))
		}
		s.wal = nil
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

	payload := chunk.Bytes()
	record := s.appendWALRecord(s.walRecordBuffer[:0], coord, payload)
	if err := s.appendWAL(record); err != nil {
		return err
	}
	s.walRecordBuffer = record[:0]
	syncCheckpoint := s.options.Durability == DurabilityFsyncCheckpoint
	if err := s.persistChunk(coord, payload, syncCheckpoint); err != nil {
		return fmt.Errorf("persist chunk: %w", err)
	}
	s.walRecords++
	s.walBytes += int64(len(record))
	if s.checkpointDue() {
		if err := s.checkpointWAL(); err != nil {
			return err
		}
	}
	if err := s.cache.put(coord, payload); err != nil {
		return fmt.Errorf("cache written chunk: %w", err)
	}
	return nil
}

func (s *Store) ReadChunk(coord geometry.Coord) (chunk *storage.Chunk, returnErr error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, errors.New("read chunk: store is closed")
	}
	if !s.options.ReadOnly {
		if chunk, found, err := s.cache.get(coord); found || err != nil {
			if err != nil {
				return nil, fmt.Errorf("read cached chunk: %w", err)
			}
			return chunk, nil
		}
	}
	path := s.chunkPath(coord)
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open chunk: %w", err)
	}
	defer func() {
		if err := file.Close(); err != nil && returnErr == nil {
			chunk = nil
			returnErr = fmt.Errorf("close chunk: %w", err)
		}
	}()

	expectedSize := int64(headerBytes) + int64(s.geometry.PayloadBytes()) + checksumSize
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat chunk: %w", err)
	}
	if info.Size() != expectedSize {
		return nil, fmt.Errorf("%w: file size is %d, want %d", ErrCorrupt, info.Size(), expectedSize)
	}

	encoded := make([]byte, int(expectedSize))
	if _, err := io.ReadFull(file, encoded); err != nil {
		return nil, fmt.Errorf("%w: read chunk: %v", ErrCorrupt, err)
	}
	payload, err := s.decode(coord, encoded)
	if err != nil {
		return nil, err
	}
	chunk, err = storage.ChunkFromBytes(s.geometry, payload)
	if err != nil {
		return nil, fmt.Errorf("%w: decode payload: %v", ErrCorrupt, err)
	}
	if !s.options.ReadOnly {
		if err := s.cache.put(coord, payload); err != nil {
			return nil, fmt.Errorf("cache read chunk: %w", err)
		}
	}
	return chunk, nil
}

func (s *Store) encode(coord geometry.Coord, payload []byte) []byte {
	config := s.geometry.Config()
	encoded := make([]byte, 0, headerBytes+len(payload)+checksumSize)
	encoded = append(encoded, fileMagic...)
	encoded = bitcodec.AppendUint32(encoded, config.ChunkEdge)
	encoded = bitcodec.AppendUint32(encoded, config.LargeChunkEdge)
	encoded = append(encoded, config.BlockBits, 0, 0, 0)
	encoded = bitcodec.AppendUint64(encoded, uint64(coord.X))
	encoded = bitcodec.AppendUint64(encoded, uint64(coord.Y))
	encoded = bitcodec.AppendUint64(encoded, uint64(len(payload)))
	encoded = append(encoded, payload...)
	return bitcodec.AppendUint32(encoded, crc32.ChecksumIEEE(encoded))
}

func (s *Store) decode(coord geometry.Coord, encoded []byte) ([]byte, error) {
	if len(encoded) < headerBytes+checksumSize {
		return nil, fmt.Errorf("%w: file is shorter than the header", ErrCorrupt)
	}
	if string(encoded[:len(fileMagic)]) != fileMagic {
		return nil, fmt.Errorf("%w: invalid magic", ErrCorrupt)
	}
	storedChecksum, err := bitcodec.Uint32(encoded[len(encoded)-checksumSize:])
	if err != nil {
		return nil, fmt.Errorf("%w: read checksum: %v", ErrCorrupt, err)
	}
	if actual := crc32.ChecksumIEEE(encoded[:len(encoded)-checksumSize]); actual != storedChecksum {
		return nil, fmt.Errorf("%w: checksum mismatch", ErrCorrupt)
	}

	chunkEdge, err := bitcodec.Uint32(encoded[8:12])
	if err != nil {
		return nil, fmt.Errorf("%w: read chunk edge: %v", ErrCorrupt, err)
	}
	largeChunkEdge, err := bitcodec.Uint32(encoded[12:16])
	if err != nil {
		return nil, fmt.Errorf("%w: read large-chunk edge: %v", ErrCorrupt, err)
	}
	if !bytes.Equal(encoded[17:20], []byte{0, 0, 0}) {
		return nil, fmt.Errorf("%w: reserved header bytes are nonzero", ErrCorrupt)
	}
	storedX, err := bitcodec.Uint64(encoded[20:28])
	if err != nil {
		return nil, fmt.Errorf("%w: read chunk x coordinate: %v", ErrCorrupt, err)
	}
	storedY, err := bitcodec.Uint64(encoded[28:36])
	if err != nil {
		return nil, fmt.Errorf("%w: read chunk y coordinate: %v", ErrCorrupt, err)
	}
	payloadSize, err := bitcodec.Uint64(encoded[36:44])
	if err != nil {
		return nil, fmt.Errorf("%w: read payload size: %v", ErrCorrupt, err)
	}

	config := s.geometry.Config()
	if chunkEdge != config.ChunkEdge ||
		largeChunkEdge != config.LargeChunkEdge ||
		encoded[16] != config.BlockBits {
		return nil, fmt.Errorf("%w: %w", ErrCorrupt, ErrGeometryMismatch)
	}
	if int64(storedX) != coord.X || int64(storedY) != coord.Y {
		return nil, fmt.Errorf("%w: chunk coordinate mismatch", ErrCorrupt)
	}
	if payloadSize != uint64(s.geometry.PayloadBytes()) {
		return nil, fmt.Errorf("%w: payload size is %d, want %d", ErrCorrupt, payloadSize, s.geometry.PayloadBytes())
	}

	return encoded[headerBytes : len(encoded)-checksumSize], nil
}

func (s *Store) chunkPath(coord geometry.Coord) string {
	large := s.geometry.ChunkToLargeChunk(coord).LargeChunk
	largeName := "l_" + signedName(large.X) + "_" + signedName(large.Y)
	chunkName := "c_" + signedName(coord.X) + "_" + signedName(coord.Y) + ".rdb"
	return filepath.Join(s.root, largeName, chunkName)
}

func signedName(value int64) string {
	if value < 0 {
		return "n" + strconv.FormatUint(uint64(-(value+1))+1, 10)
	}
	return "p" + strconv.FormatInt(value, 10)
}

func (s *Store) persistChunk(coord geometry.Coord, payload []byte, syncData bool) error {
	path := s.chunkPath(coord)
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return fmt.Errorf("create large-chunk directory: %w", err)
	}
	if err := writeAtomic(path, s.encode(coord, payload), syncData, s.atomicWriteFailpoint); err != nil {
		return err
	}
	return nil
}

func reclaimStaleChunkTemporaryFiles(root string) error {
	return filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), chunkTemporaryPrefix) {
			return nil
		}
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove %q: %w", path, err)
		}
		return nil
	})
}

func writeAtomic(
	path string,
	data []byte,
	syncData bool,
	failpoint func(atomicWriteBoundary) error,
) (returnErr error) {
	temporary, err := os.CreateTemp(filepath.Dir(path), chunkTemporaryPrefix+"*")
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
		if err := temporary.Sync(); err != nil {
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
