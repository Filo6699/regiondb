package fs_split

import (
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"os"
	"path/filepath"
	"strings"

	"github.com/Filo6699/regiondb/internal/bitcodec"
	"github.com/Filo6699/regiondb/internal/geometry"
	"github.com/Filo6699/regiondb/internal/storage"
)

const (
	versionClockName  = ".regiondb.version"
	versionClockMagic = "RGDBVER1"
	versionFileMagic  = "RGDBCVR1"
	versionFileSuffix = ".ver"
	versionFileBytes  = 8 + 8 + 8 + 8 + checksumSize
	versionClockBytes = 8 + 8 + checksumSize
)

var (
	ErrCorruptVersion  = errors.New("corrupt version metadata")
	ErrVersionMismatch = storage.ErrVersionMismatch
	ErrVersionOverflow = errors.New("version clock exhausted")
)

func (s *Store) ChunkVersion(coord geometry.Coord) (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return 0, errors.New("read chunk version: store is closed")
	}
	if pending, found := s.pendingPublications[coord]; found {
		return pending.version, nil
	}
	return s.readChunkVersion(coord)
}

func (s *Store) CompareAndSwapChunk(
	coord geometry.Coord,
	expected uint64,
	chunk *storage.Chunk,
) (uint64, error) {
	versions, err := s.ConditionalWriteChunks([]storage.ConditionalMutation{{
		Coord: coord, ExpectedVersion: expected, Chunk: chunk,
	}})
	if err != nil {
		return 0, err
	}
	return versions[0], nil
}

func (s *Store) ConditionalWriteChunks(mutations []storage.ConditionalMutation) ([]uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(mutations) == 0 {
		return nil, errors.New("conditional write chunks: empty batch")
	}
	if s.closed {
		return nil, errors.New("conditional write chunks: store is closed")
	}
	if s.options.ReadOnly {
		return nil, fmt.Errorf("conditional write chunks: %w", ErrReadOnly)
	}
	if err := s.writerLock.checkHealthy(); err != nil {
		return nil, err
	}
	if err := s.checkDurabilityHealthy(); err != nil {
		return nil, err
	}
	s.retryPendingPublications()

	seen := make(map[geometry.Coord]struct{}, len(mutations))
	for _, mutation := range mutations {
		if mutation.Chunk == nil {
			return nil, errors.New("conditional write chunks: nil chunk")
		}
		if mutation.Chunk.Geometry() != s.geometry {
			return nil, ErrGeometryMismatch
		}
		if _, duplicate := seen[mutation.Coord]; duplicate {
			return nil, errors.New("conditional write chunks: duplicate coordinate")
		}
		seen[mutation.Coord] = struct{}{}
		current, err := s.currentChunkVersion(mutation.Coord)
		if err != nil {
			return nil, err
		}
		if current > s.versionClock {
			return nil, fmt.Errorf("%w: chunk version exceeds clock", ErrCorruptVersion)
		}
		if current != mutation.ExpectedVersion {
			return nil, ErrVersionMismatch
		}
	}
	if uint64(len(mutations)) > math.MaxUint64-s.versionClock {
		return nil, ErrVersionOverflow
	}

	versions := make([]uint64, len(mutations))
	finalVersion := s.versionClock + uint64(len(mutations))
	if err := s.persistVersionClock(finalVersion); err != nil {
		return nil, fmt.Errorf("persist version clock: %w", err)
	}
	s.versionClock = finalVersion

	boundary, err := s.walSize()
	if err != nil {
		return nil, err
	}
	if err := s.publishIntent(intentRollback, boundary); err != nil {
		if _, statErr := os.Stat(s.intentPath()); statErr == nil {
			if clearErr := s.clearIntent(); clearErr != nil {
				s.poisonDurability(fmt.Errorf("clean failed rollback intent publication: %w", clearErr))
				return nil, errors.Join(err, s.checkDurabilityHealthy())
			}
		}
		return nil, err
	}

	records := make([]byte, 0)
	for index, mutation := range mutations {
		version := finalVersion - uint64(len(mutations)-index-1)
		versions[index] = version
		records = s.appendWALRecord(
			records,
			mutation.Coord,
			mutation.Chunk.Bytes(),
			mutation.Chunk.PresenceBytes(),
		)
	}

	unsyncedBefore := s.walUnsyncedUpdates
	if err := s.appendWAL(records); err != nil {
		return nil, s.rollbackRejectedWrite(boundary, unsyncedBefore, err)
	}
	if err := s.ensureWALCommit(true); err != nil {
		return nil, s.rollbackRejectedWrite(boundary, unsyncedBefore, err)
	}

	commitErr := s.publishIntent(intentCommitted, boundary)
	if commitErr != nil {
		state, _, inspectErr := s.readIntent()
		if inspectErr != nil {
			s.poisonDurability(fmt.Errorf("inspect conditional commit decision: %w", inspectErr))
			return nil, errors.Join(commitErr, s.checkDurabilityHealthy())
		}
		if state != intentCommitted {
			return nil, s.rollbackRejectedWrite(
				boundary,
				unsyncedBefore,
				commitErr,
			)
		}
		s.reportPostCommitFailure("committed_intent_sync_failed")
	}

	s.walRecords += uint64(len(mutations))
	s.walBytes += int64(len(records))
	for index, mutation := range mutations {
		payload := mutation.Chunk.Bytes()
		presence := mutation.Chunk.PresenceBytes()
		s.pendingPublications[mutation.Coord] = pendingPublication{
			payload: payload, presence: presence, version: versions[index],
		}
		if err := s.cache.putState(mutation.Coord, payload, presence); err != nil {
			s.poisonDurability(fmt.Errorf("cache committed conditional chunk: %w", err))
			s.reportPostCommitFailure("committed_write_publication_failed")
		}
	}
	if err := s.clearIntent(); err != nil {
		s.reportPostCommitFailure("committed_intent_cleanup_failed")
		if syncErr := s.syncIntentDirectory(); syncErr != nil {
			s.poisonDurability(fmt.Errorf("finish committed intent cleanup: %w", syncErr))
		}
	}
	s.retryPendingPublications()
	return versions, nil
}

func (s *Store) currentChunkVersion(coord geometry.Coord) (uint64, error) {
	if pending, found := s.pendingPublications[coord]; found {
		return pending.version, nil
	}
	return s.readChunkVersion(coord)
}

func (s *Store) rollbackRejectedWrite(
	boundary uint64,
	unsyncedBefore uint64,
	cause error,
) error {
	if err := s.rollbackWAL(boundary); err != nil {
		s.poisonDurability(fmt.Errorf("rollback rejected WAL append: %w", err))
		return errors.Join(cause, s.checkDurabilityHealthy())
	}
	s.walUnsyncedUpdates = unsyncedBefore
	if err := s.clearIntent(); err != nil {
		s.poisonDurability(fmt.Errorf("clear rejected write intent: %w", err))
		return errors.Join(cause, s.checkDurabilityHealthy())
	}
	return cause
}

func (s *Store) readChunkVersion(coord geometry.Coord) (uint64, error) {
	encoded, err := os.ReadFile(s.chunkVersionPath(coord))
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read chunk version: %w", err)
	}
	if len(encoded) != versionFileBytes || string(encoded[:8]) != versionFileMagic {
		return 0, fmt.Errorf("%w: invalid chunk version header", ErrCorruptVersion)
	}
	if crc32.ChecksumIEEE(encoded[:len(encoded)-checksumSize]) != mustUint32(encoded[len(encoded)-checksumSize:]) {
		return 0, fmt.Errorf("%w: chunk version checksum mismatch", ErrCorruptVersion)
	}
	x := mustUint64(encoded[8:16])
	y := mustUint64(encoded[16:24])
	if int64(x) != coord.X || int64(y) != coord.Y {
		return 0, fmt.Errorf("%w: chunk version coordinate mismatch", ErrCorruptVersion)
	}
	return mustUint64(encoded[24:32]), nil
}

func (s *Store) persistChunkVersion(coord geometry.Coord, version uint64, syncData bool) error {
	encoded := make([]byte, 0, versionFileBytes)
	encoded = append(encoded, versionFileMagic...)
	encoded = bitcodec.AppendUint64(encoded, uint64(coord.X))
	encoded = bitcodec.AppendUint64(encoded, uint64(coord.Y))
	encoded = bitcodec.AppendUint64(encoded, version)
	encoded = bitcodec.AppendUint32(encoded, crc32.ChecksumIEEE(encoded))
	path := s.chunkVersionPath(coord)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create chunk version directory: %w", err)
	}
	return writeAtomic(path, encoded, syncData, nil)
}

func (s *Store) loadVersionClock() error {
	encoded, err := os.ReadFile(filepath.Join(s.root, versionClockName))
	if errors.Is(err, os.ErrNotExist) {
		version, scanErr := s.highestPersistedChunkVersion()
		if scanErr != nil {
			return scanErr
		}
		if err := s.persistVersionClock(version); err != nil {
			return fmt.Errorf("migrate version clock: %w", err)
		}
		s.versionClock = version
		return nil
	}
	if err != nil {
		return fmt.Errorf("read version clock: %w", err)
	}
	if len(encoded) != versionClockBytes || string(encoded[:8]) != versionClockMagic {
		return fmt.Errorf("%w: invalid clock header", ErrCorruptVersion)
	}
	if crc32.ChecksumIEEE(encoded[:16]) != mustUint32(encoded[16:]) {
		return fmt.Errorf("%w: clock checksum mismatch", ErrCorruptVersion)
	}
	s.versionClock = mustUint64(encoded[8:16])
	return nil
}

func (s *Store) highestPersistedChunkVersion() (uint64, error) {
	var highest uint64
	err := filepath.WalkDir(s.root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() ||
			!strings.HasPrefix(entry.Name(), "c_") ||
			!strings.HasSuffix(entry.Name(), ".rdb"+versionFileSuffix) {
			return nil
		}
		encoded, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if len(encoded) != versionFileBytes ||
			string(encoded[:8]) != versionFileMagic ||
			crc32.ChecksumIEEE(encoded[:len(encoded)-checksumSize]) !=
				mustUint32(encoded[len(encoded)-checksumSize:]) {
			return fmt.Errorf("%w: invalid chunk version file %q", ErrCorruptVersion, path)
		}
		highest = max(highest, mustUint64(encoded[24:32]))
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("scan chunk versions: %w", err)
	}
	return highest, nil
}

func (s *Store) persistVersionClock(version uint64) error {
	encoded := append([]byte(versionClockMagic), make([]byte, 0, 12)...)
	encoded = bitcodec.AppendUint64(encoded, version)
	encoded = bitcodec.AppendUint32(encoded, crc32.ChecksumIEEE(encoded))
	return writeAtomic(filepath.Join(s.root, versionClockName), encoded, true, nil)
}

func (s *Store) chunkVersionPath(coord geometry.Coord) string {
	return s.chunkPath(coord) + versionFileSuffix
}

func mustUint64(encoded []byte) uint64 {
	value, _ := bitcodec.Uint64(encoded)
	return value
}

func mustUint32(encoded []byte) uint32 {
	value, _ := bitcodec.Uint32(encoded)
	return value
}
