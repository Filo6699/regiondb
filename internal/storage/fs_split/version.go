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
		current, err := s.readChunkVersion(mutation.Coord)
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
	for index, mutation := range mutations {
		version := s.versionClock + uint64(index) + 1
		if err := s.writeChunkLocked(mutation.Coord, mutation.Chunk, version); err != nil {
			return nil, err
		}
		versions[index] = version
	}
	if err := s.persistVersionClock(versions[len(versions)-1]); err != nil {
		return nil, fmt.Errorf("persist version clock: %w", err)
	}
	s.versionClock = versions[len(versions)-1]
	return versions, nil
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
	return writeAtomic(s.chunkVersionPath(coord), encoded, syncData, nil)
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
