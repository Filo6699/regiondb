package fs_split

import (
	"errors"
	"fmt"
	"hash/crc32"
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
	generation, err := s.beginReadOnlySnapshot()
	if err != nil {
		return 0, err
	}
	version, err := s.readChunkVersion(coord)
	if validationErr := s.finishReadOnlySnapshot(generation); validationErr != nil {
		return 0, validationErr
	}
	return version, err
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
	if err := writeAtomic(path, encoded, syncData, nil); err != nil {
		return err
	}
	if !syncData {
		s.pendingSync.add(path)
	}
	return nil
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
