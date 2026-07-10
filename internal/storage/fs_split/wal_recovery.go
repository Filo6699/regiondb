package fs_split

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"

	"github.com/Filo6699/regiondb/internal/geometry"
)

func (s *Store) recoverWAL() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var records []walRecord
	completeBytes, recordCount, err := s.scanWAL(func(record walRecord) error {
		records = append(records, record)
		return nil
	})
	if err != nil {
		return fmt.Errorf("recover WAL: %w", err)
	}
	recoveryVersions, err := s.reserveRecoveryVersions(records)
	if err != nil {
		return fmt.Errorf("recover WAL versions: %w", err)
	}
	syncData := s.options.Durability != DurabilityRelaxed
	for coord, version := range recoveryVersions {
		if err := s.persistChunkVersion(coord, version, syncData); err != nil {
			return fmt.Errorf("recover WAL: publish recovered chunk version: %w", err)
		}
	}
	for _, record := range records {
		if err := s.persistChunk(record.coord, record.payload, record.presence, syncData); err != nil {
			return fmt.Errorf("recover WAL: replay WAL record: %w", err)
		}
		s.cache.remove(record.coord)
	}
	wal, err := s.walHandles.acquire(walAppendHandle)
	if err != nil {
		return fmt.Errorf("acquire WAL append handle after replay: %w", err)
	}
	info, err := wal.Stat()
	if err != nil {
		s.walHandles.release(walAppendHandle)
		return fmt.Errorf("stat WAL after replay: %w", err)
	}
	sourceBytes := info.Size()
	s.walHandles.release(walAppendHandle)
	if recordCount != 0 || completeBytes != sourceBytes {
		if err := s.replaceWALAfterReplay(syncData); err != nil {
			return fmt.Errorf("finish WAL recovery: %w", err)
		}
	}
	return nil
}

func (s *Store) reserveRecoveryVersions(records []walRecord) (map[geometry.Coord]uint64, error) {
	final := make(map[geometry.Coord]walRecord)
	for _, record := range records {
		final[record.coord] = record
	}
	changed := make([]geometry.Coord, 0, len(final))
	for coord, record := range final {
		encoded, err := os.ReadFile(s.chunkPath(coord))
		if errors.Is(err, os.ErrNotExist) {
			changed = append(changed, coord)
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("read published chunk: %w", err)
		}
		chunk, err := s.decode(coord, encoded)
		if err != nil {
			changed = append(changed, coord)
			continue
		}
		if !bytes.Equal(chunk.Bytes(), record.payload) ||
			!bytes.Equal(chunk.PresenceBytes(), record.presence) {
			changed = append(changed, coord)
			continue
		}
		version, err := s.readChunkVersion(coord)
		if err != nil {
			return nil, err
		}
		if version == 0 {
			changed = append(changed, coord)
		}
	}
	if uint64(len(changed)) > math.MaxUint64-s.versionClock {
		return nil, ErrVersionOverflow
	}
	if len(changed) == 0 {
		return nil, nil
	}
	slices.SortFunc(changed, func(left, right geometry.Coord) int {
		if left.X < right.X {
			return -1
		}
		if left.X > right.X {
			return 1
		}
		if left.Y < right.Y {
			return -1
		}
		if left.Y > right.Y {
			return 1
		}
		return 0
	})
	versions := make(map[geometry.Coord]uint64, len(changed))
	for _, coord := range changed {
		s.versionClock++
		versions[coord] = s.versionClock
	}
	if err := s.persistVersionClock(s.versionClock); err != nil {
		return nil, fmt.Errorf("persist recovery version clock: %w", err)
	}
	return versions, nil
}

// replaceWALAfterReplay commits compaction only after every complete record has
// been validated and loaded. Preparing the empty replacement cannot partially
// truncate the source WAL, so a failed recovery can retry from the same bytes.
func (s *Store) replaceWALAfterReplay(syncData bool) error {
	if err := s.walHandles.close(); err != nil {
		return fmt.Errorf("close WAL handles before replay compaction: %w", err)
	}

	path := filepath.Join(s.root, walName)
	if err := writeAtomic(path, nil, syncData, s.atomicWriteFailpoint); err != nil {
		return fmt.Errorf("replace replayed WAL: %w", err)
	}
	wal, err := openWALHandle(path)
	if err != nil {
		return fmt.Errorf("reopen compacted WAL: %w", err)
	}
	s.walHandles = newWALHandlePool(path, s.options.MaxOpenWALHandles, wal)
	s.walUnsyncedUpdates = 0
	if syncData {
		s.recordWALFlush(walCheckpointFlush)
	}
	return nil
}
