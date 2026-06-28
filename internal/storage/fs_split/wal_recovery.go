package fs_split

import (
	"fmt"
	"path/filepath"
)

func (s *Store) recoverWAL() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	completeBytes, recordCount, err := s.scanWAL(func(walRecord) error {
		return nil
	})
	if err != nil {
		return fmt.Errorf("recover WAL: %w", err)
	}
	syncData := s.options.Durability != DurabilityRelaxed
	if _, _, err := s.scanWAL(func(record walRecord) error {
		if err := s.persistChunk(record.coord, record.payload, record.presence, syncData); err != nil {
			return fmt.Errorf("replay WAL record: %w", err)
		}
		s.cache.remove(record.coord)
		return nil
	}); err != nil {
		return fmt.Errorf("recover WAL: %w", err)
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
