package fs_split

import (
	"fmt"
	"io"
	"math"
)

func checkpointRecordTrigger(lowerBound uint64) uint64 {
	if lowerBound <= 1 {
		return lowerBound
	}
	extra := max(uint64(1), lowerBound/2)
	if lowerBound > math.MaxUint64-extra {
		return math.MaxUint64
	}
	return lowerBound + extra
}

func checkpointByteTrigger(lowerBound int64) int64 {
	if lowerBound <= 1 {
		return lowerBound
	}
	extra := max(int64(1), lowerBound/2)
	if lowerBound > math.MaxInt64-extra {
		return math.MaxInt64
	}
	return lowerBound + extra
}

func (s *Store) checkpointDue() bool {
	return s.walRecords >= checkpointRecordTrigger(s.options.CheckpointRecords) ||
		s.walBytes >= checkpointByteTrigger(s.options.CheckpointBytes)
}

func (s *Store) checkpointWAL() error {
	syncData := s.options.Durability != DurabilityRelaxed
	_, _, err := s.scanWAL(func(record walRecord) error {
		if err := s.persistCheckpointChunk(
			record.coord,
			record.payload,
			record.presence,
			syncData,
		); err != nil {
			return fmt.Errorf("checkpoint WAL record: %w", err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("checkpoint WAL: %w", err)
	}
	if err := s.clearWAL(syncData); err != nil {
		return fmt.Errorf("checkpoint WAL: %w", err)
	}
	s.walRecords = 0
	s.walBytes = 0
	s.checkpointCount.Add(1)
	return nil
}

func (s *Store) clearWAL(syncData bool) error {
	wal, err := s.walHandles.acquire(walAppendHandle)
	if err != nil {
		return fmt.Errorf("acquire WAL append handle: %w", err)
	}
	defer s.walHandles.release(walAppendHandle)

	if err := wal.Truncate(0); err != nil {
		return fmt.Errorf("truncate WAL: %w", err)
	}
	if err := s.runWALFailpoint(walCheckpointTruncated); err != nil {
		return err
	}
	if _, err := wal.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("rewind WAL: %w", err)
	}
	if syncData {
		if err := syncFile(wal); err != nil {
			return fmt.Errorf("sync truncated WAL: %w", err)
		}
		if err := s.runWALFailpoint(walCheckpointSynced); err != nil {
			return err
		}
		s.recordWALFlush(walCheckpointFlush)
	}
	s.walUnsyncedUpdates = 0
	return nil
}
