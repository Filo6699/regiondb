package fs_split

import (
	"errors"
	"fmt"
	"math"

	"github.com/Filo6699/regiondb/internal/geometry"
	"github.com/Filo6699/regiondb/internal/storage"
)

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
	_ = s.retryPendingPublications()
	if err := s.beginWriteSnapshot(); err != nil {
		return nil, err
	}

	seen := make(map[geometry.Coord]struct{}, len(mutations))
	for _, mutation := range mutations {
		if mutation.Chunk == nil {
			s.finishRejectedSnapshot()
			return nil, errors.New("conditional write chunks: nil chunk")
		}
		if mutation.Chunk.Geometry() != s.geometry {
			s.finishRejectedSnapshot()
			return nil, ErrGeometryMismatch
		}
		if _, duplicate := seen[mutation.Coord]; duplicate {
			s.finishRejectedSnapshot()
			return nil, errors.New("conditional write chunks: duplicate coordinate")
		}
		seen[mutation.Coord] = struct{}{}
		current, err := s.currentChunkVersion(mutation.Coord)
		if err != nil {
			s.finishRejectedSnapshot()
			return nil, err
		}
		if current > s.versionClock {
			s.finishRejectedSnapshot()
			return nil, fmt.Errorf("%w: chunk version exceeds clock", ErrCorruptVersion)
		}
		if current != mutation.ExpectedVersion {
			s.finishRejectedSnapshot()
			return nil, ErrVersionMismatch
		}
	}
	if uint64(len(mutations)) > math.MaxUint64-s.versionClock {
		s.finishRejectedSnapshot()
		return nil, ErrVersionOverflow
	}

	versions := make([]uint64, len(mutations))
	finalVersion := s.versionClock + uint64(len(mutations))
	if err := s.persistVersionClock(finalVersion); err != nil {
		s.finishRejectedSnapshot()
		return nil, fmt.Errorf("persist version clock: %w", err)
	}
	s.versionClock = finalVersion

	boundary, err := s.walSize()
	if err != nil {
		s.finishRejectedSnapshot()
		return nil, err
	}
	if err := s.publishIntent(intentRollback, boundary); err != nil {
		if exists, _ := s.intentExists(); exists {
			if clearErr := s.clearIntent(); clearErr != nil {
				s.poisonDurability(fmt.Errorf("clean failed rollback intent publication: %w", clearErr))
				s.finishRejectedSnapshot()
				return nil, errors.Join(err, s.checkDurabilityHealthy())
			}
		}
		s.finishRejectedSnapshot()
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
		result := s.rollbackRejectedWrite(boundary, unsyncedBefore, err)
		s.finishRejectedSnapshot()
		return nil, result
	}
	if err := s.ensureWALCommit(true); err != nil {
		result := s.rollbackRejectedWrite(boundary, unsyncedBefore, err)
		s.finishRejectedSnapshot()
		return nil, result
	}

	commitErr := s.publishIntent(intentCommitted, boundary)
	if commitErr != nil {
		state, _, inspectErr := s.readIntent()
		if inspectErr != nil {
			s.poisonDurability(fmt.Errorf("inspect conditional commit decision: %w", inspectErr))
			return nil, errors.Join(commitErr, s.checkDurabilityHealthy())
		}
		if state != intentCommitted {
			result := s.rollbackRejectedWrite(
				boundary,
				unsyncedBefore,
				commitErr,
			)
			s.finishRejectedSnapshot()
			return nil, result
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
	_ = s.retryPendingPublications()
	s.finishCommittedSnapshot()
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
