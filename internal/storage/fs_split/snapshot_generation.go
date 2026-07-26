package fs_split

import (
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"os"
	"path/filepath"

	"github.com/Filo6699/regiondb/internal/bitcodec"
)

const (
	snapshotName      = ".regiondb.snapshot"
	snapshotMagic     = "RGDBSNP1"
	snapshotFileBytes = 8 + 8 + checksumSize
)

var (
	ErrCorruptSnapshot  = errors.New("corrupt snapshot generation")
	ErrSnapshotOverflow = errors.New("snapshot generation exhausted")
	ErrSnapshotChanged  = errors.New("snapshot changed during read")
	ErrSnapshotUnstable = errors.New("snapshot publication is in progress")
)

type snapshotReadBoundary uint8

const (
	snapshotReadStarted snapshotReadBoundary = iota
	snapshotReadLoaded
)

func encodeSnapshotGeneration(generation uint64) []byte {
	encoded := make([]byte, 0, snapshotFileBytes)
	encoded = append(encoded, snapshotMagic...)
	encoded = bitcodec.AppendUint64(encoded, generation)
	return bitcodec.AppendUint32(encoded, crc32.ChecksumIEEE(encoded))
}

func decodeSnapshotGeneration(encoded []byte) (uint64, error) {
	if len(encoded) != snapshotFileBytes ||
		string(encoded[:8]) != snapshotMagic ||
		crc32.ChecksumIEEE(encoded[:snapshotFileBytes-checksumSize]) !=
			mustUint32(encoded[snapshotFileBytes-checksumSize:]) {
		return 0, ErrCorruptSnapshot
	}
	return mustUint64(encoded[8:16]), nil
}

func (s *Store) snapshotPath() string {
	return filepath.Join(s.root, snapshotName)
}

func (s *Store) readSnapshotGeneration() (uint64, error) {
	encoded, err := os.ReadFile(s.snapshotPath())
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read snapshot generation: %w", err)
	}
	generation, err := decodeSnapshotGeneration(encoded)
	if err != nil {
		return 0, err
	}
	return generation, nil
}

func (s *Store) persistSnapshotGeneration(generation uint64, syncData bool) error {
	if err := writeAtomic(
		s.snapshotPath(),
		encodeSnapshotGeneration(generation),
		syncData,
		nil,
	); err != nil {
		return fmt.Errorf("persist snapshot generation %d: %w", generation, err)
	}
	s.snapshotGeneration = generation
	return nil
}

func (s *Store) beginWriterSnapshot() error {
	generation, err := s.readSnapshotGeneration()
	if err != nil {
		return err
	}
	s.snapshotGeneration = generation
	if generation%2 == 1 {
		if generation == math.MaxUint64 {
			return ErrSnapshotOverflow
		}
		return nil
	}
	return s.beginWriteSnapshot()
}

func (s *Store) beginWriteSnapshot() error {
	if s.snapshotGeneration%2 == 1 {
		return nil
	}
	if s.snapshotGeneration > math.MaxUint64-2 {
		return ErrSnapshotOverflow
	}
	return s.persistSnapshotGeneration(s.snapshotGeneration+1, true)
}

func (s *Store) finishWriterSnapshot(syncData bool) error {
	if s.snapshotGeneration%2 == 0 {
		return s.persistSnapshotGeneration(s.snapshotGeneration, syncData)
	}
	if s.snapshotGeneration == math.MaxUint64 {
		return ErrSnapshotOverflow
	}
	return s.persistSnapshotGeneration(s.snapshotGeneration+1, syncData)
}

func (s *Store) finishRejectedSnapshot() {
	if s.durabilityPoisoned != nil || len(s.pendingPublications) != 0 {
		return
	}
	if err := s.finishWriterSnapshot(false); err != nil {
		s.poisonDurability(fmt.Errorf("finish rejected snapshot: %w", err))
	}
}

func (s *Store) finishCommittedSnapshot() {
	if len(s.pendingPublications) != 0 {
		return
	}
	if err := s.finishWriterSnapshot(false); err != nil {
		s.poisonDurability(fmt.Errorf("finish committed snapshot: %w", err))
		s.reportPostCommitFailure("committed_snapshot_publication_failed")
	}
}

func (s *Store) beginReadOnlySnapshot() (uint64, error) {
	if !s.options.ReadOnly {
		return 0, nil
	}
	generation, err := s.readSnapshotGeneration()
	if err != nil {
		return 0, err
	}
	if generation%2 == 1 {
		return 0, ErrSnapshotUnstable
	}
	if s.snapshotReadpoint != nil {
		if err := s.snapshotReadpoint(snapshotReadStarted); err != nil {
			return 0, err
		}
	}
	return generation, nil
}

func (s *Store) finishReadOnlySnapshot(expected uint64) error {
	if !s.options.ReadOnly {
		return nil
	}
	if s.snapshotReadpoint != nil {
		if err := s.snapshotReadpoint(snapshotReadLoaded); err != nil {
			return err
		}
	}
	generation, err := s.readSnapshotGeneration()
	if err != nil {
		return err
	}
	if generation%2 == 1 {
		return ErrSnapshotUnstable
	}
	if generation != expected {
		return ErrSnapshotChanged
	}
	return nil
}
