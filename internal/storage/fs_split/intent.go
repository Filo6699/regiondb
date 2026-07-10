package fs_split

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"

	"github.com/Filo6699/regiondb/internal/bitcodec"
)

const (
	intentDirectoryName  = ".regiondb.intents"
	intentFileName       = "wal.rollback"
	intentRollbackMagic  = "RGDBIRB1"
	intentCommittedMagic = "RGDBICM1"
	intentRecordBytes    = 8 + 8 + checksumSize
)

var ErrCorruptIntent = errors.New("corrupt conditional intent")

type intentState uint8

const (
	intentRollback intentState = iota
	intentCommitted
)

type intentBoundary string

const (
	intentBeforeRollbackPublish  intentBoundary = "before-rollback-publish"
	intentRollbackPublished      intentBoundary = "rollback-published"
	intentBeforeCommittedPublish intentBoundary = "before-committed-publish"
	intentCommittedPublished     intentBoundary = "committed-published"
	intentBeforeClear            intentBoundary = "before-clear"
	intentAfterClear             intentBoundary = "after-clear"
)

func encodeIntent(state intentState, walBoundary uint64) []byte {
	magic := intentRollbackMagic
	if state == intentCommitted {
		magic = intentCommittedMagic
	}
	encoded := make([]byte, 0, intentRecordBytes)
	encoded = append(encoded, magic...)
	encoded = bitcodec.AppendUint64(encoded, walBoundary)
	return bitcodec.AppendUint32(encoded, crc32.ChecksumIEEE(encoded))
}

func decodeIntent(encoded []byte) (intentState, uint64, error) {
	if len(encoded) != intentRecordBytes ||
		crc32.ChecksumIEEE(encoded[:intentRecordBytes-checksumSize]) !=
			mustUint32(encoded[intentRecordBytes-checksumSize:]) {
		return 0, 0, ErrCorruptIntent
	}
	var state intentState
	switch string(encoded[:8]) {
	case intentRollbackMagic:
		state = intentRollback
	case intentCommittedMagic:
		state = intentCommitted
	default:
		return 0, 0, ErrCorruptIntent
	}
	return state, mustUint64(encoded[8:16]), nil
}

func (s *Store) intentPath() string {
	return filepath.Join(s.root, intentDirectoryName, intentFileName)
}

func (s *Store) publishIntent(state intentState, walBoundary uint64) error {
	directory := filepath.Join(s.root, intentDirectoryName)
	created := false
	if _, err := os.Stat(directory); errors.Is(err, os.ErrNotExist) {
		created = true
	} else if err != nil {
		return fmt.Errorf("inspect intent directory: %w", err)
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return fmt.Errorf("create intent directory: %w", err)
	}
	if created {
		if err := commitDirectoryEntry(
			syncParentDirectory(s.root),
			replaceCommitsDirectoryEntry,
		); err != nil {
			return fmt.Errorf("sync intent directory creation: %w", err)
		}
	}
	before := intentBeforeRollbackPublish
	if state == intentCommitted {
		before = intentBeforeCommittedPublish
	}
	if err := s.runIntentFailpoint(before); err != nil {
		return err
	}
	if err := writeAtomic(s.intentPath(), encodeIntent(state, walBoundary), true, nil); err != nil {
		return fmt.Errorf("publish intent: %w", err)
	}
	boundary := intentRollbackPublished
	if state == intentCommitted {
		boundary = intentCommittedPublished
	}
	return s.runIntentFailpoint(boundary)
}

func (s *Store) clearIntent() error {
	if err := s.runIntentFailpoint(intentBeforeClear); err != nil {
		return err
	}
	if err := os.Remove(s.intentPath()); err != nil {
		return fmt.Errorf("remove intent: %w", err)
	}
	if err := s.runIntentFailpoint(intentAfterClear); err != nil {
		return err
	}
	if err := s.syncIntentDirectory(); err != nil {
		return fmt.Errorf("sync intent removal: %w", err)
	}
	return nil
}

func (s *Store) syncIntentDirectory() error {
	return commitDirectoryEntry(
		syncParentDirectory(filepath.Dir(s.intentPath())),
		replaceCommitsDirectoryEntry,
	)
}

func (s *Store) recoverConditionalIntent() error {
	entries, err := os.ReadDir(filepath.Join(s.root, intentDirectoryName))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect intent directory: %w", err)
	}
	for _, entry := range entries {
		if entry.Name() != intentFileName || entry.IsDir() {
			return fmt.Errorf("%w: unexpected artifact %q", ErrCorruptIntent, entry.Name())
		}
	}
	if len(entries) == 0 {
		return nil
	}
	state, boundary, err := s.readIntent()
	if err != nil {
		return err
	}
	if state == intentRollback {
		if err := s.rollbackWAL(boundary); err != nil {
			return fmt.Errorf("recover rollback intent: %w", err)
		}
	}
	if err := s.clearIntent(); err != nil {
		return fmt.Errorf("clear recovered intent: %w", err)
	}
	return nil
}

func (s *Store) readIntent() (intentState, uint64, error) {
	encoded, err := os.ReadFile(s.intentPath())
	if err != nil {
		return 0, 0, fmt.Errorf("read intent: %w", err)
	}
	return decodeIntent(encoded)
}

func (s *Store) walSize() (uint64, error) {
	wal, err := s.walHandles.acquire(walAppendHandle)
	if err != nil {
		return 0, fmt.Errorf("acquire WAL append handle: %w", err)
	}
	info, statErr := wal.Stat()
	s.walHandles.release(walAppendHandle)
	if statErr != nil {
		return 0, fmt.Errorf("stat WAL: %w", statErr)
	}
	return uint64(info.Size()), nil
}

func (s *Store) rollbackWAL(boundary uint64) error {
	if s.walRollbackFailpoint != nil {
		if err := s.walRollbackFailpoint(); err != nil {
			return fmt.Errorf("WAL rollback failpoint: %w", err)
		}
	}
	wal, err := s.walHandles.acquire(walAppendHandle)
	if err != nil {
		return fmt.Errorf("acquire WAL append handle: %w", err)
	}
	defer s.walHandles.release(walAppendHandle)
	info, err := wal.Stat()
	if err != nil {
		return fmt.Errorf("stat WAL for rollback: %w", err)
	}
	if uint64(info.Size()) < boundary {
		return fmt.Errorf("%w: WAL size %d is below rollback boundary %d", ErrCorruptIntent, info.Size(), boundary)
	}
	if err := wal.Truncate(int64(boundary)); err != nil {
		return fmt.Errorf("truncate WAL rollback tail: %w", err)
	}
	if _, err := wal.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("seek WAL after rollback: %w", err)
	}
	// Clearing a rollback intent is safe only after the rejected tail is
	// durably neutralized, even when successful writes use relaxed durability.
	if err := syncFile(wal); err != nil {
		return fmt.Errorf("sync WAL rollback: %w", err)
	}
	return nil
}

func (s *Store) runIntentFailpoint(boundary intentBoundary) error {
	if s.intentFailpoint == nil {
		return nil
	}
	if err := s.intentFailpoint(boundary); err != nil {
		return fmt.Errorf("intent failpoint %q: %w", boundary, err)
	}
	return nil
}
