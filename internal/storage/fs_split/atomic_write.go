package fs_split

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type atomicWriteBoundary string

const (
	atomicWriteTemporaryCreated    atomicWriteBoundary = "temporary-created"
	atomicWriteDataWritten         atomicWriteBoundary = "data-written"
	atomicWriteDataSynced          atomicWriteBoundary = "data-synced"
	atomicWriteTemporaryClosed     atomicWriteBoundary = "temporary-closed"
	atomicWriteDestinationReplaced atomicWriteBoundary = "destination-replaced"
	atomicWriteDirectorySynced     atomicWriteBoundary = "directory-synced"
)

func writeAtomic(
	path string,
	data []byte,
	syncData bool,
	failpoint func(atomicWriteBoundary) error,
) (returnErr error) {
	temporary, err := createTemporaryFile(filepath.Dir(path), chunkTemporaryPrefix)
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
		if err := syncFile(temporary); err != nil {
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
