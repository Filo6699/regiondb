package fs_split

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

func ensureLockDirectory(path string) error {
	for {
		info, err := os.Lstat(path)
		switch {
		case errors.Is(err, os.ErrNotExist):
			if err := os.Mkdir(path, 0o700); err == nil {
				return nil
			} else if errors.Is(err, os.ErrExist) {
				continue
			} else {
				return fmt.Errorf("create writer lock directory: %w", err)
			}
		case err != nil:
			return fmt.Errorf("inspect writer lock directory: %w", err)
		case info.IsDir() && info.Mode()&os.ModeSymlink == 0:
			return nil
		case info.Mode().IsRegular():
			retry, err := migrateLegacyLockFile(path)
			if err != nil {
				return err
			}
			if retry {
				continue
			}
			return nil
		default:
			return fmt.Errorf("writer lock path is not a directory or regular file")
		}
	}
}

func migrateLegacyLockFile(path string) (retry bool, returnErr error) {
	guard, err := openExistingWriterGuard(path)
	if errors.Is(err, os.ErrNotExist) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("lock legacy writer file: %w", err)
	}
	defer func() {
		returnErr = errors.Join(returnErr, closeWriterGuard(guard))
	}()

	guardInfo, err := guard.Stat()
	if err != nil {
		return false, fmt.Errorf("inspect locked legacy writer file: %w", err)
	}
	pathInfo, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("reinspect legacy writer file: %w", err)
	}
	if pathInfo.Mode()&os.ModeSymlink != 0 || !pathInfo.Mode().IsRegular() {
		return true, nil
	}
	if !os.SameFile(guardInfo, pathInfo) {
		return true, nil
	}

	legacyPath, err := unusedLegacyLockPath(path)
	if err != nil {
		return false, err
	}
	if err := replaceFile(path, legacyPath, true); err != nil {
		return false, fmt.Errorf("move legacy writer lock file: %w", err)
	}
	if err := commitDirectoryEntry(
		syncParentDirectory(filepath.Dir(path)),
		replaceCommitsDirectoryEntry,
	); err != nil {
		return false, fmt.Errorf("commit legacy writer lock migration: %w", err)
	}

	if err := os.Mkdir(path, 0o700); errors.Is(err, os.ErrExist) {
		return true, nil
	} else if err != nil {
		return false, fmt.Errorf("create writer lock directory after legacy migration: %w", err)
	}
	if !replaceCommitsDirectoryEntry {
		if err := syncParentDirectory(filepath.Dir(path)); err != nil {
			return false, fmt.Errorf("commit writer lock directory creation: %w", err)
		}
	}
	return false, nil
}

func unusedLegacyLockPath(path string) (string, error) {
	for {
		suffix, err := newSessionID()
		if err != nil {
			return "", err
		}
		candidate := path + legacyLockMarker + suffix
		if _, err := os.Lstat(candidate); errors.Is(err, os.ErrNotExist) {
			return candidate, nil
		} else if err != nil {
			return "", fmt.Errorf("inspect legacy writer lock destination: %w", err)
		}
	}
}
