package fs_split

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
)

const maxPendingSyncPaths = 4096

type durabilitySyncSet struct {
	paths    map[string]struct{}
	overflow bool
}

func newDurabilitySyncSet() durabilitySyncSet {
	return durabilitySyncSet{paths: make(map[string]struct{})}
}

func (set *durabilitySyncSet) add(path string) {
	if set.overflow {
		return
	}
	if len(set.paths) == maxPendingSyncPaths {
		clear(set.paths)
		set.overflow = true
		return
	}
	set.paths[path] = struct{}{}
}

func (set *durabilitySyncSet) clear() {
	clear(set.paths)
	set.overflow = false
}

// FlushWAL is a global durability barrier. When it returns nil, every write
// acknowledged before the call is recoverable regardless of the configured
// steady-state durability mode.
func (s *Store) FlushWAL() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return errors.New("flush WAL: store is closed")
	}
	if s.options.ReadOnly {
		return fmt.Errorf("flush WAL: %w", ErrReadOnly)
	}
	if err := s.writerLock.checkHealthy(); err != nil {
		return err
	}
	if err := s.checkDurabilityHealthy(); err != nil {
		return err
	}
	if err := s.retryPendingPublications(); err != nil {
		return fmt.Errorf("flush WAL publications: %w", err)
	}
	if len(s.pendingPublications) != 0 {
		return errors.New("flush WAL: committed publications remain pending")
	}
	if err := s.syncWAL(walForegroundFlush); err != nil {
		return fmt.Errorf("flush WAL barrier: %w", err)
	}
	if err := s.syncPendingPublications(); err != nil {
		return fmt.Errorf("flush WAL data files: %w", err)
	}
	if err := s.finishWriterSnapshot(true); err != nil {
		return fmt.Errorf("flush WAL snapshot generation: %w", err)
	}
	s.pendingSync.clear()
	return nil
}

func (s *Store) syncPendingPublications() error {
	if s.pendingSync.overflow {
		return s.syncDataTree()
	}
	paths := make([]string, 0, len(s.pendingSync.paths))
	for path := range s.pendingSync.paths {
		paths = append(paths, path)
	}
	slices.Sort(paths)
	for _, path := range paths {
		if err := s.syncDurabilityPath(path, false); err != nil {
			return err
		}
	}

	directories := make(map[string]struct{}, len(paths)+1)
	directories[s.root] = struct{}{}
	for _, path := range paths {
		for directory := filepath.Dir(path); directory != s.root; directory = filepath.Dir(directory) {
			directories[directory] = struct{}{}
		}
	}
	paths = paths[:0]
	for directory := range directories {
		paths = append(paths, directory)
	}
	slices.SortFunc(paths, func(left, right string) int {
		leftDepth := pathDepth(left)
		rightDepth := pathDepth(right)
		if leftDepth != rightDepth {
			return rightDepth - leftDepth
		}
		if left < right {
			return -1
		}
		if left > right {
			return 1
		}
		return 0
	})
	for _, path := range paths {
		if err := s.syncDurabilityPath(path, true); err != nil {
			return err
		}
	}
	return nil
}

// syncDataTree deliberately uses two passes: no directory is committed until
// every regular file in the overflow fallback has completed its sync.
func (s *Store) syncDataTree() error {
	if err := filepath.WalkDir(s.root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.Type().IsRegular() {
			return s.syncDurabilityPath(path, false)
		}
		return nil
	}); err != nil {
		return fmt.Errorf("sync overflow files: %w", err)
	}
	if err := filepath.WalkDir(s.root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() && path != s.root {
			return s.syncDurabilityPath(path, true)
		}
		return nil
	}); err != nil {
		return fmt.Errorf("sync overflow directories: %w", err)
	}
	if err := s.syncDurabilityPath(s.root, true); err != nil {
		return fmt.Errorf("sync overflow root: %w", err)
	}
	return nil
}

func (s *Store) syncDurabilityPath(path string, directory bool) error {
	if s.durabilityFailpoint != nil {
		if err := s.durabilityFailpoint(path, directory); err != nil {
			return err
		}
	}
	if directory {
		return commitDirectoryEntry(
			syncParentDirectory(path),
			replaceCommitsDirectoryEntry,
		)
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("open %q for sync: %w", path, err)
	}
	syncErr := syncFile(file)
	closeErr := file.Close()
	if syncErr != nil {
		return fmt.Errorf("sync %q: %w", path, syncErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close synced file %q: %w", path, closeErr)
	}
	return nil
}

func pathDepth(path string) int {
	depth := 0
	for directory := filepath.Clean(path); ; directory = filepath.Dir(directory) {
		parent := filepath.Dir(directory)
		if parent == directory {
			return depth
		}
		depth++
	}
}
