package fs_split

import (
	"container/list"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"
)

type walHandleKind uint8

const (
	walAppendHandle walHandleKind = iota
	walScanHandle
)

var (
	errWALHandlePoolClosed = errors.New("WAL handle pool is closed")
	errWALHandlePoolBusy   = errors.New("WAL handle pool capacity wait timed out")
)

const walHandleAcquireTimeout = time.Second

type walHandleEntry struct {
	kind   walHandleKind
	file   *os.File
	recent *list.Element
	users  int
}

type walHandlePoolStats struct {
	open      uint64
	opens     uint64
	evictions uint64
}

// walHandlePool keeps the append and scan streams bounded independently from
// the chunk cache. A checked-out handle cannot be evicted or closed.
type walHandlePool struct {
	path       string
	maxOpen    int
	openHandle func(string, walHandleKind) (*os.File, error)

	mu          sync.Mutex
	available   *sync.Cond
	handles     map[walHandleKind]*walHandleEntry
	recent      *list.List
	active      int
	opens       uint64
	evictions   uint64
	closed      bool
	closeErr    error
	waitTimeout time.Duration
}

func newWALHandlePool(path string, maxOpen int, appendHandle *os.File) *walHandlePool {
	pool := &walHandlePool{
		path:        path,
		maxOpen:     maxOpen,
		openHandle:  openPooledWALHandle,
		handles:     make(map[walHandleKind]*walHandleEntry),
		recent:      list.New(),
		waitTimeout: walHandleAcquireTimeout,
	}
	pool.available = sync.NewCond(&pool.mu)
	if appendHandle != nil {
		entry := &walHandleEntry{kind: walAppendHandle, file: appendHandle}
		entry.recent = pool.recent.PushFront(entry)
		pool.handles[entry.kind] = entry
		pool.opens = 1
	}
	return pool
}

func openPooledWALHandle(path string, kind walHandleKind) (*os.File, error) {
	switch kind {
	case walAppendHandle:
		return openWALHandle(path)
	case walScanHandle:
		return os.Open(path)
	default:
		return nil, fmt.Errorf("open WAL handle: unknown stream kind %d", kind)
	}
}

func (pool *walHandlePool) acquire(kind walHandleKind) (*os.File, error) {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	var deadline time.Time
	var wakeAtDeadline *time.Timer
	defer func() {
		if wakeAtDeadline != nil {
			wakeAtDeadline.Stop()
		}
	}()

	for {
		if pool.closed {
			return nil, errWALHandlePoolClosed
		}
		if entry, found := pool.handles[kind]; found {
			entry.users++
			pool.active++
			pool.recent.MoveToFront(entry.recent)
			return entry.file, nil
		}
		if len(pool.handles) < pool.maxOpen {
			file, err := pool.openHandle(pool.path, kind)
			if err != nil {
				return nil, err
			}
			entry := &walHandleEntry{kind: kind, file: file, users: 1}
			entry.recent = pool.recent.PushFront(entry)
			pool.handles[kind] = entry
			pool.active++
			pool.opens++
			return entry.file, nil
		}
		evicted, err := pool.evictLeastRecentIdle()
		if err != nil {
			return nil, err
		}
		if evicted {
			continue
		}
		if wakeAtDeadline == nil {
			deadline = time.Now().Add(pool.waitTimeout)
			wakeAtDeadline = time.AfterFunc(pool.waitTimeout, func() {
				pool.mu.Lock()
				pool.available.Broadcast()
				pool.mu.Unlock()
			})
		}
		if !time.Now().Before(deadline) {
			return nil, errWALHandlePoolBusy
		}
		pool.available.Wait()
	}
}

// evictLeastRecentIdle closes at most one handle. The caller holds pool.mu.
func (pool *walHandlePool) evictLeastRecentIdle() (bool, error) {
	for element := pool.recent.Back(); element != nil; element = element.Prev() {
		entry := element.Value.(*walHandleEntry)
		if entry.users != 0 {
			continue
		}
		pool.recent.Remove(element)
		delete(pool.handles, entry.kind)
		pool.evictions++
		if err := entry.file.Close(); err != nil {
			return true, fmt.Errorf("close evicted WAL handle: %w", err)
		}
		return true, nil
	}
	return false, nil
}

func (pool *walHandlePool) release(kind walHandleKind) {
	pool.mu.Lock()
	entry := pool.handles[kind]
	entry.users--
	pool.active--
	pool.available.Broadcast()
	pool.mu.Unlock()
}

func (pool *walHandlePool) stats() walHandlePoolStats {
	pool.mu.Lock()
	defer pool.mu.Unlock()
	return walHandlePoolStats{
		open:      uint64(len(pool.handles)),
		opens:     pool.opens,
		evictions: pool.evictions,
	}
}

func (pool *walHandlePool) close() error {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	if pool.closed {
		return pool.closeErr
	}
	pool.closed = true
	pool.available.Broadcast()
	for pool.active != 0 {
		pool.available.Wait()
	}
	for element := pool.recent.Front(); element != nil; element = element.Next() {
		entry := element.Value.(*walHandleEntry)
		if err := entry.file.Close(); err != nil {
			pool.closeErr = errors.Join(pool.closeErr, fmt.Errorf("close WAL handle: %w", err))
		}
	}
	pool.recent.Init()
	pool.handles = make(map[walHandleKind]*walHandleEntry)
	return pool.closeErr
}
