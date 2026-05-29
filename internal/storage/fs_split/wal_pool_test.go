package fs_split

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/Filo6699/regiondb/internal/geometry"
)

func TestWALHandlePoolCapsAndReusesHandles(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	appendHandle, err := openWAL(root, nil)
	if err != nil {
		t.Fatal(err)
	}
	pool := newWALHandlePool(filepath.Join(root, walName), 1, appendHandle)
	t.Cleanup(func() {
		if err := pool.close(); err != nil {
			t.Errorf("close pool: %v", err)
		}
	})

	first, err := pool.acquire(walAppendHandle)
	if err != nil {
		t.Fatal(err)
	}
	if first != appendHandle {
		t.Fatal("pool did not reuse the seeded append handle")
	}
	pool.release(walAppendHandle)

	second, err := pool.acquire(walAppendHandle)
	if err != nil {
		t.Fatal(err)
	}
	if second != appendHandle {
		t.Fatal("pool reopened a cached append handle")
	}
	pool.release(walAppendHandle)

	_, err = pool.acquire(walScanHandle)
	if err != nil {
		t.Fatal(err)
	}
	if stats := pool.stats(); stats.open != 1 || stats.opens != 2 || stats.evictions != 1 {
		t.Fatalf("stats after scan acquisition = %+v, want open=1 opens=2 evictions=1", stats)
	}
	if err := appendHandle.Sync(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("evicted append handle Sync() error = %v, want %v", err, os.ErrClosed)
	}
	pool.release(walScanHandle)

	_, err = pool.acquire(walAppendHandle)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.release(walAppendHandle)
	if stats := pool.stats(); stats.open != 1 || stats.opens != 3 || stats.evictions != 2 {
		t.Fatalf("stats after append reacquisition = %+v, want open=1 opens=3 evictions=2", stats)
	}
}

func TestStoreWALHandleLimitSurvivesCheckpointAndReopen(t *testing.T) {
	t.Parallel()

	root := testTempDir(t)
	g := mustGeometry(t, geometry.Config{ChunkEdge: 1, LargeChunkEdge: 1, BlockBits: 8})
	options := Options{
		Durability:        DurabilityFsyncCheckpoint,
		CheckpointRecords: 1,
		MaxOpenWALHandles: 1,
	}
	store := mustOpenWithOptions(t, root, g, options)
	coord := geometry.Coord{X: 4, Y: -7}
	chunk := mustChunk(t, g)
	if err := chunk.Set(geometry.Offset{}, 91); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteChunk(coord, chunk); err != nil {
		t.Fatalf("WriteChunk(): %v", err)
	}
	if got := store.RuntimeStats().OpenWALHandles; got != 1 {
		t.Fatalf("open WAL handles = %d, want 1", got)
	}
	if stats := store.walHandles.stats(); stats.evictions == 0 {
		t.Fatalf("pool stats after checkpoint = %+v, want an eviction", stats)
	}
	closeStore(t, store)

	reopened := mustOpenWithOptions(t, root, g, options)
	got, err := reopened.ReadChunk(coord)
	if err != nil {
		t.Fatalf("ReadChunk() after reopen: %v", err)
	}
	value, err := got.Get(geometry.Offset{})
	if err != nil {
		t.Fatal(err)
	}
	if value != 91 {
		t.Fatalf("reopened value = %d, want 91", value)
	}
	if handles := reopened.RuntimeStats().OpenWALHandles; handles > 1 {
		t.Fatalf("open WAL handles after reopen = %d, want at most 1", handles)
	}
}

func TestWALHandlePoolCloseWaitsForLeases(t *testing.T) {
	t.Parallel()

	appendHandle, err := openWAL(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	pool := newWALHandlePool(appendHandle.Name(), 1, appendHandle)
	_, err = pool.acquire(walAppendHandle)
	if err != nil {
		t.Fatal(err)
	}

	closed := make(chan error, 1)
	go func() {
		closed <- pool.close()
	}()
	for {
		pool.mu.Lock()
		closing := pool.closed
		pool.mu.Unlock()
		if closing {
			break
		}
		runtime.Gosched()
	}
	if _, err := pool.acquire(walScanHandle); !errors.Is(err, errWALHandlePoolClosed) {
		t.Fatalf("acquire during close error = %v, want %v", err, errWALHandlePoolClosed)
	}
	select {
	case err := <-closed:
		t.Fatalf("close returned with an active lease: %v", err)
	default:
	}

	pool.release(walAppendHandle)
	if err := <-closed; err != nil {
		t.Fatalf("close pool: %v", err)
	}
	if stats := pool.stats(); stats.open != 0 {
		t.Fatalf("open handles after close = %d, want 0", stats.open)
	}
}

func TestWALHandlePoolCapacityWaitIsBounded(t *testing.T) {
	t.Parallel()

	appendHandle, err := openWAL(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	pool := newWALHandlePool(appendHandle.Name(), 1, appendHandle)
	pool.waitTimeout = 50 * time.Millisecond
	t.Cleanup(func() {
		if err := pool.close(); err != nil {
			t.Errorf("close pool: %v", err)
		}
	})

	if _, err := pool.acquire(walAppendHandle); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.acquire(walScanHandle); !errors.Is(err, errWALHandlePoolBusy) {
		t.Fatalf("acquire at capacity error = %v, want %v", err, errWALHandlePoolBusy)
	}
	pool.release(walAppendHandle)

	if _, err := pool.acquire(walScanHandle); err != nil {
		t.Fatalf("acquire after release: %v", err)
	}
	pool.release(walScanHandle)
}

func TestWriteWALRecordPropagatesShortWrite(t *testing.T) {
	t.Parallel()

	err := writeWALRecord(shortWALWriter{}, []byte("record"))
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("writeWALRecord() error = %v, want %v", err, io.ErrShortWrite)
	}
}

func TestWALHandlePoolConcurrentAcquireAndEvict(t *testing.T) {
	t.Parallel()

	appendHandle, err := openWAL(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	pool := newWALHandlePool(appendHandle.Name(), 1, appendHandle)

	const workers = 16
	start := make(chan struct{})
	var wait sync.WaitGroup
	wait.Add(workers)
	for worker := range workers {
		go func() {
			defer wait.Done()
			<-start
			kind := walHandleKind(worker % 2)
			for range 50 {
				_, err := pool.acquire(kind)
				if err != nil {
					t.Errorf("acquire kind %d: %v", kind, err)
					return
				}
				if open := pool.stats().open; open > 1 {
					t.Errorf("open handles = %d, want at most 1", open)
				}
				pool.release(kind)
			}
		}()
	}
	close(start)
	wait.Wait()
	if err := pool.close(); err != nil {
		t.Fatalf("close pool: %v", err)
	}
}

type shortWALWriter struct{}

func (shortWALWriter) Write(data []byte) (int, error) {
	return len(data) - 1, nil
}
