package fs_split

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/Filo6699/regiondb/internal/geometry"
	"github.com/Filo6699/regiondb/internal/storage"
)

func TestAuditBatchWALReplayKeepsPayloadAndPresenceRecordsSeparate(t *testing.T) {
	root := testTempDir(t)
	g := mustGeometry(t, geometry.Config{ChunkEdge: 2, LargeChunkEdge: 2, BlockBits: 3})
	store := mustOpenWithOptions(t, root, g, Options{
		CheckpointRecords: 100,
		CheckpointBytes:   1 << 20,
	})

	firstCoord := geometry.Coord{X: -7, Y: 9}
	secondCoord := geometry.Coord{X: 10, Y: -12}
	first := mustChunk(t, g)
	if err := first.Set(geometry.Offset{}, 0); err != nil {
		t.Fatal(err)
	}
	if err := first.Set(geometry.Offset{X: 1}, 5); err != nil {
		t.Fatal(err)
	}
	second := mustChunk(t, g)
	if err := second.Set(geometry.Offset{Y: 1}, 6); err != nil {
		t.Fatal(err)
	}

	versions, err := store.ConditionalWriteChunks([]storage.ConditionalMutation{
		{Coord: firstCoord, ExpectedVersion: 0, Chunk: first},
		{Coord: secondCoord, ExpectedVersion: 0, Chunk: second},
	})
	if err != nil {
		t.Fatalf("ConditionalWriteChunks(): %v", err)
	}
	if len(versions) != 2 || versions[0] == 0 || versions[1] != versions[0]+1 {
		t.Fatalf("conditional versions = %v, want two consecutive nonzero versions", versions)
	}
	closeStore(t, store)

	reopened := mustOpenWithOptions(t, root, g, Options{
		CheckpointRecords: 100,
		CheckpointBytes:   1 << 20,
	})
	defer closeStore(t, reopened)
	assertChunkState(t, reopened, firstCoord, first)
	assertChunkState(t, reopened, secondCoord, second)
	for index, coord := range []geometry.Coord{firstCoord, secondCoord} {
		if got, err := reopened.ChunkVersion(coord); err != nil || got != versions[index] {
			t.Fatalf("ChunkVersion(%v) = %d, %v; want %d, nil", coord, got, err, versions[index])
		}
	}
}

func TestAuditVersionClockPublicationFailureHasNoWriteSideEffects(t *testing.T) {
	root := testTempDir(t)
	g := testGeometry(t)
	store := mustOpenWithOptions(t, root, g, Options{
		CheckpointRecords: 100,
		CheckpointBytes:   1 << 20,
	})
	defer closeStore(t, store)

	clockPath := filepath.Join(root, versionClockName)
	clock, err := os.ReadFile(clockPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(clockPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(clockPath, 0o700); err != nil {
		t.Fatal(err)
	}
	walBefore, err := os.Stat(filepath.Join(root, walName))
	if err != nil {
		t.Fatal(err)
	}
	statsBefore := store.RuntimeStats()
	coord := geometry.Coord{X: 4, Y: -5}
	if err := store.WriteChunk(coord, testChunk(t, g, 7)); err == nil {
		t.Fatal("WriteChunk() succeeded while the version clock path was unavailable")
	}
	walAfter, err := os.Stat(filepath.Join(root, walName))
	if err != nil {
		t.Fatal(err)
	}
	if walAfter.Size() != walBefore.Size() || store.walRecords != 0 || store.versionClock != 0 {
		t.Fatalf(
			"failed clock publication changed WAL/counters: bytes %d -> %d, records %d, clock %d",
			walBefore.Size(),
			walAfter.Size(),
			store.walRecords,
			store.versionClock,
		)
	}
	if got := store.RuntimeStats(); got != statsBefore {
		t.Fatalf("failed clock publication changed runtime stats: before %+v, after %+v", statsBefore, got)
	}
	if _, err := store.ReadChunk(coord); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("ReadChunk(rejected coordinate) error = %v, want os.ErrNotExist", err)
	}
	if store.snapshotGeneration%2 != 0 {
		t.Fatalf("snapshot generation after rejection = %d, want even", store.snapshotGeneration)
	}

	if err := os.Remove(clockPath); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(clockPath, clock, 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestAuditRollbackIntentDiscardsOnlyUncommittedWALTail(t *testing.T) {
	root := testTempDir(t)
	g := testGeometry(t)
	firstCoord := geometry.Coord{X: 1, Y: 2}
	tailCoord := geometry.Coord{X: 3, Y: 4}
	store := mustOpenWithOptions(t, root, g, walRestartCrashOptions())
	if err := store.WriteChunk(firstCoord, testChunk(t, g, 7)); err != nil {
		t.Fatal(err)
	}
	boundary, err := store.walSize()
	if err != nil {
		t.Fatal(err)
	}
	record := store.appendWALRecord(
		nil,
		tailCoord,
		testChunk(t, g, 6).Bytes(),
		testChunk(t, g, 6).PresenceBytes(),
	)
	if err := store.appendWAL(record); err != nil {
		t.Fatal(err)
	}
	if err := store.publishIntent(intentRollback, boundary); err != nil {
		t.Fatal(err)
	}
	closeStore(t, store)

	reopened := mustOpenWithOptions(t, root, g, walRestartCrashOptions())
	defer closeStore(t, reopened)
	assertChunkValue(t, reopened, firstCoord, 7)
	if _, err := reopened.ReadChunk(tailCoord); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("ReadChunk(rolled-back tail) error = %v, want os.ErrNotExist", err)
	}
	if size, err := reopened.walSize(); err != nil || size != 0 {
		t.Fatalf("recovered WAL size = %d, %v; want 0, nil", size, err)
	}
	if _, err := os.Stat(reopened.intentPath()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("recovered intent stat error = %v, want os.ErrNotExist", err)
	}
}

func TestAuditConcurrentReadOnlyLoadsOfCompressedCheckpoint(t *testing.T) {
	root := testTempDir(t)
	g := mustGeometry(t, geometry.Config{ChunkEdge: 16, LargeChunkEdge: 2, BlockBits: 8})
	coord := geometry.Coord{X: -21, Y: 34}
	writer := mustOpenWithOptions(t, root, g, Options{
		CheckpointCompression: CheckpointCompressionZRLE,
		CheckpointRecords:     1,
		CheckpointBytes:       1 << 20,
	})
	want := mustChunk(t, g)
	if err := want.Set(geometry.Offset{X: 2, Y: 3}, 11); err != nil {
		t.Fatal(err)
	}
	if err := want.Set(geometry.Offset{X: 4, Y: 5}, 0); err != nil {
		t.Fatal(err)
	}
	if err := writer.WriteChunk(coord, want); err != nil {
		t.Fatal(err)
	}
	closeStore(t, writer)

	const readers = 16
	start := make(chan struct{})
	var group sync.WaitGroup
	group.Add(readers)
	for range readers {
		go func() {
			defer group.Done()
			reader, err := OpenWithOptions(root, g, Options{ReadOnly: true})
			if err != nil {
				t.Errorf("OpenWithOptions(read-only): %v", err)
				return
			}
			defer func() {
				if err := reader.Close(); err != nil {
					t.Errorf("Close(read-only): %v", err)
				}
			}()
			<-start
			for range 16 {
				chunk, err := reader.ReadChunk(coord)
				if err != nil {
					t.Errorf("ReadChunk(%v): %v", coord, err)
					return
				}
				if !bytes.Equal(chunk.Bytes(), want.Bytes()) ||
					!bytes.Equal(chunk.PresenceBytes(), want.PresenceBytes()) {
					t.Errorf(
						"read-only state = %x|%x, want %x|%x",
						chunk.Bytes(),
						chunk.PresenceBytes(),
						want.Bytes(),
						want.PresenceBytes(),
					)
					return
				}
			}
		}()
	}
	close(start)
	group.Wait()
}

func TestAuditOverflowSyncFailureStopsBeforeDirectoriesAndRetries(t *testing.T) {
	root := testTempDir(t)
	g := testGeometry(t)
	store := mustOpenWithOptions(t, root, g, Options{
		Durability:        DurabilityRelaxed,
		CheckpointRecords: 100,
		CheckpointBytes:   1 << 20,
	})
	defer closeStore(t, store)
	if err := store.WriteChunk(geometry.Coord{X: 5, Y: 6}, testChunk(t, g, 7)); err != nil {
		t.Fatal(err)
	}
	clear(store.pendingSync.paths)
	store.pendingSync.overflow = true

	injected := errors.New("injected overflow file sync failure")
	var directories int
	store.durabilityFailpoint = func(path string, directory bool) error {
		if directory {
			directories++
			return nil
		}
		if filepath.Base(path) == versionClockName {
			return injected
		}
		return nil
	}
	if err := store.FlushWAL(); !errors.Is(err, injected) {
		t.Fatalf("FlushWAL() error = %v, want injected file failure", err)
	}
	if directories != 0 {
		t.Fatalf("overflow fallback attempted %d directory syncs after a file failure", directories)
	}
	if !store.pendingSync.overflow {
		t.Fatal("failed overflow sync discarded retry bookkeeping")
	}

	store.durabilityFailpoint = nil
	if err := store.FlushWAL(); err != nil {
		t.Fatalf("retry FlushWAL(): %v", err)
	}
	if store.pendingSync.overflow || len(store.pendingSync.paths) != 0 {
		t.Fatalf("successful overflow retry retained bookkeeping: %+v", store.pendingSync)
	}
}

func TestAuditMaintenanceFailureIsReportedWithoutLosingCacheProgress(t *testing.T) {
	g := testGeometry(t)
	cache := newChunkCache(g, 1)
	injected := errors.New("injected maintenance failure")
	reported := make(chan error, 1)
	cache.maintain = func(geometry.Coord) error {
		return injected
	}
	cache.report = func(err error) {
		reported <- err
	}
	cache.startMaintenance()

	first := geometry.Coord{X: 1}
	second := geometry.Coord{X: 2}
	if err := cache.put(first, cachePayload(t, g, 1)); err != nil {
		t.Fatal(err)
	}
	if err := cache.put(second, cachePayload(t, g, 2)); err != nil {
		t.Fatalf("put after maintenance failure: %v", err)
	}
	if chunk, found, err := cache.get(second); err != nil || !found {
		t.Fatalf("get(%v) = (found %t, %v), want resident chunk", second, found, err)
	} else if want := cachePayload(t, g, 2); !bytes.Equal(chunk.Bytes(), want) {
		t.Fatalf("get(%v) payload = %x, want %x", second, chunk.Bytes(), want)
	}
	cache.close()

	select {
	case err := <-reported:
		if !errors.Is(err, injected) {
			t.Fatalf("reported maintenance error = %v, want injected failure", err)
		}
	default:
		t.Fatal("maintenance failure was not reported")
	}
	if cache.loadedChunks() != 1 {
		t.Fatalf("loaded chunks after failed maintenance = %d, want 1", cache.loadedChunks())
	}
}

func TestAuditOldChunkFixturesUpgradeToCurrentCheckpointFormat(t *testing.T) {
	for _, test := range []struct {
		name                string
		fixture             string
		explicitZeroPresent bool
	}{
		{name: "RGDBSPL1", fixture: "chunk-rgdbspl1.hex"},
		{name: "RGDBSPL2", fixture: "chunk-rgdbspl2.hex", explicitZeroPresent: true},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			root := testTempDir(t)
			path := filepath.Join(root, "l_p1_n1", "c_p3_n2.rdb")
			if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, readCompatibilityFixture(t, test.fixture), 0o600); err != nil {
				t.Fatal(err)
			}
			g := mustGeometry(t, geometry.Config{ChunkEdge: 2, LargeChunkEdge: 2, BlockBits: 3})
			store := mustOpenWithOptions(t, root, g, Options{
				CheckpointCompression: CheckpointCompressionZRLE,
				CheckpointRecords:     1,
				CheckpointBytes:       1 << 20,
			})
			legacy, err := store.ReadChunk(compatibilityFixtureCoord)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.WriteChunk(compatibilityFixtureCoord, legacy); err != nil {
				t.Fatal(err)
			}
			closeStore(t, store)

			encoded, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if string(encoded[:8]) != fileMagic {
				t.Fatalf("upgraded fixture magic = %q, want %q", encoded[:8], fileMagic)
			}
			reader := mustOpenWithOptions(t, root, g, Options{ReadOnly: true})
			defer closeStore(t, reader)
			assertCompatibilityState(t, reader, test.explicitZeroPresent)
		})
	}
}

func assertChunkState(t *testing.T, store *Store, coord geometry.Coord, want *storage.Chunk) {
	t.Helper()
	got, err := store.ReadChunk(coord)
	if err != nil {
		t.Fatalf("ReadChunk(%v): %v", coord, err)
	}
	if !bytes.Equal(got.Bytes(), want.Bytes()) ||
		!bytes.Equal(got.PresenceBytes(), want.PresenceBytes()) {
		t.Fatalf(
			"chunk %v state = %x|%x, want %x|%x",
			coord,
			got.Bytes(),
			got.PresenceBytes(),
			want.Bytes(),
			want.PresenceBytes(),
		)
	}
}
