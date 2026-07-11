package fs_split

import (
	"errors"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/Filo6699/regiondb/internal/geometry"
)

func TestWALFlushMakesAcknowledgedWritesDurableInEveryMode(t *testing.T) {
	for _, mode := range []DurabilityMode{
		DurabilityRelaxed,
		DurabilityFsyncWAL,
		DurabilityFsyncCheckpoint,
	} {
		mode := mode
		t.Run(string(mode), func(t *testing.T) {
			root := t.TempDir()
			g := testGeometry(t)
			coord := geometry.Coord{X: 7, Y: -9}
			store := mustOpenWithOptions(t, root, g, Options{
				Durability:        mode,
				CheckpointRecords: 100,
				CheckpointBytes:   1 << 20,
			})
			if err := store.WriteChunk(coord, testChunk(t, g, 5)); err != nil {
				t.Fatalf("WriteChunk() error = %v", err)
			}
			if err := store.FlushWAL(); err != nil {
				t.Fatalf("FlushWAL() error = %v", err)
			}
			if store.snapshotGeneration%2 != 0 {
				t.Fatalf("snapshot generation = %d, want even", store.snapshotGeneration)
			}
			closeStore(t, store)

			reopened := mustOpenWithOptions(t, root, g, Options{
				Durability:        mode,
				CheckpointRecords: 100,
				CheckpointBytes:   1 << 20,
			})
			assertChunkValue(t, reopened, coord, 5)
			closeStore(t, reopened)
		})
	}
}

func TestWALFlushRetriesCommittedPublication(t *testing.T) {
	root := t.TempDir()
	g := testGeometry(t)
	store := mustOpenWithOptions(t, root, g, Options{
		Durability:        DurabilityRelaxed,
		CheckpointRecords: 100,
		CheckpointBytes:   1 << 20,
		PostCommitFailure: func(string) {},
	})
	coord := geometry.Coord{X: 3, Y: 4}
	injected := errors.New("injected publication failure")
	store.atomicWriteFailpoint = func(boundary atomicWriteBoundary) error {
		if boundary == atomicWriteTemporaryCreated {
			return injected
		}
		return nil
	}
	if err := store.WriteChunk(coord, testChunk(t, g, 6)); err != nil {
		t.Fatalf("committed WriteChunk() error = %v", err)
	}
	if len(store.pendingPublications) != 1 {
		t.Fatalf("pending publications = %d, want 1", len(store.pendingPublications))
	}
	if err := store.FlushWAL(); !errors.Is(err, injected) {
		t.Fatalf("FlushWAL() error = %v, want injected failure", err)
	}
	if len(store.pendingPublications) != 1 {
		t.Fatalf("failed FlushWAL discarded pending publication")
	}

	store.atomicWriteFailpoint = nil
	if err := store.FlushWAL(); err != nil {
		t.Fatalf("retry FlushWAL() error = %v", err)
	}
	if len(store.pendingPublications) != 0 {
		t.Fatalf("pending publications after retry = %d, want 0", len(store.pendingPublications))
	}
	assertChunkValue(t, store, coord, 6)
	closeStore(t, store)
}

func TestWALFlushOverflowFallbackSyncsFilesBeforeDirectoriesAndRetries(t *testing.T) {
	root := t.TempDir()
	g := testGeometry(t)
	store := mustOpenWithOptions(t, root, g, Options{
		Durability:        DurabilityRelaxed,
		CheckpointRecords: 100,
		CheckpointBytes:   1 << 20,
	})
	if err := store.WriteChunk(geometry.Coord{X: 1, Y: 2}, testChunk(t, g, 7)); err != nil {
		t.Fatal(err)
	}
	clear(store.pendingSync.paths)
	store.pendingSync.overflow = true

	var sawDirectory bool
	injected := errors.New("injected directory sync failure")
	store.durabilityFailpoint = func(_ string, directory bool) error {
		if !directory && sawDirectory {
			t.Fatal("overflow fallback synchronized a file after a directory")
		}
		if directory {
			sawDirectory = true
			return injected
		}
		return nil
	}
	if err := store.FlushWAL(); !errors.Is(err, injected) {
		t.Fatalf("FlushWAL() error = %v, want injected failure", err)
	}
	if !store.pendingSync.overflow {
		t.Fatal("failed overflow fallback discarded retry bookkeeping")
	}

	store.durabilityFailpoint = nil
	if err := store.FlushWAL(); err != nil {
		t.Fatalf("retry FlushWAL() error = %v", err)
	}
	if store.pendingSync.overflow || len(store.pendingSync.paths) != 0 {
		t.Fatalf("successful fallback retained bookkeeping: %+v", store.pendingSync)
	}
	closeStore(t, store)
}

func TestReadOnlyLoadRejectsSnapshotABA(t *testing.T) {
	root := t.TempDir()
	g := testGeometry(t)
	coord := geometry.Coord{X: -2, Y: 8}
	writer := mustOpenWithOptions(t, root, g, Options{
		CheckpointRecords: 100,
		CheckpointBytes:   1 << 20,
	})
	if err := writer.WriteChunk(coord, testChunk(t, g, 3)); err != nil {
		t.Fatal(err)
	}
	reader := mustOpenWithOptions(t, root, g, Options{ReadOnly: true})
	reader.snapshotReadpoint = func(boundary snapshotReadBoundary) error {
		if boundary != snapshotReadLoaded {
			return nil
		}
		reader.snapshotReadpoint = nil
		current, err := reader.readSnapshotGeneration()
		if err != nil {
			return err
		}
		if err := writeAtomic(
			filepath.Join(root, snapshotName),
			encodeSnapshotGeneration(current+2),
			false,
			nil,
		); err != nil {
			return err
		}
		return nil
	}
	if _, err := reader.ReadChunk(coord); !errors.Is(err, ErrSnapshotChanged) {
		t.Fatalf("ReadChunk() error = %v, want ErrSnapshotChanged", err)
	}
	closeStore(t, reader)
	closeStore(t, writer)
}

func TestReadOnlyLoadRejectsOddAndCorruptSnapshotGeneration(t *testing.T) {
	root := t.TempDir()
	g := testGeometry(t)
	writer := mustOpenWithOptions(t, root, g, Options{})
	closeStore(t, writer)

	path := filepath.Join(root, snapshotName)
	if err := os.WriteFile(path, encodeSnapshotGeneration(3), 0o600); err != nil {
		t.Fatal(err)
	}
	reader := mustOpenWithOptions(t, root, g, Options{ReadOnly: true})
	if _, err := reader.ReadChunk(geometry.Coord{}); !errors.Is(err, ErrSnapshotUnstable) {
		t.Fatalf("ReadChunk(odd generation) error = %v, want ErrSnapshotUnstable", err)
	}
	closeStore(t, reader)

	if err := os.WriteFile(path, []byte("corrupt"), 0o600); err != nil {
		t.Fatal(err)
	}
	reader = mustOpenWithOptions(t, root, g, Options{ReadOnly: true})
	if _, err := reader.ReadChunk(geometry.Coord{}); !errors.Is(err, ErrCorruptSnapshot) {
		t.Fatalf("ReadChunk(corrupt generation) error = %v, want ErrCorruptSnapshot", err)
	}
	closeStore(t, reader)
}

func TestSnapshotGenerationOverflowFailsClosed(t *testing.T) {
	root := t.TempDir()
	g := testGeometry(t)
	if err := os.WriteFile(
		filepath.Join(root, snapshotName),
		encodeSnapshotGeneration(math.MaxUint64-1),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(root, g); !errors.Is(err, ErrSnapshotOverflow) {
		t.Fatalf("Open() error = %v, want ErrSnapshotOverflow", err)
	}
}
