package fs_split

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/Filo6699/regiondb/internal/geometry"
	"github.com/Filo6699/regiondb/internal/storage"
)

const walRestartCrashExitCode = 87

var walRestartCrashCoord = geometry.Coord{X: 5, Y: -2}

func walRestartCrashOptions() Options {
	return Options{
		Durability:        DurabilityFsyncWAL,
		CheckpointRecords: 8,
		CheckpointBytes:   1 << 20,
	}
}

// A synchronized WAL record is the only durable copy of an update until the
// chunk file is replaced. The child process crashes between those two points,
// so the restart has to recover the update from the WAL alone on every
// supported platform.
func TestCrashSyncedWALSurvivesRestart(t *testing.T) {
	if os.Getenv("REGIONDB_WAL_RESTART_CRASH_CHILD") == "1" {
		runWALRestartCrashChild(t)
		return
	}

	root := testTempDir(t)
	g := mustGeometry(t, geometry.Config{ChunkEdge: 1, LargeChunkEdge: 1, BlockBits: 8})
	options := walRestartCrashOptions()

	store := mustOpenWithOptions(t, root, g, options)
	previous := mustChunk(t, g)
	if err := previous.Set(geometry.Offset{}, 1); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteChunk(walRestartCrashCoord, previous); err != nil {
		t.Fatalf("WriteChunk(): %v", err)
	}
	previousEncoded := store.encode(walRestartCrashCoord, previous.Bytes())
	chunkPath := store.chunkPath(walRestartCrashCoord)
	closeStore(t, store)

	command := exec.Command(os.Args[0], "-test.run=^TestCrashSyncedWALSurvivesRestart$")
	command.Env = append(
		os.Environ(),
		"REGIONDB_WAL_RESTART_CRASH_CHILD=1",
		"REGIONDB_WAL_RESTART_CRASH_ROOT="+root,
	)
	output, err := command.CombinedOutput()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != walRestartCrashExitCode {
		t.Fatalf("crash child error = %v, output:\n%s", err, output)
	}

	// The chunk file still holds the previous generation, so a successful
	// restart can only come from the synchronized WAL.
	onDisk, err := os.ReadFile(chunkPath)
	if err != nil {
		t.Fatalf("read chunk file after crash: %v", err)
	}
	if !bytes.Equal(onDisk, previousEncoded) {
		t.Fatal("chunk file after crash does not hold the previous generation")
	}
	records := readWALRecords(t, store, filepath.Join(root, walName))
	if len(records) != 1 {
		t.Fatalf("synchronized WAL records after crash = %d, want 1", len(records))
	}
	if records[0].coord != walRestartCrashCoord {
		t.Fatalf("record coordinate = %+v, want %+v", records[0].coord, walRestartCrashCoord)
	}
	pending, err := storage.ChunkFromBytes(g, records[0].payload)
	if err != nil {
		t.Fatalf("decode record payload: %v", err)
	}
	if value, err := pending.Get(geometry.Offset{}); err != nil {
		t.Fatal(err)
	} else if value != 2 {
		t.Fatalf("record value = %d, want 2", value)
	}

	expireWriterOwner(t, root)
	reopened := mustOpenWithOptions(t, root, g, options)
	recovered, err := reopened.ReadChunk(walRestartCrashCoord)
	if err != nil {
		t.Fatalf("ReadChunk() after restart: %v", err)
	}
	value, err := recovered.Get(geometry.Offset{})
	if err != nil {
		t.Fatal(err)
	}
	if value != 2 {
		t.Fatalf("recovered value = %d, want 2", value)
	}
	info, err := os.Stat(filepath.Join(root, walName))
	if err != nil {
		t.Fatalf("stat WAL after restart: %v", err)
	}
	if info.Size() != 0 {
		t.Fatalf("WAL size after restart = %d, want 0", info.Size())
	}
	closeStore(t, reopened)
	assertNoChunkTemporaryFiles(t, root)
}

func TestWALSyncFailurePropagatesAndRecoversAfterReopen(t *testing.T) {
	t.Parallel()

	root := testTempDir(t)
	g := mustGeometry(t, geometry.Config{ChunkEdge: 1, LargeChunkEdge: 1, BlockBits: 8})
	options := walRestartCrashOptions()
	store := mustOpenWithOptions(t, root, g, options)
	chunk := mustChunk(t, g)
	if err := chunk.Set(geometry.Offset{}, 73); err != nil {
		t.Fatal(err)
	}

	injected := errors.New("injected WAL sync result failure")
	store.walFailpoint = func(boundary walBoundary) error {
		if boundary == walRecordSynced {
			return injected
		}
		return nil
	}
	if err := store.WriteChunk(walRestartCrashCoord, chunk); !errors.Is(err, injected) {
		t.Fatalf("WriteChunk() error = %v, want %v", err, injected)
	}
	store.walFailpoint = nil
	closeStore(t, store)

	reopened := mustOpenWithOptions(t, root, g, options)
	recovered, err := reopened.ReadChunk(walRestartCrashCoord)
	if err != nil {
		t.Fatalf("ReadChunk() after WAL failure restart: %v", err)
	}
	value, err := recovered.Get(geometry.Offset{})
	if err != nil {
		t.Fatal(err)
	}
	if value != 73 {
		t.Fatalf("recovered value = %d, want 73", value)
	}
	closeStore(t, reopened)
}

func TestRecoveryCompactionFailurePreservesSourceWAL(t *testing.T) {
	t.Parallel()

	root := testTempDir(t)
	g := mustGeometry(t, geometry.Config{ChunkEdge: 1, LargeChunkEdge: 1, BlockBits: 8})
	options := Options{
		CheckpointRecords: 8,
		CheckpointBytes:   1 << 20,
	}
	store := mustOpenWithOptions(t, root, g, options)
	chunk := mustChunk(t, g)
	if err := chunk.Set(geometry.Offset{}, 29); err != nil {
		t.Fatal(err)
	}
	record := store.encodeWALRecord(walRestartCrashCoord, chunk.Bytes())
	if err := store.appendWAL(record); err != nil {
		t.Fatalf("append complete WAL record: %v", err)
	}
	wal, err := store.walHandles.acquire(walAppendHandle)
	if err != nil {
		t.Fatalf("acquire WAL append handle: %v", err)
	}
	partial := store.encodeWALRecord(geometry.Coord{X: 8, Y: 9}, mustChunk(t, g).Bytes())
	err = appendWALHandle(wal, partial[:len(partial)-1])
	store.walHandles.release(walAppendHandle)
	if err != nil {
		t.Fatalf("append partial WAL record: %v", err)
	}

	walPath := filepath.Join(root, walName)
	source, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("read source WAL: %v", err)
	}
	injected := errors.New("injected replay compaction failure")
	dataWrites := 0
	store.atomicWriteFailpoint = func(boundary atomicWriteBoundary) error {
		if boundary == atomicWriteDataWritten {
			dataWrites++
			if dataWrites == 2 {
				return injected
			}
		}
		return nil
	}
	if err := store.recoverWAL(); !errors.Is(err, injected) {
		t.Fatalf("recoverWAL() error = %v, want %v", err, injected)
	}
	got, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("read WAL after failed recovery: %v", err)
	}
	if !bytes.Equal(got, source) {
		t.Fatal("failed recovery changed the source WAL")
	}

	store.atomicWriteFailpoint = nil
	closeStore(t, store)
	reopened := mustOpenWithOptions(t, root, g, options)
	recovered, err := reopened.ReadChunk(walRestartCrashCoord)
	if err != nil {
		t.Fatalf("ReadChunk() after retry: %v", err)
	}
	value, err := recovered.Get(geometry.Offset{})
	if err != nil {
		t.Fatal(err)
	}
	if value != 29 {
		t.Fatalf("recovered value = %d, want 29", value)
	}
	info, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("stat WAL after retry: %v", err)
	}
	if info.Size() != 0 {
		t.Fatalf("WAL size after retry = %d, want 0", info.Size())
	}
	closeStore(t, reopened)
}

func runWALRestartCrashChild(t *testing.T) {
	root := os.Getenv("REGIONDB_WAL_RESTART_CRASH_ROOT")
	g := mustGeometry(t, geometry.Config{ChunkEdge: 1, LargeChunkEdge: 1, BlockBits: 8})
	store := mustOpenWithOptions(t, root, g, walRestartCrashOptions())
	store.atomicWriteFailpoint = func(boundary atomicWriteBoundary) error {
		if boundary == atomicWriteTemporaryCreated {
			os.Exit(walRestartCrashExitCode)
		}
		return nil
	}
	chunk := mustChunk(t, g)
	if err := chunk.Set(geometry.Offset{}, 2); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteChunk(walRestartCrashCoord, chunk); err != nil {
		t.Fatalf("WriteChunk(): %v", err)
	}
	t.Fatal("the chunk write completed instead of crashing after the WAL sync")
}
