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
