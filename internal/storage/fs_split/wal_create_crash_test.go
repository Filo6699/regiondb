package fs_split

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/Filo6699/regiondb/internal/geometry"
)

const firstWALCrashExitCode = 89

func TestFirstWALCreationCommitsDirectoryEntry(t *testing.T) {
	t.Parallel()

	root := testTempDir(t)
	var got []atomicWriteBoundary
	wal, err := openWAL(root, func(boundary atomicWriteBoundary) error {
		got = append(got, boundary)
		return nil
	})
	if err != nil {
		t.Fatalf("openWAL(): %v", err)
	}
	if err := wal.Close(); err != nil {
		t.Fatalf("close WAL: %v", err)
	}

	want := []atomicWriteBoundary{
		atomicWriteTemporaryCreated,
		atomicWriteDataWritten,
		atomicWriteDataSynced,
		atomicWriteTemporaryClosed,
		atomicWriteDestinationReplaced,
		atomicWriteDirectorySynced,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("first WAL creation boundaries = %q, want %q", got, want)
	}

	got = nil
	wal, err = openWAL(root, func(boundary atomicWriteBoundary) error {
		got = append(got, boundary)
		return nil
	})
	if err != nil {
		t.Fatalf("reopen WAL: %v", err)
	}
	if err := wal.Close(); err != nil {
		t.Fatalf("close reopened WAL: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("existing WAL creation boundaries = %q, want none", got)
	}
}

func TestCrashFirstWALRecordSurvivesRestart(t *testing.T) {
	if os.Getenv("REGIONDB_FIRST_WAL_CRASH_CHILD") == "1" {
		runFirstWALCrashChild(t)
		return
	}

	root := testTempDir(t)
	command := exec.Command(os.Args[0], "-test.run=^TestCrashFirstWALRecordSurvivesRestart$")
	command.Env = append(
		os.Environ(),
		"REGIONDB_FIRST_WAL_CRASH_CHILD=1",
		"REGIONDB_FIRST_WAL_CRASH_ROOT="+root,
	)
	output, err := command.CombinedOutput()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != firstWALCrashExitCode {
		t.Fatalf("crash child error = %v, output:\n%s", err, output)
	}

	g := mustGeometry(t, geometry.Config{ChunkEdge: 1, LargeChunkEdge: 1, BlockBits: 8})
	walPath := filepath.Join(root, walName)
	decoder := &Store{geometry: g}
	records := readWALRecords(t, decoder, walPath)
	if len(records) != 1 {
		t.Fatalf("first synchronized WAL records after crash = %d, want 1", len(records))
	}
	if records[0].coord != (geometry.Coord{}) {
		t.Fatalf("record coordinate = %+v, want origin", records[0].coord)
	}

	expireWriterOwner(t, root)
	reopened := mustOpenWithOptions(t, root, g, Options{
		Durability:        DurabilityFsyncWAL,
		CheckpointRecords: 8,
		CheckpointBytes:   1 << 20,
	})
	recovered, err := reopened.ReadChunk(geometry.Coord{})
	if err != nil {
		t.Fatalf("ReadChunk() after first-WAL crash: %v", err)
	}
	value, err := recovered.Get(geometry.Offset{})
	if err != nil {
		t.Fatal(err)
	}
	if value != 9 {
		t.Fatalf("recovered value = %d, want 9", value)
	}
	info, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("stat WAL after recovery: %v", err)
	}
	if info.Size() != 0 {
		t.Fatalf("WAL size after recovery = %d, want 0", info.Size())
	}
	closeStore(t, reopened)
	assertNoChunkTemporaryFiles(t, root)
}

func runFirstWALCrashChild(t *testing.T) {
	root := os.Getenv("REGIONDB_FIRST_WAL_CRASH_ROOT")
	walPath := filepath.Join(root, walName)
	if _, err := os.Stat(walPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("WAL exists before first open: %v", err)
	}

	g := mustGeometry(t, geometry.Config{ChunkEdge: 1, LargeChunkEdge: 1, BlockBits: 8})
	store := mustOpenWithOptions(t, root, g, Options{
		Durability:        DurabilityFsyncWAL,
		CheckpointRecords: 8,
		CheckpointBytes:   1 << 20,
	})
	store.atomicWriteFailpoint = func(boundary atomicWriteBoundary) error {
		if boundary == atomicWriteTemporaryCreated {
			os.Exit(firstWALCrashExitCode)
		}
		return nil
	}
	chunk := mustChunk(t, g)
	if err := chunk.Set(geometry.Offset{}, 9); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteChunk(geometry.Coord{}, chunk); err != nil {
		t.Fatalf("WriteChunk(): %v", err)
	}
	t.Fatal("the first WAL write completed instead of crashing after its sync")
}
