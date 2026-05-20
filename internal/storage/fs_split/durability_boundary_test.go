package fs_split

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/Filo6699/regiondb/internal/geometry"
	"github.com/Filo6699/regiondb/internal/storage"
)

const checkpointBoundaryCrashExitCode = 88

func TestWriteDurabilityBoundaryOrdering(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		durability DurabilityMode
		want       []string
	}{
		{
			name:       "relaxed",
			durability: DurabilityRelaxed,
			want: []string{
				"wal:" + string(walRecordAppended),
				"chunk:" + string(atomicWriteTemporaryCreated),
				"chunk:" + string(atomicWriteDataWritten),
				"chunk:" + string(atomicWriteTemporaryClosed),
				"chunk:" + string(atomicWriteDestinationReplaced),
			},
		},
		{
			name:       "fsync WAL",
			durability: DurabilityFsyncWAL,
			want: []string{
				"wal:" + string(walRecordAppended),
				"wal:" + string(walRecordSynced),
				"chunk:" + string(atomicWriteTemporaryCreated),
				"chunk:" + string(atomicWriteDataWritten),
				"chunk:" + string(atomicWriteTemporaryClosed),
				"chunk:" + string(atomicWriteDestinationReplaced),
			},
		},
		{
			name:       "fsync checkpoint",
			durability: DurabilityFsyncCheckpoint,
			want: []string{
				"wal:" + string(walRecordAppended),
				"chunk:" + string(atomicWriteTemporaryCreated),
				"chunk:" + string(atomicWriteDataWritten),
				"chunk:" + string(atomicWriteDataSynced),
				"chunk:" + string(atomicWriteTemporaryClosed),
				"chunk:" + string(atomicWriteDestinationReplaced),
				"chunk:" + string(atomicWriteDirectorySynced),
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			store, chunk := newDurabilityBoundaryStore(t, test.durability, 8)
			var got []string
			store.walFailpoint = func(boundary walBoundary) error {
				got = append(got, "wal:"+string(boundary))
				return nil
			}
			store.atomicWriteFailpoint = func(boundary atomicWriteBoundary) error {
				got = append(got, "chunk:"+string(boundary))
				return nil
			}

			if err := store.WriteChunk(geometry.Coord{}, chunk); err != nil {
				t.Fatalf("WriteChunk(): %v", err)
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("durability boundaries = %q, want %q", got, test.want)
			}
			closeStore(t, store)
		})
	}
}

func TestCheckpointSyncCompletesBeforeWriteReturns(t *testing.T) {
	t.Parallel()

	store, chunk := newDurabilityBoundaryStore(t, DurabilityFsyncCheckpoint, 1)
	var got []string
	store.walFailpoint = func(boundary walBoundary) error {
		got = append(got, "wal:"+string(boundary))
		return nil
	}
	store.atomicWriteFailpoint = func(boundary atomicWriteBoundary) error {
		got = append(got, "chunk:"+string(boundary))
		return nil
	}

	if err := store.WriteChunk(geometry.Coord{}, chunk); err != nil {
		t.Fatalf("WriteChunk(): %v", err)
	}
	wantSuffix := []string{
		"chunk:" + string(atomicWriteDirectorySynced),
		"wal:" + string(walCheckpointTruncated),
		"wal:" + string(walCheckpointSynced),
	}
	if len(got) < len(wantSuffix) || !reflect.DeepEqual(got[len(got)-len(wantSuffix):], wantSuffix) {
		t.Fatalf("final durability boundaries = %q, want suffix %q", got, wantSuffix)
	}
	if stats := store.RuntimeStats(); stats.Checkpoints != 1 ||
		stats.WALFlushes != 1 ||
		stats.WALForegroundFlushes != 0 ||
		stats.WALGroupFlushes != 0 ||
		stats.WALEvictionFlushes != 0 ||
		stats.WALCheckpointFlushes != 1 {
		t.Fatalf("stats after checkpoint = %+v, want one completed checkpoint flush", stats)
	}
	closeStore(t, store)
}

func TestDurabilityFailpointsPreventSuccessfulWrite(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		durability        DurabilityMode
		checkpointRecords uint64
		fail              walBoundary
		wantChunk         bool
	}{
		{
			name:              "WAL append",
			durability:        DurabilityFsyncWAL,
			checkpointRecords: 8,
			fail:              walRecordAppended,
		},
		{
			name:              "WAL sync",
			durability:        DurabilityFsyncWAL,
			checkpointRecords: 8,
			fail:              walRecordSynced,
		},
		{
			name:              "checkpoint truncate",
			durability:        DurabilityFsyncCheckpoint,
			checkpointRecords: 1,
			fail:              walCheckpointTruncated,
			wantChunk:         true,
		},
		{
			name:              "checkpoint sync",
			durability:        DurabilityFsyncCheckpoint,
			checkpointRecords: 1,
			fail:              walCheckpointSynced,
			wantChunk:         true,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			store, chunk := newDurabilityBoundaryStore(t, test.durability, test.checkpointRecords)
			injected := errors.New("injected durability failure")
			store.walFailpoint = func(boundary walBoundary) error {
				if boundary == test.fail {
					return injected
				}
				return nil
			}

			err := store.WriteChunk(geometry.Coord{}, chunk)
			if !errors.Is(err, injected) {
				t.Fatalf("WriteChunk() error = %v, want injected failure", err)
			}
			_, statErr := os.Stat(store.chunkPath(geometry.Coord{}))
			if test.wantChunk && statErr != nil {
				t.Fatalf("stat durable chunk after checkpoint failure: %v", statErr)
			}
			if !test.wantChunk && !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("chunk exists before the WAL durability boundary completed: %v", statErr)
			}
			if stats := store.RuntimeStats(); stats.Checkpoints != 0 {
				t.Fatalf("checkpoints after failed write = %d, want 0", stats.Checkpoints)
			}
			store.walFailpoint = nil
			closeStore(t, store)
		})
	}
}

func TestCrashCheckpointBoundariesSurviveRestart(t *testing.T) {
	if os.Getenv("REGIONDB_CHECKPOINT_BOUNDARY_CRASH_CHILD") == "1" {
		runCheckpointBoundaryCrashChild(t)
		return
	}

	for _, boundary := range []walBoundary{
		walCheckpointTruncated,
		walCheckpointSynced,
	} {
		boundary := boundary
		t.Run(string(boundary), func(t *testing.T) {
			root := testTempDir(t)
			command := exec.Command(os.Args[0], "-test.run=^TestCrashCheckpointBoundariesSurviveRestart$")
			command.Env = append(
				os.Environ(),
				"REGIONDB_CHECKPOINT_BOUNDARY_CRASH_CHILD=1",
				"REGIONDB_CHECKPOINT_BOUNDARY_CRASH_ROOT="+root,
				"REGIONDB_CHECKPOINT_BOUNDARY_CRASH_POINT="+string(boundary),
			)
			output, err := command.CombinedOutput()
			var exitErr *exec.ExitError
			if !errors.As(err, &exitErr) || exitErr.ExitCode() != checkpointBoundaryCrashExitCode {
				t.Fatalf("crash child error = %v, output:\n%s", err, output)
			}

			expireWriterOwner(t, root)
			g := mustGeometry(t, geometry.Config{ChunkEdge: 1, LargeChunkEdge: 1, BlockBits: 8})
			store := mustOpenWithOptions(t, root, g, Options{
				Durability:        DurabilityFsyncCheckpoint,
				CheckpointRecords: 1,
				CheckpointBytes:   1 << 20,
			})
			chunk, err := store.ReadChunk(geometry.Coord{})
			if err != nil {
				t.Fatalf("ReadChunk() after checkpoint crash: %v", err)
			}
			value, err := chunk.Get(geometry.Offset{})
			if err != nil {
				t.Fatal(err)
			}
			if value != 7 {
				t.Fatalf("recovered value = %d, want 7", value)
			}
			info, err := os.Stat(filepath.Join(root, walName))
			if err != nil {
				t.Fatalf("stat WAL after checkpoint crash: %v", err)
			}
			if info.Size() != 0 {
				t.Fatalf("WAL size after checkpoint crash = %d, want 0", info.Size())
			}
			closeStore(t, store)
			assertNoChunkTemporaryFiles(t, root)
		})
	}
}

func runCheckpointBoundaryCrashChild(t *testing.T) {
	root := os.Getenv("REGIONDB_CHECKPOINT_BOUNDARY_CRASH_ROOT")
	boundary := walBoundary(os.Getenv("REGIONDB_CHECKPOINT_BOUNDARY_CRASH_POINT"))
	g := mustGeometry(t, geometry.Config{ChunkEdge: 1, LargeChunkEdge: 1, BlockBits: 8})
	store := mustOpenWithOptions(t, root, g, Options{
		Durability:        DurabilityFsyncCheckpoint,
		CheckpointRecords: 1,
		CheckpointBytes:   1 << 20,
	})
	store.walFailpoint = func(observed walBoundary) error {
		if observed == boundary {
			os.Exit(checkpointBoundaryCrashExitCode)
		}
		return nil
	}
	chunk := mustChunk(t, g)
	if err := chunk.Set(geometry.Offset{}, 7); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteChunk(geometry.Coord{}, chunk); err != nil {
		t.Fatalf("WriteChunk(): %v", err)
	}
	t.Fatalf("checkpoint did not reach crash boundary %q", boundary)
}

func newDurabilityBoundaryStore(
	t *testing.T,
	durability DurabilityMode,
	checkpointRecords uint64,
) (*Store, *storage.Chunk) {
	t.Helper()

	g := mustGeometry(t, geometry.Config{ChunkEdge: 1, LargeChunkEdge: 1, BlockBits: 8})
	store := mustOpenWithOptions(t, testTempDir(t), g, Options{
		Durability:        durability,
		CheckpointRecords: checkpointRecords,
		CheckpointBytes:   1 << 20,
	})
	return store, mustChunk(t, g)
}
