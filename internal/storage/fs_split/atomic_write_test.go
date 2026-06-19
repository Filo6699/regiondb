package fs_split

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/Filo6699/regiondb/internal/geometry"
)

const atomicWriteCrashExitCode = 86

func TestAtomicWriteBoundaries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		syncData bool
		want     []atomicWriteBoundary
	}{
		{
			name:     "relaxed",
			syncData: false,
			want: []atomicWriteBoundary{
				atomicWriteTemporaryCreated,
				atomicWriteDataWritten,
				atomicWriteTemporaryClosed,
				atomicWriteDestinationReplaced,
			},
		},
		{
			name:     "synchronized",
			syncData: true,
			want: []atomicWriteBoundary{
				atomicWriteTemporaryCreated,
				atomicWriteDataWritten,
				atomicWriteDataSynced,
				atomicWriteTemporaryClosed,
				atomicWriteDestinationReplaced,
				atomicWriteDirectorySynced,
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(testTempDir(t), "chunk.rdb")
			var got []atomicWriteBoundary
			err := writeAtomic(path, []byte("chunk"), test.syncData, func(boundary atomicWriteBoundary) error {
				got = append(got, boundary)
				return nil
			})
			if err != nil {
				t.Fatalf("writeAtomic(): %v", err)
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("boundaries = %q, want %q", got, test.want)
			}
		})
	}
}

func TestAtomicWriteFailpointCleansTemporaryFile(t *testing.T) {
	t.Parallel()

	root := testTempDir(t)
	injected := errors.New("injected failure")
	err := writeAtomic(
		filepath.Join(root, "chunk.rdb"),
		[]byte("chunk"),
		true,
		func(boundary atomicWriteBoundary) error {
			if boundary == atomicWriteDataSynced {
				return injected
			}
			return nil
		},
	)
	if !errors.Is(err, injected) {
		t.Fatalf("writeAtomic() error = %v, want injected failure", err)
	}
	assertNoChunkTemporaryFiles(t, root)
}

func TestOpenReclaimsStaleChunkTemporaryFiles(t *testing.T) {
	t.Parallel()

	root := testTempDir(t)
	directory := filepath.Join(root, "l_p0_p0")
	if err := os.MkdirAll(directory, 0o755); err != nil {
		t.Fatal(err)
	}
	stale := filepath.Join(directory, chunkTemporaryPrefix+"stale")
	if err := os.WriteFile(stale, []byte("incomplete"), 0o600); err != nil {
		t.Fatal(err)
	}
	unrelated := filepath.Join(directory, ".regiondb-unrelated")
	if err := os.WriteFile(unrelated, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}

	g := mustGeometry(t, geometry.Config{ChunkEdge: 1, LargeChunkEdge: 1, BlockBits: 1})
	store := mustOpen(t, root, g)
	closeStore(t, store)

	if _, err := os.Stat(stale); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale temporary file still exists: %v", err)
	}
	if _, err := os.Stat(unrelated); err != nil {
		t.Fatalf("unrelated file was removed: %v", err)
	}
}

func TestStaleTemporaryFileScanStopsAtLimit(t *testing.T) {
	t.Parallel()

	root := testTempDir(t)
	for _, name := range []string{"a", "b", "c"} {
		path := filepath.Join(root, chunkTemporaryPrefix+name)
		if err := os.WriteFile(path, []byte("incomplete"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	result, err := reclaimStaleChunkTemporaryFiles(root, 2)
	if err != nil {
		t.Fatalf("reclaimStaleChunkTemporaryFiles() error = %v", err)
	}
	if result.entries != 2 || !result.capped {
		t.Fatalf("startup scan result = %+v, want entries=2 capped=true", result)
	}
	matches, err := filepath.Glob(filepath.Join(root, chunkTemporaryPrefix+"*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 1 {
		t.Fatalf("remaining temporary files = %v, want one file beyond the scan cap", matches)
	}
}

func TestCrashAtomicReplaceBoundaries(t *testing.T) {
	if os.Getenv("REGIONDB_ATOMIC_WRITE_CRASH_CHILD") == "1" {
		runAtomicWriteCrashChild(t)
		return
	}

	boundaries := []atomicWriteBoundary{
		atomicWriteTemporaryCreated,
		atomicWriteDataWritten,
		atomicWriteDataSynced,
		atomicWriteTemporaryClosed,
		atomicWriteDestinationReplaced,
		atomicWriteDirectorySynced,
	}
	for _, boundary := range boundaries {
		boundary := boundary
		t.Run(string(boundary), func(t *testing.T) {
			root := testTempDir(t)
			g := mustGeometry(t, geometry.Config{ChunkEdge: 1, LargeChunkEdge: 1, BlockBits: 8})
			options := Options{
				Durability:        DurabilityFsyncCheckpoint,
				CheckpointRecords: 8,
				CheckpointBytes:   1 << 20,
			}
			coord := geometry.Coord{X: 2, Y: -3}
			store := mustOpenWithOptions(t, root, g, options)
			oldChunk := mustChunk(t, g)
			if err := oldChunk.Set(geometry.Offset{}, 1); err != nil {
				t.Fatal(err)
			}
			if err := store.WriteChunk(coord, oldChunk); err != nil {
				t.Fatal(err)
			}
			oldEncoded := store.encode(coord, oldChunk.Bytes())
			newChunk := mustChunk(t, g)
			if err := newChunk.Set(geometry.Offset{}, 2); err != nil {
				t.Fatal(err)
			}
			newEncoded := store.encode(coord, newChunk.Bytes())
			path := store.chunkPath(coord)
			closeStore(t, store)

			command := exec.Command(os.Args[0], "-test.run=^TestCrashAtomicReplaceBoundaries$")
			command.Env = append(
				os.Environ(),
				"REGIONDB_ATOMIC_WRITE_CRASH_CHILD=1",
				"REGIONDB_ATOMIC_WRITE_CRASH_ROOT="+root,
				"REGIONDB_ATOMIC_WRITE_CRASH_BOUNDARY="+string(boundary),
			)
			output, err := command.CombinedOutput()
			var exitErr *exec.ExitError
			if !errors.As(err, &exitErr) || exitErr.ExitCode() != atomicWriteCrashExitCode {
				t.Fatalf("crash child error = %v, output:\n%s", err, output)
			}

			got, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read destination after crash: %v", err)
			}
			want := oldEncoded
			if boundary == atomicWriteDestinationReplaced || boundary == atomicWriteDirectorySynced {
				want = newEncoded
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("destination after %q crash is neither expected generation", boundary)
			}

			expireWriterOwner(t, root)
			reopened := mustOpenWithOptions(t, root, g, options)
			gotChunk, err := reopened.ReadChunk(coord)
			if err != nil {
				t.Fatalf("ReadChunk() after crash recovery: %v", err)
			}
			value, err := gotChunk.Get(geometry.Offset{})
			if err != nil {
				t.Fatal(err)
			}
			if value != 2 {
				t.Fatalf("recovered value = %d, want 2", value)
			}
			closeStore(t, reopened)
			assertNoChunkTemporaryFiles(t, root)
		})
	}
}

func runAtomicWriteCrashChild(t *testing.T) {
	root := os.Getenv("REGIONDB_ATOMIC_WRITE_CRASH_ROOT")
	boundary := atomicWriteBoundary(os.Getenv("REGIONDB_ATOMIC_WRITE_CRASH_BOUNDARY"))
	g := mustGeometry(t, geometry.Config{ChunkEdge: 1, LargeChunkEdge: 1, BlockBits: 8})
	store := mustOpenWithOptions(t, root, g, Options{
		Durability:        DurabilityFsyncCheckpoint,
		CheckpointRecords: 8,
		CheckpointBytes:   1 << 20,
	})
	store.atomicWriteFailpoint = func(observed atomicWriteBoundary) error {
		if observed == boundary {
			os.Exit(atomicWriteCrashExitCode)
		}
		return nil
	}
	chunk := mustChunk(t, g)
	if err := chunk.Set(geometry.Offset{}, 2); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteChunk(geometry.Coord{X: 2, Y: -3}, chunk); err != nil {
		t.Fatalf("WriteChunk(): %v", err)
	}
	t.Fatalf("atomic write did not reach crash boundary %q", boundary)
}

func expireWriterOwner(t *testing.T, root string) {
	t.Helper()

	path := filepath.Join(root, lockName, lockOwnerName)
	owner, found, err := readOwnerMetadata(path)
	if err != nil {
		t.Fatalf("read crashed writer owner: %v", err)
	}
	if !found {
		t.Fatal("crashed writer owner metadata is missing")
	}
	owner.StartedAt = owner.StartedAt.Add(-2 * writerStaleAfter)
	owner.HeartbeatAt = owner.StartedAt
	encoded, err := json.Marshal(owner)
	if err != nil {
		t.Fatal(err)
	}
	encoded = append(encoded, '\n')
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatalf("expire crashed writer owner: %v", err)
	}
}

func assertNoChunkTemporaryFiles(t *testing.T, root string) {
	t.Helper()

	var matches []string
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !entry.IsDir() && len(entry.Name()) >= len(chunkTemporaryPrefix) &&
			entry.Name()[:len(chunkTemporaryPrefix)] == chunkTemporaryPrefix {
			matches = append(matches, path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 0 {
		t.Fatalf("stale temporary files remain: %v", matches)
	}
}
