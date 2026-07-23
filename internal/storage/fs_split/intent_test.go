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

const intentCrashExitCode = 89

func TestConditionalIntentPreCommitFailureRollsBack(t *testing.T) {
	root := testTempDir(t)
	g := mustGeometry(t, geometry.Config{ChunkEdge: 1, LargeChunkEdge: 1, BlockBits: 8})
	store := mustOpenWithOptions(t, root, g, walRestartCrashOptions())
	coord := geometry.Coord{X: 2, Y: -3}
	if err := store.WriteChunk(coord, testChunk(t, g, 11)); err != nil {
		t.Fatalf("seed WriteChunk() error = %v", err)
	}
	version, err := store.ChunkVersion(coord)
	if err != nil {
		t.Fatal(err)
	}
	walBefore, err := os.Stat(filepath.Join(root, walName))
	if err != nil {
		t.Fatal(err)
	}

	injected := errors.New("injected pre-commit failure")
	fired := false
	store.intentFailpoint = func(boundary intentBoundary) error {
		if boundary == intentBeforeCommittedPublish && !fired {
			fired = true
			return injected
		}
		return nil
	}
	if _, err := store.CompareAndSwapChunk(coord, version, testChunk(t, g, 22)); !errors.Is(err, injected) {
		t.Fatalf("CompareAndSwapChunk() error = %v, want injected failure", err)
	}
	assertChunkValue(t, store, coord, 11)
	if got, err := store.ChunkVersion(coord); err != nil || got != version {
		t.Fatalf("ChunkVersion() = %d, %v; want %d, nil", got, err, version)
	}
	walAfter, err := os.Stat(filepath.Join(root, walName))
	if err != nil {
		t.Fatal(err)
	}
	if walAfter.Size() != walBefore.Size() {
		t.Fatalf("WAL size after rollback = %d, want %d", walAfter.Size(), walBefore.Size())
	}
	if _, err := os.Stat(store.intentPath()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("intent after rollback stat error = %v, want os.ErrNotExist", err)
	}
	store.intentFailpoint = nil
	closeStore(t, store)
}

func TestConditionalBatchPreCommitFailureRollsBackEveryChunk(t *testing.T) {
	root := testTempDir(t)
	g := mustGeometry(t, geometry.Config{ChunkEdge: 1, LargeChunkEdge: 1, BlockBits: 8})
	store := mustOpenWithOptions(t, root, g, walRestartCrashOptions())
	first := geometry.Coord{X: 8, Y: 9}
	second := geometry.Coord{X: 10, Y: 11}
	if err := store.WriteChunk(first, testChunk(t, g, 61)); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteChunk(second, testChunk(t, g, 62)); err != nil {
		t.Fatal(err)
	}
	firstVersion, err := store.ChunkVersion(first)
	if err != nil {
		t.Fatal(err)
	}
	secondVersion, err := store.ChunkVersion(second)
	if err != nil {
		t.Fatal(err)
	}
	injected := errors.New("injected batch commit failure")
	fired := false
	store.intentFailpoint = func(boundary intentBoundary) error {
		if boundary == intentBeforeCommittedPublish && !fired {
			fired = true
			return injected
		}
		return nil
	}
	_, err = store.ConditionalWriteChunks([]storage.ConditionalMutation{
		{Coord: first, ExpectedVersion: firstVersion, Chunk: testChunk(t, g, 71)},
		{Coord: second, ExpectedVersion: secondVersion, Chunk: testChunk(t, g, 72)},
	})
	if !errors.Is(err, injected) {
		t.Fatalf("ConditionalWriteChunks() error = %v, want injected failure", err)
	}
	assertChunkValue(t, store, first, 61)
	assertChunkValue(t, store, second, 62)
	if version, err := store.ChunkVersion(first); err != nil || version != firstVersion {
		t.Fatalf("first version = %d, %v; want %d, nil", version, err, firstVersion)
	}
	if version, err := store.ChunkVersion(second); err != nil || version != secondVersion {
		t.Fatalf("second version = %d, %v; want %d, nil", version, err, secondVersion)
	}
	store.intentFailpoint = nil
	closeStore(t, store)
}

func TestCommittedFailuresReturnSuccessAndRetryPublication(t *testing.T) {
	root := testTempDir(t)
	g := mustGeometry(t, geometry.Config{ChunkEdge: 1, LargeChunkEdge: 1, BlockBits: 8})
	var events []string
	store := mustOpenWithOptions(t, root, g, Options{
		Durability:        DurabilityFsyncWAL,
		CheckpointRecords: 100,
		CheckpointBytes:   1 << 20,
		PostCommitFailure: func(event string) {
			events = append(events, event)
		},
	})
	first := geometry.Coord{X: 1, Y: 1}
	second := geometry.Coord{X: 2, Y: 2}
	injected := errors.New("injected post-commit publication failure")
	fired := false
	store.atomicWriteFailpoint = func(boundary atomicWriteBoundary) error {
		if boundary == atomicWriteTemporaryCreated && !fired {
			fired = true
			return injected
		}
		return nil
	}
	if err := store.WriteChunk(first, testChunk(t, g, 31)); err != nil {
		t.Fatalf("committed WriteChunk() error = %v, want nil", err)
	}
	if _, err := os.Stat(store.chunkPath(first)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("chunk after injected publication failure stat error = %v, want os.ErrNotExist", err)
	}
	store.atomicWriteFailpoint = nil
	if err := store.WriteChunk(second, testChunk(t, g, 32)); err != nil {
		t.Fatalf("retrying WriteChunk() error = %v", err)
	}
	if len(events) != 1 || events[0] != "committed_chunk_publication_failed" {
		t.Fatalf("post-commit events = %q, want committed chunk publication failure", events)
	}
	if _, err := os.Stat(store.chunkPath(first)); err != nil {
		t.Fatalf("stat retried chunk publication: %v", err)
	}
	assertChunkValue(t, store, first, 31)
	assertChunkValue(t, store, second, 32)
	closeStore(t, store)
}

func TestRollbackRepairFailurePoisonsUntilRecovery(t *testing.T) {
	root := testTempDir(t)
	g := mustGeometry(t, geometry.Config{ChunkEdge: 1, LargeChunkEdge: 1, BlockBits: 8})
	store := mustOpenWithOptions(t, root, g, walRestartCrashOptions())
	coord := geometry.Coord{X: 4, Y: 5}
	if err := store.WriteChunk(coord, testChunk(t, g, 41)); err != nil {
		t.Fatal(err)
	}
	injectedAppend := errors.New("injected append failure")
	store.walFailpoint = func(boundary walBoundary) error {
		if boundary == walRecordAppended {
			return injectedAppend
		}
		return nil
	}
	store.walRollbackFailpoint = func() error {
		return errors.New("injected rollback repair failure")
	}
	err := store.WriteChunk(coord, testChunk(t, g, 42))
	if !errors.Is(err, injectedAppend) || !errors.Is(err, store.durabilityPoisoned) {
		t.Fatalf("WriteChunk() error = %v, want append and fail-closed errors", err)
	}
	store.walFailpoint = nil
	store.walRollbackFailpoint = nil
	if err := store.WriteChunk(coord, testChunk(t, g, 43)); err == nil {
		t.Fatal("WriteChunk() after repair failure succeeded, want fail-closed error")
	}
	closeStore(t, store)

	reopened := mustOpenWithOptions(t, root, g, walRestartCrashOptions())
	assertChunkValue(t, reopened, coord, 41)
	if _, err := os.Stat(reopened.intentPath()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("intent after recovery stat error = %v, want os.ErrNotExist", err)
	}
	closeStore(t, reopened)
}

func TestIntentCorruptionFailsClosed(t *testing.T) {
	root := testTempDir(t)
	g := mustGeometry(t, geometry.Config{ChunkEdge: 1, LargeChunkEdge: 1, BlockBits: 8})
	store := mustOpenWithOptions(t, root, g, walRestartCrashOptions())
	closeStore(t, store)
	intentDirectory := filepath.Join(root, intentDirectoryName)
	if err := os.MkdirAll(intentDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(intentDirectory, intentFileName), []byte("broken"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenWithOptions(root, g, walRestartCrashOptions()); !errors.Is(err, ErrCorruptIntent) {
		t.Fatalf("OpenWithOptions() error = %v, want ErrCorruptIntent", err)
	}
}

func TestIntentControlPathRejectsTraversalAndUnsafeGrammar(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	for _, component := range []string{
		"",
		".",
		"..",
		"../escape",
		`..\escape`,
		"/absolute",
		"contains space",
		"UPPERCASE",
	} {
		if path, err := containedControlPath(root, component); err == nil {
			t.Fatalf("containedControlPath(%q) = %q, want error", component, path)
		}
	}
	for _, component := range []string{intentDirectoryName, intentFileName} {
		path, err := containedControlPath(root, component)
		if err != nil {
			t.Fatalf("containedControlPath(%q): %v", component, err)
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			t.Fatal(err)
		}
		if relative != component {
			t.Fatalf("relative path = %q, want %q", relative, component)
		}
	}
}

func TestIntentDirectorySymlinkFailsClosed(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	outside := t.TempDir()
	target := filepath.Join(outside, intentFileName)
	if err := os.WriteFile(target, encodeIntent(intentCommitted, 0), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, intentDirectoryName)); err != nil {
		t.Skipf("create intent-directory symlink: %v", err)
	}
	g := mustGeometry(t, geometry.Config{ChunkEdge: 1, LargeChunkEdge: 1, BlockBits: 1})
	if _, err := OpenWithOptions(root, g, walRestartCrashOptions()); !errors.Is(err, ErrCorruptIntent) {
		t.Fatalf("OpenWithOptions() error = %v, want ErrCorruptIntent", err)
	}
	contents, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(contents, encodeIntent(intentCommitted, 0)) {
		t.Fatal("intent symlink target was modified")
	}
}

func TestCrashConditionalIntentBoundariesRecoverDecision(t *testing.T) {
	if os.Getenv("REGIONDB_INTENT_CRASH_CHILD") == "1" {
		runIntentCrashChild(t)
		return
	}

	tests := []struct {
		boundary intentBoundary
		want     uint64
	}{
		{intentBeforeRollbackPublish, 51},
		{intentRollbackPublished, 51},
		{intentBeforeCommittedPublish, 51},
		{intentCommittedPublished, 52},
		{intentBeforeClear, 52},
		{intentAfterClear, 52},
	}
	for _, test := range tests {
		test := test
		t.Run(string(test.boundary), func(t *testing.T) {
			root := testTempDir(t)
			g := mustGeometry(t, geometry.Config{ChunkEdge: 1, LargeChunkEdge: 1, BlockBits: 8})
			store := mustOpenWithOptions(t, root, g, walRestartCrashOptions())
			coord := geometry.Coord{X: -6, Y: 7}
			if err := store.WriteChunk(coord, testChunk(t, g, 51)); err != nil {
				t.Fatal(err)
			}
			closeStore(t, store)

			command := exec.Command(os.Args[0], "-test.run=^TestCrashConditionalIntentBoundariesRecoverDecision$")
			command.Env = append(
				os.Environ(),
				"REGIONDB_INTENT_CRASH_CHILD=1",
				"REGIONDB_INTENT_CRASH_ROOT="+root,
				"REGIONDB_INTENT_CRASH_BOUNDARY="+string(test.boundary),
			)
			output, err := command.CombinedOutput()
			var exitErr *exec.ExitError
			if !errors.As(err, &exitErr) || exitErr.ExitCode() != intentCrashExitCode {
				t.Fatalf("crash child error = %v, output:\n%s", err, output)
			}

			expireWriterOwner(t, root)
			reopened := mustOpenWithOptions(t, root, g, walRestartCrashOptions())
			assertChunkValue(t, reopened, coord, test.want)
			if _, err := os.Stat(reopened.intentPath()); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("intent after crash recovery stat error = %v, want os.ErrNotExist", err)
			}
			closeStore(t, reopened)
		})
	}
}

func runIntentCrashChild(t *testing.T) {
	root := os.Getenv("REGIONDB_INTENT_CRASH_ROOT")
	target := intentBoundary(os.Getenv("REGIONDB_INTENT_CRASH_BOUNDARY"))
	g := mustGeometry(t, geometry.Config{ChunkEdge: 1, LargeChunkEdge: 1, BlockBits: 8})
	store := mustOpenWithOptions(t, root, g, walRestartCrashOptions())
	coord := geometry.Coord{X: -6, Y: 7}
	version, err := store.ChunkVersion(coord)
	if err != nil {
		t.Fatal(err)
	}
	store.intentFailpoint = func(boundary intentBoundary) error {
		if boundary == target {
			os.Exit(intentCrashExitCode)
		}
		return nil
	}
	if _, err := store.CompareAndSwapChunk(coord, version, testChunk(t, g, 52)); err != nil {
		t.Fatalf("CompareAndSwapChunk(): %v", err)
	}
	t.Fatalf("conditional write did not reach crash boundary %q", target)
}
