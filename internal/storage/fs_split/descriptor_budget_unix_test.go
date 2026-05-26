//go:build darwin || linux

package fs_split

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/Filo6699/regiondb/internal/geometry"
	"golang.org/x/sys/unix"
)

const lowDescriptorHelper = "REGIONDB_LOW_DESCRIPTOR_HELPER"

func TestStoreClampsWALStreamsUnderLowDescriptorLimit(t *testing.T) {
	if os.Getenv(lowDescriptorHelper) == "1" {
		runLowDescriptorStore(t)
		return
	}

	root := filepath.Join(t.TempDir(), "data")
	command := exec.Command(
		os.Args[0],
		"-test.run=^TestStoreClampsWALStreamsUnderLowDescriptorLimit$",
	)
	command.Env = append(os.Environ(), lowDescriptorHelper+"=1", "REGIONDB_LOW_DESCRIPTOR_ROOT="+root)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("low-descriptor subprocess failed: %v\n%s", err, output)
	}
}

func runLowDescriptorStore(t *testing.T) {
	t.Helper()

	var limit unix.Rlimit
	if err := unix.Getrlimit(unix.RLIMIT_NOFILE, &limit); err != nil {
		t.Fatalf("get descriptor limit: %v", err)
	}
	const lowLimit = uint64(64)
	limitedSoft := min(limit.Cur, lowLimit)
	if limitedSoft < descriptorReserve+16 {
		t.Skipf("descriptor limit is too low for the subprocess test: %d", limit.Cur)
	}
	limit.Cur = limitedSoft
	if err := unix.Setrlimit(unix.RLIMIT_NOFILE, &limit); err != nil {
		t.Fatalf("set descriptor limit: %v", err)
	}

	g, err := geometry.New(geometry.Config{ChunkEdge: 1, LargeChunkEdge: 1, BlockBits: 8})
	if err != nil {
		t.Fatal(err)
	}
	store, err := OpenWithOptions(os.Getenv("REGIONDB_LOW_DESCRIPTOR_ROOT"), g, Options{
		CheckpointRecords: 1,
		MaxOpenWALHandles: 1024,
	})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	wantCap := int(limitedSoft - descriptorReserve)
	if got := store.options.MaxOpenWALHandles; got != wantCap {
		t.Fatalf("effective WAL stream cap = %d, want %d", got, wantCap)
	}
	chunk := mustChunk(t, g)
	if err := chunk.Set(geometry.Offset{}, 7); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteChunk(geometry.Coord{X: 1, Y: 1}, chunk); err != nil {
		t.Fatalf("write chunk: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
}
