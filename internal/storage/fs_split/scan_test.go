package fs_split

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/Filo6699/regiondb/internal/geometry"
)

func TestBoundedChunkCoordsDeduplicatesAndKeepsSmallestWindow(t *testing.T) {
	t.Parallel()

	candidates := newBoundedChunkCoords(true, geometry.Coord{X: -1, Y: 9}, 3)
	for _, coord := range []geometry.Coord{
		{X: 4}, {X: 1}, {X: 3}, {X: 1}, {X: -1, Y: 9}, {X: 2}, {X: 5},
	} {
		candidates.insert(coord)
		if len(candidates.coords) > 3 {
			t.Fatalf("candidate count = %d, want at most 3", len(candidates.coords))
		}
	}
	want := []geometry.Coord{{X: 1}, {X: 2}, {X: 3}}
	got := candidates.sorted()
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("sorted candidates = %v, want %v", got, want)
	}
	if !candidates.overflow {
		t.Fatal("overflow = false, want true")
	}
}

func TestScanChunkCoordsPaginatesDirtyWorld(t *testing.T) {
	t.Parallel()

	root := testTempDir(t)
	g := mustGeometry(t, geometry.Config{ChunkEdge: 1, LargeChunkEdge: 4, BlockBits: 1})
	store := mustOpen(t, root, g)
	defer closeStore(t, store)
	chunk := mustChunk(t, g)
	if err := chunk.Set(geometry.Offset{}, 1); err != nil {
		t.Fatal(err)
	}
	for index := range 300 {
		if err := store.WriteChunk(geometry.Coord{X: int64(index)}, chunk); err != nil {
			t.Fatalf("WriteChunk(%d): %v", index, err)
		}
	}

	dirtyDirectory := filepath.Join(root, "l_p999_p999")
	if err := os.MkdirAll(dirtyDirectory, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{
		"c_p0_p0.rdb",
		"c_p001_p0.rdb",
		"c_n0_p0.rdb",
		"c_p999_p0.tmp",
		"unrelated.rdb",
	} {
		if err := os.WriteFile(filepath.Join(dirtyDirectory, name), []byte("dirty"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Mkdir(filepath.Join(dirtyDirectory, "c_p400_p0.rdb"), 0o755); err != nil {
		t.Fatal(err)
	}
	unrelatedDirectory := filepath.Join(root, "unrelated")
	if err := os.MkdirAll(unrelatedDirectory, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(unrelatedDirectory, "c_n1_p0.rdb"),
		[]byte("dirty"),
		0o600,
	); err != nil {
		t.Fatal(err)
	}

	first, more, err := store.ScanChunkCoords(false, geometry.Coord{}, 256)
	if err != nil {
		t.Fatalf("ScanChunkCoords(first): %v", err)
	}
	if len(first) != 256 || first[0] != (geometry.Coord{}) ||
		first[len(first)-1] != (geometry.Coord{X: 255}) {
		t.Fatalf("first page bounds = %v..%v (%d), want (0,0)..(255,0) (256)", first[0], first[len(first)-1], len(first))
	}
	if !more {
		t.Fatal("first page more = false, want true")
	}

	second, more, err := store.ScanChunkCoords(true, first[len(first)-1], 256)
	if err != nil {
		t.Fatalf("ScanChunkCoords(second): %v", err)
	}
	if len(second) != 44 || second[0] != (geometry.Coord{X: 256}) ||
		second[len(second)-1] != (geometry.Coord{X: 299}) {
		t.Fatalf("second page bounds = %v..%v (%d), want (256,0)..(299,0) (44)", second[0], second[len(second)-1], len(second))
	}
	if more {
		t.Fatal("second page more = true, want false")
	}
}

func TestParseChunkFileNameCoversInt64Domain(t *testing.T) {
	t.Parallel()

	for _, coord := range []geometry.Coord{
		{},
		{X: -1, Y: 1},
		{X: -9223372036854775808, Y: 9223372036854775807},
	} {
		name := "c_" + signedName(coord.X) + "_" + signedName(coord.Y) + ".rdb"
		got, ok := parseChunkFileName(name)
		if !ok || got != coord {
			t.Fatalf("parseChunkFileName(%q) = %v, %t, want %v, true", name, got, ok, coord)
		}
	}
}

func TestScanChunkCoordsRejectsUnboundedLimit(t *testing.T) {
	t.Parallel()

	g := mustGeometry(t, geometry.Config{ChunkEdge: 1, LargeChunkEdge: 1, BlockBits: 1})
	store := mustOpen(t, testTempDir(t), g)
	defer closeStore(t, store)
	if _, _, err := store.ScanChunkCoords(false, geometry.Coord{}, maxScanChunkCandidates+1); err == nil {
		t.Fatal("ScanChunkCoords() accepted an unbounded limit")
	}
}
