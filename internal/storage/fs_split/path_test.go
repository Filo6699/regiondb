package fs_split

import (
	"bytes"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Filo6699/regiondb/internal/geometry"
)

// Windows rejects these characters in path components outright.
const windowsForbiddenPathCharacters = `<>:"/\|?*`

// Windows treats these names as devices, with or without an extension.
var windowsReservedNames = map[string]struct{}{
	"con": {}, "prn": {}, "aux": {}, "nul": {},
	"com1": {}, "com2": {}, "com3": {}, "com4": {}, "com5": {},
	"com6": {}, "com7": {}, "com8": {}, "com9": {},
	"lpt1": {}, "lpt2": {}, "lpt3": {}, "lpt4": {}, "lpt5": {},
	"lpt6": {}, "lpt7": {}, "lpt8": {}, "lpt9": {},
}

func TestChunkPathsUsePortableComponents(t *testing.T) {
	t.Parallel()

	root := testTempDir(t)
	g := mustGeometry(t, geometry.Config{ChunkEdge: 2, LargeChunkEdge: 4, BlockBits: 8})
	// Path generation is independent of opening files. Keeping this test
	// handle-free lets Windows clean up the temporary directory immediately.
	store := &Store{root: root, geometry: g}

	coords := []geometry.Coord{
		{},
		{X: 1, Y: 1},
		{X: -1, Y: -1},
		{X: 5, Y: -5},
		{X: -5, Y: 5},
		{X: math.MinInt64, Y: math.MaxInt64},
		{X: math.MaxInt64, Y: math.MinInt64},
	}
	// Case-insensitive filesystems, including default Windows and macOS
	// volumes, collapse components that differ only in case, so distinct
	// coordinates must stay distinct after folding case.
	folded := make(map[string]geometry.Coord, len(coords))
	for _, coord := range coords {
		path := store.chunkPath(coord)
		relative, err := filepath.Rel(root, path)
		if err != nil {
			t.Fatalf("chunkPath(%v) is not below the data directory: %v", coord, err)
		}
		components := strings.Split(relative, string(filepath.Separator))
		if len(components) != 2 {
			t.Fatalf("chunkPath(%v) relative components = %v, want a large-chunk directory and a file", coord, components)
		}
		for _, component := range components {
			assertPortablePathComponent(t, coord, component)
		}
		key := strings.ToLower(relative)
		if previous, clash := folded[key]; clash {
			t.Fatalf("chunkPath(%v) and chunkPath(%v) collide as %q", previous, coord, key)
		}
		folded[key] = coord
	}
}

func TestWriteAtomicReleasesFileHandlesBeforeReturn(t *testing.T) {
	t.Parallel()

	root := testTempDir(t)
	path := filepath.Join(root, "chunk.rdb")
	payload := []byte("packed chunk")
	if err := writeAtomic(path, payload, false); err != nil {
		t.Fatalf("writeAtomic(): %v", err)
	}

	// Windows rejects rename and delete while this process still has an
	// incompatible file handle open. Immediate success verifies lifecycle
	// ordering without depending on filesystem timing or sleeps.
	renamed := filepath.Join(root, "renamed.rdb")
	if err := os.Rename(path, renamed); err != nil {
		t.Fatalf("rename completed file: %v", err)
	}
	got, err := os.ReadFile(renamed)
	if err != nil {
		t.Fatalf("read renamed file: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("renamed payload = %q, want %q", got, payload)
	}
	if err := os.Remove(renamed); err != nil {
		t.Fatalf("remove renamed file: %v", err)
	}
}

func assertPortablePathComponent(t *testing.T, coord geometry.Coord, component string) {
	t.Helper()

	if component == "" || component == "." || component == ".." {
		t.Fatalf("chunkPath(%v) has the non-name component %q", coord, component)
	}
	if index := strings.IndexAny(component, windowsForbiddenPathCharacters); index >= 0 {
		t.Fatalf("chunkPath(%v) component %q contains the reserved character %q", coord, component, component[index])
	}
	for _, character := range component {
		if character < 0x20 || character == 0x7f {
			t.Fatalf("chunkPath(%v) component %q contains a control character", coord, component)
		}
	}
	if strings.HasSuffix(component, " ") || strings.HasSuffix(component, ".") {
		t.Fatalf("chunkPath(%v) component %q ends with a space or dot", coord, component)
	}
	if lowered := strings.ToLower(component); lowered != component {
		t.Fatalf("chunkPath(%v) component %q is not case-stable", coord, component)
	}
	base := strings.TrimSuffix(component, filepath.Ext(component))
	if _, reserved := windowsReservedNames[base]; reserved {
		t.Fatalf("chunkPath(%v) component %q uses the reserved device name %q", coord, component, base)
	}
}
