package fs_split

import (
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Filo6699/regiondb/internal/geometry"
)

var compatibilityFixtureCoord = geometry.Coord{X: 3, Y: -2}

func TestCompatibilityChunkFixtures(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name                string
		fixture             string
		explicitZeroPresent bool
	}{
		{name: "RGDBSPL1", fixture: "chunk-rgdbspl1.hex"},
		{name: "RGDBSPL2", fixture: "chunk-rgdbspl2.hex", explicitZeroPresent: true},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			root := testTempDir(t)
			path := filepath.Join(root, "l_p1_n1", "c_p3_n2.rdb")
			if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, readCompatibilityFixture(t, test.fixture), 0o600); err != nil {
				t.Fatal(err)
			}

			g := mustGeometry(t, geometry.Config{ChunkEdge: 2, LargeChunkEdge: 2, BlockBits: 3})
			store := mustOpenWithOptions(t, root, g, Options{ReadOnly: true})
			defer closeStore(t, store)
			assertCompatibilityState(t, store, test.explicitZeroPresent)
		})
	}
}

func TestCompatibilityWALFixtures(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name                string
		fixture             string
		explicitZeroPresent bool
	}{
		{name: "RGDBWAL1", fixture: "wal-rgdbwal1.hex"},
		{name: "RGDBWAL2", fixture: "wal-rgdbwal2.hex", explicitZeroPresent: true},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			root := testTempDir(t)
			if err := os.WriteFile(
				filepath.Join(root, walName),
				readCompatibilityFixture(t, test.fixture),
				0o600,
			); err != nil {
				t.Fatal(err)
			}

			g := mustGeometry(t, geometry.Config{ChunkEdge: 2, LargeChunkEdge: 2, BlockBits: 3})
			store := mustOpen(t, root, g)
			defer closeStore(t, store)
			assertCompatibilityState(t, store, test.explicitZeroPresent)
		})
	}
}

func readCompatibilityFixture(t *testing.T, name string) []byte {
	t.Helper()

	encoded, err := os.ReadFile(filepath.Join("testdata", "compatibility", name))
	if err != nil {
		t.Fatalf("read compatibility fixture %q: %v", name, err)
	}
	decoded, err := hex.DecodeString(strings.TrimSpace(string(encoded)))
	if err != nil {
		t.Fatalf("decode compatibility fixture %q: %v", name, err)
	}
	return decoded
}

func assertCompatibilityState(t *testing.T, store *Store, explicitZeroPresent bool) {
	t.Helper()

	chunk, err := store.ReadChunk(compatibilityFixtureCoord)
	if err != nil {
		t.Fatalf("ReadChunk(%+v): %v", compatibilityFixtureCoord, err)
	}
	if value, err := chunk.Get(geometry.Offset{X: 1}); err != nil || value != 5 {
		t.Fatalf("fixture nonzero value = %d, %v, want 5", value, err)
	}
	if exists, err := chunk.Exists(geometry.Offset{}); err != nil || exists != explicitZeroPresent {
		t.Fatalf(
			"fixture explicit-zero presence = %t, %v, want %t",
			exists,
			err,
			explicitZeroPresent,
		)
	}
	if exists, err := chunk.Exists(geometry.Offset{X: 1}); err != nil || !exists {
		t.Fatalf("fixture nonzero presence = %t, %v, want true", exists, err)
	}
	if exists, err := chunk.Exists(geometry.Offset{Y: 1}); err != nil || exists {
		t.Fatalf("fixture absent-block presence = %t, %v, want false", exists, err)
	}
}
