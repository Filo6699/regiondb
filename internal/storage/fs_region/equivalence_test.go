//go:build regiondb_experimental

package fs_region

import (
	"errors"
	"fmt"
	"io/fs"
	"slices"
	"testing"

	"github.com/Filo6699/regiondb/internal/benchmark"
	"github.com/Filo6699/regiondb/internal/geometry"
	"github.com/Filo6699/regiondb/internal/storage"
	"github.com/Filo6699/regiondb/internal/storage/fs_split"
)

// layoutStore is the chunk behavior the experimental layout has to reproduce
// from the production layout. Both the equivalence test and the layout benchmark
// drive a backend through this slice only.
type layoutStore interface {
	WriteChunk(geometry.Coord, *storage.Chunk) error
	ReadChunk(geometry.Coord) (*storage.Chunk, error)
	Close() error
}

type layoutBackend struct {
	name string
	open func(root string, g geometry.Geometry) (layoutStore, error)
}

func layoutBackends() []layoutBackend {
	return []layoutBackend{
		{
			name: FormatName,
			open: func(root string, g geometry.Geometry) (layoutStore, error) {
				return Open(root, g)
			},
		},
		{
			name: "fs_split_v1",
			open: func(root string, g geometry.Geometry) (layoutStore, error) {
				return fs_split.Open(root, g)
			},
		},
	}
}

// TestLayoutsAgreeOnChunkOperations records the observable outcome of one
// deterministic operation script per backend and requires both records to be
// identical. Only the outcome class is recorded, because the layouts phrase
// their error messages differently.
func TestLayoutsAgreeOnChunkOperations(t *testing.T) {
	t.Parallel()

	g := mustGeometry(t, geometry.Config{ChunkEdge: 4, LargeChunkEdge: 2, BlockBits: 5})
	records := make(map[string][]string)
	for _, backend := range layoutBackends() {
		records[backend.name] = observeLayoutScript(t, g, backend)
	}
	reference := records[FormatName]
	for name, record := range records {
		if !slices.Equal(record, reference) {
			t.Fatalf("%s observations = %q, want %q", name, record, reference)
		}
	}
	if len(reference) == 0 {
		t.Fatal("layout script recorded no observations")
	}
}

func observeLayoutScript(t *testing.T, g geometry.Geometry, backend layoutBackend) []string {
	t.Helper()

	root := t.TempDir()
	store, err := backend.open(root, g)
	if err != nil {
		t.Fatalf("open %s: %v", backend.name, err)
	}
	coords := []geometry.Coord{
		{X: 0, Y: 0},
		{X: 1, Y: 1},
		{X: -1, Y: -1},
		{X: 5, Y: 3},
		{X: -7, Y: 6},
	}
	var observations []string

	for _, coord := range coords {
		observations = append(observations, observeRead(t, store, coord))
	}
	for index, coord := range coords {
		observations = append(
			observations,
			observeWrite(t, store, g, coord, byte(index+1)),
			observeRead(t, store, coord),
		)
	}
	// An overwrite has to replace the published payload of the first chunk only.
	observations = append(
		observations,
		observeWrite(t, store, g, coords[0], 0xa5),
	)
	for _, coord := range coords {
		observations = append(observations, observeRead(t, store, coord))
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close %s: %v", backend.name, err)
	}

	reopened, err := backend.open(root, g)
	if err != nil {
		t.Fatalf("reopen %s: %v", backend.name, err)
	}
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Errorf("close reopened %s: %v", backend.name, err)
		}
	}()
	for _, coord := range coords {
		observations = append(observations, observeRead(t, reopened, coord))
	}
	observations = append(observations, observeRead(t, reopened, geometry.Coord{X: 128, Y: -128}))
	return observations
}

func observeWrite(
	t *testing.T,
	store layoutStore,
	g geometry.Geometry,
	coord geometry.Coord,
	value byte,
) string {
	t.Helper()

	chunk, err := storage.ChunkFromBytes(g, benchmark.Payload(g, value))
	if err != nil {
		t.Fatalf("storage.ChunkFromBytes(): %v", err)
	}
	return fmt.Sprintf("write(%d,%d)=%s", coord.X, coord.Y, outcome(store.WriteChunk(coord, chunk)))
}

func observeRead(t *testing.T, store layoutStore, coord geometry.Coord) string {
	t.Helper()

	chunk, err := store.ReadChunk(coord)
	if err != nil {
		return fmt.Sprintf("read(%d,%d)=%s", coord.X, coord.Y, outcome(err))
	}
	return fmt.Sprintf("read(%d,%d)=%x", coord.X, coord.Y, chunk.Bytes())
}

func outcome(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, fs.ErrNotExist):
		return "absent"
	default:
		return "error"
	}
}
