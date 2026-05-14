package fs_split

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/Filo6699/regiondb/internal/geometry"
	"github.com/Filo6699/regiondb/internal/storage"
)

// os.File.Sync resolves to FlushFileBuffers on Windows and to fsync elsewhere.
// Both flush the handle that is still open, so the store must never close the
// WAL append handle in order to make its records durable.
func TestWALSyncFlushesWhileAppendHandleStaysOpen(t *testing.T) {
	t.Parallel()

	root := testTempDir(t)
	g := mustGeometry(t, geometry.Config{ChunkEdge: 1, LargeChunkEdge: 1, BlockBits: 8})
	store := mustOpenWithOptions(t, root, g, Options{
		Durability:        DurabilityFsyncWAL,
		CheckpointRecords: 8,
		CheckpointBytes:   1 << 20,
	})
	coord := geometry.Coord{X: 3, Y: -4}

	for update, value := range []uint64{7, 9} {
		chunk := mustChunk(t, g)
		if err := chunk.Set(geometry.Offset{}, value); err != nil {
			t.Fatal(err)
		}
		if err := store.WriteChunk(coord, chunk); err != nil {
			t.Fatalf("WriteChunk(%d): %v", value, err)
		}
		if flushes := store.RuntimeStats().WALFlushes; flushes != uint64(update)+1 {
			t.Fatalf("WAL flushes after %d updates = %d, want %d", update+1, flushes, update+1)
		}
		// The store still owns the append handle; an independent reader must
		// already observe every synchronized record.
		records := readWALRecords(t, store, filepath.Join(root, walName))
		if len(records) != update+1 {
			t.Fatalf("WAL records visible to an independent handle = %d, want %d", len(records), update+1)
		}
		if records[update].coord != coord {
			t.Fatalf("record %d coordinate = %+v, want %+v", update, records[update].coord, coord)
		}
		replayed, err := storage.ChunkFromBytes(g, records[update].payload)
		if err != nil {
			t.Fatalf("decode record %d payload: %v", update, err)
		}
		got, err := replayed.Get(geometry.Offset{})
		if err != nil {
			t.Fatal(err)
		}
		if got != value {
			t.Fatalf("record %d value = %d, want %d", update, got, value)
		}
	}
}

// The historical Windows workaround closed the append handle before flushing
// it. Go rejects that ordering outright, so Close must synchronize the pending
// WAL tail while the handle is still valid.
func TestCloseSyncsWALWhileHandleIsStillOpen(t *testing.T) {
	t.Parallel()

	g := mustGeometry(t, geometry.Config{ChunkEdge: 1, LargeChunkEdge: 1, BlockBits: 8})
	store := mustOpenWithOptions(t, testTempDir(t), g, Options{
		Durability:            DurabilityFsyncWAL,
		CheckpointRecords:     8,
		CheckpointBytes:       1 << 20,
		WALGroupCommitUpdates: 4,
	})
	handle := store.walHandles.handles[walAppendHandle].file

	chunk := mustChunk(t, g)
	if err := chunk.Set(geometry.Offset{}, 5); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteChunk(geometry.Coord{}, chunk); err != nil {
		t.Fatalf("WriteChunk(): %v", err)
	}
	if flushes := store.RuntimeStats().WALFlushes; flushes != 0 {
		t.Fatalf("WAL flushes before close = %d, want 0", flushes)
	}

	closeStore(t, store)

	if flushes := store.RuntimeStats().WALFlushes; flushes != 1 {
		t.Fatalf("WAL flushes after close = %d, want 1", flushes)
	}
	if err := handle.Sync(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("sync of the closed WAL handle = %v, want %v", err, os.ErrClosed)
	}
}

func TestStoreReusesAppendHandleAcrossCheckpoints(t *testing.T) {
	t.Parallel()

	g := mustGeometry(t, geometry.Config{ChunkEdge: 1, LargeChunkEdge: 1, BlockBits: 8})
	store := mustOpenWithOptions(t, testTempDir(t), g, Options{
		Durability:        DurabilityFsyncCheckpoint,
		CheckpointRecords: 1,
	})
	appendHandle := store.walHandles.handles[walAppendHandle].file

	for update := range 8 {
		chunk := mustChunk(t, g)
		if err := chunk.Set(geometry.Offset{}, uint64(update)); err != nil {
			t.Fatalf("Set(%d): %v", update, err)
		}
		if err := store.WriteChunk(geometry.Coord{X: int64(update)}, chunk); err != nil {
			t.Fatalf("WriteChunk(%d): %v", update, err)
		}
		if store.walHandles.handles[walAppendHandle].file != appendHandle {
			t.Fatalf("WAL append handle changed after checkpoint %d", update)
		}
	}
}

func readWALRecords(t *testing.T, store *Store, path string) []walRecord {
	t.Helper()

	encoded, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read WAL through an independent handle: %v", err)
	}
	recordBytes := walHeaderBytes + store.geometry.PayloadBytes() + checksumSize
	if len(encoded)%recordBytes != 0 {
		t.Fatalf("WAL size %d is not a multiple of the %d byte record", len(encoded), recordBytes)
	}
	records := make([]walRecord, 0, len(encoded)/recordBytes)
	for offset := 0; offset < len(encoded); offset += recordBytes {
		record, err := store.decodeWALRecord(encoded[offset : offset+recordBytes])
		if err != nil {
			t.Fatalf("decode WAL record at offset %d: %v", offset, err)
		}
		records = append(records, record)
	}
	return records
}
