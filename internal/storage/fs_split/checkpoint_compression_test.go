package fs_split

import (
	"bytes"
	"errors"
	"hash/crc32"
	"os"
	"testing"

	"github.com/Filo6699/regiondb/internal/bitcodec"
	"github.com/Filo6699/regiondb/internal/geometry"
	"github.com/Filo6699/regiondb/internal/storage"
)

func TestCheckpointImageV3CompressionModes(t *testing.T) {
	t.Parallel()

	g := mustGeometry(t, geometry.Config{ChunkEdge: 16, LargeChunkEdge: 2, BlockBits: 8})
	coord := geometry.Coord{X: -3, Y: 5}
	for _, test := range []struct {
		name        string
		compression CheckpointCompression
		wantCodec   byte
	}{
		{name: "off by default", wantCodec: imageCodecNone},
		{name: "zrle", compression: CheckpointCompressionZRLE, wantCodec: imageCodecZRLE},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			root := testTempDir(t)
			store := mustOpenWithOptions(t, root, g, Options{
				CheckpointCompression: test.compression,
				CheckpointRecords:     1,
				CheckpointBytes:       1 << 20,
			})
			chunk := mustChunk(t, g)
			explicitZero := geometry.Offset{X: 3, Y: 4}
			if err := chunk.Set(explicitZero, 0); err != nil {
				t.Fatal(err)
			}
			if err := store.WriteChunk(coord, chunk); err != nil {
				t.Fatalf("WriteChunk(): %v", err)
			}
			encoded, err := os.ReadFile(store.chunkPath(coord))
			if err != nil {
				t.Fatal(err)
			}
			if string(encoded[:8]) != fileMagic || encoded[17] != test.wantCodec {
				t.Fatalf(
					"checkpoint image = magic %q codec %d, want %q codec %d",
					encoded[:8],
					encoded[17],
					fileMagic,
					test.wantCodec,
				)
			}
			if test.wantCodec == imageCodecZRLE {
				rawSize := headerBytes + g.PayloadBytes() + g.PresenceBytes() + checksumSize
				if len(encoded) >= rawSize {
					t.Fatalf("compressed image size = %d, want less than %d", len(encoded), rawSize)
				}
			}
			closeStore(t, store)

			reopened := mustOpen(t, root, g)
			defer closeStore(t, reopened)
			got, err := reopened.ReadChunk(coord)
			if err != nil {
				t.Fatalf("ReadChunk(v3): %v", err)
			}
			exists, err := got.Exists(explicitZero)
			if err != nil || !exists {
				t.Fatalf("explicit zero presence = %t, %v, want true", exists, err)
			}
			value, err := got.Get(explicitZero)
			if err != nil || value != 0 {
				t.Fatalf("explicit zero value = %d, %v, want 0", value, err)
			}
		})
	}
}

func TestCheckpointZRLEFallsBackToRawImage(t *testing.T) {
	t.Parallel()

	g := mustGeometry(t, geometry.Config{ChunkEdge: 2, LargeChunkEdge: 1, BlockBits: 8})
	store := &Store{
		geometry: g,
		options: Options{
			CheckpointCompression: CheckpointCompressionZRLE,
		},
	}
	payload := []byte{1, 2, 3, 4}
	presence := []byte{0x0f}
	encoded := store.encodeCheckpointState(geometry.Coord{}, payload, presence)
	if encoded[17] != imageCodecNone {
		t.Fatalf("incompressible image codec = %d, want none", encoded[17])
	}
	chunk, err := store.decode(geometry.Coord{}, encoded)
	if err != nil {
		t.Fatalf("decode raw v3 image: %v", err)
	}
	if !bytes.Equal(chunk.Bytes(), payload) || !bytes.Equal(chunk.PresenceBytes(), presence) {
		t.Fatalf("raw v3 state = %x|%x, want %x|%x", chunk.Bytes(), chunk.PresenceBytes(), payload, presence)
	}
}

func TestCheckpointRejectsMalformedZRLEImage(t *testing.T) {
	t.Parallel()

	g := mustGeometry(t, geometry.Config{ChunkEdge: 4, LargeChunkEdge: 1, BlockBits: 8})
	store := &Store{
		geometry: g,
		options: Options{
			CheckpointCompression: CheckpointCompressionZRLE,
		},
	}
	encoded := store.encodeCheckpointState(
		geometry.Coord{},
		make([]byte, g.PayloadBytes()),
		[]byte{1, 0},
	)
	if encoded[17] != imageCodecZRLE {
		t.Fatal("test image did not use zrle")
	}
	bodyEnd := len(encoded) - checksumSize
	encoded[headerBytes] = 0xff
	binaryChecksum := crc32.ChecksumIEEE(encoded[:bodyEnd])
	copy(encoded[bodyEnd:], bitcodec.AppendUint32(nil, binaryChecksum))
	if _, err := store.decode(geometry.Coord{}, encoded); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("decode malformed zrle error = %v, want ErrCorrupt", err)
	}
}

func TestCheckpointCollectsEmptyChunksButKeepsExplicitZero(t *testing.T) {
	t.Parallel()

	g := mustGeometry(t, geometry.Config{ChunkEdge: 2, LargeChunkEdge: 2, BlockBits: 3})
	store := mustOpenWithOptions(t, testTempDir(t), g, Options{
		CheckpointCompression: CheckpointCompressionZRLE,
		CheckpointRecords:     1,
		CheckpointBytes:       1 << 20,
	})
	defer closeStore(t, store)

	emptyCoord := geometry.Coord{X: 1, Y: 1}
	if err := store.WriteChunk(emptyCoord, mustChunk(t, g)); err != nil {
		t.Fatalf("write empty chunk: %v", err)
	}
	if _, err := store.ReadChunk(emptyCoord); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("ReadChunk(collected empty) error = %v, want os.ErrNotExist", err)
	}
	if version, err := store.ChunkVersion(emptyCoord); err != nil || version == 0 {
		t.Fatalf("collected chunk version = %d, %v, want persisted nonzero version", version, err)
	}
	assertNoChunkTemporaryFiles(t, store.root)

	explicitZeroCoord := geometry.Coord{X: 2, Y: 2}
	explicitZero := mustChunk(t, g)
	if err := explicitZero.Set(geometry.Offset{}, 0); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteChunk(explicitZeroCoord, explicitZero); err != nil {
		t.Fatalf("write explicit zero chunk: %v", err)
	}
	got, err := store.ReadChunk(explicitZeroCoord)
	if err != nil {
		t.Fatalf("ReadChunk(explicit zero): %v", err)
	}
	exists, err := got.Exists(geometry.Offset{})
	if err != nil || !exists {
		t.Fatalf("explicit zero presence = %t, %v, want true", exists, err)
	}
}

func TestV3DecodeRetainsExplicitState(t *testing.T) {
	t.Parallel()

	g := mustGeometry(t, geometry.Config{ChunkEdge: 2, LargeChunkEdge: 2, BlockBits: 3})
	store := &Store{
		geometry: g,
		options: Options{
			CheckpointCompression: CheckpointCompressionZRLE,
		},
	}
	payload := []byte{0x40, 0}
	presence := []byte{0x03}
	encoded := store.encodeCheckpointState(geometry.Coord{X: 3, Y: -2}, payload, presence)
	chunk, err := store.decode(geometry.Coord{X: 3, Y: -2}, encoded)
	if err != nil {
		t.Fatal(err)
	}
	want, err := storage.ChunkFromState(g, payload, presence)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(chunk.Bytes(), want.Bytes()) ||
		!bytes.Equal(chunk.PresenceBytes(), want.PresenceBytes()) {
		t.Fatalf("decoded state = %x|%x, want %x|%x",
			chunk.Bytes(), chunk.PresenceBytes(), want.Bytes(), want.PresenceBytes())
	}
}
