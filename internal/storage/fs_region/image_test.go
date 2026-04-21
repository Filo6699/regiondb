package fs_region

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/Filo6699/regiondb/internal/geometry"
)

func TestRegionImageRejectsDamagedFile(t *testing.T) {
	t.Parallel()

	config := geometry.Config{ChunkEdge: 2, LargeChunkEdge: 2, BlockBits: 5}
	tests := map[string]struct {
		read   geometry.Coord
		mutate func(image []byte, layout imageLayout) []byte
		want   error
	}{
		"invalid magic": {
			mutate: func(image []byte, _ imageLayout) []byte {
				image[0] ^= 0xff
				return image
			},
			want: ErrCorrupt,
		},
		"header checksum mismatch": {
			mutate: func(image []byte, _ imageLayout) []byte {
				image[12] ^= 0x01
				return image
			},
			want: ErrCorrupt,
		},
		"unsupported format version": {
			mutate: func(image []byte, _ imageLayout) []byte {
				binary.LittleEndian.PutUint32(image[8:12], FormatVersion+1)
				resealHeader(image)
				return image
			},
			want: ErrUnsupportedVersion,
		},
		"geometry mismatch": {
			mutate: func(image []byte, _ imageLayout) []byte {
				binary.LittleEndian.PutUint32(image[12:16], config.ChunkEdge+1)
				resealHeader(image)
				return image
			},
			want: ErrGeometryMismatch,
		},
		"nonzero reserved header byte": {
			mutate: func(image []byte, _ imageLayout) []byte {
				image[21] = 1
				resealHeader(image)
				return image
			},
			want: ErrCorrupt,
		},
		"region coordinate mismatch": {
			mutate: func(image []byte, _ imageLayout) []byte {
				binary.LittleEndian.PutUint64(image[24:32], 7)
				resealHeader(image)
				return image
			},
			want: ErrCorrupt,
		},
		"slot size mismatch": {
			mutate: func(image []byte, layout imageLayout) []byte {
				binary.LittleEndian.PutUint64(image[48:56], layout.slotBytes+1)
				resealHeader(image)
				return image
			},
			want: ErrCorrupt,
		},
		"slot directory checksum mismatch": {
			mutate: func(image []byte, _ imageLayout) []byte {
				image[headerBytes+checksumSize] ^= 0x01
				return image
			},
			want: ErrCorrupt,
		},
		"truncated image": {
			mutate: func(image []byte, _ imageLayout) []byte {
				return image[:len(image)-1]
			},
			want: ErrCorrupt,
		},
		"damaged slot payload": {
			mutate: func(image []byte, layout imageLayout) []byte {
				image[layout.slotOffset(0)] ^= 0x80
				return image
			},
			want: ErrCorrupt,
		},
		"presence bit without a published slot": {
			// Slot 3 holds chunk (1,1) of region (0,0) and was never written, so a
			// forged presence bit must not pass its slot checksum.
			read: geometry.Coord{X: 1, Y: 1},
			mutate: func(image []byte, layout imageLayout) []byte {
				image[headerBytes+checksumSize] |= 1 << 3
				resealDirectory(image, layout)
				return image
			},
			want: ErrCorrupt,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			root := t.TempDir()
			g := mustGeometry(t, config)
			layout, err := newImageLayout(g)
			if err != nil {
				t.Fatal(err)
			}
			store := mustOpen(t, root, g)
			if err := store.WriteChunk(geometry.Coord{X: 0, Y: 0}, mustChunk(t, g)); err != nil {
				t.Fatal(err)
			}
			closeStore(t, store)

			path := filepath.Join(root, "r_p0_p0.rdbregion")
			image, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, test.mutate(image, layout), 0o600); err != nil {
				t.Fatal(err)
			}

			damaged := mustOpen(t, root, g)
			defer closeStore(t, damaged)
			if _, err := damaged.ReadChunk(test.read); !errors.Is(err, test.want) {
				t.Fatalf("ReadChunk() error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestRegionImageWithoutHeaderHoldsNoChunk(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	g := mustGeometry(t, geometry.Config{ChunkEdge: 2, LargeChunkEdge: 2, BlockBits: 5})
	path := filepath.Join(root, "r_p0_p0.rdbregion")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	store := mustOpen(t, root, g)
	defer closeStore(t, store)

	// A writer interrupted between creating and initializing an image published
	// no slot, so a reader reports the chunk as missing and a writer reinitializes
	// the image in place.
	coord := geometry.Coord{X: 1, Y: 0}
	if _, err := store.ReadChunk(coord); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("ReadChunk(uninitialized image) error = %v, want fs.ErrNotExist", err)
	}
	chunk := mustChunk(t, g)
	if err := chunk.Set(geometry.Offset{X: 1, Y: 1}, 9); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteChunk(coord, chunk); err != nil {
		t.Fatalf("WriteChunk(uninitialized image): %v", err)
	}
	stored, err := store.ReadChunk(coord)
	if err != nil {
		t.Fatalf("ReadChunk(): %v", err)
	}
	got, err := stored.Get(geometry.Offset{X: 1, Y: 1})
	if err != nil {
		t.Fatal(err)
	}
	if got != 9 {
		t.Fatalf("Get() = %d, want 9", got)
	}
}

func resealHeader(image []byte) {
	binary.LittleEndian.PutUint32(image[56:60], crc32.ChecksumIEEE(image[:56]))
}

func resealDirectory(image []byte, layout imageLayout) {
	directory := image[headerBytes : headerBytes+int(layout.directoryBytes)]
	binary.LittleEndian.PutUint32(
		directory[:checksumSize],
		crc32.ChecksumIEEE(directory[checksumSize:]),
	)
}
