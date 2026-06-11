package storage

import (
	"errors"
	"testing"

	"github.com/Filo6699/regiondb/internal/bitcodec"
	"github.com/Filo6699/regiondb/internal/geometry"
)

func TestChunkRoundTripArbitraryBlockWidth(t *testing.T) {
	t.Parallel()

	g, err := geometry.New(geometry.Config{ChunkEdge: 3, LargeChunkEdge: 2, BlockBits: 5})
	if err != nil {
		t.Fatal(err)
	}
	chunk, err := NewChunk(g)
	if err != nil {
		t.Fatal(err)
	}
	values := map[geometry.Offset]uint64{
		{X: 0, Y: 0}: 1,
		{X: 2, Y: 0}: 31,
		{X: 1, Y: 1}: 18,
		{X: 2, Y: 2}: 7,
	}
	for offset, value := range values {
		if err := chunk.Set(offset, value); err != nil {
			t.Fatalf("Set(%v): %v", offset, err)
		}
	}
	for offset, want := range values {
		got, err := chunk.Get(offset)
		if err != nil {
			t.Fatalf("Get(%v): %v", offset, err)
		}
		if got != want {
			t.Fatalf("Get(%v) = %d, want %d", offset, got, want)
		}
		if exists, err := chunk.Exists(offset); err != nil || !exists {
			t.Fatalf("Exists(%v) = %t, %v, want true", offset, exists, err)
		}
	}

	reopened, err := ChunkFromBytes(g, chunk.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if got, err := reopened.Get(geometry.Offset{X: 1, Y: 1}); err != nil || got != 18 {
		t.Fatalf("reopened Get() = %d, %v", got, err)
	}
}

func TestChunkDistinguishesZeroFromAbsent(t *testing.T) {
	t.Parallel()

	g, err := geometry.New(geometry.Config{ChunkEdge: 2, LargeChunkEdge: 2, BlockBits: 3})
	if err != nil {
		t.Fatal(err)
	}
	chunk, err := NewChunk(g)
	if err != nil {
		t.Fatal(err)
	}
	offset := geometry.Offset{X: 1, Y: 1}
	if exists, err := chunk.Exists(offset); err != nil || exists {
		t.Fatalf("Exists() = %t, %v, want false", exists, err)
	}
	if err := chunk.Set(offset, 0); err != nil {
		t.Fatalf("Set(0): %v", err)
	}
	if exists, err := chunk.Exists(offset); err != nil || !exists {
		t.Fatalf("Exists() after Set(0) = %t, %v, want true", exists, err)
	}
	if err := chunk.Unset(offset); err != nil {
		t.Fatalf("Unset(): %v", err)
	}
	if exists, err := chunk.Exists(offset); err != nil || exists {
		t.Fatalf("Exists() after Unset() = %t, %v, want false", exists, err)
	}
	if value, err := chunk.Get(offset); err != nil || value != 0 {
		t.Fatalf("Get() after Unset() = %d, %v, want 0", value, err)
	}
}

func TestChunkLegacyPresenceUsesNonzeroValues(t *testing.T) {
	t.Parallel()

	g, err := geometry.New(geometry.Config{ChunkEdge: 2, LargeChunkEdge: 2, BlockBits: 3})
	if err != nil {
		t.Fatal(err)
	}
	chunk, err := ChunkFromLegacyBytes(g, []byte{0x38, 0x00})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		offset geometry.Offset
		want   bool
	}{
		{offset: geometry.Offset{X: 0, Y: 0}, want: false},
		{offset: geometry.Offset{X: 1, Y: 0}, want: true},
		{offset: geometry.Offset{X: 0, Y: 1}, want: false},
		{offset: geometry.Offset{X: 1, Y: 1}, want: false},
	} {
		exists, err := chunk.Exists(test.offset)
		if err != nil {
			t.Fatal(err)
		}
		if exists != test.want {
			t.Fatalf("Exists(%v) = %t, want %t", test.offset, exists, test.want)
		}
	}
}

func TestChunkRejectsInvalidAccess(t *testing.T) {
	t.Parallel()

	g, err := geometry.New(geometry.Config{ChunkEdge: 2, LargeChunkEdge: 2, BlockBits: 3})
	if err != nil {
		t.Fatal(err)
	}
	chunk, err := NewChunk(g)
	if err != nil {
		t.Fatal(err)
	}
	if err := chunk.Set(geometry.Offset{X: 2}, 1); !errors.Is(err, ErrBlockOutOfRange) {
		t.Fatalf("Set(out of range) error = %v", err)
	}
	if err := chunk.Set(geometry.Offset{}, 8); !errors.Is(err, bitcodec.ErrValueTooWide) {
		t.Fatalf("Set(wide value) error = %v", err)
	}
	if _, err := ChunkFromBytes(g, make([]byte, g.PayloadBytes()-1)); !errors.Is(err, ErrPayloadSize) {
		t.Fatalf("ChunkFromBytes(short) error = %v", err)
	}
	if _, err := ChunkFromState(g, make([]byte, g.PayloadBytes()), nil); !errors.Is(err, ErrPresenceSize) {
		t.Fatalf("ChunkFromState(short presence) error = %v", err)
	}
}
