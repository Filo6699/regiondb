package geometry

import (
	"errors"
	"math"
	"testing"
)

func TestBlockToChunkUsesFloorDivision(t *testing.T) {
	t.Parallel()

	g := mustGeometry(t, Config{ChunkEdge: 16, LargeChunkEdge: 8, BlockBits: 5})
	tests := []struct {
		name  string
		block Coord
		want  BlockMapping
	}{
		{"origin", Coord{0, 0}, BlockMapping{Coord{0, 0}, Offset{0, 0}}},
		{"positive boundary", Coord{16, 31}, BlockMapping{Coord{1, 1}, Offset{0, 15}}},
		{"negative one", Coord{-1, -1}, BlockMapping{Coord{-1, -1}, Offset{15, 15}}},
		{"negative boundary", Coord{-16, -17}, BlockMapping{Coord{-1, -2}, Offset{0, 15}}},
		{"minimum coordinate", Coord{math.MinInt64, math.MinInt64}, BlockMapping{
			Coord{math.MinInt64 / 16, math.MinInt64 / 16},
			Offset{0, 0},
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := g.BlockToChunk(test.block); got != test.want {
				t.Fatalf("BlockToChunk(%v) = %v, want %v", test.block, got, test.want)
			}
		})
	}
}

func TestChunkToLargeChunkUsesFloorDivision(t *testing.T) {
	t.Parallel()

	g := mustGeometry(t, Config{ChunkEdge: 16, LargeChunkEdge: 4, BlockBits: 1})
	tests := []struct {
		chunk Coord
		want  ChunkMapping
	}{
		{Coord{3, 4}, ChunkMapping{Coord{0, 1}, Offset{3, 0}}},
		{Coord{-1, -4}, ChunkMapping{Coord{-1, -1}, Offset{3, 0}}},
		{Coord{-5, -8}, ChunkMapping{Coord{-2, -2}, Offset{3, 0}}},
	}

	for _, test := range tests {
		if got := g.ChunkToLargeChunk(test.chunk); got != test.want {
			t.Errorf("ChunkToLargeChunk(%v) = %v, want %v", test.chunk, got, test.want)
		}
	}
}

func TestNewRejectsInvalidGeometry(t *testing.T) {
	t.Parallel()

	tests := []Config{
		{ChunkEdge: 0, LargeChunkEdge: 1, BlockBits: 1},
		{ChunkEdge: 1, LargeChunkEdge: 0, BlockBits: 1},
		{ChunkEdge: 1, LargeChunkEdge: 1, BlockBits: 0},
		{ChunkEdge: 1, LargeChunkEdge: 1, BlockBits: 65},
		{ChunkEdge: math.MaxUint32, LargeChunkEdge: 1, BlockBits: 64},
	}

	for _, config := range tests {
		config := config
		t.Run(config.String(), func(t *testing.T) {
			t.Parallel()
			_, err := New(config)
			if !errors.Is(err, ErrInvalid) {
				t.Fatalf("New(%+v) error = %v, want ErrInvalid", config, err)
			}
		})
	}
}

func TestPayloadSizeRoundsPartialBytes(t *testing.T) {
	t.Parallel()

	g := mustGeometry(t, Config{ChunkEdge: 3, LargeChunkEdge: 2, BlockBits: 5})
	if got, want := g.BlockCount(), uint64(9); got != want {
		t.Fatalf("BlockCount() = %d, want %d", got, want)
	}
	if got, want := g.PayloadBytes(), 6; got != want {
		t.Fatalf("PayloadBytes() = %d, want %d", got, want)
	}
}

func mustGeometry(t *testing.T, config Config) Geometry {
	t.Helper()
	g, err := New(config)
	if err != nil {
		t.Fatalf("New(%+v): %v", config, err)
	}
	return g
}
