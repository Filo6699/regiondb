package storage

import (
	"errors"
	"fmt"

	"github.com/Filo6699/regiondb/internal/bitcodec"
	"github.com/Filo6699/regiondb/internal/geometry"
)

var (
	ErrBlockOutOfRange = errors.New("block offset out of range")
	ErrPayloadSize     = errors.New("invalid packed payload size")
)

type Chunk struct {
	geometry geometry.Geometry
	codec    bitcodec.Codec
	payload  []byte
}

func NewChunk(g geometry.Geometry) (*Chunk, error) {
	codec, err := bitcodec.New(g.Config().BlockBits, bitcodec.LSBFirst)
	if err != nil {
		return nil, fmt.Errorf("create block codec: %w", err)
	}
	return &Chunk{
		geometry: g,
		codec:    codec,
		payload:  make([]byte, g.PayloadBytes()),
	}, nil
}

func ChunkFromBytes(g geometry.Geometry, payload []byte) (*Chunk, error) {
	if len(payload) != g.PayloadBytes() {
		return nil, fmt.Errorf("%w: got %d bytes, want %d", ErrPayloadSize, len(payload), g.PayloadBytes())
	}
	chunk, err := NewChunk(g)
	if err != nil {
		return nil, err
	}
	copy(chunk.payload, payload)
	return chunk, nil
}

func (c *Chunk) Geometry() geometry.Geometry {
	return c.geometry
}

func (c *Chunk) Bytes() []byte {
	return append([]byte(nil), c.payload...)
}

func (c *Chunk) Get(offset geometry.Offset) (uint64, error) {
	index, err := c.index(offset)
	if err != nil {
		return 0, err
	}
	value, err := c.codec.Get(c.payload, index)
	if err != nil {
		return 0, fmt.Errorf("read packed block: %w", err)
	}
	return value, nil
}

func (c *Chunk) Set(offset geometry.Offset, value uint64) error {
	index, err := c.index(offset)
	if err != nil {
		return err
	}
	if err := c.codec.Set(c.payload, index, value); err != nil {
		return fmt.Errorf("write packed block: %w", err)
	}
	return nil
}

func (c *Chunk) index(offset geometry.Offset) (uint64, error) {
	edge := c.geometry.Config().ChunkEdge
	if offset.X >= edge || offset.Y >= edge {
		return 0, fmt.Errorf("%w: (%d,%d)", ErrBlockOutOfRange, offset.X, offset.Y)
	}
	return uint64(offset.Y)*uint64(edge) + uint64(offset.X), nil
}
