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
	ErrPresenceSize    = errors.New("invalid presence bitmap size")
	ErrVersionMismatch = errors.New("chunk version mismatch")
)

type Chunk struct {
	geometry geometry.Geometry
	codec    bitcodec.Codec
	payload  []byte
	presence []byte
}

type ConditionalMutation struct {
	Coord           geometry.Coord
	ExpectedVersion uint64
	Chunk           *Chunk
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
		presence: make([]byte, g.PresenceBytes()),
	}, nil
}

func ChunkFromBytes(g geometry.Geometry, payload []byte) (*Chunk, error) {
	presence := make([]byte, g.PresenceBytes())
	for index := uint64(0); index < g.BlockCount(); index++ {
		presence[index/8] |= byte(1 << (index % 8))
	}
	return ChunkFromState(g, payload, presence)
}

func ChunkFromLegacyBytes(g geometry.Geometry, payload []byte) (*Chunk, error) {
	if len(payload) != g.PayloadBytes() {
		return nil, fmt.Errorf("%w: got %d bytes, want %d", ErrPayloadSize, len(payload), g.PayloadBytes())
	}
	chunk, err := NewChunk(g)
	if err != nil {
		return nil, err
	}
	copy(chunk.payload, payload)
	for index := uint64(0); index < g.BlockCount(); index++ {
		value, err := chunk.codec.Get(chunk.payload, index)
		if err != nil {
			return nil, fmt.Errorf("read legacy packed block: %w", err)
		}
		if value != 0 {
			chunk.setPresent(index)
		}
	}
	return chunk, nil
}

func ChunkFromState(g geometry.Geometry, payload, presence []byte) (*Chunk, error) {
	if len(payload) != g.PayloadBytes() {
		return nil, fmt.Errorf("%w: got %d bytes, want %d", ErrPayloadSize, len(payload), g.PayloadBytes())
	}
	if len(presence) != g.PresenceBytes() {
		return nil, fmt.Errorf("%w: got %d bytes, want %d", ErrPresenceSize, len(presence), g.PresenceBytes())
	}
	chunk, err := NewChunk(g)
	if err != nil {
		return nil, err
	}
	copy(chunk.payload, payload)
	copy(chunk.presence, presence)
	if unused := uint(g.PresenceBytes()*8) - uint(g.BlockCount()); unused != 0 {
		mask := byte(0xff >> unused)
		if chunk.presence[len(chunk.presence)-1]&^mask != 0 {
			return nil, fmt.Errorf("%w: unused high bits are nonzero", ErrPresenceSize)
		}
	}
	return chunk, nil
}

func (c *Chunk) Geometry() geometry.Geometry {
	return c.geometry
}

func (c *Chunk) Bytes() []byte {
	return append([]byte(nil), c.payload...)
}

func (c *Chunk) PresenceBytes() []byte {
	return append([]byte(nil), c.presence...)
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
	c.setPresent(index)
	return nil
}

func (c *Chunk) Unset(offset geometry.Offset) error {
	index, err := c.index(offset)
	if err != nil {
		return err
	}
	if err := c.codec.Set(c.payload, index, 0); err != nil {
		return fmt.Errorf("clear packed block: %w", err)
	}
	c.presence[index/8] &^= byte(1 << (index % 8))
	return nil
}

func (c *Chunk) Exists(offset geometry.Offset) (bool, error) {
	index, err := c.index(offset)
	if err != nil {
		return false, err
	}
	return c.presence[index/8]&(byte(1<<(index%8))) != 0, nil
}

func (c *Chunk) setPresent(index uint64) {
	c.presence[index/8] |= byte(1 << (index % 8))
}

func (c *Chunk) index(offset geometry.Offset) (uint64, error) {
	edge := c.geometry.Config().ChunkEdge
	if offset.X >= edge || offset.Y >= edge {
		return 0, fmt.Errorf("%w: (%d,%d)", ErrBlockOutOfRange, offset.X, offset.Y)
	}
	return uint64(offset.Y)*uint64(edge) + uint64(offset.X), nil
}
