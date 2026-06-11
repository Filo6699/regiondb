package geometry

import (
	"errors"
	"fmt"
	"math"
	"strconv"
)

var ErrInvalid = errors.New("invalid geometry")

type Config struct {
	ChunkEdge      uint32
	LargeChunkEdge uint32
	BlockBits      uint8
}

type Geometry struct {
	config        Config
	blockCount    uint64
	payloadBytes  int
	presenceBytes int
}

type Coord struct {
	X int64
	Y int64
}

type Offset struct {
	X uint32
	Y uint32
}

type BlockMapping struct {
	Chunk  Coord
	Offset Offset
}

type ChunkMapping struct {
	LargeChunk Coord
	Offset     Offset
}

func New(config Config) (Geometry, error) {
	if config.ChunkEdge == 0 {
		return Geometry{}, fmt.Errorf("%w: chunk edge must be positive", ErrInvalid)
	}
	if config.LargeChunkEdge == 0 {
		return Geometry{}, fmt.Errorf("%w: large chunk edge must be positive", ErrInvalid)
	}
	if config.BlockBits == 0 || config.BlockBits > 64 {
		return Geometry{}, fmt.Errorf("%w: block bits must be between 1 and 64", ErrInvalid)
	}

	blockCount, ok := checkedMul(uint64(config.ChunkEdge), uint64(config.ChunkEdge))
	if !ok {
		return Geometry{}, fmt.Errorf("%w: chunk block count overflows", ErrInvalid)
	}
	payloadBits, ok := checkedMul(blockCount, uint64(config.BlockBits))
	if !ok {
		return Geometry{}, fmt.Errorf("%w: chunk payload bit count overflows", ErrInvalid)
	}
	payloadBytes := payloadBits / 8
	if payloadBits%8 != 0 {
		payloadBytes++
	}
	if payloadBytes > uint64(maxInt()) {
		return Geometry{}, fmt.Errorf("%w: chunk payload is too large", ErrInvalid)
	}
	presenceBytes := blockCount / 8
	if blockCount%8 != 0 {
		presenceBytes++
	}
	if presenceBytes > uint64(maxInt()) {
		return Geometry{}, fmt.Errorf("%w: chunk presence bitmap is too large", ErrInvalid)
	}

	return Geometry{
		config:        config,
		blockCount:    blockCount,
		payloadBytes:  int(payloadBytes),
		presenceBytes: int(presenceBytes),
	}, nil
}

func (g Geometry) Config() Config {
	return g.config
}

func (g Geometry) BlockCount() uint64 {
	return g.blockCount
}

func (g Geometry) PayloadBytes() int {
	return g.payloadBytes
}

func (g Geometry) PresenceBytes() int {
	return g.presenceBytes
}

func (g Geometry) BlockToChunk(block Coord) BlockMapping {
	x, localX := floorDiv(block.X, int64(g.config.ChunkEdge))
	y, localY := floorDiv(block.Y, int64(g.config.ChunkEdge))
	return BlockMapping{
		Chunk:  Coord{X: x, Y: y},
		Offset: Offset{X: uint32(localX), Y: uint32(localY)},
	}
}

func (g Geometry) ChunkToLargeChunk(chunk Coord) ChunkMapping {
	x, localX := floorDiv(chunk.X, int64(g.config.LargeChunkEdge))
	y, localY := floorDiv(chunk.Y, int64(g.config.LargeChunkEdge))
	return ChunkMapping{
		LargeChunk: Coord{X: x, Y: y},
		Offset:     Offset{X: uint32(localX), Y: uint32(localY)},
	}
}

func floorDiv(value, divisor int64) (int64, int64) {
	quotient := value / divisor
	remainder := value % divisor
	if remainder < 0 {
		quotient--
		remainder += divisor
	}
	return quotient, remainder
}

func checkedMul(left, right uint64) (uint64, bool) {
	if left != 0 && right > math.MaxUint64/left {
		return 0, false
	}
	return left * right, true
}

func maxInt() int {
	return int(^uint(0) >> 1)
}

func (c Config) String() string {
	return strconv.FormatUint(uint64(c.ChunkEdge), 10) + "x" +
		strconv.FormatUint(uint64(c.LargeChunkEdge), 10) + "x" +
		strconv.FormatUint(uint64(c.BlockBits), 10)
}
