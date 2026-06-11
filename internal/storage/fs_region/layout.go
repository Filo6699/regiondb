// Package fs_region implements the experimental fs_region_v1 layout, which
// stores every regular chunk of one large chunk as a fixed slot inside a single
// region image guarded by a presence bitmap.
//
// The layout exists to measure region images against the production
// fs_split_v1 backend. It is opt-in, versioned independently of fs_split_v1,
// carries no compatibility promise, and is not wired into the server.
package fs_region

import (
	"errors"
	"fmt"
	"math"
	"strconv"

	"github.com/Filo6699/regiondb/internal/geometry"
)

const (
	// FormatName identifies the experimental layout in diagnostics.
	FormatName = "fs_region_v1"
	// FormatVersion is versioned separately from the production layout: an
	// experimental image never claims fs_split_v1 compatibility.
	FormatVersion uint32 = 2

	imageMagic   = "RGDBRGN1"
	imageSuffix  = ".rdbregion"
	headerBytes  = 60
	checksumSize = 4

	// maxImageBytes bounds the addressable size of one region image. The layout
	// sizes an image to all of its slots when the image is created, so an
	// unbounded geometry would reserve the whole large chunk on filesystems that
	// do not support sparse files.
	maxImageBytes int64 = 1 << 30
)

var (
	// ErrCorrupt reports a region image that cannot be trusted.
	ErrCorrupt = errors.New("corrupt fs_region_v1 region image")
	// ErrGeometryMismatch reports geometry that does not match the store.
	ErrGeometryMismatch = errors.New("region image geometry does not match store")
	// ErrUnsupportedVersion reports an image written by a different revision of
	// the experimental format.
	ErrUnsupportedVersion = errors.New("unsupported fs_region_v1 format version")
	// ErrImageTooLarge reports geometry whose region image cannot be addressed
	// as a single bounded file.
	ErrImageTooLarge = errors.New("fs_region_v1 region image is too large")
)

// imageLayout describes the byte geometry of one region image. Every offset is
// derived from the store geometry, so slot positions never depend on values
// read back from a file.
type imageLayout struct {
	slotCount      uint64
	slotBytes      uint64
	bitmapBytes    uint64
	directoryBytes uint64
	slotsOffset    int64
	imageBytes     int64
}

func newImageLayout(g geometry.Geometry) (imageLayout, error) {
	edge := uint64(g.Config().LargeChunkEdge)
	slotCount, ok := checkedMul(edge, edge)
	if !ok {
		return imageLayout{}, fmt.Errorf("%w: slot count overflows", ErrImageTooLarge)
	}
	slotBytes, ok := checkedAdd(uint64(g.PayloadBytes()), uint64(g.PresenceBytes()))
	if !ok {
		return imageLayout{}, fmt.Errorf("%w: slot size overflows", ErrImageTooLarge)
	}

	bitmapBytes := slotCount / 8
	if slotCount%8 != 0 {
		bitmapBytes++
	}
	slotChecksumBytes, ok := checkedMul(slotCount, checksumSize)
	if !ok {
		return imageLayout{}, fmt.Errorf("%w: slot checksum table overflows", ErrImageTooLarge)
	}
	directoryBytes, ok := checkedAdd(checksumSize+bitmapBytes, slotChecksumBytes)
	if !ok || directoryBytes > uint64(math.MaxInt) {
		return imageLayout{}, fmt.Errorf("%w: slot directory overflows", ErrImageTooLarge)
	}
	slotAreaBytes, ok := checkedMul(slotCount, slotBytes)
	if !ok {
		return imageLayout{}, fmt.Errorf("%w: slot area overflows", ErrImageTooLarge)
	}
	slotsOffset, ok := checkedAdd(headerBytes, directoryBytes)
	if !ok {
		return imageLayout{}, fmt.Errorf("%w: slot area offset overflows", ErrImageTooLarge)
	}
	imageBytes, ok := checkedAdd(slotsOffset, slotAreaBytes)
	if !ok || imageBytes > uint64(maxImageBytes) {
		return imageLayout{}, fmt.Errorf(
			"%w: %d bytes exceed the %d byte limit",
			ErrImageTooLarge,
			imageBytes,
			maxImageBytes,
		)
	}

	return imageLayout{
		slotCount:      slotCount,
		slotBytes:      slotBytes,
		bitmapBytes:    bitmapBytes,
		directoryBytes: directoryBytes,
		slotsOffset:    int64(slotsOffset),
		imageBytes:     int64(imageBytes),
	}, nil
}

// slotOffset is only called with an index below slotCount, so the result stays
// inside the image sized by newImageLayout.
func (layout imageLayout) slotOffset(index uint64) int64 {
	return layout.slotsOffset + int64(index)*int64(layout.slotBytes)
}

// slotChecksumOffset addresses the slot checksum that follows the presence
// bitmap inside the slot directory.
func (layout imageLayout) slotChecksumOffset(index uint64) uint64 {
	return checksumSize + layout.bitmapBytes + index*checksumSize
}

// locate maps a chunk coordinate to its region image and slot index. Presence
// and slot position are therefore pure functions of geometry.
func locate(g geometry.Geometry, coord geometry.Coord) (geometry.Coord, uint64) {
	mapping := g.ChunkToLargeChunk(coord)
	edge := uint64(g.Config().LargeChunkEdge)
	return mapping.LargeChunk, uint64(mapping.Offset.Y)*edge + uint64(mapping.Offset.X)
}

func imageName(region geometry.Coord) string {
	return "r_" + signedName(region.X) + "_" + signedName(region.Y) + imageSuffix
}

func signedName(value int64) string {
	if value < 0 {
		return "n" + strconv.FormatUint(uint64(-(value+1))+1, 10)
	}
	return "p" + strconv.FormatInt(value, 10)
}

func checkedMul(left, right uint64) (uint64, bool) {
	if left != 0 && right > math.MaxUint64/left {
		return 0, false
	}
	return left * right, true
}

func checkedAdd(left, right uint64) (uint64, bool) {
	if right > math.MaxUint64-left {
		return 0, false
	}
	return left + right, true
}
