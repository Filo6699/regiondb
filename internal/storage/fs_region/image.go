package fs_region

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"

	"github.com/Filo6699/regiondb/internal/bitcodec"
	"github.com/Filo6699/regiondb/internal/geometry"
)

var (
	// errSlotAbsent reports a slot whose presence bit is clear.
	errSlotAbsent = errors.New("region slot is absent")
	// errImageUnpublished reports an image file that was created but never
	// received its header, which is possible only when a writer was interrupted
	// between creation and initialization. No slot of such an image was ever
	// published.
	errImageUnpublished = errors.New("region image was never initialized")
)

// regionImage owns one open region image file together with an in-memory copy
// of its slot directory. The directory is small and fully rewritten by every
// slot publication, so a reader never has to trust a partially updated
// presence bitmap.
type regionImage struct {
	file      *os.File
	coord     geometry.Coord
	layout    imageLayout
	directory []byte
}

func openRegionImage(
	path string,
	region geometry.Coord,
	g geometry.Geometry,
	layout imageLayout,
	create bool,
) (_ *regionImage, returnErr error) {
	flags := os.O_RDWR
	if create {
		flags |= os.O_CREATE
	}
	// The caller distinguishes a missing image from a real failure, so this
	// error is returned unwrapped.
	file, err := os.OpenFile(path, flags, 0o600)
	if err != nil {
		return nil, err
	}
	defer func() {
		if returnErr != nil {
			if err := file.Close(); err != nil {
				returnErr = errors.Join(returnErr, fmt.Errorf("close region image: %w", err))
			}
		}
	}()

	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat region image: %w", err)
	}
	image := &regionImage{
		file:      file,
		coord:     region,
		layout:    layout,
		directory: make([]byte, int(layout.directoryBytes)),
	}
	switch {
	case info.Size() == 0 && create:
		if err := image.initialize(g); err != nil {
			return nil, err
		}
	case info.Size() == 0:
		return nil, errImageUnpublished
	case info.Size() == layout.imageBytes:
		if err := image.load(g); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf(
			"%w: image size is %d, want %d",
			ErrCorrupt,
			info.Size(),
			layout.imageBytes,
		)
	}
	return image, nil
}

func (image *regionImage) close() error {
	if err := image.file.Close(); err != nil {
		return fmt.Errorf("close region image: %w", err)
	}
	return nil
}

// initialize publishes an empty image: a header, an all-absent slot directory
// and the full slot area. Sizing the slot area once keeps every later slot
// write a fixed-offset overwrite.
func (image *regionImage) initialize(g geometry.Geometry) error {
	if _, err := image.file.WriteAt(image.encodeHeader(g), 0); err != nil {
		return fmt.Errorf("write region image header: %w", err)
	}
	if err := image.flushDirectory(); err != nil {
		return err
	}
	if err := image.file.Truncate(image.layout.imageBytes); err != nil {
		return fmt.Errorf("size region image: %w", err)
	}
	return nil
}

func (image *regionImage) load(g geometry.Geometry) error {
	header := make([]byte, headerBytes)
	if _, err := image.file.ReadAt(header, 0); err != nil {
		return fmt.Errorf("%w: read header: %v", ErrCorrupt, err)
	}
	if err := image.validateHeader(g, header); err != nil {
		return err
	}
	if _, err := image.file.ReadAt(image.directory, headerBytes); err != nil {
		return fmt.Errorf("%w: read slot directory: %v", ErrCorrupt, err)
	}
	storedChecksum, err := bitcodec.Uint32(image.directory[:checksumSize])
	if err != nil {
		return fmt.Errorf("%w: read slot directory checksum: %v", ErrCorrupt, err)
	}
	if actual := crc32.ChecksumIEEE(image.directory[checksumSize:]); actual != storedChecksum {
		return fmt.Errorf("%w: slot directory checksum mismatch", ErrCorrupt)
	}
	return nil
}

func (image *regionImage) validateHeader(g geometry.Geometry, header []byte) error {
	if string(header[:len(imageMagic)]) != imageMagic {
		return fmt.Errorf("%w: invalid magic", ErrCorrupt)
	}
	storedChecksum, err := bitcodec.Uint32(header[56:60])
	if err != nil {
		return fmt.Errorf("%w: read header checksum: %v", ErrCorrupt, err)
	}
	if actual := crc32.ChecksumIEEE(header[:56]); actual != storedChecksum {
		return fmt.Errorf("%w: header checksum mismatch", ErrCorrupt)
	}

	version, err := bitcodec.Uint32(header[8:12])
	if err != nil {
		return fmt.Errorf("%w: read format version: %v", ErrCorrupt, err)
	}
	if version != FormatVersion {
		return fmt.Errorf("%w: version %d, want %d", ErrUnsupportedVersion, version, FormatVersion)
	}
	chunkEdge, err := bitcodec.Uint32(header[12:16])
	if err != nil {
		return fmt.Errorf("%w: read chunk edge: %v", ErrCorrupt, err)
	}
	largeChunkEdge, err := bitcodec.Uint32(header[16:20])
	if err != nil {
		return fmt.Errorf("%w: read large-chunk edge: %v", ErrCorrupt, err)
	}
	config := g.Config()
	if chunkEdge != config.ChunkEdge ||
		largeChunkEdge != config.LargeChunkEdge ||
		header[20] != config.BlockBits {
		return fmt.Errorf("%w: %w", ErrCorrupt, ErrGeometryMismatch)
	}
	if !bytes.Equal(header[21:24], []byte{0, 0, 0}) {
		return fmt.Errorf("%w: reserved header bytes are nonzero", ErrCorrupt)
	}

	storedX, err := bitcodec.Uint64(header[24:32])
	if err != nil {
		return fmt.Errorf("%w: read region x coordinate: %v", ErrCorrupt, err)
	}
	storedY, err := bitcodec.Uint64(header[32:40])
	if err != nil {
		return fmt.Errorf("%w: read region y coordinate: %v", ErrCorrupt, err)
	}
	if int64(storedX) != image.coord.X || int64(storedY) != image.coord.Y {
		return fmt.Errorf("%w: region coordinate mismatch", ErrCorrupt)
	}

	slotCount, err := bitcodec.Uint64(header[40:48])
	if err != nil {
		return fmt.Errorf("%w: read slot count: %v", ErrCorrupt, err)
	}
	slotBytes, err := bitcodec.Uint64(header[48:56])
	if err != nil {
		return fmt.Errorf("%w: read slot size: %v", ErrCorrupt, err)
	}
	if slotCount != image.layout.slotCount || slotBytes != image.layout.slotBytes {
		return fmt.Errorf(
			"%w: image holds %d slots of %d bytes, want %d of %d",
			ErrCorrupt,
			slotCount,
			slotBytes,
			image.layout.slotCount,
			image.layout.slotBytes,
		)
	}
	return nil
}

func (image *regionImage) encodeHeader(g geometry.Geometry) []byte {
	config := g.Config()
	header := make([]byte, 0, headerBytes)
	header = append(header, imageMagic...)
	header = bitcodec.AppendUint32(header, FormatVersion)
	header = bitcodec.AppendUint32(header, config.ChunkEdge)
	header = bitcodec.AppendUint32(header, config.LargeChunkEdge)
	header = append(header, config.BlockBits, 0, 0, 0)
	header = bitcodec.AppendUint64(header, uint64(image.coord.X))
	header = bitcodec.AppendUint64(header, uint64(image.coord.Y))
	header = bitcodec.AppendUint64(header, image.layout.slotCount)
	header = bitcodec.AppendUint64(header, image.layout.slotBytes)
	return bitcodec.AppendUint32(header, crc32.ChecksumIEEE(header))
}

func (image *regionImage) present(index uint64) bool {
	return image.directory[checksumSize+index/8]&byte(1<<(index%8)) != 0
}

func (image *regionImage) markPresent(index uint64, checksum uint32) {
	image.directory[checksumSize+index/8] |= byte(1 << (index % 8))
	offset := image.layout.slotChecksumOffset(index)
	binary.LittleEndian.PutUint32(image.directory[offset:offset+checksumSize], checksum)
}

func (image *regionImage) slotChecksum(index uint64) (uint32, error) {
	offset := image.layout.slotChecksumOffset(index)
	checksum, err := bitcodec.Uint32(image.directory[offset : offset+checksumSize])
	if err != nil {
		return 0, fmt.Errorf("%w: read slot checksum: %v", ErrCorrupt, err)
	}
	return checksum, nil
}

func (image *regionImage) readSlot(index uint64) ([]byte, error) {
	if !image.present(index) {
		return nil, errSlotAbsent
	}
	expected, err := image.slotChecksum(index)
	if err != nil {
		return nil, err
	}
	payload := make([]byte, int(image.layout.slotBytes))
	if _, err := image.file.ReadAt(payload, image.layout.slotOffset(index)); err != nil {
		return nil, fmt.Errorf("%w: read slot %d: %v", ErrCorrupt, index, err)
	}
	if actual := crc32.ChecksumIEEE(payload); actual != expected {
		return nil, fmt.Errorf("%w: slot %d checksum mismatch", ErrCorrupt, index)
	}
	return payload, nil
}

func (image *regionImage) writeSlot(index uint64, payload []byte) error {
	if index >= image.layout.slotCount {
		return fmt.Errorf(
			"write region slot: index %d is outside %d slots",
			index,
			image.layout.slotCount,
		)
	}
	if uint64(len(payload)) != image.layout.slotBytes {
		return fmt.Errorf(
			"write region slot: payload is %d bytes, want %d",
			len(payload),
			image.layout.slotBytes,
		)
	}
	// The slot payload is written before its presence bit and checksum. An
	// interrupted first publication therefore leaves the slot absent instead of
	// present with unverifiable bytes. Overwriting a published slot updates it in
	// place: the experimental layout has no write-ahead log, so an interrupted
	// overwrite is reported as a checksum mismatch rather than repaired.
	if _, err := image.file.WriteAt(payload, image.layout.slotOffset(index)); err != nil {
		return fmt.Errorf("write region slot %d: %w", index, err)
	}
	image.markPresent(index, crc32.ChecksumIEEE(payload))
	return image.flushDirectory()
}

// flushDirectory rewrites the whole slot directory, checksum first, in a single
// write so presence bits and their slot checksums never reach the file
// separately.
func (image *regionImage) flushDirectory() error {
	binary.LittleEndian.PutUint32(
		image.directory[:checksumSize],
		crc32.ChecksumIEEE(image.directory[checksumSize:]),
	)
	if _, err := image.file.WriteAt(image.directory, headerBytes); err != nil {
		return fmt.Errorf("write region slot directory: %w", err)
	}
	return nil
}
