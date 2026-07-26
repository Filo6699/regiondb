package fs_split

import (
	"bytes"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strconv"

	"github.com/Filo6699/regiondb/internal/bitcodec"
	"github.com/Filo6699/regiondb/internal/geometry"
	"github.com/Filo6699/regiondb/internal/storage"
)

const (
	fileMagic       = "RGDBSPL3"
	v2FileMagic     = "RGDBSPL2"
	legacyFileMagic = "RGDBSPL1"
	headerBytes     = 44
	checksumSize    = 4
	imageCodecNone  = 0
	imageCodecZRLE  = 1
)

var (
	ErrCorrupt          = errors.New("corrupt fs_split_v1 chunk")
	ErrGeometryMismatch = errors.New("chunk geometry does not match store")
)

func (s *Store) readChunkFile(coord geometry.Coord) (chunk *storage.Chunk, returnErr error) {
	path := s.chunkPath(coord)
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open chunk: %w", err)
	}
	defer func() {
		if err := file.Close(); err != nil && returnErr == nil {
			chunk = nil
			returnErr = fmt.Errorf("close chunk: %w", err)
		}
	}()

	legacySize := int64(headerBytes) + int64(s.geometry.PayloadBytes()) + checksumSize
	stateSize := s.geometry.PayloadBytes() + s.geometry.PresenceBytes()
	expectedSize := int64(headerBytes) + int64(stateSize) + checksumSize
	maxV3Size := int64(headerBytes) + int64(stateSize)*2 + checksumSize
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat chunk: %w", err)
	}
	if info.Size() != legacySize &&
		info.Size() != expectedSize &&
		(info.Size() < headerBytes+checksumSize || info.Size() > maxV3Size) {
		return nil, fmt.Errorf(
			"%w: file size is %d, want v1 size %d, v2 size %d, or bounded v3 size",
			ErrCorrupt,
			info.Size(),
			legacySize,
			expectedSize,
		)
	}

	encoded := make([]byte, int(info.Size()))
	if _, err := io.ReadFull(file, encoded); err != nil {
		return nil, fmt.Errorf("%w: read chunk: %v", ErrCorrupt, err)
	}
	chunk, err = s.decode(coord, encoded)
	if err != nil {
		return nil, err
	}
	if !s.options.ReadOnly {
		if err := s.cache.putState(coord, chunk.Bytes(), chunk.PresenceBytes()); err != nil {
			return nil, fmt.Errorf("cache read chunk: %w", err)
		}
	}
	return chunk, nil
}

func (s *Store) encode(coord geometry.Coord, payload []byte) []byte {
	presence := make([]byte, s.geometry.PresenceBytes())
	for index := uint64(0); index < s.geometry.BlockCount(); index++ {
		presence[index/8] |= byte(1 << (index % 8))
	}
	return s.encodeState(coord, payload, presence)
}

func (s *Store) encodeState(coord geometry.Coord, payload, presence []byte) []byte {
	return s.encodeImage(coord, payload, presence, v2FileMagic, imageCodecNone)
}

func (s *Store) encodeCheckpointState(coord geometry.Coord, payload, presence []byte) []byte {
	codec := byte(imageCodecNone)
	if s.options.CheckpointCompression == CheckpointCompressionZRLE {
		state := make([]byte, 0, len(payload)+len(presence))
		state = append(state, payload...)
		state = append(state, presence...)
		compressed := storage.EncodeZRLE(state)
		if len(compressed) < len(state) {
			return s.encodeImage(coord, compressed, nil, fileMagic, imageCodecZRLE)
		}
	}
	return s.encodeImage(coord, payload, presence, fileMagic, codec)
}

func (s *Store) encodeImage(
	coord geometry.Coord,
	payload, presence []byte,
	magic string,
	codec byte,
) []byte {
	config := s.geometry.Config()
	encoded := make([]byte, 0, headerBytes+len(payload)+len(presence)+checksumSize)
	encoded = append(encoded, magic...)
	encoded = bitcodec.AppendUint32(encoded, config.ChunkEdge)
	encoded = bitcodec.AppendUint32(encoded, config.LargeChunkEdge)
	encoded = append(encoded, config.BlockBits, codec, 0, 0)
	encoded = bitcodec.AppendUint64(encoded, uint64(coord.X))
	encoded = bitcodec.AppendUint64(encoded, uint64(coord.Y))
	encoded = bitcodec.AppendUint64(encoded, uint64(s.geometry.PayloadBytes()))
	encoded = append(encoded, payload...)
	encoded = append(encoded, presence...)
	return bitcodec.AppendUint32(encoded, crc32.ChecksumIEEE(encoded))
}

func (s *Store) decode(coord geometry.Coord, encoded []byte) (*storage.Chunk, error) {
	if len(encoded) < headerBytes+checksumSize {
		return nil, fmt.Errorf("%w: file is shorter than the header", ErrCorrupt)
	}
	magic := string(encoded[:len(fileMagic)])
	if magic != fileMagic && magic != v2FileMagic && magic != legacyFileMagic {
		return nil, fmt.Errorf("%w: invalid magic", ErrCorrupt)
	}
	storedChecksum, err := bitcodec.Uint32(encoded[len(encoded)-checksumSize:])
	if err != nil {
		return nil, fmt.Errorf("%w: read checksum: %v", ErrCorrupt, err)
	}
	if actual := crc32.ChecksumIEEE(encoded[:len(encoded)-checksumSize]); actual != storedChecksum {
		return nil, fmt.Errorf("%w: checksum mismatch", ErrCorrupt)
	}

	chunkEdge, err := bitcodec.Uint32(encoded[8:12])
	if err != nil {
		return nil, fmt.Errorf("%w: read chunk edge: %v", ErrCorrupt, err)
	}
	largeChunkEdge, err := bitcodec.Uint32(encoded[12:16])
	if err != nil {
		return nil, fmt.Errorf("%w: read large-chunk edge: %v", ErrCorrupt, err)
	}
	if !bytes.Equal(encoded[18:20], []byte{0, 0}) {
		return nil, fmt.Errorf("%w: reserved header bytes are nonzero", ErrCorrupt)
	}
	codec := encoded[17]
	if magic != fileMagic && codec != imageCodecNone {
		return nil, fmt.Errorf("%w: reserved header bytes are nonzero", ErrCorrupt)
	}
	if magic == fileMagic && codec != imageCodecNone && codec != imageCodecZRLE {
		return nil, fmt.Errorf("%w: unsupported image codec %d", ErrCorrupt, codec)
	}
	storedX, err := bitcodec.Uint64(encoded[20:28])
	if err != nil {
		return nil, fmt.Errorf("%w: read chunk x coordinate: %v", ErrCorrupt, err)
	}
	storedY, err := bitcodec.Uint64(encoded[28:36])
	if err != nil {
		return nil, fmt.Errorf("%w: read chunk y coordinate: %v", ErrCorrupt, err)
	}
	payloadSize, err := bitcodec.Uint64(encoded[36:44])
	if err != nil {
		return nil, fmt.Errorf("%w: read payload size: %v", ErrCorrupt, err)
	}

	config := s.geometry.Config()
	if chunkEdge != config.ChunkEdge ||
		largeChunkEdge != config.LargeChunkEdge ||
		encoded[16] != config.BlockBits {
		return nil, fmt.Errorf("%w: %w", ErrCorrupt, ErrGeometryMismatch)
	}
	if int64(storedX) != coord.X || int64(storedY) != coord.Y {
		return nil, fmt.Errorf("%w: chunk coordinate mismatch", ErrCorrupt)
	}
	if payloadSize != uint64(s.geometry.PayloadBytes()) {
		return nil, fmt.Errorf("%w: payload size is %d, want %d", ErrCorrupt, payloadSize, s.geometry.PayloadBytes())
	}

	var chunk *storage.Chunk
	switch magic {
	case legacyFileMagic:
		payloadEnd := headerBytes + s.geometry.PayloadBytes()
		if len(encoded) != payloadEnd+checksumSize {
			return nil, fmt.Errorf("%w: invalid legacy file size", ErrCorrupt)
		}
		payload := encoded[headerBytes:payloadEnd]
		chunk, err = storage.ChunkFromLegacyBytes(s.geometry, payload)
	case v2FileMagic:
		payloadEnd := headerBytes + s.geometry.PayloadBytes()
		if len(encoded) != payloadEnd+s.geometry.PresenceBytes()+checksumSize {
			return nil, fmt.Errorf("%w: invalid file size", ErrCorrupt)
		}
		payload := encoded[headerBytes:payloadEnd]
		presence := encoded[payloadEnd : len(encoded)-checksumSize]
		chunk, err = storage.ChunkFromState(s.geometry, payload, presence)
	case fileMagic:
		stateSize := s.geometry.PayloadBytes() + s.geometry.PresenceBytes()
		state := encoded[headerBytes : len(encoded)-checksumSize]
		switch codec {
		case imageCodecNone:
			if len(state) != stateSize {
				return nil, fmt.Errorf("%w: invalid uncompressed v3 file size", ErrCorrupt)
			}
		case imageCodecZRLE:
			state, err = storage.DecodeZRLE(state, stateSize)
			if err != nil {
				return nil, fmt.Errorf("%w: decode zrle image: %v", ErrCorrupt, err)
			}
		}
		payloadEnd := s.geometry.PayloadBytes()
		chunk, err = storage.ChunkFromState(
			s.geometry,
			state[:payloadEnd],
			state[payloadEnd:],
		)
	}
	if err != nil {
		return nil, fmt.Errorf("%w: decode payload: %v", ErrCorrupt, err)
	}
	return chunk, nil
}

func (s *Store) chunkPath(coord geometry.Coord) string {
	large := s.geometry.ChunkToLargeChunk(coord).LargeChunk
	largeName := "l_" + signedName(large.X) + "_" + signedName(large.Y)
	chunkName := "c_" + signedName(coord.X) + "_" + signedName(coord.Y) + ".rdb"
	return filepath.Join(s.root, largeName, chunkName)
}

func signedName(value int64) string {
	if value < 0 {
		return "n" + strconv.FormatUint(uint64(-(value+1))+1, 10)
	}
	return "p" + strconv.FormatInt(value, 10)
}

func (s *Store) persistChunk(coord geometry.Coord, payload, presence []byte, syncData bool) error {
	path := s.chunkPath(coord)
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return fmt.Errorf("create large-chunk directory: %w", err)
	}
	if err := writeAtomic(path, s.encodeState(coord, payload, presence), syncData, s.atomicWriteFailpoint); err != nil {
		return err
	}
	if !syncData {
		s.pendingSync.add(path)
	}
	return nil
}

func (s *Store) persistCheckpointChunk(
	coord geometry.Coord,
	payload, presence []byte,
	syncData bool,
) error {
	if emptyPresence(presence) {
		return s.collectEmptyChunk(coord, syncData)
	}
	path := s.chunkPath(coord)
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return fmt.Errorf("create large-chunk directory: %w", err)
	}
	if err := writeAtomic(
		path,
		s.encodeCheckpointState(coord, payload, presence),
		syncData,
		s.atomicWriteFailpoint,
	); err != nil {
		return err
	}
	if !syncData {
		s.pendingSync.add(path)
	}
	return nil
}

func emptyPresence(presence []byte) bool {
	for _, value := range presence {
		if value != 0 {
			return false
		}
	}
	return true
}

func (s *Store) collectEmptyChunk(coord geometry.Coord, _ bool) error {
	path := s.chunkPath(coord)
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		if err := commitDirectoryEntry(
			syncParentDirectory(filepath.Dir(path)),
			replaceCommitsDirectoryEntry,
		); err != nil {
			return fmt.Errorf("commit absent empty chunk: %w", err)
		}
		delete(s.pendingSync.paths, path)
		s.cache.remove(coord)
		return nil
	} else if err != nil {
		return fmt.Errorf("stat empty chunk: %w", err)
	}

	directory := filepath.Dir(path)
	tombstone, err := createTemporaryFile(directory, chunkTemporaryPrefix+"gc-")
	if err != nil {
		return fmt.Errorf("create empty chunk tombstone: %w", err)
	}
	tombstonePath := tombstone.Name()
	if err := tombstone.Close(); err != nil {
		_ = os.Remove(tombstonePath)
		return fmt.Errorf("close empty chunk tombstone: %w", err)
	}
	if err := replaceFile(path, tombstonePath, true); err != nil {
		_ = os.Remove(tombstonePath)
		return fmt.Errorf("replace empty chunk with tombstone: %w", err)
	}
	delete(s.pendingSync.paths, path)
	if err := commitDirectoryEntry(
		syncParentDirectory(directory),
		replaceCommitsDirectoryEntry,
	); err != nil {
		return fmt.Errorf("commit empty chunk collection: %w", err)
	}
	s.cache.remove(coord)
	if err := os.Remove(tombstonePath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove empty chunk tombstone: %w", err)
	}
	return nil
}
