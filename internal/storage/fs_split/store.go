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
	"sync"

	"github.com/Filo6699/regiondb/internal/bitcodec"
	"github.com/Filo6699/regiondb/internal/geometry"
	"github.com/Filo6699/regiondb/internal/storage"
)

const (
	fileMagic    = "RGDBSPL1"
	headerBytes  = 44
	checksumSize = 4
)

var (
	ErrCorrupt          = errors.New("corrupt fs_split_v1 chunk")
	ErrGeometryMismatch = errors.New("chunk geometry does not match store")
)

type Store struct {
	root     string
	geometry geometry.Geometry
	mu       sync.RWMutex
}

func Open(root string, g geometry.Geometry) (*Store, error) {
	if root == "" {
		return nil, errors.New("data directory must not be empty")
	}
	validated, err := geometry.New(g.Config())
	if err != nil || validated != g {
		return nil, fmt.Errorf("open fs_split_v1: %w", geometry.ErrInvalid)
	}
	absoluteRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve data directory: %w", err)
	}
	if err := os.MkdirAll(absoluteRoot, 0o755); err != nil {
		return nil, fmt.Errorf("create data directory: %w", err)
	}
	return &Store{root: absoluteRoot, geometry: g}, nil
}

func (s *Store) WriteChunk(coord geometry.Coord, chunk *storage.Chunk) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if chunk == nil {
		return errors.New("write chunk: nil chunk")
	}
	if chunk.Geometry() != s.geometry {
		return ErrGeometryMismatch
	}

	encoded := s.encode(coord, chunk.Bytes())
	path := s.chunkPath(coord)
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return fmt.Errorf("create large-chunk directory: %w", err)
	}
	if err := writeAtomic(path, encoded); err != nil {
		return fmt.Errorf("persist chunk: %w", err)
	}
	return nil
}

func (s *Store) ReadChunk(coord geometry.Coord) (chunk *storage.Chunk, returnErr error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

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

	expectedSize := int64(headerBytes) + int64(s.geometry.PayloadBytes()) + checksumSize
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat chunk: %w", err)
	}
	if info.Size() != expectedSize {
		return nil, fmt.Errorf("%w: file size is %d, want %d", ErrCorrupt, info.Size(), expectedSize)
	}

	encoded := make([]byte, int(expectedSize))
	if _, err := io.ReadFull(file, encoded); err != nil {
		return nil, fmt.Errorf("%w: read chunk: %v", ErrCorrupt, err)
	}
	payload, err := s.decode(coord, encoded)
	if err != nil {
		return nil, err
	}
	chunk, err = storage.ChunkFromBytes(s.geometry, payload)
	if err != nil {
		return nil, fmt.Errorf("%w: decode payload: %v", ErrCorrupt, err)
	}
	return chunk, nil
}

func (s *Store) encode(coord geometry.Coord, payload []byte) []byte {
	config := s.geometry.Config()
	encoded := make([]byte, 0, headerBytes+len(payload)+checksumSize)
	encoded = append(encoded, fileMagic...)
	encoded = bitcodec.AppendUint32(encoded, config.ChunkEdge)
	encoded = bitcodec.AppendUint32(encoded, config.LargeChunkEdge)
	encoded = append(encoded, config.BlockBits, 0, 0, 0)
	encoded = bitcodec.AppendUint64(encoded, uint64(coord.X))
	encoded = bitcodec.AppendUint64(encoded, uint64(coord.Y))
	encoded = bitcodec.AppendUint64(encoded, uint64(len(payload)))
	encoded = append(encoded, payload...)
	return bitcodec.AppendUint32(encoded, crc32.ChecksumIEEE(encoded))
}

func (s *Store) decode(coord geometry.Coord, encoded []byte) ([]byte, error) {
	if len(encoded) < headerBytes+checksumSize {
		return nil, fmt.Errorf("%w: file is shorter than the header", ErrCorrupt)
	}
	if string(encoded[:len(fileMagic)]) != fileMagic {
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
	if !bytes.Equal(encoded[17:20], []byte{0, 0, 0}) {
		return nil, fmt.Errorf("%w: reserved header bytes are nonzero", ErrCorrupt)
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

	return encoded[headerBytes : len(encoded)-checksumSize], nil
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

func writeAtomic(path string, data []byte) (returnErr error) {
	temporary, err := os.CreateTemp(filepath.Dir(path), ".regiondb-chunk-*")
	if err != nil {
		return fmt.Errorf("create temporary file: %w", err)
	}
	temporaryPath := temporary.Name()
	closed := false
	renamed := false
	defer func() {
		if !closed {
			if err := temporary.Close(); err != nil && returnErr == nil {
				returnErr = fmt.Errorf("close temporary file: %w", err)
			}
		}
		if !renamed {
			if err := os.Remove(temporaryPath); err != nil && !errors.Is(err, os.ErrNotExist) && returnErr == nil {
				returnErr = fmt.Errorf("remove temporary file: %w", err)
			}
		}
	}()

	if err := temporary.Chmod(0o600); err != nil {
		return fmt.Errorf("set temporary file permissions: %w", err)
	}
	if _, err := io.Copy(temporary, bytes.NewReader(data)); err != nil {
		return fmt.Errorf("write temporary file: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close temporary file: %w", err)
	}
	closed = true
	if err := os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("replace destination: %w", err)
	}
	renamed = true
	return nil
}
