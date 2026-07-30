package fs_region

import (
	"container/list"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"

	"github.com/Filo6699/regiondb/internal/geometry"
	"github.com/Filo6699/regiondb/internal/storage"
)

// DefaultMaxOpenRegions bounds region image handles when Options leaves the
// limit unset.
const DefaultMaxOpenRegions = 64

// Options configures the experimental store. It intentionally offers no
// durability mode: the layout is measured against the relaxed production path
// only.
type Options struct {
	// MaxOpenRegions bounds the region image handles the store keeps open. The
	// least recently used image is closed when the limit is reached, so a large
	// world cannot grow the handle set without bound.
	MaxOpenRegions int
}

func (options Options) validated() (Options, error) {
	if options.MaxOpenRegions == 0 {
		options.MaxOpenRegions = DefaultMaxOpenRegions
	}
	if options.MaxOpenRegions < 0 {
		return Options{}, errors.New("maximum open region images must be positive")
	}
	return options, nil
}

// Store is an experimental region-image chunk store. Its methods are safe for
// concurrent use inside one process, but the layout has no writer ownership
// metadata: it must not share a data directory with another writer or with a
// production fs_split_v1 store.
type Store struct {
	root     string
	geometry geometry.Geometry
	layout   imageLayout
	options  Options
	mu       sync.Mutex
	images   map[geometry.Coord]*list.Element
	recent   *list.List
	closed   bool
}

func Open(root string, g geometry.Geometry) (*Store, error) {
	return OpenWithOptions(root, g, Options{})
}

func OpenWithOptions(root string, g geometry.Geometry, options Options) (*Store, error) {
	if root == "" {
		return nil, errors.New("data directory must not be empty")
	}
	validated, err := geometry.New(g.Config())
	if err != nil || validated != g {
		return nil, fmt.Errorf("open %s: %w", FormatName, geometry.ErrInvalid)
	}
	options, err = options.validated()
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", FormatName, err)
	}
	layout, err := newImageLayout(g)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", FormatName, err)
	}
	absoluteRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve data directory: %w", err)
	}
	if err := os.MkdirAll(absoluteRoot, 0o755); err != nil {
		return nil, fmt.Errorf("create data directory: %w", err)
	}
	return &Store{
		root:     absoluteRoot,
		geometry: g,
		layout:   layout,
		options:  options,
		images:   make(map[geometry.Coord]*list.Element),
		recent:   list.New(),
	}, nil
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}
	s.closed = true
	var result error
	for element := s.recent.Front(); element != nil; element = element.Next() {
		if err := element.Value.(*regionImage).close(); err != nil {
			result = errors.Join(result, err)
		}
	}
	s.recent.Init()
	s.images = make(map[geometry.Coord]*list.Element)
	return result
}

func (s *Store) WriteChunk(coord geometry.Coord, chunk *storage.Chunk) error {
	if chunk == nil {
		return errors.New("write chunk: nil chunk")
	}
	if chunk.Geometry() != s.geometry {
		return ErrGeometryMismatch
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return errors.New("write chunk: store is closed")
	}
	region, index := locate(s.geometry, coord)
	image, err := s.acquireImage(region, true)
	if err != nil {
		return fmt.Errorf("open region image: %w", err)
	}
	if err := image.writeSlot(index, chunk.Bytes()); err != nil {
		// A failed publication leaves the in-memory slot directory possibly ahead
		// of the file, so the image is dropped and revalidated from disk on the
		// next access instead of answering reads from unpublished state.
		return errors.Join(err, s.discardImage(region))
	}
	return nil
}

func (s *Store) ReadChunk(coord geometry.Coord) (*storage.Chunk, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, errors.New("read chunk: store is closed")
	}
	region, index := locate(s.geometry, coord)
	image, err := s.acquireImage(region, false)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) || errors.Is(err, errImageUnpublished) {
			return nil, absentChunkError(coord)
		}
		return nil, fmt.Errorf("open region image: %w", err)
	}
	payload, err := image.readSlot(index)
	if err != nil {
		if errors.Is(err, errSlotAbsent) {
			return nil, absentChunkError(coord)
		}
		return nil, err
	}
	chunk, err := storage.ChunkFromBytes(s.geometry, payload)
	if err != nil {
		return nil, fmt.Errorf("%w: decode payload: %v", ErrCorrupt, err)
	}
	return chunk, nil
}

// acquireImage returns the open image for a region, opening it if necessary and
// closing the least recently used image once the bound is reached. The caller
// holds s.mu.
func (s *Store) acquireImage(region geometry.Coord, create bool) (*regionImage, error) {
	if element, found := s.images[region]; found {
		s.recent.MoveToFront(element)
		return element.Value.(*regionImage), nil
	}
	if s.recent.Len() >= s.options.MaxOpenRegions {
		if err := s.evictImage(); err != nil {
			return nil, err
		}
	}
	image, err := openRegionImage(
		filepath.Join(s.root, imageName(region)),
		region,
		s.geometry,
		s.layout,
		create,
	)
	if err != nil {
		return nil, err
	}
	s.images[region] = s.recent.PushFront(image)
	return image, nil
}

// evictImage closes the least recently used image so the handle bound holds
// before another image is opened. The caller holds s.mu.
func (s *Store) evictImage() error {
	oldest := s.recent.Back()
	if oldest == nil {
		return nil
	}
	return s.discardImage(oldest.Value.(*regionImage).coord)
}

// discardImage closes one open image and forgets its in-memory slot directory.
// The caller holds s.mu.
func (s *Store) discardImage(region geometry.Coord) error {
	element, found := s.images[region]
	if !found {
		return nil
	}
	s.recent.Remove(element)
	delete(s.images, region)
	return element.Value.(*regionImage).close()
}

// absentChunkError keeps the missing-chunk class equivalent to the production
// layout, where a chunk without a file surfaces as fs.ErrNotExist.
func absentChunkError(coord geometry.Coord) error {
	return fmt.Errorf("read chunk (%d,%d): %w", coord.X, coord.Y, fs.ErrNotExist)
}
