package fs_split

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/Filo6699/regiondb/internal/geometry"
)

const maxScanChunkCandidates = 257

type boundedChunkCoords struct {
	hasCursor bool
	cursor    geometry.Coord
	limit     int
	coords    map[geometry.Coord]struct{}
	overflow  bool
}

func newBoundedChunkCoords(hasCursor bool, cursor geometry.Coord, limit int) *boundedChunkCoords {
	return &boundedChunkCoords{
		hasCursor: hasCursor,
		cursor:    cursor,
		limit:     limit,
		coords:    make(map[geometry.Coord]struct{}, limit),
	}
}

func (c *boundedChunkCoords) insert(coord geometry.Coord) {
	if c.hasCursor && !chunkCoordLess(c.cursor, coord) {
		return
	}
	if _, exists := c.coords[coord]; exists {
		return
	}
	if len(c.coords) < c.limit {
		c.coords[coord] = struct{}{}
		return
	}

	var largest geometry.Coord
	first := true
	for candidate := range c.coords {
		if first || chunkCoordLess(largest, candidate) {
			largest = candidate
			first = false
		}
	}
	c.overflow = true
	if !chunkCoordLess(coord, largest) {
		return
	}
	delete(c.coords, largest)
	c.coords[coord] = struct{}{}
}

func (c *boundedChunkCoords) sorted() []geometry.Coord {
	result := make([]geometry.Coord, 0, len(c.coords))
	for coord := range c.coords {
		result = append(result, coord)
	}
	for index := 1; index < len(result); index++ {
		for current := index; current > 0 && chunkCoordLess(result[current], result[current-1]); current-- {
			result[current], result[current-1] = result[current-1], result[current]
		}
	}
	return result
}

func chunkCoordLess(left, right geometry.Coord) bool {
	return left.X < right.X || left.X == right.X && left.Y < right.Y
}

// ScanChunkCoords returns the smallest persisted chunk coordinates strictly
// after the optional cursor. Candidate storage is bounded by limit even when
// stale or duplicate artifacts make the data directory much larger.
func (s *Store) ScanChunkCoords(
	hasCursor bool,
	cursor geometry.Coord,
	limit int,
) ([]geometry.Coord, bool, error) {
	if limit <= 0 || limit > maxScanChunkCandidates {
		return nil, false, errors.New("scan chunk coordinates: limit must be between 1 and 257")
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, false, errors.New("scan chunk coordinates: store is closed")
	}
	generation, err := s.beginReadOnlySnapshot()
	if err != nil {
		return nil, false, err
	}

	candidates := newBoundedChunkCoords(hasCursor, cursor, limit)
	directories, err := os.ReadDir(s.root)
	if err != nil {
		return nil, false, fmt.Errorf("scan data directory: %w", err)
	}
	for _, directory := range directories {
		if !directory.IsDir() {
			continue
		}
		largeCoord, ok := parseCoordinateName(directory.Name(), "l_", "")
		if !ok ||
			directory.Name() != "l_"+signedName(largeCoord.X)+"_"+signedName(largeCoord.Y) {
			continue
		}
		files, err := os.ReadDir(filepath.Join(s.root, directory.Name()))
		if err != nil {
			return nil, false, fmt.Errorf("scan chunk directory %q: %w", directory.Name(), err)
		}
		for _, file := range files {
			if !file.Type().IsRegular() {
				continue
			}
			coord, ok := parseChunkFileName(file.Name())
			if !ok {
				continue
			}
			candidates.insert(coord)
		}
	}
	if err := s.finishReadOnlySnapshot(generation); err != nil {
		return nil, false, err
	}
	return candidates.sorted(), candidates.overflow, nil
}

func parseChunkFileName(name string) (geometry.Coord, bool) {
	coord, ok := parseCoordinateName(name, "c_", ".rdb")
	if !ok {
		return geometry.Coord{}, false
	}
	if name != "c_"+signedName(coord.X)+"_"+signedName(coord.Y)+".rdb" {
		return geometry.Coord{}, false
	}
	return coord, true
}

func parseCoordinateName(name, prefix, suffix string) (geometry.Coord, bool) {
	if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, suffix) {
		return geometry.Coord{}, false
	}
	coordinates := strings.TrimSuffix(strings.TrimPrefix(name, prefix), suffix)
	xText, yText, found := strings.Cut(coordinates, "_")
	if !found || strings.Contains(yText, "_") {
		return geometry.Coord{}, false
	}
	x, ok := parseSignedName(xText)
	if !ok {
		return geometry.Coord{}, false
	}
	y, ok := parseSignedName(yText)
	if !ok {
		return geometry.Coord{}, false
	}
	return geometry.Coord{X: x, Y: y}, true
}

func parseSignedName(value string) (int64, bool) {
	if len(value) < 2 {
		return 0, false
	}
	magnitude, err := strconv.ParseUint(value[1:], 10, 64)
	if err != nil {
		return 0, false
	}
	switch value[0] {
	case 'p':
		if magnitude > math.MaxInt64 {
			return 0, false
		}
		return int64(magnitude), true
	case 'n':
		if magnitude == 0 || magnitude > uint64(math.MaxInt64)+1 {
			return 0, false
		}
		if magnitude == uint64(math.MaxInt64)+1 {
			return math.MinInt64, true
		}
		return -int64(magnitude), true
	default:
		return 0, false
	}
}
