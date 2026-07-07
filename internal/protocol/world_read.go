package protocol

import (
	"encoding/hex"
	"math"
	"strconv"

	"github.com/Filo6699/regiondb/internal/geometry"
	"github.com/Filo6699/regiondb/internal/storage"
)

const (
	maxWorldReadChunks       = 256
	maxWorldReadResponseSize = 64 * 1024 * 1024
)

func (s *Session) chunkScan(args []string) Response {
	if len(args) != 1 && len(args) != 3 {
		return errorResponse("ARITY", "wrong number of arguments")
	}
	limit, err := parseUint(args[0])
	if err != nil {
		return errorResponse("NUMBER", "limit must be an unsigned decimal integer")
	}
	if limit == 0 || limit > maxWorldReadChunks {
		return errorResponse("INVALID_ARGUMENT", "limit must be between 1 and 256")
	}

	var cursor geometry.Coord
	hasCursor := len(args) == 3
	if hasCursor {
		var response *Response
		cursor, response = parseCoord(args[1:])
		if response != nil {
			return *response
		}
	}

	s.engine.mu.RLock()
	defer s.engine.mu.RUnlock()

	coords := make([]geometry.Coord, 0, int(limit))
	for len(coords) <= int(limit) {
		window := int(limit) + 1 - len(coords)
		candidates, more, err := s.engine.store.ScanChunkCoords(hasCursor, cursor, window)
		if err != nil {
			return errorResponse("STORAGE", "scan failed")
		}
		if len(candidates) == 0 {
			break
		}
		for _, candidate := range candidates {
			chunk, found, err := s.readChunk(candidate)
			if err != nil {
				return errorResponse("STORAGE", "read failed")
			}
			if !found || !chunkPopulated(chunk) {
				continue
			}
			if len(coords) == int(limit) {
				return chunkScanResponse(coords, true)
			}
			coords = append(coords, candidate)
		}
		if !more {
			break
		}
		hasCursor = true
		cursor = candidates[len(candidates)-1]
	}
	return chunkScanResponse(coords, false)
}

func chunkScanResponse(coords []geometry.Coord, more bool) Response {
	items := make([][]byte, 0, len(coords)+1)
	if more && len(coords) != 0 {
		cursor := make([]byte, 0, 48)
		cursor = append(cursor, "CURSOR "...)
		cursor = strconv.AppendInt(cursor, coords[len(coords)-1].X, 10)
		cursor = append(cursor, ' ')
		cursor = strconv.AppendInt(cursor, coords[len(coords)-1].Y, 10)
		items = append(items, cursor)
	} else {
		items = append(items, []byte("END"))
	}
	for _, coord := range coords {
		item := make([]byte, 0, 41)
		item = strconv.AppendInt(item, coord.X, 10)
		item = append(item, ' ')
		item = strconv.AppendInt(item, coord.Y, 10)
		items = append(items, item)
	}
	return arrayResponse(items)
}

func (s *Session) chunkRange(args []string) Response {
	if response := requireArity(args, 4); response != nil {
		return *response
	}
	first, response := parseCoord(args[:2])
	if response != nil {
		return *response
	}
	last, response := parseCoord(args[2:])
	if response != nil {
		return *response
	}
	if first.X > last.X || first.Y > last.Y {
		return errorResponse("INVALID_ARGUMENT", "chunk range corners must satisfy x0<=x1 and y0<=y1")
	}

	spanX := uint64(last.X) - uint64(first.X)
	spanY := uint64(last.Y) - uint64(first.Y)
	if spanX >= maxWorldReadChunks ||
		spanY >= maxWorldReadChunks ||
		(spanX+1)*(spanY+1) > maxWorldReadChunks {
		return errorResponse("INVALID_ARGUMENT", "chunk range must cover at most 256 chunks")
	}

	s.engine.mu.RLock()
	defer s.engine.mu.RUnlock()

	items := make([][]byte, 0, int((spanX+1)*(spanY+1)))
	var wireBytes uint64
	for x := first.X; ; x++ {
		for y := first.Y; ; y++ {
			var failure *Response
			items, wireBytes, failure = s.appendWorldChunk(items, wireBytes, geometry.Coord{X: x, Y: y})
			if failure != nil {
				return *failure
			}
			if y == last.Y {
				break
			}
		}
		if x == last.X {
			break
		}
	}
	return arrayResponse(items)
}

func (s *Session) chunkRadius(args []string) Response {
	if response := requireArity(args, 3); response != nil {
		return *response
	}
	center, response := parseCoord(args[:2])
	if response != nil {
		return *response
	}
	radius, err := parseInt(args[2])
	if err != nil {
		return errorResponse("NUMBER", "radius must be a signed decimal integer")
	}
	if radius < 0 || radius >= maxWorldReadChunks {
		return errorResponse("INVALID_ARGUMENT", "chunk radius must be non-negative and cover at most 256 chunks")
	}

	radiusSquared := uint64(radius) * uint64(radius)
	halfWidths := make([]int64, radius+1)
	var cells uint64
	for distance := int64(0); distance <= radius; distance++ {
		distanceSquared := uint64(distance) * uint64(distance)
		var halfWidth int64
		for uint64(halfWidth+1)*uint64(halfWidth+1)+distanceSquared <= radiusSquared {
			halfWidth++
		}
		halfWidths[distance] = halfWidth
		rowCells := 2*uint64(halfWidth) + 1
		if distance == 0 {
			cells += rowCells
		} else {
			cells += 2 * rowCells
		}
	}
	if cells > maxWorldReadChunks {
		return errorResponse("INVALID_ARGUMENT", "chunk radius must cover at most 256 chunks")
	}

	s.engine.mu.RLock()
	defer s.engine.mu.RUnlock()

	items := make([][]byte, 0, int(cells))
	var wireBytes uint64
	for dx := -radius; dx <= radius; dx++ {
		x, ok := checkedAddInt64(center.X, dx)
		if !ok {
			continue
		}
		absoluteX := dx
		if absoluteX < 0 {
			absoluteX = -absoluteX
		}
		yLimit := halfWidths[absoluteX]
		for dy := -yLimit; dy <= yLimit; dy++ {
			y, ok := checkedAddInt64(center.Y, dy)
			if !ok {
				continue
			}
			var failure *Response
			items, wireBytes, failure = s.appendWorldChunk(items, wireBytes, geometry.Coord{X: x, Y: y})
			if failure != nil {
				return *failure
			}
		}
	}
	return arrayResponse(items)
}

func (s *Session) appendWorldChunk(
	items [][]byte,
	wireBytes uint64,
	coord geometry.Coord,
) ([][]byte, uint64, *Response) {
	chunk, found, err := s.readChunk(coord)
	if err != nil {
		response := errorResponse("STORAGE", "read failed")
		return items, wireBytes, &response
	}
	if !found || !chunkPopulated(chunk) {
		return items, wireBytes, nil
	}

	payloadBytes := uint64(s.engine.geometry.PayloadBytes())
	presenceBytes := uint64(s.engine.geometry.PresenceBytes())
	itemWireBytes, ok := worldChunkWireSize(coord, payloadBytes, presenceBytes)
	if !ok {
		response := errorResponse("OUT_OF_RANGE", "world read response exceeds the 64 MiB limit")
		return items, wireBytes, &response
	}
	totalBytes := arrayHeaderSize(len(items)+1) + wireBytes + itemWireBytes
	if totalBytes > maxWorldReadResponseSize {
		response := errorResponse("OUT_OF_RANGE", "world read response exceeds the 64 MiB limit")
		return items, wireBytes, &response
	}

	payload := chunk.Bytes()
	presence := chunk.PresenceBytes()
	itemBytes := uint64(len(strconv.FormatInt(coord.X, 10))) +
		uint64(len(strconv.FormatInt(coord.Y, 10))) +
		3 + 2*payloadBytes + 2*presenceBytes
	item := make([]byte, 0, int(itemBytes))
	item = strconv.AppendInt(item, coord.X, 10)
	item = append(item, ' ')
	item = strconv.AppendInt(item, coord.Y, 10)
	item = append(item, ' ')
	payloadStart := len(item)
	item = append(item, make([]byte, hex.EncodedLen(len(payload)))...)
	hex.Encode(item[payloadStart:], payload)
	item = append(item, '|')
	presenceStart := len(item)
	item = append(item, make([]byte, hex.EncodedLen(len(presence)))...)
	hex.Encode(item[presenceStart:], presence)
	return append(items, item), wireBytes + itemWireBytes, nil
}

func worldChunkWireSize(coord geometry.Coord, payloadBytes, presenceBytes uint64) (uint64, bool) {
	if payloadBytes > maxWorldReadResponseSize/2 ||
		presenceBytes > maxWorldReadResponseSize/2 {
		return 0, false
	}
	itemBytes := uint64(len(strconv.FormatInt(coord.X, 10))) +
		uint64(len(strconv.FormatInt(coord.Y, 10))) +
		3 + 2*payloadBytes + 2*presenceBytes
	return 1 + decimalUint64Length(itemBytes) + 2 + itemBytes + 2, true
}

func chunkPopulated(chunk *storage.Chunk) bool {
	for _, value := range chunk.PresenceBytes() {
		if value != 0 {
			return true
		}
	}
	return false
}

func checkedAddInt64(left, right int64) (int64, bool) {
	if right > 0 && left > math.MaxInt64-right {
		return 0, false
	}
	if right < 0 && left < math.MinInt64-right {
		return 0, false
	}
	return left + right, true
}

func decimalUint64Length(value uint64) uint64 {
	return uint64(len(strconv.FormatUint(value, 10)))
}

func arrayHeaderSize(items int) uint64 {
	return 1 + uint64(len(strconv.Itoa(items))) + 2
}
