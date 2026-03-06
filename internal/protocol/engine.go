package protocol

import (
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"

	"github.com/Filo6699/regiondb/internal/bitcodec"
	"github.com/Filo6699/regiondb/internal/geometry"
	"github.com/Filo6699/regiondb/internal/storage"
)

type ChunkStore interface {
	ReadChunk(geometry.Coord) (*storage.Chunk, error)
	WriteChunk(geometry.Coord, *storage.Chunk) error
}

type Engine struct {
	geometry geometry.Geometry
	store    ChunkStore
	token    string
	mu       sync.RWMutex
}

type Session struct {
	engine        *Engine
	authenticated bool
	closed        bool
}

func NewEngine(g geometry.Geometry, store ChunkStore, token string) (*Engine, error) {
	validated, err := geometry.New(g.Config())
	if err != nil || validated != g {
		return nil, fmt.Errorf("create protocol engine: %w", geometry.ErrInvalid)
	}
	if store == nil {
		return nil, errors.New("create protocol engine: chunk store must not be nil")
	}
	if token == "" {
		return nil, errors.New("create protocol engine: authentication token must not be empty")
	}
	for _, character := range []byte(token) {
		if character < 0x21 || character > 0x7e {
			return nil, errors.New("create protocol engine: authentication token must be printable ASCII without spaces")
		}
	}
	return &Engine{geometry: g, store: store, token: token}, nil
}

func (e *Engine) NewSession() *Session {
	return &Session{engine: e}
}

func (s *Session) Authenticated() bool {
	return s.authenticated
}

func (s *Session) Closed() bool {
	return s.closed
}

func (s *Session) Handle(frame []byte) Response {
	if s.closed {
		return errorResponse("CLOSED", "session is closed")
	}
	command, err := ParseFrame(frame)
	if err != nil {
		return errorResponse("FRAME", err.Error())
	}
	return s.Execute(command)
}

func (s *Session) Execute(command Command) Response {
	if s.closed {
		return errorResponse("CLOSED", "session is closed")
	}

	switch command.Name {
	case "AUTH":
		return s.authenticate(command.Args)
	case "QUIT":
		if response := requireArity(command.Args, 0); response != nil {
			return *response
		}
		s.closed = true
		return okResponse("")
	}

	if !s.authenticated {
		return errorResponse("NOAUTH", "authentication required")
	}

	switch command.Name {
	case "PING":
		if response := requireArity(command.Args, 0); response != nil {
			return *response
		}
		return okResponse("PONG")
	case "GET":
		return s.get(command.Args)
	case "SET":
		return s.set(command.Args)
	case "EXISTS":
		return s.exists(command.Args)
	case "CHUNK":
		return s.chunk(command.Args)
	case "CHUNKSET":
		return s.chunkSet(command.Args)
	default:
		return errorResponse("COMMAND", "unknown command")
	}
}

func (s *Session) authenticate(args []string) Response {
	if response := requireArity(args, 1); response != nil {
		return *response
	}
	if args[0] != s.engine.token {
		s.authenticated = false
		return errorResponse("AUTH", "authentication failed")
	}
	s.authenticated = true
	return okResponse("")
}

func (s *Session) get(args []string) Response {
	if response := requireArity(args, 2); response != nil {
		return *response
	}
	block, response := parseCoord(args)
	if response != nil {
		return *response
	}
	s.engine.mu.RLock()
	defer s.engine.mu.RUnlock()
	value, response := s.readBlock(block)
	if response != nil {
		return *response
	}
	return bulkResponse([]byte(strconv.FormatUint(value, 10)))
}

func (s *Session) set(args []string) Response {
	if response := requireArity(args, 3); response != nil {
		return *response
	}
	block, response := parseCoord(args[:2])
	if response != nil {
		return *response
	}
	value, err := parseUint(args[2])
	if err != nil {
		return errorResponse("NUMBER", "value must be an unsigned decimal integer")
	}
	if !fitsBits(value, s.engine.geometry.Config().BlockBits) {
		return errorResponse("BIT_VALUE", "value exceeds configured block width")
	}

	s.engine.mu.Lock()
	defer s.engine.mu.Unlock()
	mapping := s.engine.geometry.BlockToChunk(block)
	chunk, found, err := s.readChunk(mapping.Chunk)
	if err != nil {
		return errorResponse("STORAGE", "read failed")
	}
	if !found {
		chunk, err = storage.NewChunk(s.engine.geometry)
		if err != nil {
			return errorResponse("STORAGE", "chunk allocation failed")
		}
	}
	if err := chunk.Set(mapping.Offset, value); err != nil {
		if errors.Is(err, bitcodec.ErrValueTooWide) {
			return errorResponse("BIT_VALUE", "value exceeds configured block width")
		}
		return errorResponse("STORAGE", "block update failed")
	}
	if err := s.engine.store.WriteChunk(mapping.Chunk, chunk); err != nil {
		return errorResponse("STORAGE", "write failed")
	}
	return okResponse("")
}

func (s *Session) exists(args []string) Response {
	if response := requireArity(args, 2); response != nil {
		return *response
	}
	block, response := parseCoord(args)
	if response != nil {
		return *response
	}
	s.engine.mu.RLock()
	defer s.engine.mu.RUnlock()
	value, response := s.readBlock(block)
	if response != nil {
		return *response
	}
	if value == 0 {
		return okResponse("0")
	}
	return okResponse("1")
}

func (s *Session) chunk(args []string) Response {
	if response := requireArity(args, 2); response != nil {
		return *response
	}
	coord, response := parseCoord(args)
	if response != nil {
		return *response
	}
	s.engine.mu.RLock()
	defer s.engine.mu.RUnlock()
	chunk, found, err := s.readChunk(coord)
	if err != nil {
		return errorResponse("STORAGE", "read failed")
	}
	if !found {
		return errorResponse("NOT_FOUND", "chunk does not exist")
	}
	payload := chunk.Bytes()
	encoded := make([]byte, hex.EncodedLen(len(payload)))
	hex.Encode(encoded, payload)
	return bulkResponse(encoded)
}

func (s *Session) chunkSet(args []string) Response {
	if response := requireArity(args, 3); response != nil {
		return *response
	}
	coord, response := parseCoord(args[:2])
	if response != nil {
		return *response
	}
	payloadBytes := s.engine.geometry.PayloadBytes()
	if payloadBytes > int(^uint(0)>>1)/2 || len(args[2]) != hex.EncodedLen(payloadBytes) {
		return errorResponse("PAYLOAD", "packed chunk has an invalid length")
	}
	payload := make([]byte, payloadBytes)
	if _, err := hex.Decode(payload, []byte(args[2])); err != nil {
		return errorResponse("PAYLOAD", "packed chunk must be hexadecimal")
	}
	chunk, err := storage.ChunkFromBytes(s.engine.geometry, payload)
	if err != nil {
		return errorResponse("PAYLOAD", "packed chunk is invalid")
	}
	s.engine.mu.Lock()
	defer s.engine.mu.Unlock()
	if err := s.engine.store.WriteChunk(coord, chunk); err != nil {
		return errorResponse("STORAGE", "write failed")
	}
	return okResponse("")
}

func (s *Session) readBlock(block geometry.Coord) (uint64, *Response) {
	mapping := s.engine.geometry.BlockToChunk(block)
	chunk, found, err := s.readChunk(mapping.Chunk)
	if err != nil {
		response := errorResponse("STORAGE", "read failed")
		return 0, &response
	}
	if !found {
		return 0, nil
	}
	value, err := chunk.Get(mapping.Offset)
	if err != nil {
		response := errorResponse("STORAGE", "block read failed")
		return 0, &response
	}
	return value, nil
}

func (s *Session) readChunk(coord geometry.Coord) (*storage.Chunk, bool, error) {
	chunk, err := s.engine.store.ReadChunk(coord)
	if err == nil {
		if chunk == nil {
			return nil, false, errors.New("chunk store returned a nil chunk")
		}
		return chunk, true, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	return nil, false, err
}

func requireArity(args []string, want int) *Response {
	if len(args) == want {
		return nil
	}
	response := errorResponse("ARITY", "wrong number of arguments")
	return &response
}

func parseCoord(args []string) (geometry.Coord, *Response) {
	x, err := parseInt(args[0])
	if err != nil {
		response := errorResponse("NUMBER", "coordinates must be signed decimal integers")
		return geometry.Coord{}, &response
	}
	y, err := parseInt(args[1])
	if err != nil {
		response := errorResponse("NUMBER", "coordinates must be signed decimal integers")
		return geometry.Coord{}, &response
	}
	return geometry.Coord{X: x, Y: y}, nil
}

func parseInt(value string) (int64, error) {
	if !validDecimal(value, true) {
		return 0, strconv.ErrSyntax
	}
	return strconv.ParseInt(value, 10, 64)
}

func parseUint(value string) (uint64, error) {
	if !validDecimal(value, false) {
		return 0, strconv.ErrSyntax
	}
	return strconv.ParseUint(value, 10, 64)
}

func validDecimal(value string, signed bool) bool {
	if value == "" {
		return false
	}
	start := 0
	if signed && value[0] == '-' {
		if len(value) == 1 {
			return false
		}
		start = 1
	}
	for index := start; index < len(value); index++ {
		if value[index] < '0' || value[index] > '9' {
			return false
		}
	}
	return true
}

func fitsBits(value uint64, width uint8) bool {
	return width == 64 || value < uint64(1)<<width
}
