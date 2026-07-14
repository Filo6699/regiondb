package protocol

import (
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Filo6699/regiondb/internal/bitcodec"
	"github.com/Filo6699/regiondb/internal/geometry"
	"github.com/Filo6699/regiondb/internal/storage"
	"github.com/Filo6699/regiondb/internal/telemetry"
)

type ChunkStore interface {
	ReadChunk(geometry.Coord) (*storage.Chunk, error)
	ScanChunkCoords(bool, geometry.Coord, int) ([]geometry.Coord, bool, error)
	WriteChunk(geometry.Coord, *storage.Chunk) error
	RuntimeStats() storage.RuntimeStats
}

type VersionedChunkStore interface {
	ChunkVersion(geometry.Coord) (uint64, error)
	CompareAndSwapChunk(geometry.Coord, uint64, *storage.Chunk) (uint64, error)
	ConditionalWriteChunks([]storage.ConditionalMutation) ([]uint64, error)
}

type WALFlusher interface {
	FlushWAL() error
}

type Engine struct {
	geometry geometry.Geometry
	store    ChunkStore
	token    string
	auth     bool
	mu       sync.RWMutex
	metrics  telemetry.Metrics
}

type Session struct {
	engine        *Engine
	authenticated bool
	closed        bool
	commandArgs   []string
	handleMu      sync.Mutex
	authFailed    bool
}

func NewEngine(g geometry.Geometry, store ChunkStore, token string) (*Engine, error) {
	return newEngine(g, store, token, true)
}

func NewEngineWithoutAuth(g geometry.Geometry, store ChunkStore) (*Engine, error) {
	return newEngine(g, store, "", false)
}

func newEngine(g geometry.Geometry, store ChunkStore, token string, auth bool) (*Engine, error) {
	validated, err := geometry.New(g.Config())
	if err != nil || validated != g {
		return nil, fmt.Errorf("create protocol engine: %w", geometry.ErrInvalid)
	}
	if store == nil {
		return nil, errors.New("create protocol engine: chunk store must not be nil")
	}
	if auth && token == "" {
		return nil, errors.New("create protocol engine: authentication token must not be empty")
	}
	for _, character := range []byte(token) {
		if character < 0x21 || character > 0x7e {
			return nil, errors.New("create protocol engine: authentication token must be printable ASCII without spaces")
		}
	}
	return &Engine{geometry: g, store: store, token: token, auth: auth}, nil
}

func (e *Engine) NewSession() *Session {
	return &Session{
		engine:        e,
		authenticated: !e.auth,
		commandArgs:   make([]string, 0, 4),
	}
}

func (e *Engine) Metrics() *telemetry.Metrics {
	return &e.metrics
}

func (s *Session) Authenticated() bool {
	return s.authenticated
}

func (s *Session) Closed() bool {
	return s.closed
}

func (s *Session) AuthenticationFailed() bool {
	return s.authFailed
}

func (s *Session) Handle(frame []byte) Response {
	s.handleMu.Lock()
	defer s.handleMu.Unlock()
	s.authFailed = false

	if s.closed {
		return errorResponse("CLOSED", "session is closed")
	}
	command, err := parseFrame(frame, s.commandArgs)
	if err != nil {
		return errorResponse("FRAME", err.Error())
	}
	s.commandArgs = command.Args[:0]
	return s.Execute(command)
}

func (s *Session) Execute(command Command) (response Response) {
	started := time.Now()
	defer func() {
		s.engine.metrics.ObserveCommand(time.Since(started), response.kind == responseError)
	}()

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
	case "INFO":
		if response := requireArity(command.Args, 0); response != nil {
			return *response
		}
		return bulkResponse(infoPayload(s.engine.store.RuntimeStats()))
	case "METRICS":
		if response := requireArity(command.Args, 0); response != nil {
			return *response
		}
		return bulkResponse(s.engine.metrics.AppendPrometheus(nil, s.engine.store.RuntimeStats()))
	case "WALFLUSH":
		if response := requireArity(command.Args, 0); response != nil {
			return *response
		}
		store, ok := s.engine.store.(WALFlusher)
		if !ok {
			return errorResponse("STORAGE", "durability barrier unavailable")
		}
		s.engine.mu.Lock()
		defer s.engine.mu.Unlock()
		if err := store.FlushWAL(); err != nil {
			return errorResponse("STORAGE", "durability barrier failed")
		}
		return okResponse("")
	case "GET":
		return s.get(command.Args)
	case "MGET":
		return s.mget(command.Args)
	case "SET":
		return s.set(command.Args)
	case "MSET":
		return s.mset(command.Args)
	case "UNSET":
		return s.unset(command.Args)
	case "EXISTS":
		return s.exists(command.Args)
	case "CHUNK":
		return s.chunk(command.Args)
	case "CHUNKBIN":
		return s.chunkBinary(command.Args)
	case "CHUNKBINC":
		return s.chunkBinaryCompressed(command.Args)
	case "CHUNKEXISTS":
		return s.chunkExists(command.Args)
	case "CHUNKSET":
		return s.chunkSet(command.Args)
	case "CHUNKVER":
		return s.chunkVersion(command.Args)
	case "CHUNKCAS":
		return s.chunkCAS(command.Args)
	case "CHUNKBATCH":
		return s.chunkBatch(command.Args)
	case "CHUNKSCAN":
		return s.chunkScan(command.Args)
	case "CHUNKRANGE":
		return s.chunkRange(command.Args)
	case "CHUNKRADIUS":
		return s.chunkRadius(command.Args)
	default:
		return errorResponse("COMMAND", "unknown command")
	}
}

func (s *Session) chunkVersion(args []string) Response {
	if response := requireArity(args, 2); response != nil {
		return *response
	}
	coord, response := parseCoord(args)
	if response != nil {
		return *response
	}
	s.engine.mu.RLock()
	defer s.engine.mu.RUnlock()
	store, ok := s.engine.store.(VersionedChunkStore)
	if !ok {
		return errorResponse("STORAGE", "version operations unavailable")
	}
	version, err := store.ChunkVersion(coord)
	if err != nil {
		return errorResponse("STORAGE", "read version failed")
	}
	return okResponse(strconv.FormatUint(version, 10))
}

func (s *Session) chunkCAS(args []string) Response {
	if len(args) != 4 {
		return errorResponse("ARITY", "wrong number of arguments")
	}
	mutation, response := s.parseConditionalChunk(args)
	if response != nil {
		return *response
	}
	s.engine.mu.Lock()
	defer s.engine.mu.Unlock()
	store, ok := s.engine.store.(VersionedChunkStore)
	if !ok {
		return errorResponse("STORAGE", "version operations unavailable")
	}
	version, err := store.CompareAndSwapChunk(
		mutation.Coord,
		mutation.ExpectedVersion,
		mutation.Chunk,
	)
	if errors.Is(err, storage.ErrVersionMismatch) {
		return errorResponse("VERSION_MISMATCH", "chunk version changed")
	}
	if err != nil {
		return errorResponse("STORAGE", "conditional write failed")
	}
	return okResponse(strconv.FormatUint(version, 10))
}

func (s *Session) chunkBatch(args []string) Response {
	if len(args) == 0 || len(args)%4 != 0 {
		return errorResponse("ARITY", "wrong number of arguments")
	}
	mutations := make([]storage.ConditionalMutation, 0, len(args)/4)
	for index := 0; index < len(args); index += 4 {
		mutation, response := s.parseConditionalChunk(args[index : index+4])
		if response != nil {
			return *response
		}
		mutations = append(mutations, mutation)
	}
	s.engine.mu.Lock()
	defer s.engine.mu.Unlock()
	store, ok := s.engine.store.(VersionedChunkStore)
	if !ok {
		return errorResponse("STORAGE", "version operations unavailable")
	}
	versions, err := store.ConditionalWriteChunks(mutations)
	if errors.Is(err, storage.ErrVersionMismatch) {
		return errorResponse("VERSION_MISMATCH", "chunk version changed")
	}
	if err != nil {
		return errorResponse("STORAGE", "conditional batch failed")
	}
	items := make([][]byte, len(versions))
	for index, version := range versions {
		items[index] = strconv.AppendUint(nil, version, 10)
	}
	return arrayResponse(items)
}

func (s *Session) parseConditionalChunk(args []string) (storage.ConditionalMutation, *Response) {
	coord, response := parseCoord(args[:2])
	if response != nil {
		return storage.ConditionalMutation{}, response
	}
	expected, err := parseUint(args[2])
	if err != nil {
		result := errorResponse("NUMBER", "version must be an unsigned decimal integer")
		return storage.ConditionalMutation{}, &result
	}
	chunk, response := s.parseChunkStateText(args[3])
	if response != nil {
		return storage.ConditionalMutation{}, response
	}
	return storage.ConditionalMutation{Coord: coord, ExpectedVersion: expected, Chunk: chunk}, nil
}

func (s *Session) parseChunkStateText(value string) (*storage.Chunk, *Response) {
	payloadText, presenceText, state := strings.Cut(value, "|")
	payloadBytes := s.engine.geometry.PayloadBytes()
	if payloadBytes > int(^uint(0)>>1)/2 || len(payloadText) != hex.EncodedLen(payloadBytes) {
		result := errorResponse("PAYLOAD", "packed chunk has an invalid length")
		return nil, &result
	}
	payload := make([]byte, payloadBytes)
	if _, err := hex.Decode(payload, []byte(payloadText)); err != nil {
		result := errorResponse("PAYLOAD", "packed chunk must be hexadecimal")
		return nil, &result
	}
	if !state {
		chunk, err := storage.ChunkFromBytes(s.engine.geometry, payload)
		if err != nil {
			result := errorResponse("PAYLOAD", "packed chunk is invalid")
			return nil, &result
		}
		return chunk, nil
	}
	if strings.Contains(presenceText, "|") ||
		len(presenceText) != hex.EncodedLen(s.engine.geometry.PresenceBytes()) {
		result := errorResponse("PAYLOAD", "packed chunk state has an invalid length")
		return nil, &result
	}
	presence := make([]byte, s.engine.geometry.PresenceBytes())
	if _, err := hex.Decode(presence, []byte(presenceText)); err != nil {
		result := errorResponse("PAYLOAD", "chunk presence bitmap must be hexadecimal")
		return nil, &result
	}
	chunk, err := storage.ChunkFromState(s.engine.geometry, payload, presence)
	if err != nil {
		result := errorResponse("PAYLOAD", "packed chunk state is invalid")
		return nil, &result
	}
	if response := s.canonicalizeChunkState(chunk); response != nil {
		return nil, response
	}
	return chunk, nil
}

func infoPayload(stats storage.RuntimeStats) []byte {
	payload := make([]byte, 0, 384)
	payload = append(payload, "regiondb_version=1\n"...)
	payload = appendInfoValue(payload, "process_lock_mode", stats.ProcessLockMode)
	payload = appendInfoValue(payload, "chunk_lock_mode", stats.ChunkLockMode)
	payload = appendInfoCounter(payload, "cache_hits", stats.CacheHits)
	payload = appendInfoCounter(payload, "cache_misses", stats.CacheMisses)
	payload = appendInfoCounter(payload, "loaded_chunks", stats.LoadedChunks)
	payload = appendInfoCounter(payload, "dirty_chunks", stats.DirtyChunks)
	payload = appendInfoCounter(payload, "evictions", stats.Evictions)
	payload = appendInfoCounter(payload, "eviction_runs", stats.EvictionRuns)
	payload = appendInfoCounter(payload, "wal_flushes", stats.WALFlushes)
	payload = appendInfoCounter(payload, "wal_foreground_flushes", stats.WALForegroundFlushes)
	payload = appendInfoCounter(payload, "wal_group_flushes", stats.WALGroupFlushes)
	payload = appendInfoCounter(payload, "wal_eviction_flushes", stats.WALEvictionFlushes)
	payload = appendInfoCounter(payload, "wal_checkpoint_flushes", stats.WALCheckpointFlushes)
	payload = appendInfoCounter(payload, "open_wal_handles", stats.OpenWALHandles)
	return appendInfoCounter(payload, "checkpoints", stats.Checkpoints)
}

func appendInfoValue(payload []byte, key, value string) []byte {
	payload = append(payload, key...)
	payload = append(payload, '=')
	payload = append(payload, value...)
	return append(payload, '\n')
}

func appendInfoCounter(payload []byte, key string, value uint64) []byte {
	payload = append(payload, key...)
	payload = append(payload, '=')
	payload = strconv.AppendUint(payload, value, 10)
	return append(payload, '\n')
}

func (s *Session) authenticate(args []string) Response {
	if response := requireArity(args, 1); response != nil {
		return *response
	}
	if !s.engine.auth {
		s.authenticated = true
		return okResponse("")
	}
	if !tokensEqual(args[0], s.engine.token) {
		s.authenticated = false
		s.authFailed = true
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
	value, _, response := s.readBlock(block)
	if response != nil {
		return *response
	}
	return bulkResponse([]byte(strconv.FormatUint(value, 10)))
}

func (s *Session) mget(args []string) Response {
	if len(args) == 0 || len(args)%2 != 0 {
		return errorResponse("ARITY", "wrong number of arguments")
	}
	items := make([][]byte, 0, len(args)/2)
	for index := 0; index < len(args); index += 2 {
		response := s.get(args[index : index+2])
		if response.kind != responseBulk {
			return response
		}
		items = append(items, response.payload)
	}
	return arrayResponse(items)
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

func (s *Session) mset(args []string) Response {
	if len(args) == 0 || len(args)%3 != 0 {
		return errorResponse("ARITY", "wrong number of arguments")
	}
	for index := 0; index < len(args); index += 3 {
		response := s.set(args[index : index+3])
		if response.kind != responseOK {
			return response
		}
	}
	return okResponse("")
}

func (s *Session) unset(args []string) Response {
	if response := requireArity(args, 2); response != nil {
		return *response
	}
	block, response := parseCoord(args)
	if response != nil {
		return *response
	}
	s.engine.mu.Lock()
	defer s.engine.mu.Unlock()
	mapping := s.engine.geometry.BlockToChunk(block)
	chunk, found, err := s.readChunk(mapping.Chunk)
	if err != nil {
		return errorResponse("STORAGE", "read failed")
	}
	if !found {
		return okResponse("")
	}
	if err := chunk.Unset(mapping.Offset); err != nil {
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
	_, exists, response := s.readBlock(block)
	if response != nil {
		return *response
	}
	if !exists {
		return okResponse("0")
	}
	return okResponse("1")
}

func (s *Session) chunk(args []string) Response {
	state, response := parseChunkReadMode(args)
	if response != nil {
		return *response
	}
	chunk, response := s.readChunkCommand(args[:2])
	if response != nil {
		return *response
	}
	payload := chunk.Bytes()
	if state {
		presence := chunk.PresenceBytes()
		encoded := make([]byte, hex.EncodedLen(len(payload))+1+hex.EncodedLen(len(presence)))
		payloadEnd := hex.Encode(encoded, payload)
		encoded[payloadEnd] = '|'
		hex.Encode(encoded[payloadEnd+1:], presence)
		return bulkResponse(encoded)
	}
	encoded := make([]byte, hex.EncodedLen(len(payload)))
	hex.Encode(encoded, payload)
	return bulkResponse(encoded)
}

func (s *Session) chunkBinary(args []string) Response {
	state, response := parseChunkReadMode(args)
	if response != nil {
		return *response
	}
	chunk, response := s.readChunkCommand(args[:2])
	if response != nil {
		return *response
	}
	if state {
		payload := chunk.Bytes()
		return bulkResponse(append(payload, chunk.PresenceBytes()...))
	}
	return bulkResponse(chunk.Bytes())
}

func (s *Session) chunkBinaryCompressed(args []string) Response {
	state, response := parseChunkReadMode(args)
	if response != nil {
		return *response
	}
	chunk, response := s.readChunkCommand(args[:2])
	if response != nil {
		return *response
	}
	payload := chunk.Bytes()
	if state {
		payload = append(payload, chunk.PresenceBytes()...)
	}
	return bulkResponse(storage.EncodeZRLE(payload))
}

func (s *Session) chunkExists(args []string) Response {
	if response := requireArity(args, 2); response != nil {
		return *response
	}
	coord, response := parseCoord(args)
	if response != nil {
		return *response
	}
	s.engine.mu.RLock()
	defer s.engine.mu.RUnlock()
	_, found, err := s.readChunk(coord)
	if err != nil {
		return errorResponse("STORAGE", "read failed")
	}
	if !found {
		return okResponse("0")
	}
	return okResponse("1")
}

func (s *Session) readChunkCommand(args []string) (*storage.Chunk, *Response) {
	if response := requireArity(args, 2); response != nil {
		return nil, response
	}
	coord, response := parseCoord(args)
	if response != nil {
		return nil, response
	}
	s.engine.mu.RLock()
	defer s.engine.mu.RUnlock()
	chunk, found, err := s.readChunk(coord)
	if err != nil {
		result := errorResponse("STORAGE", "read failed")
		return nil, &result
	}
	if !found {
		result := errorResponse("NOT_FOUND", "chunk does not exist")
		return nil, &result
	}
	return chunk, nil
}

func (s *Session) chunkSet(args []string) Response {
	switch len(args) {
	case 3:
		return s.writeChunkPayload(args)
	case 4:
		if args[2] != "STATE" {
			return errorResponse("MODE", "chunk mode must be STATE")
		}
		return s.writeChunkState(args)
	default:
		return errorResponse("ARITY", "wrong number of arguments")
	}
}

func (s *Session) writeChunkPayload(args []string) Response {
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

func (s *Session) writeChunkState(args []string) Response {
	coord, response := parseCoord(args[:2])
	if response != nil {
		return *response
	}
	payloadText, presenceText, found := strings.Cut(args[3], "|")
	if !found || strings.Contains(presenceText, "|") {
		return errorResponse("PAYLOAD", "packed chunk state must be payload|presence")
	}
	payloadBytes := s.engine.geometry.PayloadBytes()
	presenceBytes := s.engine.geometry.PresenceBytes()
	if payloadBytes > int(^uint(0)>>1)/2 ||
		presenceBytes > int(^uint(0)>>1)/2 ||
		len(payloadText) != hex.EncodedLen(payloadBytes) ||
		len(presenceText) != hex.EncodedLen(presenceBytes) {
		return errorResponse("PAYLOAD", "packed chunk state has an invalid length")
	}
	payload := make([]byte, payloadBytes)
	if _, err := hex.Decode(payload, []byte(payloadText)); err != nil {
		return errorResponse("PAYLOAD", "packed chunk payload must be hexadecimal")
	}
	payloadBits := s.engine.geometry.BlockCount() * uint64(s.engine.geometry.Config().BlockBits)
	if unused := uint64(payloadBytes)*8 - payloadBits; unused != 0 {
		mask := byte(0xff >> unused)
		if payload[len(payload)-1]&^mask != 0 {
			return errorResponse("PAYLOAD", "packed chunk state is invalid")
		}
	}
	presence := make([]byte, presenceBytes)
	if _, err := hex.Decode(presence, []byte(presenceText)); err != nil {
		return errorResponse("PAYLOAD", "chunk presence bitmap must be hexadecimal")
	}
	chunk, err := storage.ChunkFromState(s.engine.geometry, payload, presence)
	if err != nil {
		return errorResponse("PAYLOAD", "packed chunk state is invalid")
	}
	if response := s.canonicalizeChunkState(chunk); response != nil {
		return *response
	}
	s.engine.mu.Lock()
	defer s.engine.mu.Unlock()
	if err := s.engine.store.WriteChunk(coord, chunk); err != nil {
		return errorResponse("STORAGE", "write failed")
	}
	return okResponse("")
}

func (s *Session) canonicalizeChunkState(chunk *storage.Chunk) *Response {
	edge := uint64(s.engine.geometry.Config().ChunkEdge)
	for index := range s.engine.geometry.BlockCount() {
		offset := geometry.Offset{
			X: uint32(index % edge),
			Y: uint32(index / edge),
		}
		exists, err := chunk.Exists(offset)
		if err != nil {
			response := errorResponse("PAYLOAD", "packed chunk state is invalid")
			return &response
		}
		if !exists {
			if err := chunk.Unset(offset); err != nil {
				response := errorResponse("PAYLOAD", "packed chunk state is invalid")
				return &response
			}
		}
	}
	return nil
}

func parseChunkReadMode(args []string) (bool, *Response) {
	switch len(args) {
	case 2:
		return false, nil
	case 3:
		if args[2] == "STATE" {
			return true, nil
		}
		response := errorResponse("MODE", "chunk mode must be STATE")
		return false, &response
	default:
		response := errorResponse("ARITY", "wrong number of arguments")
		return false, &response
	}
}

func (s *Session) readBlock(block geometry.Coord) (uint64, bool, *Response) {
	mapping := s.engine.geometry.BlockToChunk(block)
	chunk, found, err := s.readChunk(mapping.Chunk)
	if err != nil {
		response := errorResponse("STORAGE", "read failed")
		return 0, false, &response
	}
	if !found {
		return 0, false, nil
	}
	value, err := chunk.Get(mapping.Offset)
	if err != nil {
		response := errorResponse("STORAGE", "block read failed")
		return 0, false, &response
	}
	exists, err := chunk.Exists(mapping.Offset)
	if err != nil {
		response := errorResponse("STORAGE", "block presence read failed")
		return 0, false, &response
	}
	return value, exists, nil
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

func tokensEqual(left, right string) bool {
	return subtle.ConstantTimeCompare([]byte(left), []byte(right)) == 1
}
