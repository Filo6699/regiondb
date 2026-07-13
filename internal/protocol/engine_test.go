package protocol

import (
	"errors"
	"math"
	"os"
	"sort"
	"testing"

	"github.com/Filo6699/regiondb/internal/geometry"
	"github.com/Filo6699/regiondb/internal/storage"
)

func TestSessionAuthentication(t *testing.T) {
	t.Parallel()

	session := newTestSession(t)
	tests := []struct {
		name              string
		frame             string
		want              string
		wantAuthenticated bool
	}{
		{
			name:              "command requires authentication",
			frame:             "PING\r\n",
			want:              "-ERR NOAUTH authentication required\r\n",
			wantAuthenticated: false,
		},
		{
			name:              "wrong token",
			frame:             "AUTH wrong\r\n",
			want:              "-ERR AUTH authentication failed\r\n",
			wantAuthenticated: false,
		},
		{
			name:              "correct token",
			frame:             "AUTH test-token\r\n",
			want:              "+OK\r\n",
			wantAuthenticated: true,
		},
		{
			name:              "authenticated ping",
			frame:             "PING\r\n",
			want:              "+OK PONG\r\n",
			wantAuthenticated: true,
		},
		{
			name:  "authenticated info",
			frame: "INFO\r\n",
			want: "$282\r\nregiondb_version=1\nprocess_lock_mode=\nchunk_lock_mode=\n" +
				"cache_hits=0\ncache_misses=0\nloaded_chunks=0\n" +
				"dirty_chunks=0\nevictions=0\neviction_runs=0\nwal_flushes=0\n" +
				"wal_foreground_flushes=0\nwal_group_flushes=0\n" +
				"wal_eviction_flushes=0\nwal_checkpoint_flushes=0\n" +
				"open_wal_handles=0\ncheckpoints=0\n\r\n",
			wantAuthenticated: true,
		},
		{
			name:              "failed reauthentication clears state",
			frame:             "AUTH wrong\r\n",
			want:              "-ERR AUTH authentication failed\r\n",
			wantAuthenticated: false,
		},
	}

	for _, test := range tests {
		if got := string(session.Handle([]byte(test.frame)).Bytes()); got != test.want {
			t.Fatalf("%s: Handle() = %q, want %q", test.name, got, test.want)
		}
		if session.Authenticated() != test.wantAuthenticated {
			t.Fatalf("%s: Authenticated() = %t, want %t", test.name, session.Authenticated(), test.wantAuthenticated)
		}
	}
}

func TestSessionWithoutAuthentication(t *testing.T) {
	t.Parallel()

	g, err := geometry.New(geometry.Config{ChunkEdge: 1, LargeChunkEdge: 1, BlockBits: 1})
	if err != nil {
		t.Fatal(err)
	}
	store := &memoryStore{chunks: make(map[geometry.Coord]*storage.Chunk)}
	engine, err := NewEngineWithoutAuth(g, store)
	if err != nil {
		t.Fatal(err)
	}
	session := engine.NewSession()
	assertResponse(t, session, "PING\r\n", "+OK PONG\r\n")
	assertResponse(t, session, "AUTH ignored\r\n", "+OK\r\n")
	if !session.Authenticated() || session.AuthenticationFailed() {
		t.Fatal("authentication-disabled session changed authentication state")
	}
}

func TestSessionWALFlush(t *testing.T) {
	t.Parallel()

	g := testGeometry(t)
	store := &memoryStore{chunks: make(map[geometry.Coord]*storage.Chunk)}
	engine, err := NewEngineWithoutAuth(g, store)
	if err != nil {
		t.Fatal(err)
	}
	session := engine.NewSession()

	assertResponse(t, session, "WALFLUSH extra\r\n", "-ERR ARITY wrong number of arguments\r\n")
	assertResponse(t, session, "WALFLUSH\r\n", "+OK\r\n")
	if store.flushCount != 1 {
		t.Fatalf("FlushWAL() calls = %d, want 1", store.flushCount)
	}
	store.flushErr = errors.New("injected barrier failure")
	assertResponse(t, session, "WALFLUSH\r\n", "-ERR STORAGE durability barrier failed\r\n")
}

func TestSessionBlockAndChunkCommands(t *testing.T) {
	t.Parallel()

	session := newTestSession(t)
	assertResponse(t, session, "AUTH test-token\r\n", "+OK\r\n")

	tests := []struct {
		frame string
		want  string
	}{
		{frame: "GET -1 0\r\n", want: "$1\r\n0\r\n"},
		{frame: "EXISTS -1 0\r\n", want: "+OK 0\r\n"},
		{frame: "CHUNKEXISTS -1 0\r\n", want: "+OK 0\r\n"},
		{frame: "SET -1 0 7\r\n", want: "+OK\r\n"},
		{frame: "CHUNKEXISTS -1 0\r\n", want: "+OK 1\r\n"},
		{frame: "GET -1 0\r\n", want: "$1\r\n7\r\n"},
		{frame: "EXISTS -1 0\r\n", want: "+OK 1\r\n"},
		{frame: "SET -1 0 0\r\n", want: "+OK\r\n"},
		{frame: "GET -1 0\r\n", want: "$1\r\n0\r\n"},
		{frame: "EXISTS -1 0\r\n", want: "+OK 1\r\n"},
		{frame: "UNSET -1 0\r\n", want: "+OK\r\n"},
		{frame: "GET -1 0\r\n", want: "$1\r\n0\r\n"},
		{frame: "EXISTS -1 0\r\n", want: "+OK 0\r\n"},
		{frame: "CHUNKEXISTS -1 0\r\n", want: "+OK 1\r\n"},
		{frame: "CHUNK -1 0\r\n", want: "$4\r\n0000\r\n"},
		{frame: "CHUNK -1 0 STATE\r\n", want: "$7\r\n0000|00\r\n"},
		{frame: "CHUNKBIN -1 0 STATE\r\n", want: "$3\r\n\x00\x00\x00\r\n"},
		{frame: "CHUNKBINC -1 0\r\n", want: "$1\r\n\x81\r\n"},
		{frame: "CHUNKBINC -1 0 STATE\r\n", want: "$1\r\n\x82\r\n"},
		{frame: "CHUNKSET 2 -3 4104\r\n", want: "+OK\r\n"},
		{frame: "CHUNKEXISTS 2 -3\r\n", want: "+OK 1\r\n"},
		{frame: "CHUNK 2 -3\r\n", want: "$4\r\n4104\r\n"},
		{frame: "CHUNK 2 -3 STATE\r\n", want: "$7\r\n4104|0f\r\n"},
		{frame: "CHUNKBIN 2 -3\r\n", want: "$2\r\n\x41\x04\r\n"},
		{frame: "CHUNKBIN 2 -3 STATE\r\n", want: "$3\r\n\x41\x04\x0f\r\n"},
		{frame: "CHUNKBINC 2 -3\r\n", want: "$3\r\n\x01\x41\x04\r\n"},
		{frame: "CHUNKBINC 2 -3 STATE\r\n", want: "$4\r\n\x02\x41\x04\x0f\r\n"},
		{frame: "GET 4 -6\r\n", want: "$1\r\n1\r\n"},
		{frame: "GET 5 -6\r\n", want: "$1\r\n0\r\n"},
		{frame: "GET 4 -5\r\n", want: "$1\r\n1\r\n"},
		{frame: "GET 5 -5\r\n", want: "$1\r\n2\r\n"},
		{frame: "CHUNKSET 3 -3 STATE 4104|05\r\n", want: "+OK\r\n"},
		{frame: "CHUNK 3 -3 STATE\r\n", want: "$7\r\n4100|05\r\n"},
		{frame: "CHUNKBIN 3 -3 STATE\r\n", want: "$3\r\n\x41\x00\x05\r\n"},
		{frame: "GET 6 -6\r\n", want: "$1\r\n1\r\n"},
		{frame: "EXISTS 6 -6\r\n", want: "+OK 1\r\n"},
		{frame: "GET 7 -6\r\n", want: "$1\r\n0\r\n"},
		{frame: "EXISTS 7 -6\r\n", want: "+OK 0\r\n"},
		{frame: "GET 6 -5\r\n", want: "$1\r\n1\r\n"},
		{frame: "EXISTS 6 -5\r\n", want: "+OK 1\r\n"},
		{frame: "GET 7 -5\r\n", want: "$1\r\n0\r\n"},
		{frame: "EXISTS 7 -5\r\n", want: "+OK 0\r\n"},
		{frame: "CHUNK 99 99\r\n", want: "-ERR NOT_FOUND chunk does not exist\r\n"},
		{frame: "CHUNK 99 99 STATE\r\n", want: "-ERR NOT_FOUND chunk does not exist\r\n"},
		{frame: "CHUNKBIN 99 99\r\n", want: "-ERR NOT_FOUND chunk does not exist\r\n"},
		{frame: "CHUNKBIN 99 99 STATE\r\n", want: "-ERR NOT_FOUND chunk does not exist\r\n"},
		{frame: "CHUNKBINC 99 99\r\n", want: "-ERR NOT_FOUND chunk does not exist\r\n"},
	}

	for _, test := range tests {
		assertResponse(t, session, test.frame, test.want)
	}
}

func TestSessionBatchCommands(t *testing.T) {
	t.Parallel()

	session := newTestSession(t)
	assertResponse(t, session, "AUTH test-token\r\n", "+OK\r\n")
	assertResponse(t, session, "MSET -1 0 7 0 0 0 3 -4 5\r\n", "+OK\r\n")
	assertResponse(
		t,
		session,
		"MGET -1 0 0 0 3 -4 99 99\r\n",
		"*4\r\n$1\r\n7\r\n$1\r\n0\r\n$1\r\n5\r\n$1\r\n0\r\n",
	)
}

func TestSessionMSetAppliesOnlySuccessfulPrefix(t *testing.T) {
	t.Parallel()

	g := testGeometry(t)
	store := &memoryStore{
		chunks:       make(map[geometry.Coord]*storage.Chunk),
		writeErrorAt: 2,
		writeErr:     errors.New("private path: /secret/data/chunk.rdb"),
	}
	engine, err := NewEngine(g, store, "test-token")
	if err != nil {
		t.Fatal(err)
	}
	session := engine.NewSession()
	assertResponse(t, session, "AUTH test-token\r\n", "+OK\r\n")
	assertResponse(t, session, "MSET 0 0 1 2 0 2 4 0 3\r\n", "-ERR STORAGE write failed\r\n")
	assertResponse(t, session, "MGET 0 0 2 0 4 0\r\n", "*3\r\n$1\r\n1\r\n$1\r\n0\r\n$1\r\n0\r\n")
	if store.writeCount != 1 {
		t.Fatalf("successful WriteChunk() calls = %d, want 1", store.writeCount)
	}
}

func TestAuthenticationTokenComparison(t *testing.T) {
	t.Parallel()

	tests := []struct {
		left  string
		right string
		want  bool
	}{
		{left: "same-token", right: "same-token", want: true},
		{left: "same-token", right: "same-tokem"},
		{left: "short", right: "longer"},
		{left: "", right: "", want: true},
	}
	for _, test := range tests {
		if got := tokensEqual(test.left, test.right); got != test.want {
			t.Fatalf("tokensEqual(%q, %q) = %t, want %t", test.left, test.right, got, test.want)
		}
	}
}

func TestSessionInfoRuntimeCounters(t *testing.T) {
	t.Parallel()

	g := testGeometry(t)
	store := &memoryStore{
		chunks: make(map[geometry.Coord]*storage.Chunk),
		stats: storage.RuntimeStats{
			ProcessLockMode:      "flock",
			ChunkLockMode:        "shared-rwmutex",
			CacheHits:            1,
			CacheMisses:          2,
			LoadedChunks:         3,
			DirtyChunks:          4,
			Evictions:            5,
			EvictionRuns:         9,
			WALFlushes:           6,
			WALForegroundFlushes: 1,
			WALGroupFlushes:      2,
			WALEvictionFlushes:   1,
			WALCheckpointFlushes: 2,
			OpenWALHandles:       8,
			Checkpoints:          7,
		},
	}
	engine, err := NewEngine(g, store, "test-token")
	if err != nil {
		t.Fatal(err)
	}
	session := engine.NewSession()
	assertResponse(t, session, "AUTH test-token\r\n", "+OK\r\n")
	assertResponse(
		t,
		session,
		"INFO\r\n",
		"$301\r\nregiondb_version=1\nprocess_lock_mode=flock\nchunk_lock_mode=shared-rwmutex\n"+
			"cache_hits=1\ncache_misses=2\nloaded_chunks=3\n"+
			"dirty_chunks=4\nevictions=5\neviction_runs=9\nwal_flushes=6\n"+
			"wal_foreground_flushes=1\nwal_group_flushes=2\n"+
			"wal_eviction_flushes=1\nwal_checkpoint_flushes=2\n"+
			"open_wal_handles=8\ncheckpoints=7\n\r\n",
	)
}

func TestSessionChunkStateWriteIsAtomic(t *testing.T) {
	t.Parallel()

	g := testGeometry(t)
	store := &memoryStore{chunks: make(map[geometry.Coord]*storage.Chunk)}
	engine, err := NewEngine(g, store, "test-token")
	if err != nil {
		t.Fatal(err)
	}
	session := engine.NewSession()
	assertResponse(t, session, "AUTH test-token\r\n", "+OK\r\n")

	assertResponse(t, session, "CHUNKSET 1 2 STATE 4104|05\r\n", "+OK\r\n")
	if store.writeCount != 1 {
		t.Fatalf("WriteChunk() calls = %d, want 1", store.writeCount)
	}
	assertResponse(t, session, "CHUNK 1 2 STATE\r\n", "$7\r\n4100|05\r\n")

	assertResponse(
		t,
		session,
		"CHUNKSET 1 2 STATE ffff|10\r\n",
		"-ERR PAYLOAD packed chunk state is invalid\r\n",
	)
	if store.writeCount != 1 {
		t.Fatalf("WriteChunk() calls after invalid state = %d, want 1", store.writeCount)
	}
	assertResponse(t, session, "CHUNK 1 2 STATE\r\n", "$7\r\n4100|05\r\n")
}

func TestSessionWorldReadCommands(t *testing.T) {
	t.Parallel()

	g := testGeometry(t)
	store := &memoryStore{chunks: make(map[geometry.Coord]*storage.Chunk)}
	putTestChunk(t, store, g, geometry.Coord{X: -2, Y: 1}, geometry.Offset{}, 5)
	putTestChunk(t, store, g, geometry.Coord{X: 0, Y: -1}, geometry.Offset{}, 0)
	store.chunks[geometry.Coord{X: 1, Y: 0}] = mustProtocolChunk(t, g)
	putTestChunk(t, store, g, geometry.Coord{X: 2, Y: 2}, geometry.Offset{X: 1, Y: 1}, 7)
	putTestChunk(
		t,
		store,
		g,
		geometry.Coord{X: math.MaxInt64, Y: math.MaxInt64},
		geometry.Offset{},
		3,
	)

	engine, err := NewEngine(g, store, "test-token")
	if err != nil {
		t.Fatal(err)
	}
	session := engine.NewSession()
	assertResponse(t, session, "AUTH test-token\r\n", "+OK\r\n")

	assertResponse(
		t,
		session,
		"CHUNKSCAN 1\r\n",
		"*2\r\n$11\r\nCURSOR -2 1\r\n$4\r\n-2 1\r\n",
	)
	assertResponse(
		t,
		session,
		"CHUNKSCAN 1 -2 1\r\n",
		"*2\r\n$11\r\nCURSOR 0 -1\r\n$4\r\n0 -1\r\n",
	)
	assertResponse(
		t,
		session,
		"CHUNKSCAN 1 0 -1\r\n",
		"*2\r\n$10\r\nCURSOR 2 2\r\n$3\r\n2 2\r\n",
	)
	assertResponse(
		t,
		session,
		"CHUNKRANGE -2 -1 1 1\r\n",
		"*2\r\n$12\r\n-2 1 0500|01\r\n$12\r\n0 -1 0000|01\r\n",
	)
	assertResponse(
		t,
		session,
		"CHUNKRADIUS 0 0 1\r\n",
		"*1\r\n$12\r\n0 -1 0000|01\r\n",
	)
	assertResponse(
		t,
		session,
		"CHUNKRADIUS 9223372036854775807 9223372036854775807 1\r\n",
		"*1\r\n$47\r\n9223372036854775807 9223372036854775807 0300|01\r\n",
	)
	assertResponse(
		t,
		session,
		"CHUNKSCAN 1 9223372036854775807 9223372036854775807\r\n",
		"*1\r\n$3\r\nEND\r\n",
	)
	assertResponse(
		t,
		session,
		"CHUNKRANGE -9223372036854775808 -9223372036854775808 -9223372036854775808 -9223372036854775808\r\n",
		"*0\r\n",
	)
}

func TestSessionWorldReadValidationAndStorageErrors(t *testing.T) {
	t.Parallel()

	session := newTestSession(t)
	assertResponse(t, session, "AUTH test-token\r\n", "+OK\r\n")
	for _, test := range []struct {
		frame string
		want  string
	}{
		{
			frame: "CHUNKSCAN 0\r\n",
			want:  "-ERR INVALID_ARGUMENT limit must be between 1 and 256\r\n",
		},
		{
			frame: "CHUNKSCAN 257\r\n",
			want:  "-ERR INVALID_ARGUMENT limit must be between 1 and 256\r\n",
		},
		{
			frame: "CHUNKSCAN nope\r\n",
			want:  "-ERR NUMBER limit must be an unsigned decimal integer\r\n",
		},
		{
			frame: "CHUNKRANGE 1 0 0 1\r\n",
			want:  "-ERR INVALID_ARGUMENT chunk range corners must satisfy x0<=x1 and y0<=y1\r\n",
		},
		{
			frame: "CHUNKRANGE -9223372036854775808 0 9223372036854775807 0\r\n",
			want:  "-ERR INVALID_ARGUMENT chunk range must cover at most 256 chunks\r\n",
		},
		{
			frame: "CHUNKRANGE 0 0 255 1\r\n",
			want:  "-ERR INVALID_ARGUMENT chunk range must cover at most 256 chunks\r\n",
		},
		{
			frame: "CHUNKRADIUS 0 0 -1\r\n",
			want:  "-ERR INVALID_ARGUMENT chunk radius must be non-negative and cover at most 256 chunks\r\n",
		},
		{
			frame: "CHUNKRADIUS 0 0 10\r\n",
			want:  "-ERR INVALID_ARGUMENT chunk radius must cover at most 256 chunks\r\n",
		},
	} {
		assertResponse(t, session, test.frame, test.want)
	}

	g := testGeometry(t)
	store := &memoryStore{
		chunks:  make(map[geometry.Coord]*storage.Chunk),
		scanErr: errors.New("private scan failure"),
	}
	engine, err := NewEngine(g, store, "test-token")
	if err != nil {
		t.Fatal(err)
	}
	failing := engine.NewSession()
	assertResponse(t, failing, "AUTH test-token\r\n", "+OK\r\n")
	assertResponse(t, failing, "CHUNKSCAN 1\r\n", "-ERR STORAGE scan failed\r\n")
}

func TestWorldReadResponseCapIncludesFraming(t *testing.T) {
	t.Parallel()

	itemWireBytes, ok := worldChunkWireSize(geometry.Coord{}, maxWorldReadResponseSize/2, 0)
	if !ok {
		t.Fatal("worldChunkWireSize() rejected an overflow-safe input")
	}
	if total := arrayHeaderSize(1) + itemWireBytes; total <= maxWorldReadResponseSize {
		t.Fatalf("framed response size = %d, want over cap", total)
	}
	if _, ok := worldChunkWireSize(geometry.Coord{}, maxWorldReadResponseSize, 0); ok {
		t.Fatal("worldChunkWireSize() accepted an oversized payload")
	}
}

func TestSessionRejectsInvalidCommands(t *testing.T) {
	t.Parallel()

	session := newTestSession(t)
	assertResponse(t, session, "AUTH test-token\r\n", "+OK\r\n")

	tests := []struct {
		name  string
		frame string
		want  string
	}{
		{
			name:  "frame",
			frame: "PING\n",
			want:  "-ERR FRAME invalid command frame: command must end with CRLF\r\n",
		},
		{
			name:  "arity",
			frame: "GET 1\r\n",
			want:  "-ERR ARITY wrong number of arguments\r\n",
		},
		{
			name:  "coordinate syntax",
			frame: "GET +1 2\r\n",
			want:  "-ERR NUMBER coordinates must be signed decimal integers\r\n",
		},
		{
			name:  "coordinate overflow",
			frame: "GET 9223372036854775808 2\r\n",
			want:  "-ERR NUMBER coordinates must be signed decimal integers\r\n",
		},
		{
			name:  "unsigned value",
			frame: "SET 0 0 -1\r\n",
			want:  "-ERR NUMBER value must be an unsigned decimal integer\r\n",
		},
		{
			name:  "value overflow",
			frame: "SET 0 0 18446744073709551616\r\n",
			want:  "-ERR NUMBER value must be an unsigned decimal integer\r\n",
		},
		{
			name:  "bit width",
			frame: "SET 0 0 8\r\n",
			want:  "-ERR BIT_VALUE value exceeds configured block width\r\n",
		},
		{
			name:  "chunk payload length",
			frame: "CHUNKSET 0 0 00\r\n",
			want:  "-ERR PAYLOAD packed chunk has an invalid length\r\n",
		},
		{
			name:  "chunk payload encoding",
			frame: "CHUNKSET 0 0 zzzz\r\n",
			want:  "-ERR PAYLOAD packed chunk must be hexadecimal\r\n",
		},
		{
			name:  "chunk text mode",
			frame: "CHUNK 0 0 OTHER\r\n",
			want:  "-ERR MODE chunk mode must be STATE\r\n",
		},
		{
			name:  "chunk binary mode",
			frame: "CHUNKBIN 0 0 OTHER\r\n",
			want:  "-ERR MODE chunk mode must be STATE\r\n",
		},
		{
			name:  "compressed chunk binary mode",
			frame: "CHUNKBINC 0 0 OTHER\r\n",
			want:  "-ERR MODE chunk mode must be STATE\r\n",
		},
		{
			name:  "chunk state mode",
			frame: "CHUNKSET 0 0 OTHER 0000|00\r\n",
			want:  "-ERR MODE chunk mode must be STATE\r\n",
		},
		{
			name:  "chunk state separator",
			frame: "CHUNKSET 0 0 STATE 000000\r\n",
			want:  "-ERR PAYLOAD packed chunk state must be payload|presence\r\n",
		},
		{
			name:  "chunk state duplicate separator",
			frame: "CHUNKSET 0 0 STATE 0000|00|00\r\n",
			want:  "-ERR PAYLOAD packed chunk state must be payload|presence\r\n",
		},
		{
			name:  "chunk state payload length",
			frame: "CHUNKSET 0 0 STATE 00|00\r\n",
			want:  "-ERR PAYLOAD packed chunk state has an invalid length\r\n",
		},
		{
			name:  "chunk state presence length",
			frame: "CHUNKSET 0 0 STATE 0000|0000\r\n",
			want:  "-ERR PAYLOAD packed chunk state has an invalid length\r\n",
		},
		{
			name:  "chunk state payload encoding",
			frame: "CHUNKSET 0 0 STATE zzzz|00\r\n",
			want:  "-ERR PAYLOAD packed chunk payload must be hexadecimal\r\n",
		},
		{
			name:  "chunk state presence encoding",
			frame: "CHUNKSET 0 0 STATE 0000|zz\r\n",
			want:  "-ERR PAYLOAD chunk presence bitmap must be hexadecimal\r\n",
		},
		{
			name:  "chunk state unused payload bits",
			frame: "CHUNKSET 0 0 STATE 00f0|0f\r\n",
			want:  "-ERR PAYLOAD packed chunk state is invalid\r\n",
		},
		{
			name:  "chunk state unused presence bits",
			frame: "CHUNKSET 0 0 STATE 0000|10\r\n",
			want:  "-ERR PAYLOAD packed chunk state is invalid\r\n",
		},
		{
			name:  "unknown command",
			frame: "NOPE\r\n",
			want:  "-ERR COMMAND unknown command\r\n",
		},
		{
			name:  "empty MGET",
			frame: "MGET\r\n",
			want:  "-ERR ARITY wrong number of arguments\r\n",
		},
		{
			name:  "incomplete MGET item",
			frame: "MGET 0 0 1\r\n",
			want:  "-ERR ARITY wrong number of arguments\r\n",
		},
		{
			name:  "empty MSET",
			frame: "MSET\r\n",
			want:  "-ERR ARITY wrong number of arguments\r\n",
		},
		{
			name:  "incomplete MSET item",
			frame: "MSET 0 0 1 2\r\n",
			want:  "-ERR ARITY wrong number of arguments\r\n",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			assertResponse(t, session, test.frame, test.want)
		})
	}
}

func TestSessionEnforcesCommandArity(t *testing.T) {
	t.Parallel()

	session := newTestSession(t)
	assertResponse(t, session, "AUTH test-token\r\n", "+OK\r\n")

	frames := []string{
		"AUTH\r\n",
		"PING extra\r\n",
		"INFO extra\r\n",
		"GET 1\r\n",
		"MGET 1\r\n",
		"SET 1 2\r\n",
		"MSET 1 2\r\n",
		"UNSET 1\r\n",
		"EXISTS 1 2 3\r\n",
		"CHUNK 1\r\n",
		"CHUNKBIN 1\r\n",
		"CHUNKBINC 1\r\n",
		"CHUNKEXISTS 1\r\n",
		"CHUNKSET 1 2\r\n",
		"CHUNKSCAN\r\n",
		"CHUNKSCAN 1 2\r\n",
		"CHUNKRANGE 1 2 3\r\n",
		"CHUNKRADIUS 1 2\r\n",
		"QUIT extra\r\n",
	}
	for _, frame := range frames {
		assertResponse(t, session, frame, "-ERR ARITY wrong number of arguments\r\n")
	}
	if session.Closed() {
		t.Fatal("malformed QUIT closed the session")
	}
}

func BenchmarkSessionCommands(b *testing.B) {
	g := testGeometry(b)
	store := &memoryStore{chunks: make(map[geometry.Coord]*storage.Chunk)}
	chunk, err := storage.ChunkFromBytes(g, []byte{0x41, 0x04})
	if err != nil {
		b.Fatal(err)
	}
	store.chunks[geometry.Coord{X: 2, Y: -3}] = chunk
	engine, err := NewEngine(g, store, "test-token")
	if err != nil {
		b.Fatal(err)
	}

	benchmarks := []struct {
		name  string
		frame []byte
		want  string
	}{
		{name: "PING", frame: []byte("PING\r\n"), want: "+OK PONG\r\n"},
		{
			name:  "INFO",
			frame: []byte("INFO\r\n"),
			want: "$282\r\nregiondb_version=1\nprocess_lock_mode=\nchunk_lock_mode=\n" +
				"cache_hits=0\ncache_misses=0\nloaded_chunks=0\n" +
				"dirty_chunks=0\nevictions=0\neviction_runs=0\nwal_flushes=0\n" +
				"wal_foreground_flushes=0\nwal_group_flushes=0\n" +
				"wal_eviction_flushes=0\nwal_checkpoint_flushes=0\n" +
				"open_wal_handles=0\ncheckpoints=0\n\r\n",
		},
		{name: "CHUNK_text", frame: []byte("CHUNK 2 -3\r\n"), want: "$4\r\n4104\r\n"},
		{name: "CHUNK_binary", frame: []byte("CHUNKBIN 2 -3\r\n"), want: "$2\r\n\x41\x04\r\n"},
		{name: "CHUNK_compressed", frame: []byte("CHUNKBINC 2 -3\r\n"), want: "$3\r\n\x01\x41\x04\r\n"},
	}
	for _, benchmark := range benchmarks {
		b.Run(benchmark.name, func(b *testing.B) {
			session := engine.NewSession()
			if got := string(session.Handle([]byte("AUTH test-token\r\n")).Bytes()); got != "+OK\r\n" {
				b.Fatalf("AUTH response = %q", got)
			}
			if got := string(session.Handle(benchmark.frame).Bytes()); got != benchmark.want {
				b.Fatalf("warmup response = %q, want %q", got, benchmark.want)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				response := session.Handle(benchmark.frame).Bytes()
				if len(response) == 0 {
					b.Fatal("empty response")
				}
			}
		})
	}
}

func TestSessionStorageErrors(t *testing.T) {
	t.Parallel()

	g := testGeometry(t)
	store := &memoryStore{chunks: make(map[geometry.Coord]*storage.Chunk)}
	engine, err := NewEngine(g, store, "test-token")
	if err != nil {
		t.Fatal(err)
	}
	session := engine.NewSession()
	assertResponse(t, session, "AUTH test-token\r\n", "+OK\r\n")

	store.readErr = errors.New("read failure at /private/regiondb/chunk.rdb")
	assertResponse(t, session, "GET 0 0\r\n", "-ERR STORAGE read failed\r\n")
	assertResponse(t, session, "MGET 0 0\r\n", "-ERR STORAGE read failed\r\n")
	assertResponse(t, session, "CHUNKBIN 0 0\r\n", "-ERR STORAGE read failed\r\n")
	assertResponse(t, session, "CHUNKBINC 0 0\r\n", "-ERR STORAGE read failed\r\n")
	assertResponse(t, session, "CHUNKEXISTS 0 0\r\n", "-ERR STORAGE read failed\r\n")
	store.readErr = nil
	assertResponse(t, session, "SET 0 0 1\r\n", "+OK\r\n")
	store.writeErr = errors.New("write failure at /private/regiondb/chunk.rdb")
	assertResponse(t, session, "SET 0 0 2\r\n", "-ERR STORAGE write failed\r\n")
	assertResponse(t, session, "MSET 0 0 2\r\n", "-ERR STORAGE write failed\r\n")
	assertResponse(t, session, "UNSET 0 0\r\n", "-ERR STORAGE write failed\r\n")
	assertResponse(t, session, "CHUNKSET 0 0 0000\r\n", "-ERR STORAGE write failed\r\n")
	assertResponse(t, session, "CHUNKSET 0 0 STATE 0000|01\r\n", "-ERR STORAGE write failed\r\n")
}

func TestSessionQuit(t *testing.T) {
	t.Parallel()

	session := newTestSession(t)
	assertResponse(t, session, "QUIT\r\n", "+OK\r\n")
	if !session.Closed() {
		t.Fatal("Closed() = false after QUIT")
	}
	assertResponse(t, session, "PING\r\n", "-ERR CLOSED session is closed\r\n")
}

func TestNewEngineValidation(t *testing.T) {
	t.Parallel()

	g := testGeometry(t)
	store := &memoryStore{chunks: make(map[geometry.Coord]*storage.Chunk)}
	tests := []struct {
		name  string
		g     geometry.Geometry
		store ChunkStore
		token string
	}{
		{name: "geometry", g: geometry.Geometry{}, store: store, token: "token"},
		{name: "store", g: g, store: nil, token: "token"},
		{name: "empty token", g: g, store: store, token: ""},
		{name: "space in token", g: g, store: store, token: "not valid"},
		{name: "control in token", g: g, store: store, token: "not\tvalid"},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := NewEngine(test.g, test.store, test.token); err == nil {
				t.Fatal("NewEngine() succeeded")
			}
		})
	}
}

func newTestSession(t *testing.T) *Session {
	t.Helper()
	g := testGeometry(t)
	engine, err := NewEngine(g, &memoryStore{chunks: make(map[geometry.Coord]*storage.Chunk)}, "test-token")
	if err != nil {
		t.Fatal(err)
	}
	return engine.NewSession()
}

func testGeometry(t testing.TB) geometry.Geometry {
	t.Helper()
	g, err := geometry.New(geometry.Config{ChunkEdge: 2, LargeChunkEdge: 2, BlockBits: 3})
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func assertResponse(t *testing.T, session *Session, frame, want string) {
	t.Helper()
	if got := string(session.Handle([]byte(frame)).Bytes()); got != want {
		t.Fatalf("Handle(%q) = %q, want %q", frame, got, want)
	}
}

type memoryStore struct {
	chunks       map[geometry.Coord]*storage.Chunk
	readErr      error
	scanErr      error
	writeErr     error
	writeErrorAt int
	stats        storage.RuntimeStats
	writeCount   int
	flushCount   int
	flushErr     error
}

func (s *memoryStore) FlushWAL() error {
	s.flushCount++
	return s.flushErr
}

func (s *memoryStore) RuntimeStats() storage.RuntimeStats {
	return s.stats
}

func (s *memoryStore) ReadChunk(coord geometry.Coord) (*storage.Chunk, error) {
	if s.readErr != nil {
		return nil, s.readErr
	}
	chunk, ok := s.chunks[coord]
	if !ok {
		return nil, os.ErrNotExist
	}
	return storage.ChunkFromState(chunk.Geometry(), chunk.Bytes(), chunk.PresenceBytes())
}

func (s *memoryStore) ScanChunkCoords(
	hasCursor bool,
	cursor geometry.Coord,
	limit int,
) ([]geometry.Coord, bool, error) {
	if s.scanErr != nil {
		return nil, false, s.scanErr
	}
	coords := make([]geometry.Coord, 0, len(s.chunks))
	for coord := range s.chunks {
		if !hasCursor || chunkCoordBefore(cursor, coord) {
			coords = append(coords, coord)
		}
	}
	sort.Slice(coords, func(left, right int) bool {
		return chunkCoordBefore(coords[left], coords[right])
	})
	if len(coords) <= limit {
		return coords, false, nil
	}
	return coords[:limit], true, nil
}

func (s *memoryStore) WriteChunk(coord geometry.Coord, chunk *storage.Chunk) error {
	if s.writeErr != nil && (s.writeErrorAt == 0 || s.writeCount+1 == s.writeErrorAt) {
		return s.writeErr
	}
	cloned, err := storage.ChunkFromState(chunk.Geometry(), chunk.Bytes(), chunk.PresenceBytes())
	if err != nil {
		return err
	}
	s.chunks[coord] = cloned
	s.writeCount++
	return nil
}

func chunkCoordBefore(left, right geometry.Coord) bool {
	return left.X < right.X || left.X == right.X && left.Y < right.Y
}

func putTestChunk(
	t *testing.T,
	store *memoryStore,
	g geometry.Geometry,
	coord geometry.Coord,
	offset geometry.Offset,
	value uint64,
) {
	t.Helper()
	chunk := mustProtocolChunk(t, g)
	if err := chunk.Set(offset, value); err != nil {
		t.Fatal(err)
	}
	store.chunks[coord] = chunk
}

func mustProtocolChunk(t *testing.T, g geometry.Geometry) *storage.Chunk {
	t.Helper()
	chunk, err := storage.NewChunk(g)
	if err != nil {
		t.Fatal(err)
	}
	return chunk
}
