package protocol

import (
	"errors"
	"os"
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
			want: "$118\r\nregiondb_version=1\ncache_hits=0\ncache_misses=0\nloaded_chunks=0\n" +
				"dirty_chunks=0\nevictions=0\nwal_flushes=0\ncheckpoints=0\n\r\n",
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
		{frame: "SET -1 0 7\r\n", want: "+OK\r\n"},
		{frame: "GET -1 0\r\n", want: "$1\r\n7\r\n"},
		{frame: "EXISTS -1 0\r\n", want: "+OK 1\r\n"},
		{frame: "CHUNK -1 0\r\n", want: "$4\r\n3800\r\n"},
		{frame: "CHUNKSET 2 -3 4104\r\n", want: "+OK\r\n"},
		{frame: "CHUNK 2 -3\r\n", want: "$4\r\n4104\r\n"},
		{frame: "CHUNKBIN 2 -3\r\n", want: "$2\r\n\x41\x04\r\n"},
		{frame: "GET 4 -6\r\n", want: "$1\r\n1\r\n"},
		{frame: "GET 5 -6\r\n", want: "$1\r\n0\r\n"},
		{frame: "GET 4 -5\r\n", want: "$1\r\n1\r\n"},
		{frame: "GET 5 -5\r\n", want: "$1\r\n2\r\n"},
		{frame: "CHUNK 99 99\r\n", want: "-ERR NOT_FOUND chunk does not exist\r\n"},
		{frame: "CHUNKBIN 99 99\r\n", want: "-ERR NOT_FOUND chunk does not exist\r\n"},
	}

	for _, test := range tests {
		assertResponse(t, session, test.frame, test.want)
	}
}

func TestSessionInfoRuntimeCounters(t *testing.T) {
	t.Parallel()

	g := testGeometry(t)
	store := &memoryStore{
		chunks: make(map[geometry.Coord]*storage.Chunk),
		stats: storage.RuntimeStats{
			CacheHits:    1,
			CacheMisses:  2,
			LoadedChunks: 3,
			DirtyChunks:  4,
			Evictions:    5,
			WALFlushes:   6,
			Checkpoints:  7,
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
		"$118\r\nregiondb_version=1\ncache_hits=1\ncache_misses=2\nloaded_chunks=3\n"+
			"dirty_chunks=4\nevictions=5\nwal_flushes=6\ncheckpoints=7\n\r\n",
	)
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
			name:  "unknown command",
			frame: "NOPE\r\n",
			want:  "-ERR COMMAND unknown command\r\n",
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
		"SET 1 2\r\n",
		"EXISTS 1 2 3\r\n",
		"CHUNK 1\r\n",
		"CHUNKBIN 1\r\n",
		"CHUNKSET 1 2\r\n",
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
			want: "$118\r\nregiondb_version=1\ncache_hits=0\ncache_misses=0\nloaded_chunks=0\n" +
				"dirty_chunks=0\nevictions=0\nwal_flushes=0\ncheckpoints=0\n\r\n",
		},
		{name: "CHUNK_text", frame: []byte("CHUNK 2 -3\r\n"), want: "$4\r\n4104\r\n"},
		{name: "CHUNK_binary", frame: []byte("CHUNKBIN 2 -3\r\n"), want: "$2\r\n\x41\x04\r\n"},
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

	store.readErr = errors.New("read failure")
	assertResponse(t, session, "GET 0 0\r\n", "-ERR STORAGE read failed\r\n")
	assertResponse(t, session, "CHUNKBIN 0 0\r\n", "-ERR STORAGE read failed\r\n")
	store.readErr = nil
	store.writeErr = errors.New("write failure")
	assertResponse(t, session, "SET 0 0 1\r\n", "-ERR STORAGE write failed\r\n")
	assertResponse(t, session, "CHUNKSET 0 0 0000\r\n", "-ERR STORAGE write failed\r\n")
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
	chunks   map[geometry.Coord]*storage.Chunk
	readErr  error
	writeErr error
	stats    storage.RuntimeStats
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
	return storage.ChunkFromBytes(chunk.Geometry(), chunk.Bytes())
}

func (s *memoryStore) WriteChunk(coord geometry.Coord, chunk *storage.Chunk) error {
	if s.writeErr != nil {
		return s.writeErr
	}
	cloned, err := storage.ChunkFromBytes(chunk.Geometry(), chunk.Bytes())
	if err != nil {
		return err
	}
	s.chunks[coord] = cloned
	return nil
}
