package server

import (
	"bufio"
	"context"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Filo6699/regiondb/internal/geometry"
	"github.com/Filo6699/regiondb/internal/logging"
	"github.com/Filo6699/regiondb/internal/protocol"
	"github.com/Filo6699/regiondb/internal/storage/fs_split"
)

func TestServePipelinedCommands(t *testing.T) {
	t.Parallel()

	g, err := geometry.New(geometry.Config{ChunkEdge: 2, LargeChunkEdge: 2, BlockBits: 4})
	if err != nil {
		t.Fatal(err)
	}
	store, err := fs_split.Open(t.TempDir(), g)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Fatalf("Close(): %v", err)
		}
	})
	engine, err := protocol.NewEngine(g, store, "secret")
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	serveResult := make(chan error, 1)
	go func() {
		serveResult <- Serve(ctx, listener, engine)
	}()

	connection, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = connection.Close()
	})

	request := "PING\r\nAUTH secret\r\nSET 0 0 5\r\nGET 0 0\r\nCHUNKBIN 0 0\r\nQUIT\r\n"
	if _, err := io.WriteString(connection, request); err != nil {
		cancel()
		t.Fatal(err)
	}
	reader := bufio.NewReader(connection)
	want := []string{
		"-ERR NOAUTH authentication required\r\n",
		"+OK\r\n",
		"+OK\r\n",
		"$1\r\n",
		"5\r\n",
		"$2\r\n",
		"\x05\x00\r\n",
		"+OK\r\n",
	}
	for _, expected := range want {
		got, err := reader.ReadString('\n')
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		if got != expected {
			cancel()
			t.Fatalf("response = %q, want %q", got, expected)
		}
	}
	if _, err := reader.ReadByte(); err != io.EOF {
		cancel()
		t.Fatalf("read after QUIT error = %v, want EOF", err)
	}

	cancel()
	if err := <-serveResult; err != nil {
		t.Fatalf("Serve() error = %v", err)
	}
}

func TestServePreservesPartialLinesAndMalformedBoundaries(t *testing.T) {
	t.Parallel()

	engine := newTestEngine(t)
	serverConnection, clientConnection := net.Pipe()
	t.Cleanup(func() {
		_ = clientConnection.Close()
	})

	serveDone := make(chan struct{})
	go func() {
		defer close(serveDone)
		serveConnection(serverConnection, engine, 64)
		_ = serverConnection.Close()
	}()

	writeResult := make(chan error, 1)
	go func() {
		if _, err := io.WriteString(clientConnection, "AUTH sec"); err != nil {
			writeResult <- err
			return
		}
		_, err := io.WriteString(clientConnection, "ret\r\nPING\nPING\r\nQUIT\r\n")
		writeResult <- err
	}()

	reader := bufio.NewReader(clientConnection)
	for _, want := range []string{
		"+OK\r\n",
		"-ERR FRAME invalid command frame: command must end with CRLF\r\n",
		"+OK PONG\r\n",
		"+OK\r\n",
	} {
		got, err := reader.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("response = %q, want %q", got, want)
		}
	}
	if err := <-writeResult; err != nil {
		t.Fatalf("write request: %v", err)
	}
	if _, err := reader.ReadByte(); err != io.EOF {
		t.Fatalf("read after QUIT error = %v, want EOF", err)
	}
	<-serveDone
}

func TestServeRejectsOversizedLineAndContinues(t *testing.T) {
	t.Parallel()

	engine := newTestEngine(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	serveResult := make(chan error, 1)
	go func() {
		serveResult <- ServeWithOptions(ctx, listener, engine, Options{
			Workers:      1,
			AcceptQueue:  1,
			MaxLineBytes: 16,
		})
	}()

	connection, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = connection.Close()
	})
	if _, err := io.WriteString(connection, "XXXXXXXXXXXXXXXX\r\nAUTH secret\r\n"); err != nil {
		cancel()
		t.Fatal(err)
	}
	reader := bufio.NewReader(connection)
	for _, want := range []string{
		"-ERR FRAME command exceeds max_line_bytes\r\n",
		"+OK\r\n",
	} {
		got, err := reader.ReadString('\n')
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		if got != want {
			cancel()
			t.Fatalf("response = %q, want %q", got, want)
		}
	}

	cancel()
	if err := <-serveResult; err != nil {
		t.Fatalf("ServeWithOptions() error = %v", err)
	}
}

func TestServeRejectsConnectionsBeyondBoundedQueue(t *testing.T) {
	t.Parallel()

	engine := newTestEngine(t)
	listener := newCountingListener()
	ctx, cancel := context.WithCancel(context.Background())
	serveResult := make(chan error, 1)
	go func() {
		serveResult <- ServeWithOptions(ctx, listener, engine, Options{
			Workers:      1,
			AcceptQueue:  1,
			MaxLineBytes: 64,
		})
	}()

	firstServer, firstClient := net.Pipe()
	firstReadStarted := make(chan struct{})
	waitForAccept(t, listener, 1)
	listener.connections <- &readObservedConnection{
		Conn:    firstServer,
		started: firstReadStarted,
	}
	select {
	case <-firstReadStarted:
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("timed out waiting for the first worker to become occupied")
	}

	secondServer, secondClient := net.Pipe()
	waitForAccept(t, listener, 2)
	listener.connections <- secondServer

	rejectedServer, rejectedClient := net.Pipe()
	waitForAccept(t, listener, 3)
	listener.connections <- rejectedServer

	if err := rejectedClient.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		cancel()
		t.Fatal(err)
	}
	response, err := bufio.NewReader(rejectedClient).ReadString('\n')
	if err != nil {
		cancel()
		t.Fatalf("read overload response: %v", err)
	}
	if want := "-ERR BUSY server overloaded\r\n"; response != want {
		cancel()
		t.Fatalf("overload response = %q, want %q", response, want)
	}
	t.Cleanup(func() {
		for _, connection := range []net.Conn{firstClient, secondClient, rejectedClient} {
			_ = connection.Close()
		}
	})

	cancel()
	if err := <-serveResult; err != nil {
		t.Fatalf("ServeWithOptions() error = %v", err)
	}
	if got := listener.accepts.Load(); got < 3 {
		t.Fatalf("Accept() calls = %d, want at least 3", got)
	}
	for index, connection := range []net.Conn{firstClient, secondClient} {
		if _, err := connection.Write([]byte("PING\r\n")); err == nil {
			t.Fatalf("connection %d remained open after cancellation", index)
		}
	}
}

func TestOverloadResponseWriteHasDeadline(t *testing.T) {
	t.Parallel()

	serverConnection, clientConnection := net.Pipe()
	t.Cleanup(func() {
		_ = serverConnection.Close()
		_ = clientConnection.Close()
	})

	started := time.Now()
	if err := rejectOverloadedConnection(serverConnection); err == nil {
		t.Fatal("rejectOverloadedConnection() succeeded without a reader")
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("overload response blocked for %v, want at most 2s", elapsed)
	}
}

func TestServeCancellationClosesIdleConnections(t *testing.T) {
	t.Parallel()

	g, err := geometry.New(geometry.Config{ChunkEdge: 1, LargeChunkEdge: 1, BlockBits: 1})
	if err != nil {
		t.Fatal(err)
	}
	store, err := fs_split.Open(t.TempDir(), g)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Fatalf("Close(): %v", err)
		}
	})
	engine, err := protocol.NewEngine(g, store, "secret")
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	serveResult := make(chan error, 1)
	go func() {
		serveResult <- Serve(ctx, listener, engine)
	}()
	connection, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = connection.Close()
	})

	cancel()
	if err := <-serveResult; err != nil {
		t.Fatalf("Serve() error = %v", err)
	}
	if _, err := connection.Write([]byte("PING\r\n")); err == nil {
		buffer := make([]byte, 1)
		if _, err := connection.Read(buffer); err == nil {
			t.Fatal("idle connection remained open after cancellation")
		}
	}
}

func TestServeLifecycleLogging(t *testing.T) {
	t.Parallel()

	engine := newTestEngine(t)
	listener := newCountingListener()
	sink := newRecordingSink()
	logger := logging.NewWithSink(sink, func() time.Time {
		return time.Date(2026, time.May, 3, 6, 11, 12, 0, time.UTC)
	})
	ctx, cancel := context.WithCancel(context.Background())
	serveResult := make(chan error, 1)
	go func() {
		serveResult <- ServeWithOptions(ctx, listener, engine, Options{
			Workers:      1,
			AcceptQueue:  1,
			MaxLineBytes: 64,
			Logger:       logger,
		})
	}()

	started := sink.next(t)
	if got, want := started.Message, "serve_started"; got != want {
		cancel()
		t.Fatalf("start event = %q, want %q", got, want)
	}
	if got, want := started.Time, time.Date(2026, time.May, 3, 6, 11, 12, 0, time.UTC); !got.Equal(want) {
		cancel()
		t.Fatalf("start timestamp = %v, want %v", got, want)
	}
	cancel()
	if err := <-serveResult; err != nil {
		t.Fatalf("ServeWithOptions() error = %v", err)
	}
	stopped := sink.next(t)
	if got, want := stopped.Message, "serve_stopped"; got != want {
		t.Fatalf("stop event = %q, want %q", got, want)
	}
	if extra := sink.records(); len(extra) != 0 {
		t.Fatalf("unexpected lifecycle events: %+v", extra)
	}
}

func TestServeOptionsValidation(t *testing.T) {
	t.Parallel()

	engine := newTestEngine(t)
	tests := []Options{
		{Workers: 0, AcceptQueue: 1, MaxLineBytes: 1},
		{Workers: 1, AcceptQueue: -1, MaxLineBytes: 1},
		{Workers: 1, AcceptQueue: 1, MaxLineBytes: 0},
	}
	for _, options := range tests {
		listener := newCountingListener()
		if err := ServeWithOptions(context.Background(), listener, engine, options); err == nil {
			t.Fatalf("ServeWithOptions(%+v) succeeded", options)
		}
	}
}

func BenchmarkReadFrameReuse(b *testing.B) {
	const frame = "SET -12 34 7\r\n"
	reader := &repeatingReader{data: []byte(frame)}
	buffer := newLineBuffer(64)

	b.ReportAllocs()
	for range b.N {
		got, tooLong, err := buffer.readFrame(reader)
		if err != nil || tooLong || string(got) != frame {
			b.Fatalf("readFrame() = (%q, %t, %v)", got, tooLong, err)
		}
	}
}

func newTestEngine(t *testing.T) *protocol.Engine {
	t.Helper()

	g, err := geometry.New(geometry.Config{ChunkEdge: 1, LargeChunkEdge: 1, BlockBits: 1})
	if err != nil {
		t.Fatal(err)
	}
	store, err := fs_split.Open(t.TempDir(), g)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Fatalf("Close(): %v", err)
		}
	})
	engine, err := protocol.NewEngine(g, store, "secret")
	if err != nil {
		t.Fatal(err)
	}
	return engine
}

type countingListener struct {
	connections chan net.Conn
	accepted    chan int
	closed      chan struct{}
	closeOnce   sync.Once
	accepts     atomic.Int32
}

func newCountingListener() *countingListener {
	return &countingListener{
		connections: make(chan net.Conn),
		accepted:    make(chan int),
		closed:      make(chan struct{}),
	}
}

func (l *countingListener) Accept() (net.Conn, error) {
	accepts := l.accepts.Add(1)
	select {
	case l.accepted <- int(accepts):
	case <-l.closed:
		return nil, net.ErrClosed
	}
	select {
	case connection := <-l.connections:
		return connection, nil
	case <-l.closed:
		return nil, net.ErrClosed
	}
}

func (l *countingListener) Close() error {
	l.closeOnce.Do(func() {
		close(l.closed)
	})
	return nil
}

func (l *countingListener) Addr() net.Addr {
	return testAddress("test")
}

type testAddress string

func (a testAddress) Network() string { return string(a) }
func (a testAddress) String() string  { return string(a) }

type readObservedConnection struct {
	net.Conn
	started chan struct{}
	once    sync.Once
}

func (c *readObservedConnection) Read(destination []byte) (int, error) {
	c.once.Do(func() {
		close(c.started)
	})
	return c.Conn.Read(destination)
}

func waitForAccept(t *testing.T, listener *countingListener, want int) {
	t.Helper()

	select {
	case got := <-listener.accepted:
		if got != want {
			t.Fatalf("Accept() calls = %d, want %d", got, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for Accept() call %d", want)
	}
}

type repeatingReader struct {
	data   []byte
	offset int
}

func (r *repeatingReader) Read(destination []byte) (int, error) {
	for index := range destination {
		destination[index] = r.data[r.offset]
		r.offset++
		if r.offset == len(r.data) {
			r.offset = 0
		}
	}
	return len(destination), nil
}

type recordingSink struct {
	events chan slog.Record
}

func newRecordingSink() *recordingSink {
	return &recordingSink{
		events: make(chan slog.Record, 8),
	}
}

func (s *recordingSink) Enabled(context.Context, slog.Level) bool {
	return true
}

func (s *recordingSink) Handle(_ context.Context, record slog.Record) error {
	s.events <- record.Clone()
	return nil
}

func (s *recordingSink) WithAttrs([]slog.Attr) slog.Handler {
	return s
}

func (s *recordingSink) WithGroup(string) slog.Handler {
	return s
}

func (s *recordingSink) next(t *testing.T) slog.Record {
	t.Helper()

	select {
	case record := <-s.events:
		return record
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for lifecycle log")
		return slog.Record{}
	}
}

func (s *recordingSink) records() []slog.Record {
	var records []slog.Record
	for {
		select {
		case record := <-s.events:
			records = append(records, record)
		default:
			return records
		}
	}
}
