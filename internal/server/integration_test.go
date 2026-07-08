package server

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/Filo6699/regiondb/internal/geometry"
	"github.com/Filo6699/regiondb/internal/protocol"
	"github.com/Filo6699/regiondb/internal/storage/fs_split"
)

func TestIntegrationTCP(t *testing.T) {
	t.Run("command lifecycle", testIntegrationTCPCommandLifecycle)
	t.Run("conditional chunks", testIntegrationTCPConditionalChunks)
	t.Run("concurrent clients", testIntegrationTCPConcurrentClients)
	t.Run("overload response", testIntegrationTCPOverloadResponse)
	t.Run("request deadline", testIntegrationTCPRequestDeadline)
}

func testIntegrationTCPConditionalChunks(t *testing.T) {
	address := startIntegrationServer(t, DefaultOptions())
	connection := dialIntegrationServer(t, address)
	defer func() { _ = connection.Close() }()

	request := "AUTH secret\r\n" +
		"CHUNKVER 1 1\r\n" +
		"CHUNKCAS 1 1 0 1000\r\n" +
		"CHUNKBATCH 1 1 1 2000 2 2 0 3000\r\n" +
		"CHUNKBATCH 1 1 2 4000 2 2 0 5000\r\n" +
		"CHUNKVER 1 1\r\nCHUNKVER 2 2\r\nCHUNK 1 1\r\nQUIT\r\n"
	if err := writeIntegrationRequest(connection, request); err != nil {
		t.Fatalf("write request: %v", err)
	}
	want := "+OK\r\n" +
		"+OK 0\r\n" +
		"+OK 1\r\n" +
		"*2\r\n$1\r\n2\r\n$1\r\n3\r\n" +
		"-ERR VERSION_MISMATCH chunk version changed\r\n" +
		"+OK 2\r\n+OK 3\r\n$4\r\n2000\r\n+OK\r\n"
	response := make([]byte, len(want))
	if _, err := io.ReadFull(connection, response); err != nil {
		t.Fatalf("read response: %v", err)
	}
	if got := string(response); got != want {
		t.Fatalf("response = %q, want %q", got, want)
	}
}

func testIntegrationTCPRequestDeadline(t *testing.T) {
	address := startIntegrationServer(t, Options{
		Workers:         1,
		AcceptQueue:     1,
		MaxLineBytes:    DefaultMaxLineBytes,
		IdleTimeout:     time.Second,
		RequestTimeout:  100 * time.Millisecond,
		ResponseTimeout: time.Second,
	})
	connection := dialIntegrationServer(t, address)
	defer func() {
		_ = connection.Close()
	}()
	writeErr := writeIntegrationRequest(connection, "P")
	if writeErr != nil {
		t.Fatalf("write partial request: %v", writeErr)
	}
	_, readErr := connection.Read(make([]byte, 1))
	if readErr == nil {
		t.Fatal("partial request remained open past its deadline")
	}
}

func testIntegrationTCPCommandLifecycle(t *testing.T) {
	address := startIntegrationServer(t, DefaultOptions())
	connection := dialIntegrationServer(t, address)
	defer func() {
		_ = connection.Close()
	}()

	request := "PING\r\nAUTH secret\r\nSET -1 2 9\r\nINFO\r\nEXISTS -1 2\r\nGET -1 2\r\nCHUNK -1 1\r\n" +
		"MSET 10 10 3 11 10 4\r\nMGET 10 10 11 10 12 10\r\n" +
		"CHUNK -1 1 STATE\r\nCHUNKBIN -1 1 STATE\r\nCHUNKSET 2 2 STATE 9100|01\r\nCHUNK 2 2 STATE\r\n" +
		"CHUNKEXISTS -1 1\r\nCHUNKEXISTS 99 99\r\nSET -1 2 0\r\nEXISTS -1 2\r\n" +
		"UNSET -1 2\r\nGET -1 2\r\nEXISTS -1 2\r\nCHUNKEXISTS -1 1\r\n" +
		"CHUNKSCAN 1\r\nCHUNKSCAN 1 2 2\r\nCHUNKRANGE 2 2 5 5\r\nCHUNKRADIUS 2 2 0\r\nQUIT\r\n"
	writeResult := make(chan error, 1)
	go func() {
		writeResult <- writeIntegrationRequest(connection, request)
	}()

	reader := bufio.NewReader(connection)
	infoResponse := []string{
		"regiondb_version=1\n",
		"process_lock_mode=" + expectedProcessLockMode() + "\n",
		"chunk_lock_mode=shared-rwmutex\n",
		"cache_hits=0\n",
		"cache_misses=1\n",
		"loaded_chunks=1\n",
		"dirty_chunks=0\n",
		"evictions=0\n",
		"eviction_runs=0\n",
		"wal_flushes=0\n",
		"wal_foreground_flushes=0\n",
		"wal_group_flushes=0\n",
		"wal_eviction_flushes=0\n",
		"wal_checkpoint_flushes=0\n",
		"open_wal_handles=2\n",
		"checkpoints=0\n",
	}
	infoBytes := 0
	for _, line := range infoResponse {
		infoBytes += len(line)
	}
	wantResponses := []string{
		"-ERR NOAUTH authentication required\r\n",
		"+OK\r\n",
		"+OK\r\n",
		fmt.Sprintf("$%d\r\n", infoBytes),
	}
	wantResponses = append(wantResponses, infoResponse...)
	wantResponses = append(wantResponses,
		"\r\n",
		"+OK 1\r\n",
		"$1\r\n",
		"9\r\n",
		"$4\r\n",
		"9000\r\n",
		"+OK\r\n",
		"*3\r\n",
		"$1\r\n",
		"3\r\n",
		"$1\r\n",
		"4\r\n",
		"$1\r\n",
		"0\r\n",
		"$7\r\n",
		"9000|02\r\n",
		"$3\r\n",
		"\x90\x00\x02\r\n",
		"+OK\r\n",
		"$7\r\n",
		"0100|01\r\n",
		"+OK 1\r\n",
		"+OK 0\r\n",
		"+OK\r\n",
		"+OK 1\r\n",
		"+OK\r\n",
		"$1\r\n",
		"0\r\n",
		"+OK 0\r\n",
		"+OK 1\r\n",
		"*2\r\n",
		"$10\r\n",
		"CURSOR 2 2\r\n",
		"$3\r\n",
		"2 2\r\n",
		"*2\r\n",
		"$3\r\n",
		"END\r\n",
		"$3\r\n",
		"5 5\r\n",
		"*2\r\n",
		"$11\r\n",
		"2 2 0100|01\r\n",
		"$11\r\n",
		"5 5 4300|03\r\n",
		"*1\r\n",
		"$11\r\n",
		"2 2 0100|01\r\n",
		"+OK\r\n",
	)
	for _, want := range wantResponses {
		got, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("read response: %v", err)
		}
		if got != want {
			t.Fatalf("response = %q, want %q", got, want)
		}
	}
	_, readErr := reader.ReadByte()
	if readErr != io.EOF {
		t.Fatalf("read after QUIT error = %v, want EOF", readErr)
	}
	if writeErr := <-writeResult; writeErr != nil {
		t.Fatalf("write request: %v", writeErr)
	}
}

func expectedProcessLockMode() string {
	if runtime.GOOS == "windows" {
		return "lock-file-ex"
	}
	return "flock"
}

func testIntegrationTCPConcurrentClients(t *testing.T) {
	const clientCount = 8

	address := startIntegrationServer(t, Options{
		Workers:      4,
		AcceptQueue:  clientCount,
		MaxLineBytes: DefaultMaxLineBytes,
	})
	start := make(chan struct{})
	results := make(chan error, clientCount)
	var clients sync.WaitGroup
	clients.Add(clientCount)

	for client := range clientCount {
		go func(client int) {
			defer clients.Done()
			<-start

			connection, err := net.DialTimeout(address.Network(), address.String(), 5*time.Second)
			if err != nil {
				results <- fmt.Errorf("client %d dial: %w", client, err)
				return
			}
			defer func() {
				_ = connection.Close()
			}()
			if err := connection.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
				results <- fmt.Errorf("client %d set deadline: %w", client, err)
				return
			}

			request := fmt.Sprintf("AUTH secret\r\nSET %d 0 %d\r\nGET %d 0\r\nQUIT\r\n", client, client, client)
			writeErr := writeIntegrationRequest(connection, request)
			if writeErr != nil {
				results <- fmt.Errorf("client %d write: %w", client, writeErr)
				return
			}
			want := fmt.Sprintf("+OK\r\n+OK\r\n$1\r\n%d\r\n+OK\r\n", client)
			response := make([]byte, len(want))
			_, readErr := io.ReadFull(connection, response)
			if readErr != nil {
				results <- fmt.Errorf("client %d read: %w", client, readErr)
				return
			}
			if got := string(response); got != want {
				results <- fmt.Errorf("client %d response = %q, want %q", client, got, want)
				return
			}
			results <- nil
		}(client)
	}

	close(start)
	clients.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Error(err)
		}
	}
}

func testIntegrationTCPOverloadResponse(t *testing.T) {
	g, err := geometry.New(geometry.Config{ChunkEdge: 1, LargeChunkEdge: 1, BlockBits: 1})
	if err != nil {
		t.Fatal(err)
	}
	store, err := fs_split.Open(t.TempDir(), g)
	if err != nil {
		t.Fatal(err)
	}
	engine, err := protocol.NewEngine(g, store, "secret")
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	tcpListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	listener := &integrationObservedListener{
		Listener:    tcpListener,
		accepted:    make(chan struct{}, 4),
		readStarted: make(chan struct{}, 5),
	}
	ctx, cancel := context.WithCancel(context.Background())
	serveResult := make(chan error, 1)
	go func() {
		serveResult <- ServeWithOptions(ctx, listener, engine, Options{
			Workers:      1,
			AcceptQueue:  1,
			MaxLineBytes: DefaultMaxLineBytes,
		})
	}()
	t.Cleanup(func() {
		cancel()
		if err := <-serveResult; err != nil {
			t.Errorf("ServeWithOptions() error = %v", err)
		}
		if err := store.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})

	first := dialIntegrationServer(t, listener.Addr())
	t.Cleanup(func() { _ = first.Close() })
	waitForIntegrationSignal(t, listener.accepted, "first accept")
	waitForIntegrationSignal(t, listener.readStarted, "first worker read")

	candidates := make([]net.Conn, 0, 3)
	for index := range 3 {
		connection := dialIntegrationServer(t, listener.Addr())
		candidates = append(candidates, connection)
		t.Cleanup(func() { _ = connection.Close() })
		waitForIntegrationSignal(t, listener.accepted, fmt.Sprintf("candidate %d accept", index))
	}

	writeErr := writeIntegrationRequest(first, "QUIT\r\n")
	if writeErr != nil {
		t.Fatalf("release occupied worker: %v", writeErr)
	}
	response, readErr := bufio.NewReader(first).ReadString('\n')
	if readErr != nil {
		t.Fatalf("read occupied worker response: %v", readErr)
	}
	if want := "+OK\r\n"; response != want {
		t.Fatalf("occupied worker response = %q, want %q", response, want)
	}

	served := 0
	rejected := 0
	closedWithoutReply := 0
	for index, connection := range candidates {
		if err := connection.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
			t.Fatalf("set candidate %d deadline: %v", index, err)
		}
		// A rejected peer may finish its close before this write reaches the
		// kernel. The overload response can still be readable from the socket,
		// so classify the server outcome below instead of requiring this write.
		_ = writeIntegrationRequest(connection, "PING\r\n")
		reader := bufio.NewReader(connection)
		response, err := reader.ReadString('\n')
		if err != nil {
			var networkError net.Error
			if errors.As(err, &networkError) && networkError.Timeout() {
				t.Fatalf("candidate %d rejection timed out: %v", index, err)
			}
			// Windows can turn the server's close into WSAECONNABORTED when
			// this client write races with the buffered overload reply. The
			// peer still rejected the accepted connection; only the reply is
			// unavailable, unlike a timeout that would leave the outcome
			// ambiguous.
			rejected++
			closedWithoutReply++
			continue
		}
		switch response {
		case "-ERR NOAUTH authentication required\r\n":
			writeErr := writeIntegrationRequest(connection, "QUIT\r\n")
			if writeErr != nil {
				t.Fatalf("close queued candidate %d: %v", index, writeErr)
			}
			response, readErr := reader.ReadString('\n')
			if readErr != nil {
				t.Fatalf("read queued candidate %d QUIT response: %v", index, readErr)
			}
			if want := "+OK\r\n"; response != want {
				t.Fatalf("queued candidate %d QUIT response = %q, want %q", index, response, want)
			}
			_, readErr = reader.ReadByte()
			if readErr != io.EOF {
				t.Fatalf("queued candidate %d remained open after QUIT: %v", index, readErr)
			}
			served++
		case "-ERR BUSY server overloaded\r\n":
			rejected++
		default:
			t.Fatalf("candidate %d response = %q, want served or overloaded response", index, response)
		}
	}
	// TCP accept and close completion order differs across operating systems.
	// Queue capacity is the business invariant: one candidate must survive in
	// the pending queue and every connection beyond it must be rejected.
	if served != 1 || rejected != 2 {
		t.Fatalf(
			"candidate outcomes: served = %d, rejected = %d (closed without reply = %d), want 1 and 2",
			served,
			rejected,
			closedWithoutReply,
		)
	}
	for index, connection := range candidates {
		if err := connection.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Fatalf("close candidate %d: %v", index, err)
		}
	}

	waitForIntegrationRecovery(
		t,
		listener,
		"-ERR NOAUTH authentication required\r\n",
	)
}

func startIntegrationServer(t *testing.T, options Options) net.Addr {
	t.Helper()

	g, err := geometry.New(geometry.Config{ChunkEdge: 2, LargeChunkEdge: 2, BlockBits: 4})
	if err != nil {
		t.Fatal(err)
	}
	store, err := fs_split.Open(t.TempDir(), g)
	if err != nil {
		t.Fatal(err)
	}
	engine, err := protocol.NewEngine(g, store, "secret")
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	serveResult := make(chan error, 1)
	go func() {
		serveResult <- ServeWithOptions(ctx, listener, engine, options)
	}()
	t.Cleanup(func() {
		cancel()
		if err := <-serveResult; err != nil {
			t.Errorf("ServeWithOptions() error = %v", err)
		}
		if err := store.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})
	return listener.Addr()
}

func dialIntegrationServer(t *testing.T, address net.Addr) net.Conn {
	t.Helper()

	connection, err := net.DialTimeout(address.Network(), address.String(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := connection.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		_ = connection.Close()
		t.Fatal(err)
	}
	return connection
}

func writeIntegrationRequest(writer io.Writer, request string) error {
	data := []byte(request)
	for len(data) > 0 {
		written, err := writer.Write(data)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		data = data[written:]
	}
	return nil
}

func TestWriteIntegrationRequestHandlesPartialWrites(t *testing.T) {
	t.Parallel()

	var destination bytes.Buffer
	writer := &partialIntegrationWriter{
		writer: &destination,
		limit:  2,
	}
	const request = "PING\r\n"
	writeErr := writeIntegrationRequest(writer, request)
	if writeErr != nil {
		t.Fatalf("writeIntegrationRequest() error = %v", writeErr)
	}
	if got := destination.String(); got != request {
		t.Fatalf("writeIntegrationRequest() = %q, want %q", got, request)
	}
	if writer.writes < 2 {
		t.Fatalf("Write() calls = %d, want multiple calls", writer.writes)
	}
}

type partialIntegrationWriter struct {
	writer io.Writer
	limit  int
	writes int
}

func (w *partialIntegrationWriter) Write(data []byte) (int, error) {
	w.writes++
	if len(data) > w.limit {
		data = data[:w.limit]
	}
	return w.writer.Write(data)
}

type integrationObservedListener struct {
	net.Listener
	accepted    chan struct{}
	readStarted chan struct{}
}

func (l *integrationObservedListener) Accept() (net.Conn, error) {
	connection, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	l.accepted <- struct{}{}
	return &integrationObservedConnection{
		Conn: connection,
		onRead: func() {
			l.readStarted <- struct{}{}
		},
	}, nil
}

type integrationObservedConnection struct {
	net.Conn
	once   sync.Once
	onRead func()
}

func (c *integrationObservedConnection) Read(destination []byte) (int, error) {
	c.once.Do(c.onRead)
	return c.Conn.Read(destination)
}

func waitForIntegrationSignal(t *testing.T, signals <-chan struct{}, description string) {
	t.Helper()

	select {
	case <-signals:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}

func waitForIntegrationRecovery(
	t *testing.T,
	listener *integrationObservedListener,
	want string,
) {
	t.Helper()

	const (
		recoveryTimeout = 2 * time.Second
		attemptTimeout  = 500 * time.Millisecond
		retryDelay      = 50 * time.Millisecond
	)
	deadline := time.Now().Add(recoveryTimeout)
	var lastErr error
	for time.Now().Before(deadline) {
		connection, err := net.DialTimeout(
			listener.Addr().Network(),
			listener.Addr().String(),
			attemptTimeout,
		)
		if err == nil {
			attemptDeadline := time.Now().Add(attemptTimeout)
			if attemptDeadline.After(deadline) {
				attemptDeadline = deadline
			}
			err = connection.SetDeadline(attemptDeadline)
			if err == nil {
				select {
				case <-listener.accepted:
					err = writeIntegrationRequest(connection, "PING\r\n")
					if err == nil {
						response, received, readErr := readIntegrationLineWithin(
							connection,
							attemptDeadline,
						)
						if readErr != nil {
							_ = connection.Close()
							t.Fatalf("read recovery response: %v", readErr)
						}
						if received && response == want {
							_ = connection.Close()
							return
						}
						if received {
							err = fmt.Errorf("response = %q, want %q", response, want)
						} else {
							err = errors.New("recovery attempt ended without a line")
						}
					}
				case <-time.After(time.Until(attemptDeadline)):
					err = errors.New("server did not accept recovery connection")
				}
			}
			_ = connection.Close()
		}
		lastErr = err

		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		delay := retryDelay
		if remaining < delay {
			delay = remaining
		}
		timer := time.NewTimer(delay)
		<-timer.C
	}
	t.Fatalf("server did not recover within %v: %v", recoveryTimeout, lastErr)
}

func readIntegrationLineWithin(
	connection net.Conn,
	deadline time.Time,
) (string, bool, error) {
	if err := connection.SetReadDeadline(deadline); err != nil {
		return "", false, fmt.Errorf("set timed read deadline: %w", err)
	}
	line, err := bufio.NewReader(connection).ReadString('\n')
	if err == nil {
		return line, true, nil
	}
	if isExpectedIntegrationReadEnd(err) {
		return "", false, nil
	}
	return "", false, err
}

func isExpectedIntegrationReadEnd(err error) bool {
	var networkError net.Error
	return errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrClosedPipe) ||
		errors.Is(err, net.ErrClosed) ||
		errors.As(err, &networkError) && networkError.Timeout() ||
		isPeerCloseError(err)
}

func TestReadIntegrationLineWithinClassifiesExpectedEnd(t *testing.T) {
	t.Parallel()

	unexpected := errors.New("unexpected read failure")
	tests := []struct {
		name    string
		readErr error
		wantErr error
	}{
		{name: "timeout", readErr: timeoutTestError{}},
		{name: "peer reset", readErr: platformPeerCloseTestError()},
		{name: "EOF", readErr: io.EOF},
		{name: "unexpected", readErr: unexpected, wantErr: unexpected},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			connection := &integrationReadErrorConnection{readErr: test.readErr}
			line, received, err := readIntegrationLineWithin(connection, time.Now())
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("readIntegrationLineWithin() error = %v, want %v", err, test.wantErr)
			}
			if line != "" || received {
				t.Fatalf(
					"readIntegrationLineWithin() = (%q, %t), want no line",
					line,
					received,
				)
			}
			if connection.readDeadlines != 1 {
				t.Fatalf("SetReadDeadline() calls = %d, want 1", connection.readDeadlines)
			}
		})
	}
}

type integrationReadErrorConnection struct {
	net.Conn
	readErr       error
	readDeadlines int
}

func (c *integrationReadErrorConnection) Read([]byte) (int, error) {
	return 0, c.readErr
}

func (c *integrationReadErrorConnection) SetReadDeadline(time.Time) error {
	c.readDeadlines++
	return nil
}
