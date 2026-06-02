package server

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
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
		serveConnection(
			context.Background(),
			serverConnection,
			engine,
			64,
			DefaultIdleTimeout,
			DefaultRequestTimeout,
			DefaultResponseTimeout,
		)
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

func TestIdleDeadlineReleasesWorker(t *testing.T) {
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
			IdleTimeout:  100 * time.Millisecond,
		})
	}()
	t.Cleanup(func() {
		cancel()
		if err := <-serveResult; err != nil {
			t.Errorf("ServeWithOptions() error = %v", err)
		}
	})

	firstServer, firstClient := net.Pipe()
	firstReadStarted := make(chan struct{})
	waitForAccept(t, listener, 1)
	listener.connections <- &readObservedConnection{
		Conn:    firstServer,
		started: firstReadStarted,
	}
	t.Cleanup(func() { _ = firstClient.Close() })
	select {
	case <-firstReadStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for idle worker read")
	}

	secondServer, secondClient := net.Pipe()
	waitForAccept(t, listener, 2)
	listener.connections <- secondServer
	t.Cleanup(func() { _ = secondClient.Close() })
	if _, err := io.WriteString(secondClient, "PING\r\n"); err != nil {
		t.Fatalf("write queued request: %v", err)
	}
	if err := secondClient.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	response, err := bufio.NewReader(secondClient).ReadString('\n')
	if err != nil {
		t.Fatalf("read queued response: %v", err)
	}
	if want := "-ERR NOAUTH authentication required\r\n"; response != want {
		t.Fatalf("queued response = %q, want %q", response, want)
	}

	if _, err := firstClient.Read(make([]byte, 1)); err == nil {
		t.Fatal("idle connection remained open")
	}
}

func TestRequestDeadlineIsAbsoluteForTrickleClient(t *testing.T) {
	t.Parallel()

	engine := newTestEngine(t)
	serverConnection, clientConnection := net.Pipe()
	t.Cleanup(func() {
		_ = serverConnection.Close()
		_ = clientConnection.Close()
	})

	serveDone := make(chan connectionTermination, 1)
	go func() {
		defer func() { _ = serverConnection.Close() }()
		serveDone <- serveConnection(
			context.Background(),
			serverConnection,
			engine,
			64,
			time.Second,
			150*time.Millisecond,
			time.Second,
		)
	}()

	if _, err := clientConnection.Write([]byte("P")); err != nil {
		t.Fatal(err)
	}
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	overall := time.NewTimer(2 * time.Second)
	defer overall.Stop()
	writes := 1
	for {
		select {
		case termination := <-serveDone:
			if termination.phase != "read" || termination.reason != terminationTimeout {
				t.Fatalf("termination = %+v, want read timeout", termination)
			}
			if writes < 2 {
				t.Fatal("request timed out before the trickle remained active")
			}
			return
		case <-ticker.C:
			if _, err := clientConnection.Write([]byte("P")); err != nil {
				termination := <-serveDone
				if termination.phase != "read" || termination.reason != terminationTimeout {
					t.Fatalf("termination = %+v, want read timeout", termination)
				}
				return
			}
			writes++
		case <-overall.C:
			t.Fatal("trickle client extended the request phase")
		}
	}
}

func TestTLSHandshakeDeadlineIsAbsoluteForTrickleClient(t *testing.T) {
	t.Parallel()

	engine := newTestEngine(t)
	serverConnection, clientConnection := net.Pipe()
	t.Cleanup(func() {
		_ = serverConnection.Close()
		_ = clientConnection.Close()
	})
	tlsConnection := tls.Server(serverConnection, &tls.Config{
		MinVersion: tls.VersionTLS12,
	})

	serveDone := make(chan connectionTermination, 1)
	go func() {
		defer func() { _ = tlsConnection.Close() }()
		serveDone <- serveConnection(
			context.Background(),
			tlsConnection,
			engine,
			64,
			time.Second,
			150*time.Millisecond,
			time.Second,
		)
	}()

	recordHeader := []byte{0x16, 0x03, 0x03, 0x00, 0x01}
	ticker := time.NewTicker(40 * time.Millisecond)
	defer ticker.Stop()
	overall := time.NewTimer(2 * time.Second)
	defer overall.Stop()
	writes := 0
	for {
		select {
		case termination := <-serveDone:
			if termination.phase != "tls_handshake" || termination.reason != terminationTimeout {
				t.Fatalf("termination = %+v, want TLS handshake timeout", termination)
			}
			if writes < 2 {
				t.Fatal("TLS handshake timed out before the trickle remained active")
			}
			return
		case <-ticker.C:
			if writes == len(recordHeader) {
				t.Fatal("trickle client completed the TLS record header")
			}
			if _, err := clientConnection.Write(recordHeader[writes : writes+1]); err != nil {
				termination := <-serveDone
				if termination.phase != "tls_handshake" ||
					termination.reason != terminationTimeout {
					t.Fatalf("termination = %+v, want TLS handshake timeout", termination)
				}
				return
			}
			writes++
		case <-overall.C:
			t.Fatal("trickle client extended the TLS handshake")
		}
	}
}

func TestResponseDeadlineBoundsDrain(t *testing.T) {
	t.Parallel()

	engine := newTestEngine(t)
	serverConnection, clientConnection := net.Pipe()
	t.Cleanup(func() {
		_ = serverConnection.Close()
		_ = clientConnection.Close()
	})

	serveDone := make(chan connectionTermination, 1)
	go func() {
		defer func() { _ = serverConnection.Close() }()
		serveDone <- serveConnection(
			context.Background(),
			serverConnection,
			engine,
			64,
			time.Second,
			time.Second,
			100*time.Millisecond,
		)
	}()
	if _, err := io.WriteString(clientConnection, "PING\r\n"); err != nil {
		t.Fatal(err)
	}

	select {
	case termination := <-serveDone:
		if termination.phase != "write" || termination.reason != terminationTimeout {
			t.Fatalf("termination = %+v, want write timeout", termination)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("response drain remained blocked")
	}
}

func TestReadDeadlineSetupErrorsFailClosed(t *testing.T) {
	t.Parallel()

	deadlineError := errors.New("deadline setup failed")
	tests := []struct {
		name                 string
		buffer               *lineBuffer
		failReadDeadlineCall int32
		writeBeforeRead      bool
		wantReads            int32
	}{
		{
			name:                 "idle deadline",
			buffer:               newLineBuffer(64),
			failReadDeadlineCall: 1,
		},
		{
			name: "buffered request deadline",
			buffer: &lineBuffer{
				data:  make([]byte, 65),
				start: 0,
				end:   1,
			},
			failReadDeadlineCall: 1,
		},
		{
			name:                 "request deadline after first byte",
			buffer:               newLineBuffer(64),
			failReadDeadlineCall: 2,
			writeBeforeRead:      true,
			wantReads:            1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			serverConnection, clientConnection := net.Pipe()
			t.Cleanup(func() {
				_ = serverConnection.Close()
				_ = clientConnection.Close()
			})
			connection := &deadlineErrorConnection{
				Conn:                 serverConnection,
				readDeadlineErr:      deadlineError,
				failReadDeadlineCall: test.failReadDeadlineCall,
			}
			if test.writeBeforeRead {
				go func() {
					_, _ = clientConnection.Write([]byte("P"))
				}()
			}

			frame, tooLong, err := test.buffer.readFrameWithin(
				connection,
				time.Second,
				time.Second,
			)
			if !errors.Is(err, deadlineError) {
				t.Fatalf("readFrameWithin() error = %v, want deadline setup error", err)
			}
			if frame != nil || tooLong {
				t.Fatalf("readFrameWithin() = (%q, %t), want no frame", frame, tooLong)
			}
			if got := connection.reads.Load(); got != test.wantReads {
				t.Fatalf("Read() calls = %d, want %d", got, test.wantReads)
			}
		})
	}
}

func TestWriteDeadlineSetupErrorsFailClosed(t *testing.T) {
	t.Parallel()

	deadlineError := errors.New("deadline setup failed")
	tests := []struct {
		name  string
		write func(net.Conn) error
	}{
		{
			name: "response",
			write: func(connection net.Conn) error {
				return writeResponseWithin(
					connection,
					bufio.NewWriter(connection),
					[]byte("+OK\r\n"),
					time.Second,
				)
			},
		},
		{
			name:  "overload response",
			write: rejectOverloadedConnection,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			serverConnection, clientConnection := net.Pipe()
			t.Cleanup(func() {
				_ = serverConnection.Close()
				_ = clientConnection.Close()
			})
			connection := &deadlineErrorConnection{
				Conn:                  serverConnection,
				writeDeadlineErr:      deadlineError,
				failWriteDeadlineCall: 1,
			}

			if err := test.write(connection); !errors.Is(err, deadlineError) {
				t.Fatalf("write error = %v, want deadline setup error", err)
			}
			if got := connection.writes.Load(); got != 0 {
				t.Fatalf("Write() calls = %d, want 0", got)
			}
		})
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

func TestServeLogsClassifiedConnectionTermination(t *testing.T) {
	t.Parallel()

	engine := newTestEngine(t)
	listener := newCountingListener()
	sink := newRecordingSink()
	logger := logging.NewWithSink(sink, time.Now)
	ctx, cancel := context.WithCancel(context.Background())
	serveResult := make(chan error, 1)
	go func() {
		serveResult <- ServeWithOptions(ctx, listener, engine, Options{
			Workers:      1,
			AcceptQueue:  1,
			MaxLineBytes: 64,
			IdleTimeout:  time.Nanosecond,
			Logger:       logger,
		})
	}()

	if started := sink.next(t); started.Message != "serve_started" {
		cancel()
		t.Fatalf("start event = %q, want %q", started.Message, "serve_started")
	}
	waitForAccept(t, listener, 1)
	serverConnection, clientConnection := net.Pipe()
	t.Cleanup(func() { _ = clientConnection.Close() })
	listener.connections <- serverConnection

	terminated := sink.next(t)
	if got, want := terminated.Message, "connection_terminated"; got != want {
		cancel()
		t.Fatalf("termination event = %q, want %q", got, want)
	}
	if got, want := terminated.Level, slog.LevelWarn; got != want {
		cancel()
		t.Fatalf("termination level = %v, want %v", got, want)
	}
	attributes := recordAttributes(terminated)
	if got, want := attributes["phase"], "read"; got != want {
		cancel()
		t.Fatalf("termination phase = %q, want %q", got, want)
	}
	if got, want := attributes["reason"], string(terminationTimeout); got != want {
		cancel()
		t.Fatalf("termination reason = %q, want %q", got, want)
	}

	cancel()
	if err := <-serveResult; err != nil {
		t.Fatalf("ServeWithOptions() error = %v", err)
	}
	if stopped := sink.next(t); stopped.Message != "serve_stopped" {
		t.Fatalf("stop event = %q, want %q", stopped.Message, "serve_stopped")
	}
}

func TestConnectionTerminationReasonClasses(t *testing.T) {
	t.Parallel()

	serverConnection, clientConnection := net.Pipe()
	t.Cleanup(func() {
		_ = serverConnection.Close()
		_ = clientConnection.Close()
	})
	tlsConnection := tls.Server(serverConnection, &tls.Config{MinVersion: tls.VersionTLS12})
	shutdownContext, cancel := context.WithCancel(context.Background())
	cancel()

	tests := []struct {
		name        string
		ctx         context.Context
		connection  net.Conn
		err         error
		logPeer     bool
		wantReason  terminationReason
		wantLogging bool
	}{
		{
			name:       "server shutdown",
			ctx:        shutdownContext,
			connection: serverConnection,
			err:        errors.New("closed by shutdown"),
			wantReason: terminationServerShutdown,
		},
		{
			name:        "timeout",
			ctx:         context.Background(),
			connection:  serverConnection,
			err:         timeoutTestError{},
			wantReason:  terminationTimeout,
			wantLogging: true,
		},
		{
			name:       "clean peer close",
			ctx:        context.Background(),
			connection: serverConnection,
			err:        io.EOF,
			wantReason: terminationPeerClose,
		},
		{
			name:        "reset peer close",
			ctx:         context.Background(),
			connection:  serverConnection,
			err:         io.ErrClosedPipe,
			logPeer:     true,
			wantReason:  terminationPeerClose,
			wantLogging: true,
		},
		{
			name:        "TLS error",
			ctx:         context.Background(),
			connection:  tlsConnection,
			err:         errors.New("handshake failed"),
			wantReason:  terminationTLSError,
			wantLogging: true,
		},
		{
			name:        "socket error",
			ctx:         context.Background(),
			connection:  serverConnection,
			err:         errors.New("I/O failed"),
			wantReason:  terminationSocketError,
			wantLogging: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			termination := classifyConnectionTermination(
				test.ctx,
				test.connection,
				"read",
				test.err,
				test.logPeer,
			)
			if termination.reason != test.wantReason {
				t.Fatalf("reason = %q, want %q", termination.reason, test.wantReason)
			}
			if termination.shouldLog != test.wantLogging {
				t.Fatalf("shouldLog = %t, want %t", termination.shouldLog, test.wantLogging)
			}
		})
	}
}

func TestCleanPeerCloseDoesNotLogTermination(t *testing.T) {
	t.Parallel()

	engine := newTestEngine(t)
	serverConnection, clientConnection := net.Pipe()
	closed := make(chan struct{})
	go func() {
		_ = clientConnection.Close()
		close(closed)
	}()
	termination := serveConnection(
		context.Background(),
		serverConnection,
		engine,
		64,
		DefaultIdleTimeout,
		DefaultRequestTimeout,
		DefaultResponseTimeout,
	)
	_ = serverConnection.Close()
	<-closed

	if termination.reason != terminationPeerClose {
		t.Fatalf("termination reason = %q, want %q", termination.reason, terminationPeerClose)
	}
	if termination.shouldLog {
		t.Fatal("clean peer close was marked for warning logging")
	}
}

func TestServeOptionsValidation(t *testing.T) {
	t.Parallel()

	engine := newTestEngine(t)
	tests := []Options{
		{Workers: 0, AcceptQueue: 1, MaxLineBytes: 1},
		{Workers: 1, AcceptQueue: -1, MaxLineBytes: 1},
		{Workers: 1, AcceptQueue: 1, MaxLineBytes: 0},
		{Workers: 1, AcceptQueue: 1, MaxLineBytes: 1, IdleTimeout: -time.Second},
		{Workers: 1, AcceptQueue: 1, MaxLineBytes: 1, RequestTimeout: -time.Second},
		{Workers: 1, AcceptQueue: 1, MaxLineBytes: 1, ResponseTimeout: -time.Second},
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

type deadlineErrorConnection struct {
	net.Conn
	readDeadlineErr       error
	writeDeadlineErr      error
	failReadDeadlineCall  int32
	failWriteDeadlineCall int32
	readDeadlineCalls     atomic.Int32
	writeDeadlineCalls    atomic.Int32
	reads                 atomic.Int32
	writes                atomic.Int32
}

func (c *deadlineErrorConnection) SetReadDeadline(deadline time.Time) error {
	call := c.readDeadlineCalls.Add(1)
	if call == c.failReadDeadlineCall {
		return c.readDeadlineErr
	}
	return c.Conn.SetReadDeadline(deadline)
}

func (c *deadlineErrorConnection) SetWriteDeadline(deadline time.Time) error {
	call := c.writeDeadlineCalls.Add(1)
	if call == c.failWriteDeadlineCall {
		return c.writeDeadlineErr
	}
	return c.Conn.SetWriteDeadline(deadline)
}

func (c *deadlineErrorConnection) Read(destination []byte) (int, error) {
	c.reads.Add(1)
	return c.Conn.Read(destination)
}

func (c *deadlineErrorConnection) Write(source []byte) (int, error) {
	c.writes.Add(1)
	return c.Conn.Write(source)
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

func recordAttributes(record slog.Record) map[string]string {
	attributes := make(map[string]string)
	record.Attrs(func(attribute slog.Attr) bool {
		attributes[attribute.Key] = attribute.Value.String()
		return true
	})
	return attributes
}

type timeoutTestError struct{}

func (timeoutTestError) Error() string   { return "I/O timeout" }
func (timeoutTestError) Timeout() bool   { return true }
func (timeoutTestError) Temporary() bool { return true }
