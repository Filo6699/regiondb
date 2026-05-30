package server

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Filo6699/regiondb/internal/geometry"
	"github.com/Filo6699/regiondb/internal/protocol"
	"github.com/Filo6699/regiondb/internal/storage/fs_split"
)

func TestIntegrationTCP(t *testing.T) {
	t.Run("command lifecycle", testIntegrationTCPCommandLifecycle)
	t.Run("concurrent clients", testIntegrationTCPConcurrentClients)
	t.Run("overload response", testIntegrationTCPOverloadResponse)
	t.Run("request deadline", testIntegrationTCPRequestDeadline)
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
	if _, err := connection.Write([]byte("P")); err != nil {
		t.Fatalf("write partial request: %v", err)
	}
	if _, err := connection.Read(make([]byte, 1)); err == nil {
		t.Fatal("partial request remained open past its deadline")
	}
}

func testIntegrationTCPCommandLifecycle(t *testing.T) {
	address := startIntegrationServer(t, DefaultOptions())
	connection := dialIntegrationServer(t, address)
	defer func() {
		_ = connection.Close()
	}()

	request := "PING\r\nAUTH secret\r\nSET -1 2 9\r\nINFO\r\nEXISTS -1 2\r\nGET -1 2\r\nCHUNK -1 1\r\nQUIT\r\n"
	if _, err := io.WriteString(connection, request); err != nil {
		t.Fatalf("write request: %v", err)
	}

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
	if _, err := reader.ReadByte(); err != io.EOF {
		t.Fatalf("read after QUIT error = %v, want EOF", err)
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
			if _, err := io.WriteString(connection, request); err != nil {
				results <- fmt.Errorf("client %d write: %w", client, err)
				return
			}
			want := fmt.Sprintf("+OK\r\n+OK\r\n$1\r\n%d\r\n+OK\r\n", client)
			response := make([]byte, len(want))
			if _, err := io.ReadFull(connection, response); err != nil {
				results <- fmt.Errorf("client %d read: %w", client, err)
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
		accepted:    make(chan int, 3),
		readStarted: make(chan int, 1),
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
	waitForIntegrationSignal(t, listener.accepted, 1, "first accept")
	waitForIntegrationSignal(t, listener.readStarted, 1, "first worker read")

	second := dialIntegrationServer(t, listener.Addr())
	t.Cleanup(func() { _ = second.Close() })
	waitForIntegrationSignal(t, listener.accepted, 2, "second accept")

	rejected := dialIntegrationServer(t, listener.Addr())
	t.Cleanup(func() { _ = rejected.Close() })
	waitForIntegrationSignal(t, listener.accepted, 3, "third accept")
	response, err := bufio.NewReader(rejected).ReadString('\n')
	if err != nil {
		t.Fatalf("read overload response: %v", err)
	}
	if want := "-ERR BUSY server overloaded\r\n"; response != want {
		t.Fatalf("overload response = %q, want %q", response, want)
	}
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

type integrationObservedListener struct {
	net.Listener
	accepted    chan int
	readStarted chan int
	accepts     atomic.Int32
}

func (l *integrationObservedListener) Accept() (net.Conn, error) {
	connection, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	accepted := int(l.accepts.Add(1))
	l.accepted <- accepted
	return &integrationObservedConnection{
		Conn: connection,
		onRead: func() {
			l.readStarted <- accepted
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

func waitForIntegrationSignal(t *testing.T, signals <-chan int, want int, description string) {
	t.Helper()

	select {
	case got := <-signals:
		if got != want {
			t.Fatalf("%s = %d, want %d", description, got, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}
