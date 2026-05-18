package server

import (
	"bufio"
	"context"
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
	t.Run("concurrent clients", testIntegrationTCPConcurrentClients)
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
