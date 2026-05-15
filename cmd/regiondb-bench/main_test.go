package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/Filo6699/regiondb/internal/geometry"
	"github.com/Filo6699/regiondb/internal/protocol"
	"github.com/Filo6699/regiondb/internal/server"
	"github.com/Filo6699/regiondb/internal/storage/fs_split"
)

func TestRunQuickTCPBenchmark(t *testing.T) {
	t.Parallel()

	g, err := geometry.New(geometry.Config{ChunkEdge: 2, LargeChunkEdge: 2, BlockBits: 4})
	if err != nil {
		t.Fatal(err)
	}
	dataDir := t.TempDir()
	store, err := fs_split.Open(dataDir, g)
	if err != nil {
		t.Fatal(err)
	}
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
		serveResult <- server.Serve(ctx, listener, engine)
	}()
	shutdownDone := false
	shutdown := func() {
		shutdownDone = true
		cancel()
		if err := <-serveResult; err != nil {
			t.Errorf("Serve() error = %v", err)
		}
		if err := store.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	}
	t.Cleanup(func() {
		if !shutdownDone {
			shutdown()
		}
	})

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if err := run(context.Background(), []string{
		"-address", listener.Addr().String(),
		"-token", "secret",
		"-clients", "2",
		"-seed", "23",
		"-ops", "12",
		"-workload", "mixed",
		"-chunk-edge", "2",
		"-large-chunk-edge", "2",
		"-block-bits", "4",
	}, &stdout, &stderr); err != nil {
		t.Fatalf("run() error = %v, stderr = %q", err, stderr.String())
	}

	var result output
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatalf("decode output: %v; output = %q", err, stdout.String())
	}
	if result.Backend != "tcp" || result.Seed != 23 || result.Operations != 12 {
		t.Fatalf("result = %+v", result)
	}
	if result.Address != listener.Addr().String() ||
		result.ServerMode != serverModeExternal ||
		result.Geometry.ChunkEdge != 2 {
		t.Fatalf("configuration output = %+v", result)
	}
	if result.RequestedClients != 2 || result.ActiveClients != 2 || result.ConnectionFailures != 0 {
		t.Fatalf("client output = %+v", result)
	}
	if result.LockModes.Process == "" || result.LockModes.Chunk != "shared-rwmutex" {
		t.Fatalf("lock mode output = %+v", result.LockModes)
	}
	if bytes.Contains(stdout.Bytes(), []byte("secret")) {
		t.Fatalf("output contains authentication token: %q", stdout.String())
	}

	shutdown()
	if err := os.RemoveAll(dataDir); err != nil {
		t.Fatalf("remove benchmark data directory after shutdown: %v", err)
	}
	if _, err := os.Stat(dataDir); !os.IsNotExist(err) {
		t.Fatalf("benchmark data directory remains after cleanup: %v", err)
	}
}

func TestParseConfigUsesExternalServerByDefault(t *testing.T) {
	t.Parallel()

	if _, err := parseConfig(nil, &bytes.Buffer{}); err == nil {
		t.Fatal("parseConfig() succeeded without a token")
	}
	got, err := parseConfig([]string{"-token", "secret"}, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("parseConfig() error = %v", err)
	}
	if got.address != server.DefaultAddress {
		t.Fatalf("parseConfig() address = %q, want %q", got.address, server.DefaultAddress)
	}
	if got.serverMode != serverModeExternal {
		t.Fatalf("parseConfig() server mode = %q, want %q", got.serverMode, serverModeExternal)
	}
}

func TestParseConfigAcceptsExplicitSpawnMode(t *testing.T) {
	t.Parallel()

	got, err := parseConfig([]string{
		"-token", "secret",
		"-server-mode", "spawn",
		"-server-binary", "custom-regiondb",
	}, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("parseConfig() error = %v", err)
	}
	if got.serverMode != serverModeSpawn || got.serverBinary != "custom-regiondb" {
		t.Fatalf("parseConfig() spawn settings = %+v", got)
	}
	for _, args := range [][]string{
		{"-token", "secret", "-server-mode", "unknown"},
		{"-token", "secret", "-server-mode", "spawn", "-server-binary", ""},
		{"-token", "secret", "-clients", "0"},
		{"-token", "secret", "-clients", "10000001"},
	} {
		if _, err := parseConfig(args, &bytes.Buffer{}); err == nil {
			t.Fatalf("parseConfig(%q) succeeded", args)
		}
	}
}

func TestOpenClientsReportsRequestedOutcome(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("connection refused")
	clients, failures, err := openClients(3, func(index int) (*client, error) {
		if index == 1 {
			return nil, wantErr
		}
		return &client{}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(clients) != 2 || failures != 1 {
		t.Fatalf("openClients() active = %d, failures = %d", len(clients), failures)
	}

	clients, failures, err = openClients(2, func(int) (*client, error) {
		return nil, wantErr
	})
	if len(clients) != 0 || failures != 2 || !errors.Is(err, wantErr) {
		t.Fatalf("openClients() = (%d clients, %d failures, %v)", len(clients), failures, err)
	}
}

func TestRunQuickSpawnedServerBenchmark(t *testing.T) {
	serverBinary := filepath.Join(t.TempDir(), "regiondb")
	if runtime.GOOS == "windows" {
		serverBinary += ".exe"
	}
	build := exec.Command("go", "build", "-o", serverBinary, "../regiondb")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build regiondb server: %v\n%s", err, output)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if err := run(context.Background(), []string{
		"-server-mode", "spawn",
		"-server-binary", serverBinary,
		"-address", address,
		"-token", "secret",
		"-seed", "7",
		"-ops", "4",
		"-workload", "write",
		"-chunk-edge", "2",
		"-large-chunk-edge", "2",
		"-block-bits", "4",
	}, &stdout, &stderr); err != nil {
		t.Fatalf("run() error = %v, stderr = %q", err, stderr.String())
	}
	var result output
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatalf("decode output: %v; output = %q", err, stdout.String())
	}
	if result.Operations != 4 || result.Address != address || result.ServerMode != serverModeSpawn {
		t.Fatalf("result = %+v", result)
	}
}
