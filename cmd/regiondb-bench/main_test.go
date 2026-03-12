package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
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
	store, err := fs_split.Open(t.TempDir(), g)
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
	t.Cleanup(func() {
		cancel()
		if err := <-serveResult; err != nil {
			t.Errorf("Serve() error = %v", err)
		}
		if err := store.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if err := run(context.Background(), []string{
		"-address", listener.Addr().String(),
		"-token", "secret",
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
	if result.Address != listener.Addr().String() || result.Geometry.ChunkEdge != 2 {
		t.Fatalf("configuration output = %+v", result)
	}
	if bytes.Contains(stdout.Bytes(), []byte("secret")) {
		t.Fatalf("output contains authentication token: %q", stdout.String())
	}
}

func TestParseConfigUsesDefaultAddressAndRequiresToken(t *testing.T) {
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
}
