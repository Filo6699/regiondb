package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Filo6699/regiondb/internal/geometry"
	"github.com/Filo6699/regiondb/internal/protocol"
	"github.com/Filo6699/regiondb/internal/server"
	"github.com/Filo6699/regiondb/internal/storage/fs_split"
)

func TestPrintVersion(t *testing.T) {
	var output bytes.Buffer

	if err := printVersion(&output); err != nil {
		t.Fatalf("printVersion() error = %v", err)
	}

	const want = "regiondb dev\n"
	if got := output.String(); got != want {
		t.Fatalf("printVersion() = %q, want %q", got, want)
	}
}

func TestParseConfig(t *testing.T) {
	t.Parallel()

	var stderr bytes.Buffer
	got, err := parseConfig([]string{
		"-data-dir", "data",
		"-token", "secret",
		"-chunk-edge", "16",
		"-large-chunk-edge", "8",
		"-block-bits", "5",
	}, &stderr)
	if err != nil {
		t.Fatalf("parseConfig() error = %v, stderr = %q", err, stderr.String())
	}
	if got.listenAddress != server.DefaultAddress || got.dataDir != "data" || got.token != "secret" {
		t.Fatalf("parseConfig() strings = %+v", got)
	}
	if got.geometry.ChunkEdge != 16 || got.geometry.LargeChunkEdge != 8 || got.geometry.BlockBits != 5 {
		t.Fatalf("parseConfig() geometry = %+v", got.geometry)
	}
	if got.durability != fs_split.DurabilityRelaxed ||
		got.checkpointRecords != fs_split.DefaultCheckpointRecords ||
		got.checkpointBytes != fs_split.DefaultCheckpointBytes ||
		got.maxLoadedChunks != fs_split.DefaultMaxLoadedChunks ||
		got.walGroupCommitUpdates != fs_split.DefaultWALGroupCommitUpdates ||
		got.workers != server.DefaultOptions().Workers ||
		got.acceptQueue != server.DefaultAcceptQueue ||
		got.maxLineBytes != server.DefaultMaxLineBytes {
		t.Fatalf("parseConfig() options = %+v", got)
	}
}

func TestParseConfigRejectsMissingRuntimeFlags(t *testing.T) {
	t.Parallel()

	tests := [][]string{
		nil,
		{"-data-dir", "data"},
		{"-listen", "", "-data-dir", "data", "-token", "secret"},
		{
			"-listen", "127.0.0.1:0",
			"-data-dir", "data",
			"-chunk-edge", "1",
			"-large-chunk-edge", "1",
			"-block-bits", "1",
		},
		{"unexpected"},
	}
	for _, args := range tests {
		if _, err := parseConfig(args, ioDiscard{}); err == nil {
			t.Fatalf("parseConfig(%q) succeeded", args)
		}
	}
}

func TestParseConfigRejectsIncompleteTLS(t *testing.T) {
	t.Parallel()

	base := []string{
		"-listen", "127.0.0.1:0",
		"-data-dir", "data",
		"-token", "secret",
		"-chunk-edge", "1",
		"-large-chunk-edge", "1",
		"-block-bits", "1",
	}
	for _, tlsFlag := range []string{"-tls-cert", "-tls-key"} {
		args := append(append([]string(nil), base...), tlsFlag, "server.pem")
		if _, err := parseConfig(args, ioDiscard{}); err == nil {
			t.Fatalf("parseConfig(%q) succeeded", args)
		}
	}
}

func TestParseConfigStorageOptions(t *testing.T) {
	t.Parallel()

	base := []string{
		"-listen", "127.0.0.1:0",
		"-data-dir", "data",
		"-token", "secret",
		"-chunk-edge", "1",
		"-large-chunk-edge", "1",
		"-block-bits", "1",
	}
	args := append(append([]string(nil), base...),
		"-durability", "fsync-wal",
		"-checkpoint-records", "7",
		"-checkpoint-bytes", "4096",
		"-max-loaded-chunks", "3",
		"-wal-group-commit-updates", "5",
		"-workers", "2",
		"-accept-queue", "4",
		"-max-line-bytes", "8192",
	)
	got, err := parseConfig(args, ioDiscard{})
	if err != nil {
		t.Fatalf("parseConfig() error = %v", err)
	}
	if got.durability != fs_split.DurabilityFsyncWAL ||
		got.checkpointRecords != 7 || got.checkpointBytes != 4096 ||
		got.maxLoadedChunks != 3 || got.walGroupCommitUpdates != 5 || got.workers != 2 ||
		got.acceptQueue != 4 || got.maxLineBytes != 8192 {
		t.Fatalf("parseConfig() options = %+v", got)
	}

	for _, invalid := range [][]string{
		{"-durability", "unknown"},
		{"-checkpoint-records", "0"},
		{"-checkpoint-bytes", "0"},
		{"-max-loaded-chunks", "0"},
		{"-wal-group-commit-updates", "0"},
		{"-workers", "0"},
		{"-accept-queue", "-1"},
		{"-max-line-bytes", "0"},
	} {
		if _, err := parseConfig(append(append([]string(nil), base...), invalid...), ioDiscard{}); err == nil {
			t.Fatalf("parseConfig(%q) succeeded", invalid)
		}
	}
}

func TestTLSStartupSmoke(t *testing.T) {
	certificatePath, keyPath, certificate := writeTestCertificate(t)
	tlsConfig, err := loadTLSConfig(config{tlsCert: certificatePath, tlsKey: keyPath})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := listen("127.0.0.1:0", tlsConfig)
	if err != nil {
		t.Fatal(err)
	}

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

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- server.Serve(ctx, listener, engine)
	}()

	roots := x509.NewCertPool()
	roots.AddCert(certificate)
	connection, err := tls.Dial("tcp", listener.Addr().String(), &tls.Config{
		RootCAs:    roots,
		ServerName: "localhost",
		MinVersion: tls.VersionTLS12,
	})
	if err != nil {
		cancel()
		<-result
		t.Fatalf("TLS dial failed: %v", err)
	}
	if _, err := connection.Write([]byte("AUTH secret\r\nPING\r\n")); err != nil {
		cancel()
		t.Fatal(err)
	}
	response := make([]byte, len("+OK\r\n+OK PONG\r\n"))
	if _, err := io.ReadFull(connection, response); err != nil {
		cancel()
		t.Fatal(err)
	}
	if got, want := string(response), "+OK\r\n+OK PONG\r\n"; got != want {
		cancel()
		t.Fatalf("response = %q, want %q", got, want)
	}
	if err := connection.Close(); err != nil {
		cancel()
		t.Fatal(err)
	}
	cancel()
	if err := <-result; err != nil {
		t.Fatalf("run() error = %v", err)
	}
}

func TestRunRejectsInvalidTLSBeforeOpeningStore(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	err := run(context.Background(), []string{
		"-listen", "127.0.0.1:0",
		"-data-dir", dataDir,
		"-token", "secret",
		"-chunk-edge", "1",
		"-large-chunk-edge", "1",
		"-block-bits", "1",
		"-tls-cert", filepath.Join(t.TempDir(), "missing.crt"),
		"-tls-key", filepath.Join(t.TempDir(), "missing.key"),
	}, ioDiscard{}, ioDiscard{})
	if err == nil {
		t.Fatal("run() succeeded with missing TLS files")
	}
	if _, statErr := os.Stat(dataDir); !os.IsNotExist(statErr) {
		t.Fatalf("data directory was opened before TLS validation: %v", statErr)
	}
}

func writeTestCertificate(t *testing.T) (string, string, *x509.Certificate) {
	t.Helper()

	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Unix(0, 0),
		NotAfter:     time.Unix(1<<31, 0),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, publicKey, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	key, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	certificatePath := filepath.Join(directory, "server.crt")
	keyPath := filepath.Join(directory, "server.key")
	if err := os.WriteFile(certificatePath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}), 0o600); err != nil {
		t.Fatal(err)
	}
	return certificatePath, keyPath, certificate
}

func TestRunVersion(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	if err := run(context.Background(), []string{"-version"}, &stdout, ioDiscard{}); err != nil {
		t.Fatalf("run(-version) error = %v", err)
	}
	if got := stdout.String(); !strings.Contains(got, version) {
		t.Fatalf("run(-version) output = %q", got)
	}
}

type ioDiscard struct{}

func (ioDiscard) Write(data []byte) (int, error) {
	return len(data), nil
}
