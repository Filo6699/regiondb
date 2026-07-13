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
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Filo6699/regiondb/internal/defaults"
	"github.com/Filo6699/regiondb/internal/geometry"
	"github.com/Filo6699/regiondb/internal/logging"
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
	if got.listenAddress != defaults.Address || got.dataDir != "data" || got.token != "secret" {
		t.Fatalf("parseConfig() strings = %+v", got)
	}
	if got.geometry.ChunkEdge != 16 || got.geometry.LargeChunkEdge != 8 || got.geometry.BlockBits != 5 {
		t.Fatalf("parseConfig() geometry = %+v", got.geometry)
	}
	if got.durability != fs_split.DurabilityRelaxed ||
		got.checkpointCompression != fs_split.CheckpointCompressionNone ||
		got.checkpointRecords != defaults.CheckpointRecords ||
		got.checkpointBytes != defaults.CheckpointBytes ||
		got.maxLoadedChunks != defaults.MaxLoadedChunks ||
		got.maxOpenWALStreams != defaults.MaxOpenWALHandles ||
		got.walGroupCommitUpdates != defaults.WALGroupCommitUpdates ||
		got.workers != defaults.Workers() ||
		got.acceptQueue != defaults.AcceptQueue ||
		got.maxLineBytes != defaults.MaxLineBytes ||
		got.idleTimeout != defaults.IdleTimeout ||
		got.requestTimeout != defaults.RequestTimeout ||
		got.responseTimeout != defaults.ResponseTimeout ||
		got.authFailureDelay != defaults.AuthFailureDelay ||
		got.authFailureLimit != defaults.AuthFailureLimit ||
		got.authBanDuration != defaults.AuthBanDuration {
		t.Fatalf("parseConfig() options = %+v", got)
	}
}

func TestParseConfigAuthenticationPrecedence(t *testing.T) {
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("file-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("REGIONDB_TOKEN", "env-token")
	base := []string{
		"-data-dir", "data",
		"-token-file", tokenFile,
		"-chunk-edge", "1",
		"-large-chunk-edge", "1",
		"-block-bits", "1",
	}

	fromEnvironment, err := parseConfig(base, ioDiscard{})
	if err != nil {
		t.Fatal(err)
	}
	if fromEnvironment.token != "env-token" {
		t.Fatalf("environment token = %q, want env-token", fromEnvironment.token)
	}

	fromCLI, err := parseConfig(append(base, "-token", "cli-token"), ioDiscard{})
	if err != nil {
		t.Fatal(err)
	}
	if fromCLI.token != "cli-token" {
		t.Fatalf("CLI token = %q, want cli-token", fromCLI.token)
	}

	t.Setenv("REGIONDB_TOKEN", "")
	if _, err := parseConfig(base, ioDiscard{}); err == nil {
		t.Fatal("empty REGIONDB_TOKEN did not fail closed")
	}
	if err := os.Unsetenv("REGIONDB_TOKEN"); err != nil {
		t.Fatal(err)
	}
	fromFile, err := parseConfig(base, ioDiscard{})
	if err != nil {
		t.Fatal(err)
	}
	if fromFile.token != "file-token" {
		t.Fatalf("file token = %q, want file-token", fromFile.token)
	}
}

func TestParseConfigExplicitNoAuth(t *testing.T) {
	t.Setenv("REGIONDB_TOKEN", "ambient-token")
	base := []string{
		"-data-dir", "data",
		"-no-auth",
		"-chunk-edge", "1",
		"-large-chunk-edge", "1",
		"-block-bits", "1",
	}
	got, err := parseConfig(base, ioDiscard{})
	if err != nil {
		t.Fatal(err)
	}
	if !got.noAuth || got.token != "" {
		t.Fatalf("no-auth config = %+v", got)
	}
	if _, err := parseConfig(append(base, "-token", "secret"), ioDiscard{}); err == nil {
		t.Fatal("-no-auth with -token succeeded")
	}
}

func TestAuthenticationErrorsDoNotExposeSecret(t *testing.T) {
	t.Setenv("REGIONDB_TOKEN", "secret value")
	_, err := parseConfig([]string{
		"-data-dir", "data",
		"-chunk-edge", "1",
		"-large-chunk-edge", "1",
		"-block-bits", "1",
	}, ioDiscard{})
	if err == nil {
		t.Fatal("invalid secret succeeded")
	}
	if strings.Contains(err.Error(), "secret value") {
		t.Fatalf("error exposed secret: %q", err)
	}
}

func TestLoopbackListenerDetection(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		address string
		want    bool
	}{
		{address: "127.0.0.1:4242", want: true},
		{address: "[::1]:4242", want: true},
		{address: "0.0.0.0:4242", want: false},
		{address: "[::]:4242", want: false},
	} {
		host, _, err := net.SplitHostPort(test.address)
		if err != nil {
			t.Fatal(err)
		}
		got := isLoopbackListener(&net.TCPAddr{
			IP:   net.ParseIP(host),
			Port: 4242,
		})
		if got != test.want {
			t.Fatalf("isLoopbackListener(%q) = %t, want %t", test.address, got, test.want)
		}
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
		"-checkpoint-compression", "zrle",
		"-checkpoint-records", "7",
		"-checkpoint-bytes", "4096",
		"-max-loaded-chunks", "3",
		"-max-open-wal-streams", "1",
		"-wal-group-commit-updates", "5",
		"-workers", "2",
		"-accept-queue", "4",
		"-max-line-bytes", "8192",
		"-idle-timeout", "2s",
		"-request-timeout", "3s",
		"-response-timeout", "4s",
		"-auth-failure-delay", "5ms",
		"-auth-failure-limit", "6",
		"-auth-ban-duration", "7s",
	)
	got, err := parseConfig(args, ioDiscard{})
	if err != nil {
		t.Fatalf("parseConfig() error = %v", err)
	}
	if got.durability != fs_split.DurabilityFsyncWAL ||
		got.checkpointCompression != fs_split.CheckpointCompressionZRLE ||
		got.checkpointRecords != 7 || got.checkpointBytes != 4096 ||
		got.maxLoadedChunks != 3 || got.maxOpenWALStreams != 1 ||
		got.walGroupCommitUpdates != 5 || got.workers != 2 ||
		got.acceptQueue != 4 || got.maxLineBytes != 8192 ||
		got.idleTimeout != 2*time.Second ||
		got.requestTimeout != 3*time.Second ||
		got.responseTimeout != 4*time.Second ||
		got.authFailureDelay != 5*time.Millisecond ||
		got.authFailureLimit != 6 ||
		got.authBanDuration != 7*time.Second {
		t.Fatalf("parseConfig() options = %+v", got)
	}

	for _, invalid := range [][]string{
		{"-durability", "unknown"},
		{"-checkpoint-compression", "unknown"},
		{"-checkpoint-records", "0"},
		{"-checkpoint-bytes", "0"},
		{"-max-loaded-chunks", "0"},
		{"-max-open-wal-streams", "0"},
		{"-wal-group-commit-updates", "0"},
		{"-workers", "0"},
		{"-accept-queue", "-1"},
		{"-max-line-bytes", "0"},
		{"-idle-timeout", "0"},
		{"-request-timeout", "0"},
		{"-response-timeout", "0"},
		{"-auth-failure-delay", "0"},
		{"-auth-failure-limit", "0"},
		{"-auth-ban-duration", "0"},
	} {
		if _, err := parseConfig(append(append([]string(nil), base...), invalid...), ioDiscard{}); err == nil {
			t.Fatalf("parseConfig(%q) succeeded", invalid)
		}
	}
}

func TestServerDescriptorReserveIncludesListenerAndConnections(t *testing.T) {
	t.Parallel()

	got, err := serverDescriptorReserve(4, 7)
	if err != nil {
		t.Fatalf("serverDescriptorReserve() error = %v", err)
	}
	if want := 2 + 4 + 7; got != want {
		t.Fatalf("serverDescriptorReserve() = %d, want %d", got, want)
	}

	maxInt := int(^uint(0) >> 1)
	if _, err := serverDescriptorReserve(maxInt, 1); err == nil {
		t.Fatal("serverDescriptorReserve() accepted an overflowing capacity")
	}
}

func TestAutoFitDescriptorLimitsReducesPendingClientsBeforeWAL(t *testing.T) {
	t.Parallel()

	requested := config{
		workers:           2,
		acceptQueue:       8,
		maxOpenWALStreams: 4,
	}
	availableDescriptors := func(reserve int) (int, error) {
		return max(0, 10-reserve), nil
	}
	got, err := autoFitDescriptorLimits(requested, availableDescriptors)
	if err != nil {
		t.Fatalf("autoFitDescriptorLimits() error = %v", err)
	}
	if got.acceptQueue != 2 || got.maxOpenWALStreams != 4 {
		t.Fatalf("auto-fitted limits = queue %d, WAL %d; want queue 2, WAL 4",
			got.acceptQueue, got.maxOpenWALStreams)
	}
}

func TestAutoFitDescriptorLimitsClampsWALAfterPendingClients(t *testing.T) {
	t.Parallel()

	requested := config{
		workers:           2,
		acceptQueue:       8,
		maxOpenWALStreams: 4,
	}
	availableDescriptors := func(reserve int) (int, error) {
		return max(0, 5-reserve), nil
	}
	got, err := autoFitDescriptorLimits(requested, availableDescriptors)
	if err != nil {
		t.Fatalf("autoFitDescriptorLimits() error = %v", err)
	}
	if got.acceptQueue != 0 || got.maxOpenWALStreams != 1 {
		t.Fatalf("auto-fitted limits = queue %d, WAL %d; want queue 0, WAL 1",
			got.acceptQueue, got.maxOpenWALStreams)
	}
}

func TestAutoFitDescriptorLimitsRejectsWorkerDescriptorExhaustion(t *testing.T) {
	t.Parallel()

	requested := config{
		workers:           4,
		acceptQueue:       8,
		maxOpenWALStreams: 1,
	}
	availableDescriptors := func(reserve int) (int, error) {
		return max(0, 5-reserve), nil
	}
	if _, err := autoFitDescriptorLimits(requested, availableDescriptors); err == nil {
		t.Fatal("autoFitDescriptorLimits() accepted workers that exhaust the descriptor budget")
	}
}

func TestLogStartupScanCappedEmitsStructuredWarning(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	logStartupScanCapped(logging.New(&output), false)
	if output.Len() != 0 {
		t.Fatalf("uncapped startup scan logged %q", output.String())
	}
	logStartupScanCapped(logging.New(&output), true)
	got := output.String()
	if !strings.Contains(got, "level=warn") ||
		!strings.Contains(got, "event=scan_capped") ||
		!strings.Contains(got, "max_entries=100000") {
		t.Fatalf("scan cap log = %q", got)
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
	_, writeErr := connection.Write([]byte("AUTH secret\r\nPING\r\n"))
	if writeErr != nil {
		cancel()
		t.Fatal(writeErr)
	}
	response := make([]byte, len("+OK\r\n+OK PONG\r\n"))
	_, readErr := io.ReadFull(connection, response)
	if readErr != nil {
		cancel()
		t.Fatal(readErr)
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
