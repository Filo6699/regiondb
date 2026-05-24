package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Filo6699/regiondb/internal/benchmark"
	"github.com/Filo6699/regiondb/internal/geometry"
	"github.com/Filo6699/regiondb/internal/server"
)

const networkTimeout = 5 * time.Second

type serverMode string

const (
	serverModeExternal serverMode = "external"
	serverModeSpawn    serverMode = "spawn"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

type config struct {
	address      string
	token        string
	tls          bool
	serverMode   serverMode
	serverBinary string
	clients      int
	scenario     benchmark.Config
	geometry     geometry.Config
}

type output struct {
	benchmark.Result
	Address            string              `json:"address"`
	ServerMode         serverMode          `json:"server_mode"`
	RequestedClients   int                 `json:"requested_clients"`
	ActiveClients      int                 `json:"active_clients"`
	ConnectionFailures int                 `json:"connection_failures"`
	Geometry           benchmark.Geometry  `json:"geometry"`
	LockModes          benchmark.LockModes `json:"lock_modes"`
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	config, err := parseConfig(args, stderr)
	if err != nil {
		return err
	}
	g, err := geometry.New(config.geometry)
	if err != nil {
		return fmt.Errorf("configure geometry: %w", err)
	}
	var spawned *spawnedServer
	if config.serverMode == serverModeSpawn {
		spawned, err = spawnServer(config, stderr)
		if err != nil {
			return err
		}
		defer func() {
			_ = spawned.close()
		}()
	}
	clients, connectionFailures, err := openClients(config.clients, func(index int) (*client, error) {
		if index == 0 && spawned != nil {
			return dialBenchmarkClient(ctx, config, g, spawned)
		}
		return dialClient(ctx, config.address, config.token, config.tls, g)
	})
	if err != nil {
		return err
	}
	defer func() {
		for _, client := range clients {
			_ = client.close()
		}
	}()
	lockModes, err := clients[0].lockModes(ctx)
	if err != nil {
		return fmt.Errorf("read server lock modes: %w", err)
	}

	coordinates, err := benchmark.WorkingSet(config.scenario)
	if err != nil {
		return err
	}
	if config.scenario.Workload != benchmark.WorkloadWrite {
		for index, coord := range coordinates {
			client := clients[index%len(clients)]
			if err := client.writeChunk(ctx, coord, 0); err != nil {
				return fmt.Errorf("prepare chunk (%d,%d): %w", coord.X, coord.Y, err)
			}
		}
	}

	nextClient := 0
	result, err := benchmark.Run(ctx, "tcp", config.scenario, func(operation benchmark.Operation) error {
		client := clients[nextClient%len(clients)]
		nextClient++
		switch operation.Kind {
		case benchmark.OperationRead:
			return client.readChunk(ctx, operation.Coord)
		case benchmark.OperationWrite:
			return client.writeChunk(ctx, operation.Coord, operation.Value)
		default:
			return fmt.Errorf("unsupported operation %q", operation.Kind)
		}
	})
	if err != nil {
		return fmt.Errorf("run TCP benchmark: %w", err)
	}
	if err := json.NewEncoder(stdout).Encode(output{
		Result:             result,
		Address:            config.address,
		ServerMode:         config.serverMode,
		RequestedClients:   config.clients,
		ActiveClients:      len(clients),
		ConnectionFailures: connectionFailures,
		Geometry:           benchmark.GeometryFrom(config.geometry),
		LockModes:          lockModes,
	}); err != nil {
		return fmt.Errorf("write benchmark result: %w", err)
	}
	return nil
}

func parseConfig(args []string, stderr io.Writer) (config, error) {
	flags := flag.NewFlagSet("regiondb-bench", flag.ContinueOnError)
	flags.SetOutput(stderr)

	var result config
	var workload string
	var chunkEdge uint64
	var largeChunkEdge uint64
	var blockBits uint64
	var rawURI string
	flags.StringVar(&result.address, "address", server.DefaultAddress, "server TCP address in host:port form")
	flags.StringVar(&result.token, "token", "", "server authentication token")
	flags.StringVar(&rawURI, "uri", "", "server URI in region://token@host:port/ or regions:// form")
	flags.Var(&result.serverMode, "server-mode", "server mode: external or spawn")
	flags.StringVar(&result.serverBinary, "server-binary", "regiondb", "regiondb server binary used in spawn mode")
	flags.IntVar(&result.clients, "clients", 1, "number of server connections")
	flags.Int64Var(&result.scenario.Seed, "seed", benchmark.DefaultSeed, "workload random seed")
	flags.IntVar(&result.scenario.Operations, "ops", benchmark.DefaultOperations, "number of measured operations")
	flags.StringVar(&workload, "workload", string(benchmark.WorkloadMixed), "workload: read, write, or mixed")
	flags.Uint64Var(&chunkEdge, "chunk-edge", 16, "blocks per chunk edge")
	flags.Uint64Var(&largeChunkEdge, "large-chunk-edge", 8, "chunks per large-chunk edge")
	flags.Uint64Var(&blockBits, "block-bits", 5, "bits per block")
	if err := flags.Parse(args); err != nil {
		return config{}, err
	}
	if flags.NArg() != 0 {
		return config{}, errors.New("unexpected positional arguments")
	}
	provided := make(map[string]bool)
	flags.Visit(func(flag *flag.Flag) {
		provided[flag.Name] = true
	})
	if rawURI != "" {
		if provided["address"] || provided["token"] {
			return config{}, errors.New("-uri cannot be combined with -address or -token")
		}
		endpoint, err := server.ParseURI(rawURI)
		if err != nil {
			return config{}, err
		}
		result.address = endpoint.Address
		result.token = endpoint.Token
		result.tls = endpoint.TLS
	}
	if result.address == "" {
		return config{}, errors.New("-address must not be empty")
	}
	if result.token == "" {
		return config{}, errors.New("-token is required")
	}
	if result.serverMode == "" {
		result.serverMode = serverModeExternal
	}
	switch result.serverMode {
	case serverModeExternal:
	case serverModeSpawn:
		if result.serverBinary == "" {
			return config{}, errors.New("-server-binary must not be empty in spawn mode")
		}
		if result.tls {
			return config{}, errors.New("regions:// cannot be used in spawn mode")
		}
	default:
		return config{}, fmt.Errorf("unknown -server-mode %q", result.serverMode)
	}
	if result.clients <= 0 {
		return config{}, errors.New("-clients must be positive")
	}
	if result.clients > benchmark.MaxOperations {
		return config{}, fmt.Errorf("-clients must not exceed %d", benchmark.MaxOperations)
	}
	result.scenario.Workload = benchmark.Workload(workload)
	if err := result.scenario.Validate(); err != nil {
		return config{}, err
	}
	if chunkEdge > math.MaxUint32 || largeChunkEdge > math.MaxUint32 || blockBits > math.MaxUint8 {
		return config{}, errors.New("geometry flag value is out of range")
	}
	result.geometry = geometry.Config{
		ChunkEdge:      uint32(chunkEdge),
		LargeChunkEdge: uint32(largeChunkEdge),
		BlockBits:      uint8(blockBits),
	}
	return result, nil
}

func openClients(
	requested int,
	connect func(index int) (*client, error),
) ([]*client, int, error) {
	clients := make([]*client, 0, requested)
	connectionFailures := 0
	var lastErr error
	for index := range requested {
		client, err := connect(index)
		if err != nil {
			connectionFailures++
			lastErr = err
			continue
		}
		clients = append(clients, client)
	}
	if len(clients) == 0 {
		return nil, connectionFailures, fmt.Errorf(
			"establish benchmark clients: %d connection failures: %w",
			connectionFailures,
			lastErr,
		)
	}
	return clients, connectionFailures, nil
}

func (m *serverMode) Set(value string) error {
	*m = serverMode(value)
	return nil
}

func (m *serverMode) String() string {
	return string(*m)
}

type spawnedServer struct {
	command *exec.Cmd
	done    chan error
	dataDir string
}

func spawnServer(config config, stderr io.Writer) (*spawnedServer, error) {
	dataDir, err := os.MkdirTemp("", "regiondb-bench-")
	if err != nil {
		return nil, fmt.Errorf("create spawned server data directory: %w", err)
	}
	command := exec.Command(
		config.serverBinary,
		"-listen", config.address,
		"-data-dir", filepath.Join(dataDir, "data"),
		"-token", config.token,
		"-chunk-edge", strconv.FormatUint(uint64(config.geometry.ChunkEdge), 10),
		"-large-chunk-edge", strconv.FormatUint(uint64(config.geometry.LargeChunkEdge), 10),
		"-block-bits", strconv.FormatUint(uint64(config.geometry.BlockBits), 10),
	)
	command.Stdout = io.Discard
	command.Stderr = stderr
	if err := command.Start(); err != nil {
		_ = os.RemoveAll(dataDir)
		return nil, fmt.Errorf("start regiondb server: %w", err)
	}
	result := &spawnedServer{
		command: command,
		done:    make(chan error, 1),
		dataDir: dataDir,
	}
	go func() {
		result.done <- command.Wait()
		close(result.done)
	}()
	return result, nil
}

func (s *spawnedServer) close() error {
	if s == nil || s.command.Process == nil {
		return nil
	}
	defer func() {
		_ = os.RemoveAll(s.dataDir)
	}()
	select {
	case err := <-s.done:
		return err
	default:
	}
	if err := s.command.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return fmt.Errorf("stop spawned regiondb server: %w", err)
	}
	if err := <-s.done; err != nil {
		var exitError *exec.ExitError
		if !errors.As(err, &exitError) {
			return fmt.Errorf("wait for spawned regiondb server: %w", err)
		}
	}
	return nil
}

func dialBenchmarkClient(
	ctx context.Context,
	config config,
	g geometry.Geometry,
	spawned *spawnedServer,
) (*client, error) {
	if spawned == nil {
		return dialClient(ctx, config.address, config.token, config.tls, g)
	}
	retry := time.NewTicker(10 * time.Millisecond)
	defer retry.Stop()
	timeout := time.NewTimer(networkTimeout)
	defer timeout.Stop()
	var lastErr error
	for {
		result, err := dialClient(ctx, config.address, config.token, config.tls, g)
		if err == nil {
			return result, nil
		}
		lastErr = err
		select {
		case err := <-spawned.done:
			return nil, fmt.Errorf("spawned regiondb server exited before accepting connections: %w", err)
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timeout.C:
			return nil, fmt.Errorf("connect to spawned regiondb server: %w", lastErr)
		case <-retry.C:
		}
	}
}

type client struct {
	connection   net.Conn
	reader       *bufio.Reader
	writer       *bufio.Writer
	encodedChunk [256]string
	payloadBytes int
}

func dialClient(
	ctx context.Context,
	address string,
	token string,
	useTLS bool,
	g geometry.Geometry,
) (*client, error) {
	dialer := &net.Dialer{Timeout: networkTimeout}
	var connection net.Conn
	var err error
	if useTLS {
		connection, err = (&tls.Dialer{
			NetDialer: dialer,
			Config:    &tls.Config{MinVersion: tls.VersionTLS12},
		}).DialContext(ctx, "tcp", address)
	} else {
		connection, err = dialer.DialContext(ctx, "tcp", address)
	}
	if err != nil {
		return nil, fmt.Errorf("connect to %q: %w", address, err)
	}
	result := &client{
		connection:   connection,
		reader:       bufio.NewReader(connection),
		writer:       bufio.NewWriter(connection),
		payloadBytes: g.PayloadBytes(),
	}
	for value := range result.encodedChunk {
		result.encodedChunk[value] = hex.EncodeToString(benchmark.Payload(g, byte(value)))
	}
	if err := result.simple(ctx, "AUTH "+token+"\r\n"); err != nil {
		_ = connection.Close()
		return nil, fmt.Errorf("authenticate benchmark connection: %w", err)
	}
	return result, nil
}

func (c *client) close() error {
	if c.connection == nil {
		return nil
	}
	err := c.connection.Close()
	c.connection = nil
	return err
}

func (c *client) readChunk(ctx context.Context, coord geometry.Coord) error {
	command := "CHUNKBIN " + strconv.FormatInt(coord.X, 10) + " " +
		strconv.FormatInt(coord.Y, 10) + "\r\n"
	payload, err := c.bulk(ctx, command, c.payloadBytes)
	if err == nil && len(payload) != c.payloadBytes {
		return fmt.Errorf("unexpected chunk payload length %d", len(payload))
	}
	return err
}

func (c *client) writeChunk(ctx context.Context, coord geometry.Coord, value byte) error {
	command := "CHUNKSET " + strconv.FormatInt(coord.X, 10) + " " +
		strconv.FormatInt(coord.Y, 10) + " " + c.encodedChunk[value] + "\r\n"
	return c.simple(ctx, command)
}

func (c *client) simple(ctx context.Context, command string) error {
	line, err := c.request(ctx, command)
	if err != nil {
		return err
	}
	if line != "+OK\r\n" {
		return fmt.Errorf("unexpected response %q", strings.TrimSuffix(line, "\r\n"))
	}
	return nil
}

func (c *client) bulk(ctx context.Context, command string, maximumLength int) ([]byte, error) {
	line, err := c.request(ctx, command)
	if err != nil {
		return nil, err
	}
	if len(line) < 4 || line[0] != '$' || !strings.HasSuffix(line, "\r\n") {
		return nil, fmt.Errorf("unexpected response %q", strings.TrimSuffix(line, "\r\n"))
	}
	length, err := strconv.Atoi(line[1 : len(line)-2])
	if err != nil || length < 0 || length > maximumLength {
		return nil, fmt.Errorf("unexpected bulk length %q", line[1:len(line)-2])
	}
	payload := make([]byte, length+2)
	if _, err := io.ReadFull(c.reader, payload); err != nil {
		return nil, fmt.Errorf("read bulk payload: %w", err)
	}
	if payload[length] != '\r' || payload[length+1] != '\n' {
		return nil, errors.New("bulk payload is missing CRLF")
	}
	return payload[:length], nil
}

func (c *client) lockModes(ctx context.Context) (benchmark.LockModes, error) {
	payload, err := c.bulk(ctx, "INFO\r\n", 512)
	if err != nil {
		return benchmark.LockModes{}, err
	}
	var modes benchmark.LockModes
	for _, line := range strings.Split(string(payload), "\n") {
		key, value, found := strings.Cut(line, "=")
		if !found {
			continue
		}
		switch key {
		case "process_lock_mode":
			modes.Process = value
		case "chunk_lock_mode":
			modes.Chunk = value
		}
	}
	if modes.Process == "" || modes.Chunk == "" {
		return benchmark.LockModes{}, errors.New("INFO response is missing lock modes")
	}
	return modes, nil
}

func (c *client) request(ctx context.Context, command string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	stopCancellation := context.AfterFunc(ctx, func() {
		_ = c.connection.SetDeadline(time.Now())
	})
	defer stopCancellation()
	deadline := time.Now().Add(networkTimeout)
	if contextDeadline, ok := ctx.Deadline(); ok && contextDeadline.Before(deadline) {
		deadline = contextDeadline
	}
	if err := c.connection.SetDeadline(deadline); err != nil {
		return "", fmt.Errorf("set operation deadline: %w", err)
	}
	if _, err := c.writer.WriteString(command); err != nil {
		return "", fmt.Errorf("write command: %w", err)
	}
	if err := c.writer.Flush(); err != nil {
		return "", fmt.Errorf("flush command: %w", err)
	}
	line, err := c.reader.ReadString('\n')
	if err != nil {
		return "", fmt.Errorf("read response: %w", err)
	}
	return line, nil
}
