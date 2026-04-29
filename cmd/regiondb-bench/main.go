package main

import (
	"bufio"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Filo6699/regiondb/internal/benchmark"
	"github.com/Filo6699/regiondb/internal/geometry"
	"github.com/Filo6699/regiondb/internal/server"
)

const networkTimeout = 5 * time.Second

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

type config struct {
	address  string
	token    string
	scenario benchmark.Config
	geometry geometry.Config
}

type output struct {
	benchmark.Result
	Address   string              `json:"address"`
	Geometry  benchmark.Geometry  `json:"geometry"`
	LockModes benchmark.LockModes `json:"lock_modes"`
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
	client, err := dialClient(ctx, config.address, config.token, g)
	if err != nil {
		return err
	}
	defer func() {
		_ = client.close()
	}()
	lockModes, err := client.lockModes(ctx)
	if err != nil {
		return fmt.Errorf("read server lock modes: %w", err)
	}

	coordinates, err := benchmark.WorkingSet(config.scenario)
	if err != nil {
		return err
	}
	if config.scenario.Workload != benchmark.WorkloadWrite {
		for _, coord := range coordinates {
			if err := client.writeChunk(ctx, coord, 0); err != nil {
				return fmt.Errorf("prepare chunk (%d,%d): %w", coord.X, coord.Y, err)
			}
		}
	}

	result, err := benchmark.Run(ctx, "tcp", config.scenario, func(operation benchmark.Operation) error {
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
		Result:    result,
		Address:   config.address,
		Geometry:  benchmark.GeometryFrom(config.geometry),
		LockModes: lockModes,
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
	flags.StringVar(&result.address, "address", server.DefaultAddress, "server TCP address in host:port form")
	flags.StringVar(&result.token, "token", "", "server authentication token")
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
	if result.address == "" {
		return config{}, errors.New("-address must not be empty")
	}
	if result.token == "" {
		return config{}, errors.New("-token is required")
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

type client struct {
	connection   net.Conn
	reader       *bufio.Reader
	writer       *bufio.Writer
	encodedChunk [256]string
	payloadBytes int
}

func dialClient(ctx context.Context, address, token string, g geometry.Geometry) (*client, error) {
	connection, err := (&net.Dialer{Timeout: networkTimeout}).DialContext(ctx, "tcp", address)
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
