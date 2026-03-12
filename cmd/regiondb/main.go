package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/Filo6699/regiondb/internal/geometry"
	"github.com/Filo6699/regiondb/internal/protocol"
	"github.com/Filo6699/regiondb/internal/server"
	"github.com/Filo6699/regiondb/internal/storage/fs_split"
)

const version = "dev"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

type config struct {
	listenAddress     string
	dataDir           string
	token             string
	tlsCert           string
	tlsKey            string
	durability        fs_split.DurabilityMode
	checkpointRecords uint64
	checkpointBytes   int64
	maxLoadedChunks   int
	workers           int
	acceptQueue       int
	maxLineBytes      int
	geometry          geometry.Config
	version           bool
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) (returnErr error) {
	config, err := parseConfig(args, stderr)
	if err != nil {
		return err
	}
	if config.version {
		return printVersion(stdout)
	}

	tlsConfig, err := loadTLSConfig(config)
	if err != nil {
		return err
	}

	g, err := geometry.New(config.geometry)
	if err != nil {
		return fmt.Errorf("configure geometry: %w", err)
	}
	store, err := fs_split.OpenWithOptions(config.dataDir, g, fs_split.Options{
		Durability:        config.durability,
		CheckpointRecords: config.checkpointRecords,
		CheckpointBytes:   config.checkpointBytes,
		MaxLoadedChunks:   config.maxLoadedChunks,
	})
	if err != nil {
		return fmt.Errorf("open chunk store: %w", err)
	}
	defer func() {
		if err := store.Close(); err != nil && returnErr == nil {
			returnErr = fmt.Errorf("close chunk store: %w", err)
		}
	}()
	engine, err := protocol.NewEngine(g, store, config.token)
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", config.listenAddress)
	if err != nil {
		return fmt.Errorf("listen on %q: %w", config.listenAddress, err)
	}
	if tlsConfig != nil {
		listener = tls.NewListener(listener, tlsConfig)
	}
	defer func() {
		_ = listener.Close()
	}()
	return server.ServeWithOptions(ctx, listener, engine, server.Options{
		Workers:      config.workers,
		AcceptQueue:  config.acceptQueue,
		MaxLineBytes: config.maxLineBytes,
	})
}

func loadTLSConfig(config config) (*tls.Config, error) {
	if config.tlsCert == "" {
		return nil, nil
	}
	certificate, err := tls.LoadX509KeyPair(config.tlsCert, config.tlsKey)
	if err != nil {
		return nil, fmt.Errorf("load TLS certificate: %w", err)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{certificate},
		MinVersion:   tls.VersionTLS12,
	}, nil
}

func parseConfig(args []string, stderr io.Writer) (config, error) {
	flags := flag.NewFlagSet("regiondb", flag.ContinueOnError)
	flags.SetOutput(stderr)

	var result config
	var chunkEdge uint64
	var largeChunkEdge uint64
	var blockBits uint64
	var durability string
	flags.StringVar(&result.listenAddress, "listen", server.DefaultAddress, "TCP listen address in host:port form")
	flags.StringVar(&result.dataDir, "data-dir", "", "directory for chunk data")
	flags.StringVar(&result.token, "token", "", "authentication token")
	flags.StringVar(&result.tlsCert, "tls-cert", "", "PEM TLS certificate file")
	flags.StringVar(&result.tlsKey, "tls-key", "", "PEM TLS private key file")
	flags.StringVar(&durability, "durability", string(fs_split.DurabilityRelaxed), "durability mode: relaxed, fsync-wal, or fsync-checkpoint")
	flags.Uint64Var(&result.checkpointRecords, "checkpoint-records", fs_split.DefaultCheckpointRecords, "WAL records between checkpoints")
	flags.Int64Var(&result.checkpointBytes, "checkpoint-bytes", fs_split.DefaultCheckpointBytes, "WAL bytes between checkpoints")
	flags.IntVar(&result.maxLoadedChunks, "max-loaded-chunks", fs_split.DefaultMaxLoadedChunks, "maximum chunks retained in memory")
	runtimeDefaults := server.DefaultOptions()
	flags.IntVar(&result.workers, "workers", runtimeDefaults.Workers, "number of connection workers")
	flags.IntVar(&result.acceptQueue, "accept-queue", runtimeDefaults.AcceptQueue, "maximum queued connections")
	flags.IntVar(&result.maxLineBytes, "max-line-bytes", runtimeDefaults.MaxLineBytes, "maximum command line size including CRLF")
	flags.Uint64Var(&chunkEdge, "chunk-edge", 0, "blocks per chunk edge")
	flags.Uint64Var(&largeChunkEdge, "large-chunk-edge", 0, "chunks per large-chunk edge")
	flags.Uint64Var(&blockBits, "block-bits", 0, "bits per block")
	flags.BoolVar(&result.version, "version", false, "print version and exit")
	if err := flags.Parse(args); err != nil {
		return config{}, err
	}
	if flags.NArg() != 0 {
		return config{}, errors.New("unexpected positional arguments")
	}
	if result.version {
		return result, nil
	}
	if result.listenAddress == "" {
		return config{}, errors.New("-listen must not be empty")
	}
	if result.dataDir == "" {
		return config{}, errors.New("-data-dir is required")
	}
	if result.token == "" {
		return config{}, errors.New("-token is required")
	}
	if (result.tlsCert == "") != (result.tlsKey == "") {
		return config{}, errors.New("-tls-cert and -tls-key must be provided together")
	}
	result.durability = fs_split.DurabilityMode(durability)
	switch result.durability {
	case fs_split.DurabilityRelaxed, fs_split.DurabilityFsyncWAL, fs_split.DurabilityFsyncCheckpoint:
	default:
		return config{}, fmt.Errorf("invalid -durability value %q", durability)
	}
	if result.checkpointRecords == 0 {
		return config{}, errors.New("-checkpoint-records must be positive")
	}
	if result.checkpointBytes <= 0 {
		return config{}, errors.New("-checkpoint-bytes must be positive")
	}
	if result.maxLoadedChunks <= 0 {
		return config{}, errors.New("-max-loaded-chunks must be positive")
	}
	if result.workers <= 0 {
		return config{}, errors.New("-workers must be positive")
	}
	if result.acceptQueue < 0 {
		return config{}, errors.New("-accept-queue must not be negative")
	}
	if result.maxLineBytes <= 0 {
		return config{}, errors.New("-max-line-bytes must be positive")
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

func printVersion(w io.Writer) error {
	_, err := io.WriteString(w, "regiondb "+version+"\n")
	return err
}
