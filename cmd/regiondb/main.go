package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Filo6699/regiondb/internal/defaults"
	"github.com/Filo6699/regiondb/internal/geometry"
	"github.com/Filo6699/regiondb/internal/logging"
	"github.com/Filo6699/regiondb/internal/protocol"
	"github.com/Filo6699/regiondb/internal/server"
	"github.com/Filo6699/regiondb/internal/storage/fs_split"
	"github.com/Filo6699/regiondb/internal/version"
)

func main() {
	ctx, stop := notifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		logging.New(os.Stderr).Error("process", "exit_failed")
		os.Exit(1)
	}
}

func notifyContext(parent context.Context, signals ...os.Signal) (context.Context, context.CancelFunc) {
	return signal.NotifyContext(parent, signals...)
}

type config struct {
	listenAddress         string
	dataDir               string
	token                 string
	tokenFile             string
	tokenSource           string
	noAuth                bool
	tlsCert               string
	tlsKey                string
	durability            fs_split.DurabilityMode
	checkpointCompression fs_split.CheckpointCompression
	checkpointRecords     uint64
	checkpointBytes       int64
	maxLoadedChunks       int
	maxOpenWALStreams     int
	walGroupCommitUpdates uint64
	workers               int
	acceptQueue           int
	maxLineBytes          int
	idleTimeout           time.Duration
	requestTimeout        time.Duration
	responseTimeout       time.Duration
	authFailureDelay      time.Duration
	authFailureLimit      int
	authBanDuration       time.Duration
	geometry              geometry.Config
	version               bool
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) (returnErr error) {
	config, err := parseConfig(args, stderr)
	if err != nil {
		return err
	}
	if config.version {
		return printVersion(stdout)
	}
	logger := logging.New(stderr)
	logger.Info("process", "starting", slog.String("version", version.Current))
	logger.Info("authentication", "configured",
		slog.String("token_source", config.tokenSource),
	)

	requestedWALStreams := config.maxOpenWALStreams
	requestedAcceptQueue := config.acceptQueue
	config, err = autoFitDescriptorLimits(config, fs_split.AvailableWALDescriptors)
	if err != nil {
		logger.Error("server", "configuration_failed")
		return err
	}
	if config.maxOpenWALStreams != requestedWALStreams ||
		config.acceptQueue != requestedAcceptQueue {
		logger.Warn("server", "descriptor_limits_adjusted",
			slog.Int("requested_wal_streams", requestedWALStreams),
			slog.Int("effective_wal_streams", config.maxOpenWALStreams),
			slog.Int("requested_accept_queue", requestedAcceptQueue),
			slog.Int("effective_accept_queue", config.acceptQueue),
		)
	}

	tlsConfig, err := loadTLSConfig(config)
	if err != nil {
		logger.Error("tls", "configuration_failed")
		return err
	}

	g, err := geometry.New(config.geometry)
	if err != nil {
		logger.Error("geometry", "configuration_failed")
		return fmt.Errorf("configure geometry: %w", err)
	}
	descriptorReserve, err := serverDescriptorReserve(config.workers, config.acceptQueue)
	if err != nil {
		logger.Error("server", "configuration_failed")
		return err
	}
	store, err := fs_split.OpenWithOptions(config.dataDir, g, fs_split.Options{
		Durability:            config.durability,
		CheckpointCompression: config.checkpointCompression,
		CheckpointRecords:     config.checkpointRecords,
		CheckpointBytes:       config.checkpointBytes,
		MaxLoadedChunks:       config.maxLoadedChunks,
		MaxOpenWALHandles:     config.maxOpenWALStreams,
		DescriptorReserve:     descriptorReserve,
		WALGroupCommitUpdates: config.walGroupCommitUpdates,
		PostCommitFailure: func(event string) {
			logger.Warn("storage", event)
		},
	})
	if err != nil {
		logger.Error("storage", "open_failed")
		return fmt.Errorf("open chunk store: %w", err)
	}
	logger.Info("storage", "opened",
		slog.String("durability", string(config.durability)),
		slog.Int("max_loaded_chunks", config.maxLoadedChunks),
	)
	logStartupScanCapped(logger, store.StartupScanCapped())
	defer func() {
		if err := store.Close(); err != nil {
			logger.Error("storage", "close_failed")
			if returnErr == nil {
				returnErr = fmt.Errorf("close chunk store: %w", err)
			}
			return
		}
		logger.Info("storage", "closed")
	}()
	var engine *protocol.Engine
	if config.noAuth {
		engine, err = protocol.NewEngineWithoutAuth(g, store)
	} else {
		engine, err = protocol.NewEngine(g, store, config.token)
	}
	if err != nil {
		logger.Error("protocol", "engine_initialization_failed")
		return err
	}
	listener, err := listen(config.listenAddress, tlsConfig)
	if err != nil {
		logger.Error("server", "listen_failed")
		return fmt.Errorf("listen on %q: %w", config.listenAddress, err)
	}
	logger.Info("server", "listening",
		slog.String("address", listener.Addr().String()),
		slog.Bool("tls", tlsConfig != nil),
	)
	if !isLoopbackListener(listener.Addr()) {
		logger.Warn("server", "non_loopback_listener",
			slog.Bool("authentication", !config.noAuth),
			slog.Bool("tls", tlsConfig != nil),
		)
	}
	defer func() {
		_ = listener.Close()
	}()
	err = server.ServeWithOptions(ctx, listener, engine, server.Options{
		Workers:          config.workers,
		AcceptQueue:      config.acceptQueue,
		MaxLineBytes:     config.maxLineBytes,
		IdleTimeout:      config.idleTimeout,
		RequestTimeout:   config.requestTimeout,
		ResponseTimeout:  config.responseTimeout,
		AuthFailureDelay: config.authFailureDelay,
		AuthFailureLimit: config.authFailureLimit,
		AuthBanDuration:  config.authBanDuration,
		Logger:           logger,
	})
	if err != nil {
		logger.Error("server", "serve_failed")
	}
	return err
}

func logStartupScanCapped(logger *logging.Logger, capped bool) {
	if capped {
		logger.Warn("storage", "scan_capped",
			slog.Int("max_entries", fs_split.StartupScanEntryLimit),
		)
	}
}

func serverDescriptorReserve(workers, acceptQueue int) (int, error) {
	maxInt := int(^uint(0) >> 1)
	if workers > maxInt-acceptQueue-2 {
		return 0, errors.New("worker and accept queue sizes are too large")
	}
	// Reserve the listener, every worker and queued socket, and the next
	// accepted socket while overload handling decides whether to reject it.
	return 2 + workers + acceptQueue, nil
}

type availableWALDescriptors func(descriptorReserve int) (int, error)

func autoFitDescriptorLimits(
	result config,
	availableDescriptors availableWALDescriptors,
) (config, error) {
	reserve, err := serverDescriptorReserve(result.workers, result.acceptQueue)
	if err != nil {
		return config{}, err
	}
	available, err := availableDescriptors(reserve)
	if err != nil {
		return config{}, err
	}
	if available >= result.maxOpenWALStreams {
		return result, nil
	}

	zeroQueueReserve, err := serverDescriptorReserve(result.workers, 0)
	if err != nil {
		return config{}, err
	}
	zeroQueueAvailable, err := availableDescriptors(zeroQueueReserve)
	if err != nil {
		return config{}, err
	}
	if zeroQueueAvailable < 1 {
		return config{}, errors.New("descriptor limit is too low for the configured workers")
	}
	if zeroQueueAvailable < result.maxOpenWALStreams {
		result.acceptQueue = 0
		result.maxOpenWALStreams = zeroQueueAvailable
		return result, nil
	}

	low, high := 0, result.acceptQueue
	for low < high {
		candidate := low + (high-low+1)/2
		candidateReserve, reserveErr := serverDescriptorReserve(result.workers, candidate)
		if reserveErr != nil {
			return config{}, reserveErr
		}
		candidateAvailable, limitErr := availableDescriptors(candidateReserve)
		if limitErr != nil {
			return config{}, limitErr
		}
		if candidateAvailable >= result.maxOpenWALStreams {
			low = candidate
		} else {
			high = candidate - 1
		}
	}
	result.acceptQueue = low
	return result, nil
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

func listen(address string, tlsConfig *tls.Config) (net.Listener, error) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}
	if tlsConfig != nil {
		return tls.NewListener(listener, tlsConfig), nil
	}
	return listener, nil
}

func isLoopbackListener(address net.Addr) bool {
	host, _, err := net.SplitHostPort(address.String())
	if err != nil {
		return false
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func parseConfig(args []string, stderr io.Writer) (config, error) {
	flags := flag.NewFlagSet("regiondb", flag.ContinueOnError)
	flags.SetOutput(stderr)

	var result config
	var chunkEdge uint64
	var largeChunkEdge uint64
	var blockBits uint64
	var durability string
	var checkpointCompression string
	flags.StringVar(&result.listenAddress, "listen", defaults.Address, "TCP listen address in host:port form")
	flags.StringVar(&result.dataDir, "data-dir", "", "directory for chunk data")
	flags.StringVar(&result.token, "token", "", "authentication token")
	flags.StringVar(&result.tokenFile, "token-file", "", "file containing the authentication token")
	flags.BoolVar(&result.noAuth, "no-auth", false, "disable authentication explicitly")
	flags.StringVar(&result.tlsCert, "tls-cert", "", "PEM TLS certificate file")
	flags.StringVar(&result.tlsKey, "tls-key", "", "PEM TLS private key file")
	flags.StringVar(&durability, "durability", string(fs_split.DurabilityRelaxed), "durability mode: relaxed, fsync-wal, or fsync-checkpoint")
	flags.StringVar(
		&checkpointCompression,
		"checkpoint-compression",
		string(fs_split.CheckpointCompressionNone),
		"checkpoint image compression: none or zrle",
	)
	flags.Uint64Var(&result.checkpointRecords, "checkpoint-records", defaults.CheckpointRecords, "WAL records between checkpoints")
	flags.Int64Var(&result.checkpointBytes, "checkpoint-bytes", defaults.CheckpointBytes, "WAL bytes between checkpoints")
	flags.IntVar(&result.maxLoadedChunks, "max-loaded-chunks", defaults.MaxLoadedChunks, "maximum chunks retained in memory")
	flags.IntVar(
		&result.maxOpenWALStreams,
		"max-open-wal-streams",
		defaults.MaxOpenWALHandles,
		"maximum cached WAL streams before the OS descriptor clamp",
	)
	flags.Uint64Var(
		&result.walGroupCommitUpdates,
		"wal-group-commit-updates",
		defaults.WALGroupCommitUpdates,
		"WAL updates per sync",
	)
	flags.IntVar(&result.workers, "workers", defaults.Workers(), "number of connection workers")
	flags.IntVar(&result.acceptQueue, "accept-queue", defaults.AcceptQueue, "maximum queued connections")
	flags.IntVar(&result.maxLineBytes, "max-line-bytes", defaults.MaxLineBytes, "maximum command line size including CRLF")
	flags.DurationVar(&result.idleTimeout, "idle-timeout", defaults.IdleTimeout, "maximum wait for the first request byte")
	flags.DurationVar(&result.requestTimeout, "request-timeout", defaults.RequestTimeout, "maximum time to read one request")
	flags.DurationVar(&result.responseTimeout, "response-timeout", defaults.ResponseTimeout, "maximum time to write one response")
	flags.DurationVar(&result.authFailureDelay, "auth-failure-delay", defaults.AuthFailureDelay, "delay after a failed authentication attempt")
	flags.IntVar(&result.authFailureLimit, "auth-failure-limit", defaults.AuthFailureLimit, "failed authentication attempts before a temporary ban")
	flags.DurationVar(&result.authBanDuration, "auth-ban-duration", defaults.AuthBanDuration, "temporary authentication ban duration")
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
	provided := make(map[string]bool)
	flags.Visit(func(current *flag.Flag) {
		provided[current.Name] = true
	})
	if err := resolveAuthentication(&result, provided); err != nil {
		return config{}, err
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
	result.checkpointCompression = fs_split.CheckpointCompression(checkpointCompression)
	switch result.checkpointCompression {
	case fs_split.CheckpointCompressionNone, fs_split.CheckpointCompressionZRLE:
	default:
		return config{}, fmt.Errorf(
			"invalid -checkpoint-compression value %q",
			checkpointCompression,
		)
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
	if result.maxOpenWALStreams <= 0 {
		return config{}, errors.New("-max-open-wal-streams must be positive")
	}
	if result.walGroupCommitUpdates == 0 {
		return config{}, errors.New("-wal-group-commit-updates must be positive")
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
	if result.idleTimeout <= 0 {
		return config{}, errors.New("-idle-timeout must be positive")
	}
	if result.requestTimeout <= 0 {
		return config{}, errors.New("-request-timeout must be positive")
	}
	if result.responseTimeout <= 0 {
		return config{}, errors.New("-response-timeout must be positive")
	}
	if result.authFailureDelay <= 0 {
		return config{}, errors.New("-auth-failure-delay must be positive")
	}
	if result.authFailureLimit <= 0 {
		return config{}, errors.New("-auth-failure-limit must be positive")
	}
	if result.authBanDuration <= 0 {
		return config{}, errors.New("-auth-ban-duration must be positive")
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

func resolveAuthentication(result *config, provided map[string]bool) error {
	if result.noAuth {
		if provided["token"] || provided["token-file"] {
			return errors.New("-no-auth cannot be combined with -token or -token-file")
		}
		result.token = ""
		result.tokenSource = "disabled"
		return nil
	}
	if provided["token"] {
		if err := validateAuthenticationToken(result.token); err != nil {
			return err
		}
		result.tokenSource = "command_line"
		return nil
	}
	if token, ok := os.LookupEnv("REGIONDB_TOKEN"); ok {
		if err := validateAuthenticationToken(token); err != nil {
			return fmt.Errorf("invalid REGIONDB_TOKEN: %w", err)
		}
		result.token = token
		result.tokenSource = "environment"
		return nil
	}
	if result.tokenFile != "" {
		contents, err := os.ReadFile(result.tokenFile)
		if err != nil {
			return fmt.Errorf("read authentication token file: %w", err)
		}
		result.token = strings.TrimSuffix(strings.TrimSuffix(string(contents), "\n"), "\r")
		if err := validateAuthenticationToken(result.token); err != nil {
			return fmt.Errorf("invalid authentication token file: %w", err)
		}
		result.tokenSource = "file"
		return nil
	}
	return errors.New("authentication requires -token, REGIONDB_TOKEN, -token-file, or explicit -no-auth")
}

func validateAuthenticationToken(token string) error {
	if token == "" {
		return errors.New("authentication token must not be empty")
	}
	for _, character := range []byte(token) {
		if character < 0x21 || character > 0x7e {
			return errors.New("authentication token must be printable ASCII without spaces")
		}
	}
	return nil
}

func printVersion(w io.Writer) error {
	_, err := io.WriteString(w, "regiondb "+version.Current+"\n")
	return err
}
