package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"os/signal"
	"syscall"

	"github.com/Filo6699/regiondb/internal/benchmark"
	"github.com/Filo6699/regiondb/internal/geometry"
	"github.com/Filo6699/regiondb/internal/storage"
	"github.com/Filo6699/regiondb/internal/storage/fs_split"
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
	dataDir    string
	scenario   benchmark.Config
	geometry   geometry.Config
	durability fs_split.DurabilityMode
}

type output struct {
	benchmark.Result
	Geometry   benchmark.Geometry      `json:"geometry"`
	LockModes  benchmark.LockModes     `json:"lock_modes"`
	Durability fs_split.DurabilityMode `json:"durability"`
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) (returnErr error) {
	config, err := parseConfig(args, stderr)
	if err != nil {
		return err
	}
	g, err := geometry.New(config.geometry)
	if err != nil {
		return fmt.Errorf("configure geometry: %w", err)
	}
	store, err := fs_split.OpenWithOptions(config.dataDir, g, fs_split.Options{
		Durability: config.durability,
	})
	if err != nil {
		return fmt.Errorf("open direct benchmark store: %w", err)
	}
	defer func() {
		if err := store.Close(); err != nil && returnErr == nil {
			returnErr = fmt.Errorf("close direct benchmark store: %w", err)
		}
	}()

	chunks, err := benchmarkChunks(g)
	if err != nil {
		return err
	}
	coordinates, err := benchmark.WorkingSet(config.scenario)
	if err != nil {
		return err
	}
	if config.scenario.Workload != benchmark.WorkloadWrite {
		for _, coord := range coordinates {
			if err := store.WriteChunk(coord, chunks[0]); err != nil {
				return fmt.Errorf("prepare chunk (%d,%d): %w", coord.X, coord.Y, err)
			}
		}
	}

	result, err := benchmark.Run(ctx, "direct", config.scenario, func(operation benchmark.Operation) error {
		switch operation.Kind {
		case benchmark.OperationRead:
			_, err := store.ReadChunk(operation.Coord)
			return err
		case benchmark.OperationWrite:
			return store.WriteChunk(operation.Coord, chunks[operation.Value])
		default:
			return fmt.Errorf("unsupported operation %q", operation.Kind)
		}
	})
	if err != nil {
		return fmt.Errorf("run direct benchmark: %w", err)
	}
	stats := store.RuntimeStats()
	if err := json.NewEncoder(stdout).Encode(output{
		Result:   result,
		Geometry: benchmark.GeometryFrom(config.geometry),
		LockModes: benchmark.LockModes{
			Process: stats.ProcessLockMode,
			Chunk:   stats.ChunkLockMode,
		},
		Durability: config.durability,
	}); err != nil {
		return fmt.Errorf("write benchmark result: %w", err)
	}
	return nil
}

func parseConfig(args []string, stderr io.Writer) (config, error) {
	flags := flag.NewFlagSet("regiondb-direct-bench", flag.ContinueOnError)
	flags.SetOutput(stderr)

	var result config
	var workload string
	var chunkEdge uint64
	var largeChunkEdge uint64
	var blockBits uint64
	var durability string
	flags.StringVar(&result.dataDir, "data-dir", "", "directory for benchmark chunk data")
	flags.Int64Var(&result.scenario.Seed, "seed", benchmark.DefaultSeed, "workload random seed")
	flags.IntVar(&result.scenario.Operations, "ops", benchmark.DefaultOperations, "number of measured operations")
	flags.StringVar(&workload, "workload", string(benchmark.WorkloadMixed), "workload: read, write, or mixed")
	flags.Uint64Var(&chunkEdge, "chunk-edge", 16, "blocks per chunk edge")
	flags.Uint64Var(&largeChunkEdge, "large-chunk-edge", 8, "chunks per large-chunk edge")
	flags.Uint64Var(&blockBits, "block-bits", 5, "bits per block")
	flags.StringVar(&durability, "durability", string(fs_split.DurabilityRelaxed), "durability mode: relaxed, fsync-wal, or fsync-checkpoint")
	if err := flags.Parse(args); err != nil {
		return config{}, err
	}
	if flags.NArg() != 0 {
		return config{}, errors.New("unexpected positional arguments")
	}
	if result.dataDir == "" {
		return config{}, errors.New("-data-dir is required")
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
	result.durability = fs_split.DurabilityMode(durability)
	switch result.durability {
	case fs_split.DurabilityRelaxed, fs_split.DurabilityFsyncWAL, fs_split.DurabilityFsyncCheckpoint:
	default:
		return config{}, fmt.Errorf("invalid -durability value %q", durability)
	}
	return result, nil
}

func benchmarkChunks(g geometry.Geometry) ([256]*storage.Chunk, error) {
	var chunks [256]*storage.Chunk
	for value := range chunks {
		chunk, err := storage.ChunkFromBytes(g, benchmark.Payload(g, byte(value)))
		if err != nil {
			return chunks, fmt.Errorf("create benchmark chunk: %w", err)
		}
		chunks[value] = chunk
	}
	return chunks, nil
}
