package benchmark

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"time"

	"github.com/Filo6699/regiondb/internal/geometry"
)

const (
	DefaultSeed       int64 = 1
	DefaultOperations int   = 1000
	MaxOperations     int   = 10_000_000
	maxWorkingSet     int   = 1024
)

type Workload string

const (
	WorkloadRead  Workload = "read"
	WorkloadWrite Workload = "write"
	WorkloadMixed Workload = "mixed"
)

type Config struct {
	Seed       int64
	Operations int
	Workload   Workload
}

func (c Config) Validate() error {
	if c.Operations <= 0 {
		return errors.New("operation count must be positive")
	}
	if c.Operations > MaxOperations {
		return fmt.Errorf("operation count must not exceed %d", MaxOperations)
	}
	switch c.Workload {
	case WorkloadRead, WorkloadWrite, WorkloadMixed:
		return nil
	default:
		return fmt.Errorf("unknown workload %q", c.Workload)
	}
}

type OperationKind string

const (
	OperationRead  OperationKind = "read"
	OperationWrite OperationKind = "write"
)

type Operation struct {
	Kind  OperationKind
	Coord geometry.Coord
	Value byte
}

type Latencies struct {
	Minimum int64 `json:"minimum"`
	P50     int64 `json:"p50"`
	P95     int64 `json:"p95"`
	P99     int64 `json:"p99"`
	Maximum int64 `json:"maximum"`
}

type Result struct {
	Backend             string    `json:"backend"`
	Workload            Workload  `json:"workload"`
	Seed                int64     `json:"seed"`
	Operations          int       `json:"operations"`
	WorkingSet          int       `json:"working_set"`
	Reads               int       `json:"reads"`
	Writes              int       `json:"writes"`
	DurationNanoseconds int64     `json:"duration_nanoseconds"`
	OperationsPerSecond float64   `json:"operations_per_second"`
	LatencyNanoseconds  Latencies `json:"latency_nanoseconds"`
}

type Geometry struct {
	ChunkEdge      uint32 `json:"chunk_edge"`
	LargeChunkEdge uint32 `json:"large_chunk_edge"`
	BlockBits      uint8  `json:"block_bits"`
}

type LockModes struct {
	Process string `json:"process_lock_mode"`
	Chunk   string `json:"chunk_lock_mode"`
}

func GeometryFrom(config geometry.Config) Geometry {
	return Geometry{
		ChunkEdge:      config.ChunkEdge,
		LargeChunkEdge: config.LargeChunkEdge,
		BlockBits:      config.BlockBits,
	}
}

func Payload(g geometry.Geometry, value byte) []byte {
	payload := make([]byte, g.PayloadBytes())
	for index := range payload {
		payload[index] = value
	}
	usedFinalBits := (g.BlockCount() * uint64(g.Config().BlockBits)) % 8
	if usedFinalBits != 0 {
		payload[len(payload)-1] &= byte(1<<usedFinalBits) - 1
	}
	return payload
}

type Sequence struct {
	config     Config
	random     *rand.Rand
	workingSet int
}

func NewSequence(config Config) (*Sequence, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	workingSet := min(config.Operations, maxWorkingSet)
	return &Sequence{
		config:     config,
		random:     rand.New(rand.NewSource(config.Seed)),
		workingSet: workingSet,
	}, nil
}

func (s *Sequence) Next() Operation {
	kind := OperationRead
	switch s.config.Workload {
	case WorkloadWrite:
		kind = OperationWrite
	case WorkloadMixed:
		if s.random.Intn(5) == 0 {
			kind = OperationWrite
		}
	}
	index := s.random.Intn(s.workingSet)
	return Operation{
		Kind:  kind,
		Coord: coordinate(index),
		Value: byte(s.random.Intn(256)),
	}
}

func WorkingSet(config Config) ([]geometry.Coord, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	size := min(config.Operations, maxWorkingSet)
	coordinates := make([]geometry.Coord, size)
	for index := range coordinates {
		coordinates[index] = coordinate(index)
	}
	return coordinates, nil
}

func Run(
	ctx context.Context,
	backend string,
	config Config,
	execute func(Operation) error,
) (Result, error) {
	if ctx == nil {
		return Result{}, errors.New("benchmark context must not be nil")
	}
	if backend == "" {
		return Result{}, errors.New("benchmark backend must not be empty")
	}
	if execute == nil {
		return Result{}, errors.New("benchmark operation must not be nil")
	}
	sequence, err := NewSequence(config)
	if err != nil {
		return Result{}, err
	}

	latencies := make([]time.Duration, config.Operations)
	result := Result{
		Backend:    backend,
		Workload:   config.Workload,
		Seed:       config.Seed,
		Operations: config.Operations,
		WorkingSet: sequence.workingSet,
	}
	started := time.Now()
	for index := range config.Operations {
		if err := ctx.Err(); err != nil {
			return Result{}, fmt.Errorf("operation %d: %w", index, err)
		}
		operation := sequence.Next()
		operationStarted := time.Now()
		if err := execute(operation); err != nil {
			return Result{}, fmt.Errorf("operation %d: %w", index, err)
		}
		latencies[index] = time.Since(operationStarted)
		switch operation.Kind {
		case OperationRead:
			result.Reads++
		case OperationWrite:
			result.Writes++
		}
	}
	elapsed := time.Since(started)
	result.DurationNanoseconds = elapsed.Nanoseconds()
	if elapsed > 0 {
		result.OperationsPerSecond = float64(config.Operations) / elapsed.Seconds()
	}
	result.LatencyNanoseconds = summarize(latencies)
	return result, nil
}

func summarize(latencies []time.Duration) Latencies {
	sorted := append([]time.Duration(nil), latencies...)
	sort.Slice(sorted, func(left, right int) bool {
		return sorted[left] < sorted[right]
	})
	return Latencies{
		Minimum: sorted[0].Nanoseconds(),
		P50:     percentile(sorted, 50).Nanoseconds(),
		P95:     percentile(sorted, 95).Nanoseconds(),
		P99:     percentile(sorted, 99).Nanoseconds(),
		Maximum: sorted[len(sorted)-1].Nanoseconds(),
	}
}

func percentile(sorted []time.Duration, percent int) time.Duration {
	rank := int(math.Ceil(float64(percent)*float64(len(sorted))/100)) - 1
	return sorted[rank]
}

func coordinate(index int) geometry.Coord {
	const rowWidth = 32
	return geometry.Coord{
		X: int64(index % rowWidth),
		Y: int64(index / rowWidth),
	}
}
