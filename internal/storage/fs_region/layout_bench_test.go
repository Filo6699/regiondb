package fs_region

import (
	"fmt"
	"testing"

	"github.com/Filo6699/regiondb/internal/benchmark"
	"github.com/Filo6699/regiondb/internal/geometry"
	"github.com/Filo6699/regiondb/internal/storage"
)

// benchmarkWorkingSetChunks fixes the resident chunk count so a result does not
// depend on the benchmark iteration count chosen by the toolchain.
const benchmarkWorkingSetChunks = 1024

// BenchmarkLayout runs the seeded scenario workloads against the experimental
// region layout and the production split layout, so both sides of the A/B
// comparison are measured by one runner:
//
//	go test -run '^$' -bench BenchmarkLayout ./internal/storage/fs_region
//
// The numbers characterize one machine and configuration. They are not a
// throughput guarantee and no gate depends on them.
func BenchmarkLayout(b *testing.B) {
	g := mustGeometry(b, geometry.Config{ChunkEdge: 16, LargeChunkEdge: 8, BlockBits: 5})
	workloads := []benchmark.Workload{
		benchmark.WorkloadRead,
		benchmark.WorkloadWrite,
		benchmark.WorkloadMixed,
	}
	for _, workload := range workloads {
		for _, backend := range layoutBackends() {
			b.Run(fmt.Sprintf("workload=%s/layout=%s", workload, backend.name), func(b *testing.B) {
				runLayoutBenchmark(b, g, backend, workload)
			})
		}
	}
}

func runLayoutBenchmark(
	b *testing.B,
	g geometry.Geometry,
	backend layoutBackend,
	workload benchmark.Workload,
) {
	config := benchmark.Config{
		Seed:       benchmark.DefaultSeed,
		Operations: benchmarkWorkingSetChunks,
		Workload:   workload,
	}
	store, err := backend.open(b.TempDir(), g)
	if err != nil {
		b.Fatalf("open %s: %v", backend.name, err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			b.Errorf("close %s: %v", backend.name, err)
		}
	}()

	chunks := benchmarkChunks(b, g)
	coordinates, err := benchmark.WorkingSet(config)
	if err != nil {
		b.Fatal(err)
	}
	if workload != benchmark.WorkloadWrite {
		for _, coord := range coordinates {
			if err := store.WriteChunk(coord, chunks[0]); err != nil {
				b.Fatalf("prepare chunk (%d,%d): %v", coord.X, coord.Y, err)
			}
		}
	}
	sequence, err := benchmark.NewSequence(config)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		operation := sequence.Next()
		switch operation.Kind {
		case benchmark.OperationRead:
			if _, err := store.ReadChunk(operation.Coord); err != nil {
				b.Fatalf("read chunk (%d,%d): %v", operation.Coord.X, operation.Coord.Y, err)
			}
		case benchmark.OperationWrite:
			if err := store.WriteChunk(operation.Coord, chunks[operation.Value]); err != nil {
				b.Fatalf("write chunk (%d,%d): %v", operation.Coord.X, operation.Coord.Y, err)
			}
		default:
			b.Fatalf("unsupported operation %q", operation.Kind)
		}
	}
}

func benchmarkChunks(b *testing.B, g geometry.Geometry) [256]*storage.Chunk {
	b.Helper()

	var chunks [256]*storage.Chunk
	for value := range chunks {
		chunk, err := storage.ChunkFromBytes(g, benchmark.Payload(g, byte(value)))
		if err != nil {
			b.Fatalf("create benchmark chunk: %v", err)
		}
		chunks[value] = chunk
	}
	return chunks
}
