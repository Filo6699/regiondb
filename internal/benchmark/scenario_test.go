package benchmark

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/Filo6699/regiondb/internal/geometry"
)

func TestSequenceIsDeterministic(t *testing.T) {
	t.Parallel()

	config := Config{Seed: 42, Operations: 20, Workload: WorkloadMixed}
	first, err := NewSequence(config)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewSequence(config)
	if err != nil {
		t.Fatal(err)
	}
	for range config.Operations {
		if left, right := first.Next(), second.Next(); !reflect.DeepEqual(left, right) {
			t.Fatalf("operations differ: %+v != %+v", left, right)
		}
	}
}

func TestSequenceSelectsRequestedWorkload(t *testing.T) {
	t.Parallel()

	tests := []struct {
		workload Workload
		want     OperationKind
	}{
		{workload: WorkloadRead, want: OperationRead},
		{workload: WorkloadWrite, want: OperationWrite},
	}
	for _, test := range tests {
		sequence, err := NewSequence(Config{
			Seed:       1,
			Operations: 10,
			Workload:   test.workload,
		})
		if err != nil {
			t.Fatal(err)
		}
		for range 10 {
			if operation := sequence.Next(); operation.Kind != test.want {
				t.Fatalf("%s operation = %q, want %q", test.workload, operation.Kind, test.want)
			}
		}
	}
}

func TestRunReportsOperationCountsAndPercentiles(t *testing.T) {
	t.Parallel()

	result, err := Run(
		context.Background(),
		"test",
		Config{Seed: 7, Operations: 100, Workload: WorkloadMixed},
		func(Operation) error { return nil },
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.Operations != 100 {
		t.Fatalf("Operations = %d, want 100", result.Operations)
	}
	if result.WorkingSet != 100 {
		t.Fatalf("WorkingSet = %d, want 100", result.WorkingSet)
	}
	if result.Reads+result.Writes != result.Operations {
		t.Fatalf("Reads + Writes = %d, want %d", result.Reads+result.Writes, result.Operations)
	}
	if result.Reads == 0 || result.Writes == 0 {
		t.Fatalf("mixed workload counts = %d reads, %d writes", result.Reads, result.Writes)
	}
	latency := result.LatencyNanoseconds
	if latency.Minimum > latency.P50 ||
		latency.P50 > latency.P95 ||
		latency.P95 > latency.P99 ||
		latency.P99 > latency.Maximum {
		t.Fatalf("latencies are not ordered: %+v", latency)
	}
}

func TestSummarizeUsesNearestRankPercentiles(t *testing.T) {
	t.Parallel()

	latencies := make([]time.Duration, 100)
	for index := range latencies {
		latencies[index] = time.Duration(index+1) * time.Nanosecond
	}
	got := summarize(latencies)
	want := (Latencies{
		Minimum: 1,
		P50:     50,
		P95:     95,
		P99:     99,
		Maximum: 100,
	})
	if got != want {
		t.Fatalf("summarize() = %+v, want %+v", got, want)
	}
}

func TestConfigValidation(t *testing.T) {
	t.Parallel()

	tests := []Config{
		{Seed: 1, Operations: 0, Workload: WorkloadRead},
		{Seed: 1, Operations: MaxOperations + 1, Workload: WorkloadRead},
		{Seed: 1, Operations: 1, Workload: "unknown"},
	}
	for _, config := range tests {
		if err := config.Validate(); err == nil {
			t.Fatalf("Validate(%+v) succeeded", config)
		}
	}
}

func TestPayloadClearsUnusedFinalBits(t *testing.T) {
	t.Parallel()

	g, err := geometry.New(geometry.Config{
		ChunkEdge:      1,
		LargeChunkEdge: 1,
		BlockBits:      5,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := Payload(g, 0xff); !reflect.DeepEqual(got, []byte{0x1f}) {
		t.Fatalf("Payload() = %x, want 1f", got)
	}
}

func TestRunHonorsCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := Run(
		ctx,
		"test",
		Config{Seed: 1, Operations: 1, Workload: WorkloadRead},
		func(Operation) error { return errors.New("must not run") },
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want context cancellation", err)
	}
}
