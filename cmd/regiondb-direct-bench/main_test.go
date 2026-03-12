package main

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
)

func TestRunQuickDirectBenchmark(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if err := run(context.Background(), []string{
		"-data-dir", filepath.Join(t.TempDir(), "data"),
		"-seed", "17",
		"-ops", "12",
		"-workload", "mixed",
		"-chunk-edge", "2",
		"-large-chunk-edge", "2",
		"-block-bits", "4",
	}, &stdout, &stderr); err != nil {
		t.Fatalf("run() error = %v, stderr = %q", err, stderr.String())
	}

	var result output
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatalf("decode output: %v; output = %q", err, stdout.String())
	}
	if result.Backend != "direct" || result.Seed != 17 || result.Operations != 12 {
		t.Fatalf("result = %+v", result)
	}
	if result.Geometry.ChunkEdge != 2 || result.Durability != "relaxed" {
		t.Fatalf("configuration output = %+v", result)
	}
}

func TestParseConfigRejectsUnknownWorkload(t *testing.T) {
	t.Parallel()

	_, err := parseConfig([]string{
		"-data-dir", t.TempDir(),
		"-workload", "scan",
	}, &bytes.Buffer{})
	if err == nil {
		t.Fatal("parseConfig() succeeded")
	}
}
