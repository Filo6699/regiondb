package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
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
		"-listen", "127.0.0.1:0",
		"-data-dir", "data",
		"-token", "secret",
		"-chunk-edge", "16",
		"-large-chunk-edge", "8",
		"-block-bits", "5",
	}, &stderr)
	if err != nil {
		t.Fatalf("parseConfig() error = %v, stderr = %q", err, stderr.String())
	}
	if got.listenAddress != "127.0.0.1:0" || got.dataDir != "data" || got.token != "secret" {
		t.Fatalf("parseConfig() strings = %+v", got)
	}
	if got.geometry.ChunkEdge != 16 || got.geometry.LargeChunkEdge != 8 || got.geometry.BlockBits != 5 {
		t.Fatalf("parseConfig() geometry = %+v", got.geometry)
	}
}

func TestParseConfigRejectsMissingRuntimeFlags(t *testing.T) {
	t.Parallel()

	tests := [][]string{
		nil,
		{"-data-dir", "data"},
		{"-listen", "127.0.0.1:0"},
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
