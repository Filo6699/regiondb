package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Filo6699/regiondb/internal/geometry"
	"github.com/Filo6699/regiondb/internal/storage/fs_split"
)

func TestRunReturnsMachineUsableIntegrityResult(t *testing.T) {
	root := createCLIStore(t)
	name := "unexpected artifact"
	if err := os.WriteFile(filepath.Join(root, name), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := run([]string{
		"-data-dir", root,
		"-chunk-edge", "2",
		"-large-chunk-edge", "2",
		"-block-bits", "4",
	}, &stdout, &stderr)
	if code != exitIntegrity {
		t.Fatalf("run() = %d, want %d; stderr = %q", code, exitIntegrity, stderr.String())
	}
	if stderr.Len() != 0 {
		t.Fatalf("run() stderr = %q, want empty", stderr.String())
	}

	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("run() output has %d lines, want 2: %q", len(lines), stdout.String())
	}
	var issue outputRecord
	if err := json.Unmarshal([]byte(lines[0]), &issue); err != nil {
		t.Fatalf("decode issue: %v", err)
	}
	if issue.Type != "issue" || issue.Code != "misplaced_artifact" || issue.Path != name {
		t.Fatalf("issue = %+v", issue)
	}
	var summary outputRecord
	if err := json.Unmarshal([]byte(lines[1]), &summary); err != nil {
		t.Fatalf("decode summary: %v", err)
	}
	if summary.Type != "summary" || summary.Status != "corrupt" || summary.Issues != 1 {
		t.Fatalf("summary = %+v", summary)
	}
}

func TestWriteRecordEscapesFields(t *testing.T) {
	var output bytes.Buffer
	record := outputRecord{
		Type:   "issue",
		Code:   "misplaced_artifact",
		Path:   "line\n\"quoted\"",
		Detail: "detail\twith control characters",
	}
	if err := writeRecord(&output, record); err != nil {
		t.Fatalf("writeRecord() error = %v", err)
	}
	var decoded outputRecord
	if err := json.Unmarshal(bytes.TrimSpace(output.Bytes()), &decoded); err != nil {
		t.Fatalf("decode escaped record: %v; output = %q", err, output.String())
	}
	if decoded != record {
		t.Fatalf("decoded record = %+v, want %+v", decoded, record)
	}
	for _, escaped := range [][]byte{[]byte(`line\n\"quoted\"`), []byte(`detail\twith`)} {
		if !bytes.Contains(output.Bytes(), escaped) {
			t.Errorf("encoded record %q does not contain %q", output.String(), escaped)
		}
	}
	if bytes.Count(output.Bytes(), []byte{'\n'}) != 1 {
		t.Fatalf("encoded JSONL record contains an unescaped newline: %q", output.String())
	}
}

func TestRunUsesDistinctCleanAndUsageExitCodes(t *testing.T) {
	root := createCLIStore(t)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	args := []string{
		"-data-dir", root,
		"-chunk-edge", "2",
		"-large-chunk-edge", "2",
		"-block-bits", "4",
	}
	if code := run(args, &stdout, &stderr); code != exitClean {
		t.Fatalf("clean run() = %d, stderr = %q", code, stderr.String())
	}
	var summary outputRecord
	if err := json.Unmarshal(bytes.TrimSpace(stdout.Bytes()), &summary); err != nil {
		t.Fatalf("decode clean summary: %v", err)
	}
	if summary.Status != "ok" {
		t.Fatalf("clean summary = %+v", summary)
	}

	stdout.Reset()
	stderr.Reset()
	if code := run(nil, &stdout, &stderr); code != exitError {
		t.Fatalf("usage run() = %d, want %d", code, exitError)
	}
	var failure outputRecord
	if err := json.Unmarshal(bytes.TrimSpace(stderr.Bytes()), &failure); err != nil {
		t.Fatalf("decode usage error: %v", err)
	}
	if failure.Type != "error" || failure.Code != "usage" {
		t.Fatalf("usage error = %+v", failure)
	}
}

func createCLIStore(t *testing.T) string {
	t.Helper()
	g, err := geometry.New(geometry.Config{ChunkEdge: 2, LargeChunkEdge: 2, BlockBits: 4})
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	store, err := fs_split.Open(root, g)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	return root
}
