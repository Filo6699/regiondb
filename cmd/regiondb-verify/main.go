package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"

	"github.com/Filo6699/regiondb/internal/geometry"
	"github.com/Filo6699/regiondb/internal/storage/fs_split"
)

const (
	exitClean     = 0
	exitIntegrity = 1
	exitError     = 2
)

type outputRecord struct {
	Type     string `json:"type"`
	Status   string `json:"status,omitempty"`
	Code     string `json:"code,omitempty"`
	Path     string `json:"path,omitempty"`
	Detail   string `json:"detail,omitempty"`
	Images   uint64 `json:"images,omitempty"`
	Versions uint64 `json:"versions,omitempty"`
	WAL      uint64 `json:"wal_records,omitempty"`
	Issues   int    `json:"issues,omitempty"`
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	config, err := parseConfig(args)
	if err != nil {
		if err := writeRecord(stderr, outputRecord{Type: "error", Code: "usage", Detail: err.Error()}); err != nil {
			return exitError
		}
		return exitError
	}
	g, err := geometry.New(config.geometry)
	if err != nil {
		if err := writeRecord(stderr, outputRecord{Type: "error", Code: "geometry", Detail: err.Error()}); err != nil {
			return exitError
		}
		return exitError
	}
	report, err := fs_split.Verify(config.dataDir, g)
	if err != nil {
		if err := writeRecord(stderr, outputRecord{Type: "error", Code: "scan", Detail: err.Error()}); err != nil {
			return exitError
		}
		return exitError
	}
	for _, issue := range report.Issues {
		if err := writeRecord(stdout, outputRecord{
			Type: "issue", Code: issue.Code, Path: issue.Path, Detail: issue.Detail,
		}); err != nil {
			return exitError
		}
	}
	status := "ok"
	exitCode := exitClean
	if len(report.Issues) != 0 {
		status = "corrupt"
		exitCode = exitIntegrity
	}
	if err := writeRecord(stdout, outputRecord{
		Type: "summary", Status: status, Images: report.Images,
		Versions: report.Versions, WAL: report.WAL, Issues: len(report.Issues),
	}); err != nil {
		return exitError
	}
	return exitCode
}

type config struct {
	dataDir  string
	geometry geometry.Config
}

func parseConfig(args []string) (config, error) {
	var result config
	var chunkEdge uint64
	var largeChunkEdge uint64
	var blockBits uint64
	flags := flag.NewFlagSet("regiondb-verify", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.StringVar(&result.dataDir, "data-dir", "", "directory for chunk data")
	flags.Uint64Var(&chunkEdge, "chunk-edge", 0, "blocks per chunk edge")
	flags.Uint64Var(&largeChunkEdge, "large-chunk-edge", 0, "chunks per large-chunk edge")
	flags.Uint64Var(&blockBits, "block-bits", 0, "bits per block")
	if err := flags.Parse(args); err != nil {
		return config{}, err
	}
	if flags.NArg() != 0 {
		return config{}, fmt.Errorf("unexpected positional argument %q", flags.Arg(0))
	}
	if result.dataDir == "" {
		return config{}, errors.New("-data-dir is required")
	}
	if chunkEdge == 0 || largeChunkEdge == 0 || blockBits == 0 {
		return config{}, errors.New("-chunk-edge, -large-chunk-edge, and -block-bits are required")
	}
	if chunkEdge > math.MaxUint32 ||
		largeChunkEdge > math.MaxUint32 ||
		blockBits > math.MaxUint8 {
		return config{}, errors.New("geometry value is out of range")
	}
	result.geometry = geometry.Config{
		ChunkEdge: uint32(chunkEdge), LargeChunkEdge: uint32(largeChunkEdge), BlockBits: uint8(blockBits),
	}
	return result, nil
}

func writeRecord(writer io.Writer, record outputRecord) error {
	return json.NewEncoder(writer).Encode(record)
}
