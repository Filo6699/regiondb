//go:build darwin || linux

package main

import (
	"os"
	"os/exec"
	"testing"

	"github.com/Filo6699/regiondb/internal/storage/fs_split"
	"golang.org/x/sys/unix"
)

const lowServerDescriptorHelper = "REGIONDB_LOW_SERVER_DESCRIPTOR_HELPER"

func TestAutoFitDescriptorLimitsUnderLowProcessLimit(t *testing.T) {
	if os.Getenv(lowServerDescriptorHelper) == "1" {
		runLowServerDescriptorLimit(t)
		return
	}

	command := exec.Command(
		os.Args[0],
		"-test.run=^TestAutoFitDescriptorLimitsUnderLowProcessLimit$",
	)
	command.Env = append(os.Environ(), lowServerDescriptorHelper+"=1")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("low-descriptor subprocess failed: %v\n%s", err, output)
	}
}

func runLowServerDescriptorLimit(t *testing.T) {
	t.Helper()

	var limit unix.Rlimit
	if err := unix.Getrlimit(unix.RLIMIT_NOFILE, &limit); err != nil {
		t.Fatalf("get descriptor limit: %v", err)
	}
	const lowLimit = uint64(64)
	if limit.Cur < lowLimit {
		t.Skipf("descriptor limit is too low for the subprocess test: %d", limit.Cur)
	}
	limit.Cur = lowLimit
	if err := unix.Setrlimit(unix.RLIMIT_NOFILE, &limit); err != nil {
		t.Fatalf("set descriptor limit: %v", err)
	}

	got, err := autoFitDescriptorLimits(config{
		workers:           2,
		acceptQueue:       128,
		maxOpenWALStreams: 4,
	}, fs_split.AvailableWALDescriptors)
	if err != nil {
		t.Fatalf("autoFitDescriptorLimits() error = %v", err)
	}
	if got.acceptQueue != 24 || got.maxOpenWALStreams != 4 {
		t.Fatalf("auto-fitted limits = queue %d, WAL %d; want queue 24, WAL 4",
			got.acceptQueue, got.maxOpenWALStreams)
	}
}
