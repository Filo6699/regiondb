package defaults

import (
	"runtime"
	"time"
)

const (
	CheckpointRecords     = 1024
	CheckpointBytes       = 64 << 20
	MaxLoadedChunks       = 1024
	MaxOpenWALHandles     = 2
	WALGroupCommitUpdates = 1

	Address          = "127.0.0.1:4242"
	AcceptQueue      = 128
	MaxLineBytes     = 1 << 20
	IdleTimeout      = 30 * time.Second
	RequestTimeout   = 10 * time.Second
	ResponseTimeout  = 10 * time.Second
	AuthFailureDelay = 250 * time.Millisecond
	AuthFailureLimit = 5
	AuthBanDuration  = time.Minute
)

func Workers() int {
	return runtime.GOMAXPROCS(0)
}
