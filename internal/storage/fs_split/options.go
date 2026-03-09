package fs_split

import (
	"errors"
	"fmt"
)

const (
	DefaultCheckpointRecords = 1024
	DefaultCheckpointBytes   = 64 << 20
)

type DurabilityMode string

const (
	DurabilityRelaxed         DurabilityMode = "relaxed"
	DurabilityFsyncWAL        DurabilityMode = "fsync-wal"
	DurabilityFsyncCheckpoint DurabilityMode = "fsync-checkpoint"
)

type Options struct {
	Durability        DurabilityMode
	CheckpointRecords uint64
	CheckpointBytes   int64
}

func (options Options) validated() (Options, error) {
	if options.Durability == "" {
		options.Durability = DurabilityRelaxed
	}
	switch options.Durability {
	case DurabilityRelaxed, DurabilityFsyncWAL, DurabilityFsyncCheckpoint:
	default:
		return Options{}, fmt.Errorf("invalid durability mode %q", options.Durability)
	}
	if options.CheckpointRecords == 0 {
		options.CheckpointRecords = DefaultCheckpointRecords
	}
	if options.CheckpointBytes == 0 {
		options.CheckpointBytes = DefaultCheckpointBytes
	}
	if options.CheckpointBytes < 0 {
		return Options{}, errors.New("checkpoint byte threshold must be positive")
	}
	return options, nil
}
