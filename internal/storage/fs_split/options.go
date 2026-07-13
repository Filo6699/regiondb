package fs_split

import (
	"errors"
	"fmt"
	"log/slog"

	"github.com/Filo6699/regiondb/internal/defaults"
)

const (
	DefaultCheckpointRecords     = defaults.CheckpointRecords
	DefaultCheckpointBytes       = defaults.CheckpointBytes
	DefaultMaxLoadedChunks       = defaults.MaxLoadedChunks
	DefaultMaxOpenWALHandles     = defaults.MaxOpenWALHandles
	DefaultWALGroupCommitUpdates = defaults.WALGroupCommitUpdates
)

type DurabilityMode string
type CheckpointCompression string

const (
	DurabilityRelaxed         DurabilityMode = "relaxed"
	DurabilityFsyncWAL        DurabilityMode = "fsync-wal"
	DurabilityFsyncCheckpoint DurabilityMode = "fsync-checkpoint"

	CheckpointCompressionNone CheckpointCompression = "none"
	CheckpointCompressionZRLE CheckpointCompression = "zrle"
)

type Options struct {
	ReadOnly              bool
	Durability            DurabilityMode
	CheckpointCompression CheckpointCompression
	CheckpointRecords     uint64
	CheckpointBytes       int64
	MaxLoadedChunks       int
	MaxOpenWALHandles     int
	DescriptorReserve     int
	WALGroupCommitUpdates uint64
	PostCommitFailure     func(event string)
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
	if options.CheckpointCompression == "" {
		options.CheckpointCompression = CheckpointCompressionNone
	}
	switch options.CheckpointCompression {
	case CheckpointCompressionNone, CheckpointCompressionZRLE:
	default:
		return Options{}, fmt.Errorf(
			"invalid checkpoint compression %q",
			options.CheckpointCompression,
		)
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
	if options.MaxLoadedChunks == 0 {
		options.MaxLoadedChunks = DefaultMaxLoadedChunks
	}
	if options.MaxLoadedChunks < 0 {
		return Options{}, errors.New("maximum loaded chunks must be positive")
	}
	if options.MaxOpenWALHandles == 0 {
		options.MaxOpenWALHandles = DefaultMaxOpenWALHandles
	}
	effectiveWALHandles, err := EffectiveWALHandleLimit(
		options.MaxOpenWALHandles,
		options.DescriptorReserve,
	)
	if err != nil {
		return Options{}, err
	}
	options.MaxOpenWALHandles = effectiveWALHandles
	if options.WALGroupCommitUpdates == 0 {
		options.WALGroupCommitUpdates = DefaultWALGroupCommitUpdates
	}
	if options.PostCommitFailure == nil {
		options.PostCommitFailure = func(event string) {
			slog.Warn(event, slog.String("component", "storage"))
		}
	}
	return options, nil
}
