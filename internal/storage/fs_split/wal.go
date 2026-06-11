package fs_split

import (
	"bytes"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"path/filepath"

	"github.com/Filo6699/regiondb/internal/bitcodec"
	"github.com/Filo6699/regiondb/internal/geometry"
	"github.com/Filo6699/regiondb/internal/storage"
)

const (
	walName        = ".regiondb.wal"
	walMagic       = "RGDBWAL2"
	legacyWALMagic = "RGDBWAL1"
	walHeaderBytes = 36
)

var ErrCorruptWAL = errors.New("corrupt fs_split_v1 WAL")

type walBoundary string

const (
	walRecordAppended      walBoundary = "record-appended"
	walRecordSynced        walBoundary = "record-synced"
	walCheckpointTruncated walBoundary = "checkpoint-truncated"
	walCheckpointSynced    walBoundary = "checkpoint-synced"
)

type walFlushKind uint8

const (
	walForegroundFlush walFlushKind = iota
	walGroupFlush
	walCheckpointFlush
)

type walRecord struct {
	coord    geometry.Coord
	payload  []byte
	presence []byte
}

func (s *Store) appendWAL(record []byte) error {
	wal, err := s.walHandles.acquire(walAppendHandle)
	if err != nil {
		return fmt.Errorf("acquire WAL append handle: %w", err)
	}
	err = appendWALHandle(wal, record)
	s.walHandles.release(walAppendHandle)
	if err != nil {
		return err
	}
	if err := s.runWALFailpoint(walRecordAppended); err != nil {
		return err
	}
	if s.options.Durability == DurabilityFsyncWAL {
		s.walUnsyncedUpdates++
		if s.walUnsyncedUpdates >= s.options.WALGroupCommitUpdates {
			kind := walGroupFlush
			if s.options.WALGroupCommitUpdates == 1 {
				kind = walForegroundFlush
			}
			if err := s.syncWAL(kind); err != nil {
				return err
			}
		}
	}
	return nil
}

func writeWALRecord(writer io.Writer, record []byte) error {
	written, err := writer.Write(record)
	if err != nil {
		return fmt.Errorf("append WAL: %w", err)
	}
	if written != len(record) {
		return fmt.Errorf("append WAL: wrote %d of %d bytes: %w", written, len(record), io.ErrShortWrite)
	}
	return nil
}

// syncWAL flushes a checked-out append handle. os.File.Sync maps to
// FlushFileBuffers on Windows, which reports the write handle as durable
// without closing it, so the pool can reuse the handle afterwards.
func (s *Store) syncWAL(kind walFlushKind) error {
	wal, err := s.walHandles.acquire(walAppendHandle)
	if err != nil {
		return fmt.Errorf("acquire WAL append handle: %w", err)
	}
	err = syncFile(wal)
	s.walHandles.release(walAppendHandle)
	if err != nil {
		return fmt.Errorf("sync WAL: %w", err)
	}
	if err := s.runWALFailpoint(walRecordSynced); err != nil {
		return err
	}
	s.recordWALFlush(kind)
	s.walUnsyncedUpdates = 0
	return nil
}

func (s *Store) recordWALFlush(kind walFlushKind) {
	switch kind {
	case walForegroundFlush:
		s.walForegroundFlushes.Add(1)
	case walGroupFlush:
		s.walGroupFlushes.Add(1)
	case walCheckpointFlush:
		s.walCheckpointFlushes.Add(1)
	}
}

func (s *Store) encodeWALRecord(coord geometry.Coord, payload []byte) []byte {
	presence := make([]byte, s.geometry.PresenceBytes())
	for index := uint64(0); index < s.geometry.BlockCount(); index++ {
		presence[index/8] |= byte(1 << (index % 8))
	}
	return s.appendWALRecord(nil, coord, payload, presence)
}

func (s *Store) appendWALRecord(encoded []byte, coord geometry.Coord, payload, presence []byte) []byte {
	config := s.geometry.Config()
	recordBytes := walHeaderBytes + len(payload) + len(presence) + checksumSize
	if cap(encoded) < recordBytes {
		encoded = make([]byte, 0, recordBytes)
	}
	encoded = append(encoded, walMagic...)
	encoded = bitcodec.AppendUint32(encoded, config.ChunkEdge)
	encoded = bitcodec.AppendUint32(encoded, config.LargeChunkEdge)
	encoded = append(encoded, config.BlockBits, 0, 0, 0)
	encoded = bitcodec.AppendUint64(encoded, uint64(coord.X))
	encoded = bitcodec.AppendUint64(encoded, uint64(coord.Y))
	encoded = append(encoded, payload...)
	encoded = append(encoded, presence...)
	return bitcodec.AppendUint32(encoded, crc32.ChecksumIEEE(encoded))
}

func (s *Store) decodeWALRecord(encoded []byte) (walRecord, error) {
	magic := string(encoded[:len(walMagic)])
	if magic != walMagic && magic != legacyWALMagic {
		return walRecord{}, fmt.Errorf("%w: invalid record magic", ErrCorruptWAL)
	}
	storedChecksum, err := bitcodec.Uint32(encoded[len(encoded)-checksumSize:])
	if err != nil {
		return walRecord{}, fmt.Errorf("%w: read record checksum: %v", ErrCorruptWAL, err)
	}
	if actual := crc32.ChecksumIEEE(encoded[:len(encoded)-checksumSize]); actual != storedChecksum {
		return walRecord{}, fmt.Errorf("%w: record checksum mismatch", ErrCorruptWAL)
	}
	chunkEdge, err := bitcodec.Uint32(encoded[8:12])
	if err != nil {
		return walRecord{}, fmt.Errorf("%w: read chunk edge: %v", ErrCorruptWAL, err)
	}
	largeChunkEdge, err := bitcodec.Uint32(encoded[12:16])
	if err != nil {
		return walRecord{}, fmt.Errorf("%w: read large-chunk edge: %v", ErrCorruptWAL, err)
	}
	if !bytes.Equal(encoded[17:20], []byte{0, 0, 0}) {
		return walRecord{}, fmt.Errorf("%w: reserved record bytes are nonzero", ErrCorruptWAL)
	}
	config := s.geometry.Config()
	if chunkEdge != config.ChunkEdge || largeChunkEdge != config.LargeChunkEdge || encoded[16] != config.BlockBits {
		return walRecord{}, fmt.Errorf("%w: %w", ErrCorruptWAL, ErrGeometryMismatch)
	}
	x, err := bitcodec.Uint64(encoded[20:28])
	if err != nil {
		return walRecord{}, fmt.Errorf("%w: read chunk x coordinate: %v", ErrCorruptWAL, err)
	}
	y, err := bitcodec.Uint64(encoded[28:36])
	if err != nil {
		return walRecord{}, fmt.Errorf("%w: read chunk y coordinate: %v", ErrCorruptWAL, err)
	}
	payloadEnd := walHeaderBytes + s.geometry.PayloadBytes()
	payload := encoded[walHeaderBytes:payloadEnd]
	var presence []byte
	if magic == legacyWALMagic {
		chunk, err := storage.ChunkFromLegacyBytes(s.geometry, payload)
		if err != nil {
			return walRecord{}, fmt.Errorf("%w: decode legacy payload: %v", ErrCorruptWAL, err)
		}
		presence = chunk.PresenceBytes()
	} else {
		presence = encoded[payloadEnd : len(encoded)-checksumSize]
		if _, err := storage.ChunkFromState(s.geometry, payload, presence); err != nil {
			return walRecord{}, fmt.Errorf("%w: decode payload: %v", ErrCorruptWAL, err)
		}
	}
	return walRecord{
		coord:    geometry.Coord{X: int64(x), Y: int64(y)},
		payload:  append([]byte(nil), payload...),
		presence: append([]byte(nil), presence...),
	}, nil
}

func (s *Store) scanWAL(visit func(walRecord) error) (int64, uint64, error) {
	wal, err := s.walHandles.acquire(walScanHandle)
	if err != nil {
		return 0, 0, fmt.Errorf("acquire WAL scan handle: %w", err)
	}
	defer s.walHandles.release(walScanHandle)

	if _, err := wal.Seek(0, io.SeekStart); err != nil {
		return 0, 0, fmt.Errorf("seek WAL start: %w", err)
	}
	info, err := wal.Stat()
	if err != nil {
		return 0, 0, fmt.Errorf("stat WAL: %w", err)
	}
	legacyRecordBytes := walHeaderBytes + s.geometry.PayloadBytes() + checksumSize
	recordBytes := legacyRecordBytes + s.geometry.PresenceBytes()
	if recordBytes < walHeaderBytes+checksumSize {
		return 0, 0, fmt.Errorf("%w: record size overflow", ErrCorruptWAL)
	}
	if info.Size() >= int64(len(walMagic)) {
		magic := make([]byte, len(walMagic))
		if _, err := io.ReadFull(wal, magic); err != nil {
			return 0, 0, fmt.Errorf("read WAL magic: %w", err)
		}
		switch string(magic) {
		case walMagic:
		case legacyWALMagic:
			recordBytes = legacyRecordBytes
		default:
			if info.Size() < int64(legacyRecordBytes) {
				return 0, 0, nil
			}
			return 0, 0, fmt.Errorf("%w: invalid record magic", ErrCorruptWAL)
		}
		if _, err := wal.Seek(0, io.SeekStart); err != nil {
			return 0, 0, fmt.Errorf("rewind WAL after magic: %w", err)
		}
	}
	completeBytes := info.Size() / int64(recordBytes) * int64(recordBytes)
	encoded := make([]byte, recordBytes)
	var count uint64
	for offset := int64(0); offset < completeBytes; offset += int64(recordBytes) {
		if _, err := io.ReadFull(wal, encoded); err != nil {
			return 0, 0, fmt.Errorf("read WAL record: %w", err)
		}
		record, err := s.decodeWALRecord(encoded)
		if err != nil {
			return 0, 0, err
		}
		if err := visit(record); err != nil {
			return 0, 0, err
		}
		count++
	}
	return completeBytes, count, nil
}

func (s *Store) recoverWAL() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	completeBytes, recordCount, err := s.scanWAL(func(walRecord) error {
		return nil
	})
	if err != nil {
		return fmt.Errorf("recover WAL: %w", err)
	}
	syncData := s.options.Durability != DurabilityRelaxed
	if _, _, err := s.scanWAL(func(record walRecord) error {
		if err := s.persistChunk(record.coord, record.payload, record.presence, syncData); err != nil {
			return fmt.Errorf("replay WAL record: %w", err)
		}
		s.cache.remove(record.coord)
		return nil
	}); err != nil {
		return fmt.Errorf("recover WAL: %w", err)
	}
	wal, err := s.walHandles.acquire(walAppendHandle)
	if err != nil {
		return fmt.Errorf("acquire WAL append handle after replay: %w", err)
	}
	info, err := wal.Stat()
	if err != nil {
		s.walHandles.release(walAppendHandle)
		return fmt.Errorf("stat WAL after replay: %w", err)
	}
	sourceBytes := info.Size()
	s.walHandles.release(walAppendHandle)
	if recordCount != 0 || completeBytes != sourceBytes {
		if err := s.replaceWALAfterReplay(syncData); err != nil {
			return fmt.Errorf("finish WAL recovery: %w", err)
		}
	}
	return nil
}

// replaceWALAfterReplay commits compaction only after every complete record has
// been validated and loaded. Preparing the empty replacement cannot partially
// truncate the source WAL, so a failed recovery can retry from the same bytes.
func (s *Store) replaceWALAfterReplay(syncData bool) error {
	if err := s.walHandles.close(); err != nil {
		return fmt.Errorf("close WAL handles before replay compaction: %w", err)
	}

	path := filepath.Join(s.root, walName)
	if err := writeAtomic(path, nil, syncData, s.atomicWriteFailpoint); err != nil {
		return fmt.Errorf("replace replayed WAL: %w", err)
	}
	wal, err := openWALHandle(path)
	if err != nil {
		return fmt.Errorf("reopen compacted WAL: %w", err)
	}
	s.walHandles = newWALHandlePool(path, s.options.MaxOpenWALHandles, wal)
	s.walUnsyncedUpdates = 0
	if syncData {
		s.recordWALFlush(walCheckpointFlush)
	}
	return nil
}

func checkpointRecordTrigger(lowerBound uint64) uint64 {
	if lowerBound <= 1 {
		return lowerBound
	}
	extra := max(uint64(1), lowerBound/2)
	if lowerBound > math.MaxUint64-extra {
		return math.MaxUint64
	}
	return lowerBound + extra
}

func checkpointByteTrigger(lowerBound int64) int64 {
	if lowerBound <= 1 {
		return lowerBound
	}
	extra := max(int64(1), lowerBound/2)
	if lowerBound > math.MaxInt64-extra {
		return math.MaxInt64
	}
	return lowerBound + extra
}

func (s *Store) checkpointDue() bool {
	return s.walRecords >= checkpointRecordTrigger(s.options.CheckpointRecords) ||
		s.walBytes >= checkpointByteTrigger(s.options.CheckpointBytes)
}

func (s *Store) checkpointWAL() error {
	syncData := s.options.Durability != DurabilityRelaxed
	if syncData {
		_, _, err := s.scanWAL(func(record walRecord) error {
			if err := s.persistChunk(record.coord, record.payload, record.presence, true); err != nil {
				return fmt.Errorf("checkpoint WAL record: %w", err)
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("checkpoint WAL: %w", err)
		}
	}
	if err := s.clearWAL(syncData); err != nil {
		return fmt.Errorf("checkpoint WAL: %w", err)
	}
	s.walRecords = 0
	s.walBytes = 0
	s.checkpointCount.Add(1)
	return nil
}

func (s *Store) clearWAL(syncData bool) error {
	wal, err := s.walHandles.acquire(walAppendHandle)
	if err != nil {
		return fmt.Errorf("acquire WAL append handle: %w", err)
	}
	defer s.walHandles.release(walAppendHandle)

	if err := wal.Truncate(0); err != nil {
		return fmt.Errorf("truncate WAL: %w", err)
	}
	if err := s.runWALFailpoint(walCheckpointTruncated); err != nil {
		return err
	}
	if _, err := wal.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("rewind WAL: %w", err)
	}
	if syncData {
		if err := syncFile(wal); err != nil {
			return fmt.Errorf("sync truncated WAL: %w", err)
		}
		if err := s.runWALFailpoint(walCheckpointSynced); err != nil {
			return err
		}
		s.recordWALFlush(walCheckpointFlush)
	}
	s.walUnsyncedUpdates = 0
	return nil
}

func (s *Store) runWALFailpoint(boundary walBoundary) error {
	if s.walFailpoint == nil {
		return nil
	}
	if err := s.walFailpoint(boundary); err != nil {
		return fmt.Errorf("WAL failpoint %q: %w", boundary, err)
	}
	return nil
}
