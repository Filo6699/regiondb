package fs_split

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/Filo6699/regiondb/internal/geometry"
)

// VerificationIssue describes one independently actionable integrity failure.
type VerificationIssue struct {
	Code   string
	Path   string
	Detail string
}

// VerificationReport is the summary returned by Verify.
type VerificationReport struct {
	Images   uint64
	Versions uint64
	WAL      uint64
	Issues   []VerificationIssue
}

type verifier struct {
	root       string
	store      Store
	report     VerificationReport
	images     map[geometry.Coord]string
	versions   map[geometry.Coord]uint64
	clock      uint64
	clockValid bool
	walBytes   uint64
	walRecord  uint64
	walFound   bool
	intentOK   bool
}

// Verify inspects an fs_split_v1 data directory without creating, removing, or
// modifying any file. A completed scan reports integrity failures in Issues;
// errors are reserved for conditions that prevent the scan from completing.
func Verify(root string, g geometry.Geometry) (VerificationReport, error) {
	if root == "" {
		return VerificationReport{}, errors.New("verify fs_split_v1: data directory must not be empty")
	}
	validated, err := geometry.New(g.Config())
	if err != nil || validated != g {
		return VerificationReport{}, fmt.Errorf("verify fs_split_v1: %w", geometry.ErrInvalid)
	}
	absoluteRoot, err := filepath.Abs(root)
	if err != nil {
		return VerificationReport{}, fmt.Errorf("verify fs_split_v1: resolve data directory: %w", err)
	}
	info, err := os.Stat(absoluteRoot)
	if err != nil {
		return VerificationReport{}, fmt.Errorf("verify fs_split_v1: inspect data directory: %w", err)
	}
	if !info.IsDir() {
		return VerificationReport{}, errors.New("verify fs_split_v1: data path is not a directory")
	}

	v := verifier{
		root:     absoluteRoot,
		store:    Store{root: absoluteRoot, geometry: g},
		images:   make(map[geometry.Coord]string),
		versions: make(map[geometry.Coord]uint64),
	}
	generation, generationValid := v.verifyGeneration()
	v.verifyClock()
	if err := v.verifyRoot(); err != nil {
		return VerificationReport{}, err
	}
	v.verifyImageVersions()
	v.verifyIntent()
	if generationValid {
		v.verifyFinalGeneration(generation)
	}
	sort.Slice(v.report.Issues, func(i, j int) bool {
		left, right := v.report.Issues[i], v.report.Issues[j]
		if left.Path != right.Path {
			return left.Path < right.Path
		}
		if left.Code != right.Code {
			return left.Code < right.Code
		}
		return left.Detail < right.Detail
	})
	return v.report, nil
}

func (v *verifier) issue(code, path, detail string) {
	v.report.Issues = append(v.report.Issues, VerificationIssue{
		Code: code, Path: filepath.ToSlash(path), Detail: detail,
	})
}

func (v *verifier) relative(path string) string {
	relative, err := filepath.Rel(v.root, path)
	if err != nil {
		return path
	}
	return relative
}

func (v *verifier) verifyGeneration() (uint64, bool) {
	encoded, err := readFileBounded(filepath.Join(v.root, snapshotName), snapshotFileBytes)
	if errors.Is(err, os.ErrNotExist) {
		v.issue("generation_missing", snapshotName, "snapshot generation file is missing")
		return 0, false
	}
	if err != nil {
		v.issue("generation_unreadable", snapshotName, err.Error())
		return 0, false
	}
	generation, err := decodeSnapshotGeneration(encoded)
	if err != nil {
		v.issue("generation_corrupt", snapshotName, err.Error())
		return 0, false
	}
	if generation%2 != 0 {
		v.issue("generation_unstable", snapshotName, fmt.Sprintf("generation %d is odd", generation))
	}
	return generation, true
}

func (v *verifier) verifyFinalGeneration(expected uint64) {
	encoded, err := readFileBounded(filepath.Join(v.root, snapshotName), snapshotFileBytes)
	if err != nil {
		v.issue("generation_changed", snapshotName, err.Error())
		return
	}
	generation, err := decodeSnapshotGeneration(encoded)
	if err != nil {
		v.issue("generation_changed", snapshotName, err.Error())
		return
	}
	if generation != expected {
		v.issue(
			"generation_changed",
			snapshotName,
			fmt.Sprintf("generation changed from %d to %d during verification", expected, generation),
		)
	}
}

func (v *verifier) verifyClock() {
	path := filepath.Join(v.root, versionClockName)
	encoded, err := readFileBounded(path, versionClockBytes)
	if errors.Is(err, os.ErrNotExist) {
		v.issue("version_clock_missing", versionClockName, "version clock file is missing")
		return
	}
	if err != nil {
		v.issue("version_clock_unreadable", versionClockName, err.Error())
		return
	}
	if len(encoded) != versionClockBytes || string(encoded[:min(len(encoded), 8)]) != versionClockMagic {
		v.issue("version_clock_corrupt", versionClockName, "invalid version clock header")
		return
	}
	if crc32.ChecksumIEEE(encoded[:16]) != mustUint32(encoded[16:]) {
		v.issue("version_clock_corrupt", versionClockName, "version clock checksum mismatch")
		return
	}
	v.clock = mustUint64(encoded[8:16])
	v.clockValid = true
}

func (v *verifier) verifyRoot() error {
	entries, err := os.ReadDir(v.root)
	if err != nil {
		return fmt.Errorf("verify fs_split_v1: read data directory: %w", err)
	}
	for _, entry := range entries {
		path := filepath.Join(v.root, entry.Name())
		switch entry.Name() {
		case walName:
			v.walFound = true
			v.verifyWAL(path, entry)
		case versionClockName, snapshotName:
			if !entry.Type().IsRegular() {
				v.issue("misplaced_artifact", entry.Name(), "control artifact is not a regular file")
			}
		case intentDirectoryName:
			if !entry.IsDir() {
				v.issue("misplaced_artifact", entry.Name(), "intent artifact is not a directory")
			} else {
				v.intentOK = true
			}
		case lockName:
			if !entry.IsDir() && !entry.Type().IsRegular() {
				v.issue("misplaced_artifact", entry.Name(), "lock artifact has an unsupported type")
			}
		default:
			if strings.HasPrefix(entry.Name(), lockName+legacyLockMarker) &&
				entry.Type().IsRegular() {
				continue
			}
			largeCoord, ok := parseCoordinateName(entry.Name(), "l_", "")
			if !ok || entry.Name() != "l_"+signedName(largeCoord.X)+"_"+signedName(largeCoord.Y) || !entry.IsDir() {
				v.issue("misplaced_artifact", entry.Name(), "unexpected top-level artifact")
				continue
			}
			if err := v.verifyLargeDirectory(path, largeCoord); err != nil {
				return err
			}
		}
	}
	if !v.walFound {
		v.issue("wal_missing", walName, "WAL file is missing")
	}
	return nil
}

func (v *verifier) verifyLargeDirectory(path string, largeCoord geometry.Coord) error {
	entries, err := os.ReadDir(path)
	if err != nil {
		return fmt.Errorf("verify fs_split_v1: read %q: %w", v.relative(path), err)
	}
	for _, entry := range entries {
		entryPath := filepath.Join(path, entry.Name())
		relative := v.relative(entryPath)
		if !entry.Type().IsRegular() {
			v.issue("misplaced_artifact", relative, "chunk artifact is not a regular file")
			continue
		}
		switch {
		case strings.HasSuffix(entry.Name(), versionFileSuffix):
			imageName := strings.TrimSuffix(entry.Name(), versionFileSuffix)
			coord, ok := parseChunkFileName(imageName)
			if !ok {
				v.issue("misplaced_artifact", relative, "invalid chunk version filename")
				continue
			}
			if v.store.geometry.ChunkToLargeChunk(coord).LargeChunk != largeCoord {
				v.issue("misplaced_artifact", relative, "chunk version is in the wrong large-chunk directory")
				continue
			}
			v.verifyVersion(entryPath, relative, coord)
		case strings.HasSuffix(entry.Name(), ".rdb"):
			coord, ok := parseChunkFileName(entry.Name())
			if !ok {
				v.issue("misplaced_artifact", relative, "invalid chunk image filename")
				continue
			}
			if v.store.geometry.ChunkToLargeChunk(coord).LargeChunk != largeCoord {
				v.issue("misplaced_artifact", relative, "chunk image is in the wrong large-chunk directory")
				continue
			}
			v.verifyImage(entryPath, relative, coord)
		default:
			v.issue("misplaced_artifact", relative, "unexpected chunk-directory artifact")
		}
	}
	return nil
}

func (v *verifier) verifyImage(path, relative string, coord geometry.Coord) {
	stateBytes := int64(v.store.geometry.PayloadBytes()) + int64(v.store.geometry.PresenceBytes())
	if stateBytes > (math.MaxInt64-headerBytes-checksumSize)/2 {
		v.issue("image_corrupt", relative, "maximum image size overflows")
		return
	}
	encoded, err := readFileBounded(path, int64(headerBytes+checksumSize)+stateBytes*2)
	if err != nil {
		v.issue("image_unreadable", relative, err.Error())
		return
	}
	if _, err := v.store.decode(coord, encoded); err != nil {
		v.issue("image_corrupt", relative, err.Error())
		return
	}
	v.images[coord] = relative
	v.report.Images++
}

func (v *verifier) verifyVersion(path, relative string, coord geometry.Coord) {
	encoded, err := readFileBounded(path, versionFileBytes)
	if err != nil {
		v.issue("version_corrupt", relative, err.Error())
		return
	}
	if len(encoded) != versionFileBytes || string(encoded[:8]) != versionFileMagic {
		v.issue("version_corrupt", relative, "invalid chunk version header")
		return
	}
	if crc32.ChecksumIEEE(encoded[:len(encoded)-checksumSize]) !=
		mustUint32(encoded[len(encoded)-checksumSize:]) {
		v.issue("version_corrupt", relative, "chunk version checksum mismatch")
		return
	}
	if int64(mustUint64(encoded[8:16])) != coord.X ||
		int64(mustUint64(encoded[16:24])) != coord.Y {
		v.issue("version_corrupt", relative, "chunk version coordinate mismatch")
		return
	}
	version := mustUint64(encoded[24:32])
	v.versions[coord] = version
	v.report.Versions++
	if version == 0 {
		v.issue("version_invalid", relative, "persisted chunk version is zero")
	}
	if v.clockValid && version > v.clock {
		v.issue(
			"version_ahead_of_clock",
			relative,
			fmt.Sprintf("chunk version %d exceeds clock %d", version, v.clock),
		)
	}
}

func (v *verifier) verifyImageVersions() {
	for coord, path := range v.images {
		if _, ok := v.versions[coord]; !ok {
			v.issue("version_missing", path+versionFileSuffix, "chunk image has no version metadata")
		}
	}
	for coord := range v.versions {
		if _, ok := v.images[coord]; !ok {
			v.issue("image_missing", v.relative(v.store.chunkPath(coord)), "chunk version has no image")
		}
	}
}

func (v *verifier) verifyWAL(path string, entry os.DirEntry) {
	if !entry.Type().IsRegular() {
		v.issue("misplaced_artifact", walName, "WAL is not a regular file")
		return
	}
	wal, err := os.Open(path)
	if err != nil {
		v.issue("wal_unreadable", walName, err.Error())
		return
	}
	defer func() {
		if err := wal.Close(); err != nil {
			v.issue("wal_unreadable", walName, fmt.Sprintf("close WAL: %v", err))
		}
	}()
	info, err := wal.Stat()
	if err != nil {
		v.issue("wal_unreadable", walName, err.Error())
		return
	}
	if info.Size() < 0 {
		v.issue("wal_corrupt", walName, "WAL has a negative size")
		return
	}
	v.walBytes = uint64(info.Size())
	if info.Size() == 0 {
		return
	}
	if info.Size() < int64(len(walMagic)) {
		v.issue("wal_truncated", walName, fmt.Sprintf("WAL has %d trailing bytes", info.Size()))
		return
	}
	magic := make([]byte, len(walMagic))
	if _, err := io.ReadFull(wal, magic); err != nil {
		v.issue("wal_unreadable", walName, err.Error())
		return
	}
	recordBytes64 := int64(walHeaderBytes+checksumSize) + int64(v.store.geometry.PayloadBytes())
	switch string(magic) {
	case walMagic:
		recordBytes64 += int64(v.store.geometry.PresenceBytes())
	case legacyWALMagic:
	default:
		v.issue("wal_corrupt", walName, "invalid record magic")
		return
	}
	if recordBytes64 > int64(int(^uint(0)>>1)) {
		v.issue("wal_corrupt", walName, "WAL record size exceeds platform limits")
		return
	}
	recordBytes := int(recordBytes64)
	v.walRecord = uint64(recordBytes)
	if _, err := wal.Seek(0, io.SeekStart); err != nil {
		v.issue("wal_unreadable", walName, err.Error())
		return
	}
	completeBytes := info.Size() / int64(recordBytes) * int64(recordBytes)
	encoded := make([]byte, recordBytes)
	for offset := int64(0); offset < completeBytes; offset += int64(recordBytes) {
		if _, err := io.ReadFull(wal, encoded); err != nil {
			v.issue("wal_unreadable", walName, fmt.Sprintf("record at byte %d: %v", offset, err))
			return
		}
		if _, err := v.store.decodeWALRecord(encoded); err != nil {
			v.issue("wal_corrupt", walName, fmt.Sprintf("record at byte %d: %v", offset, err))
			return
		}
		v.report.WAL++
	}
	if completeBytes != info.Size() {
		v.issue(
			"wal_truncated",
			walName,
			fmt.Sprintf("WAL has %d trailing bytes after complete records", info.Size()-completeBytes),
		)
	}
}

func (v *verifier) verifyIntent() {
	directory := filepath.Join(v.root, intentDirectoryName)
	if !v.intentOK {
		return
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		v.issue("intent_unreadable", intentDirectoryName, err.Error())
		return
	}
	for _, entry := range entries {
		relative := filepath.Join(intentDirectoryName, entry.Name())
		if entry.Name() != intentFileName || !entry.Type().IsRegular() {
			v.issue("misplaced_artifact", relative, "unexpected intent artifact")
		}
	}
	if len(entries) != 1 || entries[0].Name() != intentFileName || !entries[0].Type().IsRegular() {
		return
	}
	path := filepath.Join(directory, intentFileName)
	encoded, err := readFileBounded(path, intentRecordBytes)
	if err != nil {
		v.issue("intent_unreadable", v.relative(path), err.Error())
		return
	}
	state, boundary, err := decodeIntent(encoded)
	if err != nil {
		v.issue("intent_corrupt", v.relative(path), err.Error())
		return
	}
	if boundary > v.walBytes {
		v.issue(
			"intent_boundary_invalid",
			v.relative(path),
			fmt.Sprintf("WAL boundary %d exceeds WAL size %d", boundary, v.walBytes),
		)
	}
	if v.walRecord != 0 && boundary%v.walRecord != 0 {
		v.issue(
			"intent_boundary_invalid",
			v.relative(path),
			fmt.Sprintf("WAL boundary %d is not aligned to record size %d", boundary, v.walRecord),
		)
	}
	stateName := "rollback"
	if state == intentCommitted {
		stateName = "committed"
	}
	v.issue(
		"intent_pending",
		v.relative(path),
		fmt.Sprintf("%s intent remains at WAL boundary %d", stateName, boundary),
	)
}

func readFileBounded(path string, maximum int64) (_ []byte, returnErr error) {
	pathInfo, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if pathInfo.Mode()&os.ModeSymlink != 0 || !pathInfo.Mode().IsRegular() {
		return nil, errors.New("artifact is not a regular file")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() {
		returnErr = errors.Join(returnErr, file.Close())
	}()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("artifact is not a regular file")
	}
	if info.Size() < 0 || info.Size() > maximum {
		return nil, fmt.Errorf("artifact size %d exceeds maximum %d", info.Size(), maximum)
	}
	if info.Size() > int64(int(^uint(0)>>1)) {
		return nil, fmt.Errorf("artifact size %d exceeds platform limits", info.Size())
	}
	encoded := make([]byte, int(info.Size()))
	if _, err := io.ReadFull(file, encoded); err != nil {
		return nil, err
	}
	return encoded, nil
}
