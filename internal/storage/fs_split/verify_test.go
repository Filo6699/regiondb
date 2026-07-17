package fs_split

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/Filo6699/regiondb/internal/geometry"
	"github.com/Filo6699/regiondb/internal/storage"
)

func TestVerifyAcceptsConsistentStoreWithoutWriting(t *testing.T) {
	root, g, _ := createVerifierStore(t)
	before := snapshotVerifierFiles(t, root)

	report, err := Verify(root, g)
	if err != nil {
		t.Fatalf("Verify() error = %v", err)
	}
	if len(report.Issues) != 0 {
		t.Fatalf("Verify() issues = %+v, want none", report.Issues)
	}
	if report.Images != 1 || report.Versions != 1 || report.WAL != 1 {
		t.Fatalf("Verify() counts = %+v, want one image, one version, one WAL record", report)
	}
	after := snapshotVerifierFiles(t, root)
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("Verify() changed data directory\nbefore: %q\nafter:  %q", before, after)
	}
}

func TestVerifyReportsEachIntegrityClass(t *testing.T) {
	root, g, coord := createVerifierStore(t)
	imagePath := (&Store{root: root, geometry: g}).chunkPath(coord)

	image, err := os.ReadFile(imagePath)
	if err != nil {
		t.Fatal(err)
	}
	image[len(image)-1] ^= 0xff
	if err := os.WriteFile(imagePath, image, 0o644); err != nil {
		t.Fatal(err)
	}
	versionPath := imagePath + versionFileSuffix
	version, err := os.ReadFile(versionPath)
	if err != nil {
		t.Fatal(err)
	}
	version[len(version)-1] ^= 0xff
	if err := os.WriteFile(versionPath, version, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, walName), []byte("NOT_A_WAL"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(root, snapshotName),
		encodeSnapshotGeneration(3),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	intentDirectory := filepath.Join(root, intentDirectoryName)
	if err := os.MkdirAll(intentDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(intentDirectory, intentFileName),
		encodeIntent(intentRollback, 100),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "unexpected artifact"), nil, 0o600); err != nil {
		t.Fatal(err)
	}

	report, err := Verify(root, g)
	if err != nil {
		t.Fatalf("Verify() error = %v", err)
	}
	for _, code := range []string{
		"generation_unstable",
		"image_corrupt",
		"intent_boundary_invalid",
		"intent_pending",
		"misplaced_artifact",
		"version_corrupt",
		"wal_corrupt",
	} {
		if !hasVerificationCode(report, code) {
			t.Errorf("Verify() codes do not contain %q: %+v", code, report.Issues)
		}
	}
}

func TestVerifyReportsMisplacedChunkArtifacts(t *testing.T) {
	root, g, coord := createVerifierStore(t)
	store := &Store{root: root, geometry: g}
	imagePath := store.chunkPath(coord)
	versionPath := imagePath + versionFileSuffix
	wrongDirectory := filepath.Join(root, "l_p9_p9")
	if err := os.Mkdir(wrongDirectory, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, source := range []string{imagePath, versionPath} {
		if err := os.Rename(source, filepath.Join(wrongDirectory, filepath.Base(source))); err != nil {
			t.Fatal(err)
		}
	}

	report, err := Verify(root, g)
	if err != nil {
		t.Fatalf("Verify() error = %v", err)
	}
	if countVerificationCode(report, "misplaced_artifact") != 2 {
		t.Fatalf("Verify() issues = %+v, want two misplaced artifacts", report.Issues)
	}
}

func createVerifierStore(t *testing.T) (string, geometry.Geometry, geometry.Coord) {
	t.Helper()
	g, err := geometry.New(geometry.Config{ChunkEdge: 2, LargeChunkEdge: 2, BlockBits: 4})
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	store, err := Open(root, g)
	if err != nil {
		t.Fatal(err)
	}
	coord := geometry.Coord{X: -3, Y: 4}
	chunk, err := storage.NewChunk(g)
	if err != nil {
		t.Fatal(err)
	}
	if err := chunk.Set(geometry.Offset{X: 1, Y: 1}, 7); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteChunk(coord, chunk); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	return root, g, coord
}

func snapshotVerifierFiles(t *testing.T, root string) []string {
	t.Helper()
	var result []string
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		result = append(result, relative+":"+entry.Type().String())
		if entry.Type().IsRegular() {
			encoded, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			result = append(result, string(encoded))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func hasVerificationCode(report VerificationReport, code string) bool {
	return countVerificationCode(report, code) != 0
}

func countVerificationCode(report VerificationReport, code string) int {
	count := 0
	for _, issue := range report.Issues {
		if issue.Code == code {
			count++
		}
	}
	return count
}
