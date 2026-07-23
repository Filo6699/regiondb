//go:build unix

package fs_split

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestTemporaryFileCreationDoesNotFollowSymlink(t *testing.T) {
	t.Parallel()

	root := testTempDir(t)
	target := filepath.Join(root, "target")
	if err := os.WriteFile(target, []byte("target"), 0o600); err != nil {
		t.Fatal(err)
	}
	name := chunkTemporaryPrefix + "00000000000000000000000000000000"
	if err := os.Symlink(target, filepath.Join(root, name)); err != nil {
		t.Fatal(err)
	}
	_, err := createTemporaryFileWithRandom(
		root,
		chunkTemporaryPrefix,
		bytes.NewReader(make([]byte, temporaryRandomBytes)),
	)
	if err == nil {
		t.Fatal("temporary creation followed or replaced a pre-existing symlink")
	}
	contents, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	if string(contents) != "target" {
		t.Fatalf("symlink target = %q, want unchanged content", contents)
	}
}
