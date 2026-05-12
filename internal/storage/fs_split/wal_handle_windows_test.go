package fs_split

import (
	"io"
	"testing"
)

func TestWALAppendHandleSupportsCheckpointTruncate(t *testing.T) {
	t.Parallel()

	wal, err := openWAL(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := wal.Close(); err != nil {
			t.Errorf("close WAL: %v", err)
		}
	})

	if err := appendWALHandle(wal, []byte("before")); err != nil {
		t.Fatalf("first append: %v", err)
	}
	if err := wal.Truncate(0); err != nil {
		t.Fatalf("checkpoint truncate: %v", err)
	}
	if _, err := wal.Seek(0, io.SeekStart); err != nil {
		t.Fatalf("rewind after checkpoint: %v", err)
	}
	if err := appendWALHandle(wal, []byte("after")); err != nil {
		t.Fatalf("append after checkpoint: %v", err)
	}
	if _, err := wal.Seek(0, io.SeekStart); err != nil {
		t.Fatalf("rewind for read: %v", err)
	}
	got, err := io.ReadAll(wal)
	if err != nil {
		t.Fatalf("read WAL: %v", err)
	}
	if string(got) != "after" {
		t.Fatalf("WAL contents = %q, want %q", got, "after")
	}
}
