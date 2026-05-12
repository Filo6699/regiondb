package fs_split

import (
	"fmt"
	"io"
	"os"
)

func openWALHandle(path string) (*os.File, error) {
	// Go removes FILE_WRITE_DATA from O_APPEND handles on Windows. Checkpoint
	// truncation requires that right, so retain a read/write handle and position
	// it explicitly before each append.
	return os.OpenFile(path, os.O_RDWR, 0o600)
}

func appendWALHandle(wal *os.File, record []byte) error {
	if _, err := wal.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("seek WAL end: %w", err)
	}
	if _, err := wal.Write(record); err != nil {
		return fmt.Errorf("append WAL: %w", err)
	}
	return nil
}
