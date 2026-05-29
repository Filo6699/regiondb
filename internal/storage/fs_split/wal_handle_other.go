//go:build !windows

package fs_split

import "os"

func openWALHandle(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0o600)
}

func appendWALHandle(wal *os.File, record []byte) error {
	return writeWALRecord(wal, record)
}
