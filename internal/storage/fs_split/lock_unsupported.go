//go:build !darwin && !dragonfly && !freebsd && !illumos && !linux && !netbsd && !openbsd && !windows

package fs_split

import (
	"errors"
	"os"
)

func openWriterGuard(string) (*os.File, error) {
	return nil, errors.New("exclusive data-directory locking is unsupported on this platform")
}

func closeWriterGuard(*os.File) error {
	return nil
}
