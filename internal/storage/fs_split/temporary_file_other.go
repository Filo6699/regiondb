//go:build !unix && !windows

package fs_split

import (
	"fmt"
	"os"
)

func openExclusiveTemporaryFile(path string) (*os.File, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, fmt.Errorf("create exclusive temporary file: %w", err)
	}
	return file, nil
}
