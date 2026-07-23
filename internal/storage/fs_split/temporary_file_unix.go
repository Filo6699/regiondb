//go:build unix

package fs_split

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

func openExclusiveTemporaryFile(path string) (*os.File, error) {
	descriptor, err := unix.Open(
		path,
		unix.O_RDWR|unix.O_CREAT|unix.O_EXCL|unix.O_CLOEXEC|unix.O_NOFOLLOW,
		0o600,
	)
	if err != nil {
		return nil, fmt.Errorf("create exclusive no-follow temporary file: %w", err)
	}
	return os.NewFile(uintptr(descriptor), path), nil
}
