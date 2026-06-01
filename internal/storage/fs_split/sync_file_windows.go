//go:build windows

package fs_split

import (
	"errors"

	"golang.org/x/sys/windows"
)

func isUnsupportedFileSyncError(err error) bool {
	return errors.Is(err, windows.ERROR_INVALID_FUNCTION) ||
		errors.Is(err, windows.ERROR_NOT_SUPPORTED)
}
