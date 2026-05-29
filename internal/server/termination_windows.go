package server

import (
	"errors"
	"syscall"
)

func isPeerCloseError(err error) bool {
	return errors.Is(err, syscall.WSAECONNRESET) ||
		errors.Is(err, syscall.WSAECONNABORTED)
}
