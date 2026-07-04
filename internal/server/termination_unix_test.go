//go:build !windows

package server

import (
	"syscall"
)

func platformPeerCloseTestError() error {
	return syscall.ECONNRESET
}
