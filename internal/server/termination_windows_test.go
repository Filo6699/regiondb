//go:build windows

package server

import (
	"context"
	"fmt"
	"net"
	"syscall"
	"testing"
)

func TestWindowsConnectionAbortIsClassifiedAsPeerClose(t *testing.T) {
	t.Parallel()

	serverConnection, clientConnection := net.Pipe()
	t.Cleanup(func() {
		_ = serverConnection.Close()
		_ = clientConnection.Close()
	})

	for _, socketError := range []error{
		syscall.WSAECONNRESET,
		syscall.WSAECONNABORTED,
		fmt.Errorf("wrapped socket error: %w", syscall.WSAECONNABORTED),
	} {
		termination := classifyConnectionTermination(
			context.Background(),
			serverConnection,
			"read",
			socketError,
			true,
		)
		if termination.reason != terminationPeerClose || !termination.shouldLog {
			t.Fatalf(
				"classifyConnectionTermination(%v) = %+v, want logged peer close",
				socketError,
				termination,
			)
		}
	}
}

func platformPeerCloseTestError() error {
	return syscall.WSAECONNRESET
}
