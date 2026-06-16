//go:build !windows

package main

import (
	"context"
	"os"
	"syscall"
	"testing"
	"time"
)

func TestNotifyContextCancelsFromSignalWatcher(t *testing.T) {
	ctx, stop := notifyContext(context.Background(), syscall.SIGUSR1)
	defer stop()

	process, err := os.FindProcess(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	if err := process.Signal(syscall.SIGUSR1); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("signal watcher did not cancel the context")
	}
}
