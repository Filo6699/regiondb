//go:build unix

package server

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

const highDescriptorTarget = 1100

func TestIntegrationHighFileDescriptorBusyResponse(t *testing.T) {
	engine := newTestEngine(t)

	var original unix.Rlimit
	if err := unix.Getrlimit(unix.RLIMIT_NOFILE, &original); err != nil {
		t.Skipf("read descriptor limit: %v", err)
	}
	if original.Max <= highDescriptorTarget {
		t.Skipf("hard descriptor limit %d cannot reach %d", original.Max, highDescriptorTarget)
	}
	raised := original
	if raised.Cur <= highDescriptorTarget {
		raised.Cur = highDescriptorTarget + 64
		if raised.Cur > raised.Max {
			raised.Cur = raised.Max
		}
		if err := unix.Setrlimit(unix.RLIMIT_NOFILE, &raised); err != nil {
			t.Skipf("raise descriptor limit: %v", err)
		}
		t.Cleanup(func() {
			if err := unix.Setrlimit(unix.RLIMIT_NOFILE, &original); err != nil {
				t.Errorf("restore descriptor limit: %v", err)
			}
		})
	}

	var fillers []*os.File
	for len(fillers) <= highDescriptorTarget {
		file, err := os.Open(os.DevNull)
		if err != nil {
			t.Skipf("open descriptor filler: %v", err)
		}
		fillers = append(fillers, file)
		if file.Fd() > highDescriptorTarget {
			break
		}
	}
	t.Cleanup(func() {
		for _, file := range fillers {
			if err := file.Close(); err != nil {
				t.Errorf("close descriptor filler: %v", err)
			}
		}
	})

	tcpListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	listenerDescriptor, err := socketDescriptor(tcpListener)
	if err != nil {
		_ = tcpListener.Close()
		t.Fatal(err)
	}
	if listenerDescriptor <= highDescriptorTarget {
		_ = tcpListener.Close()
		t.Fatalf(
			"listener descriptor = %d, want above %d",
			listenerDescriptor,
			highDescriptorTarget,
		)
	}
	listener := &integrationObservedListener{
		Listener:    tcpListener,
		accepted:    make(chan struct{}, 2),
		readStarted: make(chan struct{}, 2),
	}
	ctx, cancel := context.WithCancel(context.Background())
	serveResult := make(chan error, 1)
	go func() {
		serveResult <- ServeWithOptions(ctx, listener, engine, Options{
			Workers:      1,
			AcceptQueue:  0,
			MaxLineBytes: DefaultMaxLineBytes,
		})
	}()
	t.Cleanup(func() {
		cancel()
		if err := <-serveResult; err != nil {
			t.Errorf("ServeWithOptions() error = %v", err)
		}
	})

	occupied := dialIntegrationServer(t, listener.Addr())
	t.Cleanup(func() { _ = occupied.Close() })
	waitForIntegrationSignal(t, listener.accepted, "high-fd worker accept")
	waitForIntegrationSignal(t, listener.readStarted, "high-fd worker read")

	rejected := dialIntegrationServer(t, listener.Addr())
	t.Cleanup(func() { _ = rejected.Close() })
	waitForIntegrationSignal(t, listener.accepted, "high-fd overload accept")
	if err := rejected.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	response, err := bufio.NewReader(rejected).ReadString('\n')
	if err != nil {
		t.Fatalf("read high-fd overload response: %v", err)
	}
	if want := "-ERR BUSY server overloaded\r\n"; response != want {
		t.Fatalf("high-fd overload response = %q, want %q", response, want)
	}
}

func socketDescriptor(listener net.Listener) (int, error) {
	syscallListener, ok := listener.(syscall.Conn)
	if !ok {
		return 0, fmt.Errorf("listener %T does not expose a raw connection", listener)
	}
	raw, err := syscallListener.SyscallConn()
	if err != nil {
		return 0, fmt.Errorf("get listener raw connection: %w", err)
	}
	descriptor := -1
	if err := raw.Control(func(fd uintptr) {
		descriptor = int(fd)
	}); err != nil {
		return 0, fmt.Errorf("inspect listener descriptor: %w", err)
	}
	if descriptor < 0 {
		return 0, fmt.Errorf("listener descriptor was not reported")
	}
	return descriptor, nil
}
