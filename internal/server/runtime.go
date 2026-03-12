package server

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
	"sync"

	"github.com/Filo6699/regiondb/internal/protocol"
)

const (
	DefaultAddress      = "127.0.0.1:4242"
	DefaultAcceptQueue  = 128
	DefaultMaxLineBytes = 1 << 20
)

type Options struct {
	Workers      int
	AcceptQueue  int
	MaxLineBytes int
}

func DefaultOptions() Options {
	return Options{
		Workers:      runtime.GOMAXPROCS(0),
		AcceptQueue:  DefaultAcceptQueue,
		MaxLineBytes: DefaultMaxLineBytes,
	}
}

func Serve(ctx context.Context, listener net.Listener, engine *protocol.Engine) error {
	return ServeWithOptions(ctx, listener, engine, DefaultOptions())
}

func ServeWithOptions(
	ctx context.Context,
	listener net.Listener,
	engine *protocol.Engine,
	options Options,
) error {
	if ctx == nil {
		return errors.New("serve: context must not be nil")
	}
	if listener == nil {
		return errors.New("serve: listener must not be nil")
	}
	if engine == nil {
		return errors.New("serve: protocol engine must not be nil")
	}
	if options.Workers <= 0 {
		return errors.New("serve: worker count must be positive")
	}
	if options.AcceptQueue < 0 {
		return errors.New("serve: accept queue size must not be negative")
	}
	if options.MaxLineBytes <= 0 {
		return errors.New("serve: maximum line size must be positive")
	}
	maxInt := int(^uint(0) >> 1)
	if options.AcceptQueue > maxInt-options.Workers {
		return errors.New("serve: worker and accept queue sizes are too large")
	}
	if options.MaxLineBytes == maxInt {
		return errors.New("serve: maximum line size is too large")
	}

	serveCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var connections sync.Map
	var shutdownOnce sync.Once
	var workers sync.WaitGroup
	shutdown := func() {
		shutdownOnce.Do(func() {
			_ = listener.Close()
			connections.Range(func(key, _ any) bool {
				_ = key.(net.Conn).Close()
				return true
			})
		})
	}
	stop := context.AfterFunc(serveCtx, shutdown)
	defer stop()
	waitForWorkers := func() {
		shutdown()
		workers.Wait()
	}

	queue := make(chan net.Conn, options.AcceptQueue)
	slots := make(chan struct{}, options.Workers+options.AcceptQueue)
	workers.Add(options.Workers)
	for range options.Workers {
		go func() {
			defer workers.Done()
			for {
				select {
				case <-serveCtx.Done():
					return
				case connection := <-queue:
					if serveCtx.Err() != nil {
						connections.Delete(connection)
						_ = connection.Close()
						<-slots
						return
					}
					serveConnection(connection, engine, options.MaxLineBytes)
					connections.Delete(connection)
					_ = connection.Close()
					<-slots
				}
			}
		}()
	}

	for {
		select {
		case slots <- struct{}{}:
		case <-serveCtx.Done():
			waitForWorkers()
			return nil
		}

		connection, err := listener.Accept()
		if err != nil {
			<-slots
			if serveCtx.Err() != nil {
				waitForWorkers()
				return nil
			}
			cancel()
			waitForWorkers()
			return fmt.Errorf("accept connection: %w", err)
		}

		connections.Store(connection, struct{}{})
		if serveCtx.Err() != nil {
			_ = connection.Close()
			connections.Delete(connection)
			<-slots
			waitForWorkers()
			return nil
		}
		select {
		case queue <- connection:
		case <-serveCtx.Done():
			_ = connection.Close()
			connections.Delete(connection)
			<-slots
			waitForWorkers()
			return nil
		}
	}
}

func serveConnection(connection net.Conn, engine *protocol.Engine, maxLineBytes int) {
	reader := bufio.NewReaderSize(connection, maxLineBytes+1)
	writer := bufio.NewWriter(connection)
	session := engine.NewSession()
	for {
		frame, tooLong, err := readFrame(reader, maxLineBytes)
		if tooLong {
			if writeErr := writeResponse(writer, []byte("-ERR FRAME command exceeds max_line_bytes\r\n")); writeErr != nil {
				return
			}
			if err != nil {
				return
			}
			continue
		}
		if err != nil {
			if !errors.Is(err, io.EOF) {
				return
			}
			if len(frame) == 0 {
				return
			}
		}

		if err := writeResponse(writer, session.Handle(frame).Bytes()); err != nil {
			return
		}
		if session.Closed() || errors.Is(err, io.EOF) {
			return
		}
	}
}

func readFrame(reader *bufio.Reader, maxLineBytes int) ([]byte, bool, error) {
	var frame []byte
	for {
		fragment, err := reader.ReadSlice('\n')
		if len(frame) > maxLineBytes-len(fragment) {
			for errors.Is(err, bufio.ErrBufferFull) {
				_, err = reader.ReadSlice('\n')
			}
			return nil, true, err
		}
		frame = append(frame, fragment...)
		if !errors.Is(err, bufio.ErrBufferFull) {
			return frame, false, err
		}
	}
}

func writeResponse(writer *bufio.Writer, data []byte) error {
	if err := writeAll(writer, data); err != nil {
		return err
	}
	return writer.Flush()
}

func writeAll(writer io.Writer, data []byte) error {
	for len(data) != 0 {
		written, err := writer.Write(data)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		data = data[written:]
	}
	return nil
}
