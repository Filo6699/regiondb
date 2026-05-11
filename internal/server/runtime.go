package server

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"runtime"
	"sync"

	"github.com/Filo6699/regiondb/internal/logging"
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
	Logger       *logging.Logger
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
	if options.Logger != nil {
		options.Logger.Info("server", "serve_started",
			slog.Int("workers", options.Workers),
			slog.Int("accept_queue", options.AcceptQueue),
		)
		defer options.Logger.Info("server", "serve_stopped")
	}
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
			if options.Logger != nil {
				options.Logger.Error("server", "accept_failed")
			}
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
	if err := setNoDelay(connection); err != nil {
		return
	}
	reader := newLineBuffer(maxLineBytes)
	writer := bufio.NewWriter(connection)
	session := engine.NewSession()
	for {
		frame, tooLong, err := reader.readFrame(connection)
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

func setNoDelay(connection net.Conn) error {
	for {
		if tcpConnection, ok := connection.(*net.TCPConn); ok {
			return tcpConnection.SetNoDelay(true)
		}
		wrapped, ok := connection.(interface {
			NetConn() net.Conn
		})
		if !ok {
			return nil
		}
		connection = wrapped.NetConn()
	}
}

type lineBuffer struct {
	data       []byte
	start      int
	end        int
	pendingErr error
}

func newLineBuffer(maxLineBytes int) *lineBuffer {
	return &lineBuffer{
		data: make([]byte, maxLineBytes+1),
	}
}

func (b *lineBuffer) readFrame(reader io.Reader) ([]byte, bool, error) {
	for {
		buffered := b.data[b.start:b.end]
		if newline := bytes.IndexByte(buffered, '\n'); newline >= 0 {
			frameEnd := b.start + newline + 1
			frame := b.data[b.start:frameEnd]
			tooLong := len(frame) > b.maxLineBytes()
			b.consume(frameEnd)
			if tooLong {
				return nil, true, nil
			}
			return frame, false, nil
		}
		if b.pendingErr != nil {
			err := b.pendingErr
			b.pendingErr = nil
			frame := buffered
			tooLong := len(frame) > b.maxLineBytes()
			b.consume(b.end)
			return frame, tooLong, err
		}
		if len(buffered) > b.maxLineBytes() {
			return b.discardOversized(reader)
		}

		b.compact()
		read, err := reader.Read(b.data[b.end:])
		b.end += read
		if err != nil {
			b.pendingErr = err
		}
		if read == 0 && err == nil {
			return nil, false, io.ErrNoProgress
		}
	}
}

func (b *lineBuffer) discardOversized(reader io.Reader) ([]byte, bool, error) {
	b.start = 0
	b.end = 0
	for {
		read, err := reader.Read(b.data)
		if newline := bytes.IndexByte(b.data[:read], '\n'); newline >= 0 {
			b.start = newline + 1
			b.end = read
			b.pendingErr = err
			return nil, true, nil
		}
		if err != nil {
			return nil, true, err
		}
		if read == 0 {
			return nil, true, io.ErrNoProgress
		}
	}
}

func (b *lineBuffer) compact() {
	if b.start == 0 {
		return
	}
	copy(b.data, b.data[b.start:b.end])
	b.end -= b.start
	b.start = 0
}

func (b *lineBuffer) consume(end int) {
	b.start = end
	if b.start == b.end {
		b.start = 0
		b.end = 0
	}
}

func (b *lineBuffer) maxLineBytes() int {
	return len(b.data) - 1
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
