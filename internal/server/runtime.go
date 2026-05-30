package server

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"runtime"
	"sync"
	"time"

	"github.com/Filo6699/regiondb/internal/logging"
	"github.com/Filo6699/regiondb/internal/protocol"
)

const (
	DefaultAddress         = "127.0.0.1:4242"
	DefaultAcceptQueue     = 128
	DefaultMaxLineBytes    = 1 << 20
	DefaultIdleTimeout     = 30 * time.Second
	DefaultRequestTimeout  = 10 * time.Second
	DefaultResponseTimeout = 10 * time.Second
	overloadWriteTimeout   = 100 * time.Millisecond
)

type Options struct {
	Workers         int
	AcceptQueue     int
	MaxLineBytes    int
	IdleTimeout     time.Duration
	RequestTimeout  time.Duration
	ResponseTimeout time.Duration
	Logger          *logging.Logger
}

type terminationReason string

const (
	terminationPeerClose      terminationReason = "peer_close"
	terminationProtocolClose  terminationReason = "protocol_close"
	terminationServerShutdown terminationReason = "server_shutdown"
	terminationSocketError    terminationReason = "socket_error"
	terminationTLSError       terminationReason = "tls_error"
	terminationTimeout        terminationReason = "timeout"
)

type connectionTermination struct {
	phase     string
	reason    terminationReason
	err       error
	shouldLog bool
}

func DefaultOptions() Options {
	return Options{
		Workers:         runtime.GOMAXPROCS(0),
		AcceptQueue:     DefaultAcceptQueue,
		MaxLineBytes:    DefaultMaxLineBytes,
		IdleTimeout:     DefaultIdleTimeout,
		RequestTimeout:  DefaultRequestTimeout,
		ResponseTimeout: DefaultResponseTimeout,
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
	if options.IdleTimeout < 0 {
		return errors.New("serve: idle timeout must not be negative")
	}
	if options.IdleTimeout == 0 {
		options.IdleTimeout = DefaultIdleTimeout
	}
	if options.RequestTimeout < 0 {
		return errors.New("serve: request timeout must not be negative")
	}
	if options.RequestTimeout == 0 {
		options.RequestTimeout = DefaultRequestTimeout
	}
	if options.ResponseTimeout < 0 {
		return errors.New("serve: response timeout must not be negative")
	}
	if options.ResponseTimeout == 0 {
		options.ResponseTimeout = DefaultResponseTimeout
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

	capacity := options.Workers + options.AcceptQueue
	queue := make(chan net.Conn, capacity)
	slots := make(chan struct{}, capacity)
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
					termination := serveConnection(
						serveCtx,
						connection,
						engine,
						options.MaxLineBytes,
						options.IdleTimeout,
						options.RequestTimeout,
						options.ResponseTimeout,
					)
					logConnectionTermination(options.Logger, termination)
					connections.Delete(connection)
					_ = connection.Close()
					<-slots
				}
			}
		}()
	}

	for {
		connection, err := listener.Accept()
		if err != nil {
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
			waitForWorkers()
			return nil
		}
		select {
		case slots <- struct{}{}:
			select {
			case queue <- connection:
			case <-serveCtx.Done():
				_ = connection.Close()
				connections.Delete(connection)
				<-slots
				waitForWorkers()
				return nil
			}
		case <-serveCtx.Done():
			_ = connection.Close()
			connections.Delete(connection)
			waitForWorkers()
			return nil
		default:
			if err := rejectOverloadedConnection(connection); err != nil {
				logConnectionTermination(
					options.Logger,
					classifyConnectionTermination(
						serveCtx,
						connection,
						"busy_response",
						err,
						true,
					),
				)
			}
			_ = connection.Close()
			connections.Delete(connection)
		}
	}
}

func rejectOverloadedConnection(connection net.Conn) error {
	if err := connection.SetWriteDeadline(time.Now().Add(overloadWriteTimeout)); err != nil {
		return fmt.Errorf("set overload response deadline: %w", err)
	}
	if err := writeAll(connection, []byte("-ERR BUSY server overloaded\r\n")); err != nil {
		return fmt.Errorf("write overload response: %w", err)
	}
	return nil
}

func serveConnection(
	ctx context.Context,
	connection net.Conn,
	engine *protocol.Engine,
	maxLineBytes int,
	idleTimeout time.Duration,
	requestTimeout time.Duration,
	responseTimeout time.Duration,
) connectionTermination {
	if err := setNoDelay(connection); err != nil {
		return classifyConnectionTermination(ctx, connection, "setup", err, true)
	}
	reader := newLineBuffer(maxLineBytes)
	writer := bufio.NewWriter(connection)
	session := engine.NewSession()
	for {
		frame, tooLong, err := reader.readFrameWithin(
			connection,
			idleTimeout,
			requestTimeout,
		)
		if tooLong {
			if writeErr := writeResponseWithin(
				connection,
				writer,
				[]byte("-ERR FRAME command exceeds max_line_bytes\r\n"),
				responseTimeout,
			); writeErr != nil {
				return classifyConnectionTermination(ctx, connection, "write", writeErr, true)
			}
			if err != nil {
				return classifyConnectionTermination(ctx, connection, "read", err, false)
			}
			continue
		}
		if err != nil {
			if !errors.Is(err, io.EOF) {
				return classifyConnectionTermination(ctx, connection, "read", err, true)
			}
			if len(frame) == 0 {
				return classifyConnectionTermination(ctx, connection, "read", err, false)
			}
		}

		if err := writeResponseWithin(
			connection,
			writer,
			session.Handle(frame).Bytes(),
			responseTimeout,
		); err != nil {
			return classifyConnectionTermination(ctx, connection, "write", err, true)
		}
		if session.Closed() {
			return connectionTermination{
				phase:  "protocol",
				reason: terminationProtocolClose,
			}
		}
		if errors.Is(err, io.EOF) {
			return classifyConnectionTermination(ctx, connection, "read", err, false)
		}
	}
}

func classifyConnectionTermination(
	ctx context.Context,
	connection net.Conn,
	phase string,
	err error,
	logPeerClose bool,
) connectionTermination {
	termination := connectionTermination{
		phase:     phase,
		err:       err,
		shouldLog: true,
	}
	if ctx != nil && ctx.Err() != nil {
		termination.reason = terminationServerShutdown
		termination.shouldLog = false
		return termination
	}
	var networkError net.Error
	if errors.As(err, &networkError) && networkError.Timeout() {
		termination.reason = terminationTimeout
		return termination
	}
	if errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrClosedPipe) ||
		errors.Is(err, net.ErrClosed) ||
		isPeerCloseError(err) {
		termination.reason = terminationPeerClose
		termination.shouldLog = logPeerClose
		return termination
	}
	if isTLSConnection(connection) {
		termination.reason = terminationTLSError
		return termination
	}
	termination.reason = terminationSocketError
	return termination
}

func isTLSConnection(connection net.Conn) bool {
	for {
		if _, ok := connection.(*tls.Conn); ok {
			return true
		}
		wrapped, ok := connection.(interface {
			NetConn() net.Conn
		})
		if !ok {
			return false
		}
		connection = wrapped.NetConn()
	}
}

func logConnectionTermination(logger *logging.Logger, termination connectionTermination) {
	if logger == nil || !termination.shouldLog {
		return
	}
	attributes := []slog.Attr{
		slog.String("phase", termination.phase),
		slog.String("reason", string(termination.reason)),
	}
	if termination.err != nil {
		attributes = append(attributes, slog.String("error", termination.err.Error()))
	}
	logger.Warn("server", "connection_terminated", attributes...)
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

func (b *lineBuffer) readFrameWithin(
	connection net.Conn,
	idleTimeout time.Duration,
	requestTimeout time.Duration,
) ([]byte, bool, error) {
	if b.start != b.end || b.pendingErr != nil {
		if err := connection.SetReadDeadline(time.Now().Add(requestTimeout)); err != nil {
			return nil, false, err
		}
		return b.readFrame(connection)
	}
	if err := connection.SetReadDeadline(time.Now().Add(idleTimeout)); err != nil {
		return nil, false, err
	}

	read, err := connection.Read(b.data)
	b.end = read
	if read != 0 {
		if deadlineErr := connection.SetReadDeadline(
			time.Now().Add(requestTimeout),
		); deadlineErr != nil {
			return nil, false, deadlineErr
		}
	}
	if err != nil {
		b.pendingErr = err
	}
	if read == 0 && err == nil {
		return nil, false, io.ErrNoProgress
	}
	return b.readFrame(connection)
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

func writeResponseWithin(
	connection net.Conn,
	writer *bufio.Writer,
	data []byte,
	responseTimeout time.Duration,
) error {
	if err := connection.SetWriteDeadline(time.Now().Add(responseTimeout)); err != nil {
		return err
	}
	return writeResponse(writer, data)
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
