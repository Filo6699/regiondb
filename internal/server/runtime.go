package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/Filo6699/regiondb/internal/defaults"
	"github.com/Filo6699/regiondb/internal/logging"
	"github.com/Filo6699/regiondb/internal/protocol"
)

const (
	DefaultAddress         = defaults.Address
	DefaultAcceptQueue     = defaults.AcceptQueue
	DefaultMaxLineBytes    = defaults.MaxLineBytes
	DefaultIdleTimeout     = defaults.IdleTimeout
	DefaultRequestTimeout  = defaults.RequestTimeout
	DefaultResponseTimeout = defaults.ResponseTimeout
)

type Options struct {
	Workers          int
	AcceptQueue      int
	MaxLineBytes     int
	IdleTimeout      time.Duration
	RequestTimeout   time.Duration
	ResponseTimeout  time.Duration
	AuthFailureDelay time.Duration
	AuthFailureLimit int
	AuthBanDuration  time.Duration
	Logger           *logging.Logger
}

func DefaultOptions() Options {
	return Options{
		Workers:          defaults.Workers(),
		AcceptQueue:      DefaultAcceptQueue,
		MaxLineBytes:     DefaultMaxLineBytes,
		IdleTimeout:      DefaultIdleTimeout,
		RequestTimeout:   DefaultRequestTimeout,
		ResponseTimeout:  DefaultResponseTimeout,
		AuthFailureDelay: defaults.AuthFailureDelay,
		AuthFailureLimit: defaults.AuthFailureLimit,
		AuthBanDuration:  defaults.AuthBanDuration,
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
	if options.AuthFailureDelay < 0 {
		return errors.New("serve: authentication failure delay must not be negative")
	}
	if options.AuthFailureDelay == 0 {
		options.AuthFailureDelay = defaults.AuthFailureDelay
	}
	if options.AuthFailureLimit < 0 {
		return errors.New("serve: authentication failure limit must not be negative")
	}
	if options.AuthFailureLimit == 0 {
		options.AuthFailureLimit = defaults.AuthFailureLimit
	}
	if options.AuthBanDuration < 0 {
		return errors.New("serve: authentication ban duration must not be negative")
	}
	if options.AuthBanDuration == 0 {
		options.AuthBanDuration = defaults.AuthBanDuration
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
	authFailures := newAuthFailureTracker(
		options.AuthFailureDelay,
		options.AuthFailureLimit,
		options.AuthBanDuration,
		engine.Metrics(),
	)
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
						engine.Metrics().ConnectionClosed()
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
						authFailures,
					)
					logConnectionTermination(options.Logger, termination)
					connections.Delete(connection)
					_ = connection.Close()
					engine.Metrics().ConnectionClosed()
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
		engine.Metrics().ConnectionOpened()
		if serveCtx.Err() != nil {
			_ = connection.Close()
			connections.Delete(connection)
			engine.Metrics().ConnectionClosed()
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
				engine.Metrics().ConnectionClosed()
				<-slots
				waitForWorkers()
				return nil
			}
		case <-serveCtx.Done():
			_ = connection.Close()
			connections.Delete(connection)
			engine.Metrics().ConnectionClosed()
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
			engine.Metrics().ConnectionClosed()
		}
	}
}
