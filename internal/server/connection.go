package server

import (
	"bufio"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"time"

	"github.com/Filo6699/regiondb/internal/defaults"
	"github.com/Filo6699/regiondb/internal/logging"
	"github.com/Filo6699/regiondb/internal/protocol"
)

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

func serveConnection(
	ctx context.Context,
	connection net.Conn,
	engine *protocol.Engine,
	maxLineBytes int,
	idleTimeout time.Duration,
	requestTimeout time.Duration,
	responseTimeout time.Duration,
	trackers ...*authFailureTracker,
) connectionTermination {
	authFailures := newAuthFailureTracker(
		defaults.AuthFailureDelay,
		defaults.AuthFailureLimit,
		defaults.AuthBanDuration,
	)
	if len(trackers) != 0 {
		authFailures = trackers[0]
	}
	if err := handshakeTLSWithin(ctx, connection, requestTimeout); err != nil {
		return classifyConnectionTermination(ctx, connection, "tls_handshake", err, true)
	}
	if err := setNoDelay(connection); err != nil {
		return classifyConnectionTermination(ctx, connection, "setup", err, true)
	}
	reader := newLineBuffer(maxLineBytes)
	writer := bufio.NewWriter(connection)
	session := engine.NewSession()
	source := authSource(connection)
	for {
		if !session.Authenticated() && authFailures.banned(source, time.Now()) {
			return connectionTermination{
				phase:  "authentication",
				reason: terminationProtocolClose,
			}
		}
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
				logPeerClose := !errors.Is(err, io.ErrClosedPipe) &&
					!errors.Is(err, net.ErrClosed)
				return classifyConnectionTermination(
					ctx,
					connection,
					"read",
					err,
					logPeerClose,
				)
			}
			if len(frame) == 0 {
				return classifyConnectionTermination(ctx, connection, "read", err, false)
			}
		}

		wasAuthenticated := session.Authenticated()
		response := session.Handle(frame)
		if session.AuthenticationFailed() {
			if err := waitForAuthPenalty(
				ctx,
				authFailures.registerFailure(source, time.Now()),
			); err != nil {
				return classifyConnectionTermination(ctx, connection, "auth_delay", err, false)
			}
		} else if !wasAuthenticated && session.Authenticated() {
			authFailures.authenticated(source)
		}
		if err := writeResponseWithin(
			connection,
			writer,
			response.Bytes(),
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
