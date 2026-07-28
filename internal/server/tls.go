package server

import (
	"context"
	"crypto/tls"
	"net"
	"time"
)

func handshakeTLSWithin(ctx context.Context, connection net.Conn, timeout time.Duration) error {
	tlsConnection := unwrapTLSConnection(connection)
	if tlsConnection == nil {
		return nil
	}
	handshakeContext, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return tlsConnection.HandshakeContext(handshakeContext)
}

func isTLSConnection(connection net.Conn) bool {
	return unwrapTLSConnection(connection) != nil
}

func unwrapTLSConnection(connection net.Conn) *tls.Conn {
	for {
		if tlsConnection, ok := connection.(*tls.Conn); ok {
			return tlsConnection
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
