package server

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/Filo6699/regiondb/internal/protocol"
)

func Serve(ctx context.Context, listener net.Listener, engine *protocol.Engine) error {
	if ctx == nil {
		return errors.New("serve: context must not be nil")
	}
	if listener == nil {
		return errors.New("serve: listener must not be nil")
	}
	if engine == nil {
		return errors.New("serve: protocol engine must not be nil")
	}

	var connections sync.Map
	var handlers sync.WaitGroup
	stop := context.AfterFunc(ctx, func() {
		_ = listener.Close()
		connections.Range(func(key, _ any) bool {
			_ = key.(net.Conn).Close()
			return true
		})
	})
	defer stop()

	for {
		connection, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				handlers.Wait()
				return nil
			}
			connections.Range(func(key, _ any) bool {
				_ = key.(net.Conn).Close()
				return true
			})
			handlers.Wait()
			return fmt.Errorf("accept connection: %w", err)
		}

		connections.Store(connection, struct{}{})
		if ctx.Err() != nil {
			_ = connection.Close()
		}
		handlers.Add(1)
		go func() {
			defer handlers.Done()
			defer connections.Delete(connection)
			defer func() {
				_ = connection.Close()
			}()
			serveConnection(connection, engine)
		}()
	}
}

func serveConnection(connection net.Conn, engine *protocol.Engine) {
	reader := bufio.NewReader(connection)
	session := engine.NewSession()
	for {
		frame, err := reader.ReadBytes('\n')
		if err != nil {
			if !errors.Is(err, io.EOF) {
				return
			}
			if len(frame) == 0 {
				return
			}
		}

		if err := writeAll(connection, session.Handle(frame).Bytes()); err != nil {
			return
		}
		if session.Closed() || errors.Is(err, io.EOF) {
			return
		}
	}
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
