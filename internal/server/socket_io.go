package server

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"time"
)

const overloadWriteTimeout = 100 * time.Millisecond

func rejectOverloadedConnection(connection net.Conn) error {
	if err := connection.SetWriteDeadline(time.Now().Add(overloadWriteTimeout)); err != nil {
		return fmt.Errorf("set overload response deadline: %w", err)
	}
	if err := writeAll(connection, []byte("-ERR BUSY server overloaded\r\n")); err != nil {
		return fmt.Errorf("write overload response: %w", err)
	}
	return nil
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
		buffered := b.data[b.start:b.end]
		if bytes.IndexByte(buffered, '\n') >= 0 || b.pendingErr != nil {
			return b.readFrame(connection)
		}
		if err := connection.SetReadDeadline(time.Now().Add(requestTimeout)); err != nil {
			return nil, false, fmt.Errorf("set request read deadline: %w", err)
		}
		return b.readFrame(connection)
	}
	if err := connection.SetReadDeadline(time.Now().Add(idleTimeout)); err != nil {
		return nil, false, fmt.Errorf("set idle read deadline: %w", err)
	}

	read, err := connection.Read(b.data)
	b.end = read
	if read != 0 {
		if deadlineErr := connection.SetReadDeadline(
			time.Now().Add(requestTimeout),
		); deadlineErr != nil {
			return nil, false, fmt.Errorf(
				"set request read deadline after first byte: %w",
				deadlineErr,
			)
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
		return fmt.Errorf("set response write deadline: %w", err)
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
