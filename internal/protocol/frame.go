package protocol

import (
	"errors"
	"fmt"
	"strings"
)

var ErrInvalidFrame = errors.New("invalid command frame")

type Command struct {
	Name string
	Args []string
}

func ParseFrame(frame []byte) (Command, error) {
	if len(frame) < 3 || frame[len(frame)-2] != '\r' || frame[len(frame)-1] != '\n' {
		return Command{}, fmt.Errorf("%w: command must end with CRLF", ErrInvalidFrame)
	}

	line := frame[:len(frame)-2]
	if len(line) == 0 {
		return Command{}, fmt.Errorf("%w: command is empty", ErrInvalidFrame)
	}
	for _, character := range line {
		if character == '\r' || character == '\n' {
			return Command{}, fmt.Errorf("%w: embedded line break", ErrInvalidFrame)
		}
		if character < 0x20 || character > 0x7e {
			return Command{}, fmt.Errorf("%w: command must contain printable ASCII", ErrInvalidFrame)
		}
	}

	parts := strings.Split(string(line), " ")
	for _, part := range parts {
		if part == "" {
			return Command{}, fmt.Errorf("%w: tokens must use single spaces", ErrInvalidFrame)
		}
	}
	for _, character := range parts[0] {
		if character < 'A' || character > 'Z' {
			return Command{}, fmt.Errorf("%w: command name must be uppercase ASCII", ErrInvalidFrame)
		}
	}

	return Command{Name: parts[0], Args: parts[1:]}, nil
}
