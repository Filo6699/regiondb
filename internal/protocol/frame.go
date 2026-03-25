package protocol

import (
	"errors"
	"fmt"
)

var ErrInvalidFrame = errors.New("invalid command frame")

type Command struct {
	Name string
	Args []string
}

func ParseFrame(frame []byte) (Command, error) {
	return parseFrame(frame, nil)
}

func parseFrame(frame []byte, scratch []string) (Command, error) {
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

	nameEnd := len(line)
	for index, character := range line {
		if character == ' ' {
			nameEnd = index
			break
		}
	}
	if nameEnd == 0 {
		return Command{}, fmt.Errorf("%w: tokens must use single spaces", ErrInvalidFrame)
	}
	for _, character := range line[:nameEnd] {
		if character < 'A' || character > 'Z' {
			return Command{}, fmt.Errorf("%w: command name must be uppercase ASCII", ErrInvalidFrame)
		}
	}

	text := string(line)
	args := scratch[:0]
	for start := nameEnd + 1; start <= len(line); {
		end := start
		for end < len(line) && line[end] != ' ' {
			end++
		}
		if end == start {
			return Command{}, fmt.Errorf("%w: tokens must use single spaces", ErrInvalidFrame)
		}
		args = append(args, text[start:end])
		if end == len(line) {
			break
		}
		start = end + 1
	}
	if args == nil {
		args = []string{}
	}
	return Command{Name: text[:nameEnd], Args: args}, nil
}
