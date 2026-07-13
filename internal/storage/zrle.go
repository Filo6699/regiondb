package storage

import (
	"errors"
	"fmt"
)

const zrleRunLimit = 128

var ErrInvalidZRLE = errors.New("invalid zrle stream")

// EncodeZRLE encodes bytes as alternating literal and zero runs. The high bit
// identifies a zero run and the remaining seven bits store the run length
// minus one.
func EncodeZRLE(source []byte) []byte {
	encoded := make([]byte, 0, len(source))
	for start := 0; start < len(source); {
		if source[start] == 0 {
			end := start + 1
			for end < len(source) && source[end] == 0 && end-start < zrleRunLimit {
				end++
			}
			encoded = append(encoded, 0x80|byte(end-start-1))
			start = end
			continue
		}

		end := start + 1
		for end < len(source) && source[end] != 0 && end-start < zrleRunLimit {
			end++
		}
		encoded = append(encoded, byte(end-start-1))
		encoded = append(encoded, source[start:end]...)
		start = end
	}
	return encoded
}

// DecodeZRLE decodes exactly size bytes and rejects truncated, trailing, or
// over-expanding input before it can allocate beyond the caller's bound.
func DecodeZRLE(encoded []byte, size int) ([]byte, error) {
	if size < 0 {
		return nil, fmt.Errorf("%w: negative decoded size", ErrInvalidZRLE)
	}
	decoded := make([]byte, 0, size)
	for offset := 0; offset < len(encoded); {
		control := encoded[offset]
		offset++
		run := int(control&0x7f) + 1
		if run > size-len(decoded) {
			return nil, fmt.Errorf("%w: decoded size exceeds %d bytes", ErrInvalidZRLE, size)
		}
		if control&0x80 != 0 {
			decoded = decoded[:len(decoded)+run]
			continue
		}
		if run > len(encoded)-offset {
			return nil, fmt.Errorf("%w: truncated literal run", ErrInvalidZRLE)
		}
		decoded = append(decoded, encoded[offset:offset+run]...)
		offset += run
	}
	if len(decoded) != size {
		return nil, fmt.Errorf(
			"%w: decoded size is %d, want %d",
			ErrInvalidZRLE,
			len(decoded),
			size,
		)
	}
	return decoded, nil
}
