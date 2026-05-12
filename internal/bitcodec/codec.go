package bitcodec

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

var (
	ErrInvalidWidth = errors.New("invalid bit width")
	ErrInvalidOrder = errors.New("invalid bit order")
	ErrOutOfRange   = errors.New("bit field out of range")
	ErrValueTooWide = errors.New("value exceeds bit width")
	ErrShortBuffer  = errors.New("buffer is too short")
)

type Order uint8

const (
	LSBFirst Order = iota
	MSBFirst
)

type Codec struct {
	width uint8
	order Order
}

func New(width uint8, order Order) (Codec, error) {
	if width == 0 || width > 64 {
		return Codec{}, ErrInvalidWidth
	}
	if order != LSBFirst && order != MSBFirst {
		return Codec{}, ErrInvalidOrder
	}
	return Codec{width: width, order: order}, nil
}

func (c Codec) PackedBytes(count uint64) (int, error) {
	width := uint64(c.width)
	if count != 0 && width > math.MaxUint64/count {
		return 0, fmt.Errorf("%w: packed bit count overflows", ErrOutOfRange)
	}
	bits := count * width
	bytes := bits / 8
	if bits%8 != 0 {
		bytes++
	}
	if bytes > uint64(math.MaxInt) {
		return 0, fmt.Errorf("%w: packed byte count overflows int", ErrOutOfRange)
	}
	return int(bytes), nil
}

func (c Codec) Get(data []byte, index uint64) (uint64, error) {
	start, err := c.fieldStart(data, index)
	if err != nil {
		return 0, err
	}
	if c.width%8 == 0 {
		offset := int(start / 8)
		return c.getBytes(data[offset : offset+int(c.width)/8]), nil
	}

	var value uint64
	for fieldBit := uint64(0); fieldBit < uint64(c.width); fieldBit++ {
		streamBit := start + fieldBit
		byteIndex := streamBit / 8
		bitIndex := streamBit % 8
		if c.order == MSBFirst {
			bitIndex = 7 - bitIndex
		}
		bit := (data[byteIndex] >> bitIndex) & 1
		valueBit := fieldBit
		if c.order == MSBFirst {
			valueBit = uint64(c.width) - 1 - fieldBit
		}
		value |= uint64(bit) << valueBit
	}
	return value, nil
}

func (c Codec) Set(data []byte, index, value uint64) error {
	if c.width < 64 && value >= uint64(1)<<uint64(c.width) {
		return ErrValueTooWide
	}
	start, err := c.fieldStart(data, index)
	if err != nil {
		return err
	}
	if c.width%8 == 0 {
		offset := int(start / 8)
		c.setBytes(data[offset:offset+int(c.width)/8], value)
		return nil
	}

	for fieldBit := uint64(0); fieldBit < uint64(c.width); fieldBit++ {
		streamBit := start + fieldBit
		byteIndex := streamBit / 8
		bitIndex := streamBit % 8
		valueBit := fieldBit
		if c.order == MSBFirst {
			bitIndex = 7 - bitIndex
			valueBit = uint64(c.width) - 1 - fieldBit
		}
		mask := byte(1 << bitIndex)
		if value&(uint64(1)<<valueBit) != 0 {
			data[byteIndex] |= mask
		} else {
			data[byteIndex] &^= mask
		}
	}
	return nil
}

func (c Codec) getBytes(field []byte) uint64 {
	if c.order == LSBFirst {
		switch len(field) {
		case 1:
			return uint64(field[0])
		case 2:
			return uint64(binary.LittleEndian.Uint16(field))
		case 4:
			return uint64(binary.LittleEndian.Uint32(field))
		case 8:
			return binary.LittleEndian.Uint64(field)
		default:
			var value uint64
			for index, current := range field {
				value |= uint64(current) << (uint(index) * 8)
			}
			return value
		}
	}

	switch len(field) {
	case 1:
		return uint64(field[0])
	case 2:
		return uint64(binary.BigEndian.Uint16(field))
	case 4:
		return uint64(binary.BigEndian.Uint32(field))
	case 8:
		return binary.BigEndian.Uint64(field)
	default:
		var value uint64
		for _, current := range field {
			value = value<<8 | uint64(current)
		}
		return value
	}
}

func (c Codec) setBytes(field []byte, value uint64) {
	if c.order == LSBFirst {
		switch len(field) {
		case 1:
			field[0] = byte(value)
		case 2:
			binary.LittleEndian.PutUint16(field, uint16(value))
		case 4:
			binary.LittleEndian.PutUint32(field, uint32(value))
		case 8:
			binary.LittleEndian.PutUint64(field, value)
		default:
			for index := range field {
				field[index] = byte(value >> (uint(index) * 8))
			}
		}
		return
	}

	switch len(field) {
	case 1:
		field[0] = byte(value)
	case 2:
		binary.BigEndian.PutUint16(field, uint16(value))
	case 4:
		binary.BigEndian.PutUint32(field, uint32(value))
	case 8:
		binary.BigEndian.PutUint64(field, value)
	default:
		for index := range field {
			shift := uint(len(field)-1-index) * 8
			field[index] = byte(value >> shift)
		}
	}
}

func (c Codec) fieldStart(data []byte, index uint64) (uint64, error) {
	width := uint64(c.width)
	if index > math.MaxUint64/width {
		return 0, ErrOutOfRange
	}
	start := index * width
	if width > math.MaxUint64-start {
		return 0, ErrOutOfRange
	}
	end := start + width
	requiredBytes := end / 8
	if end%8 != 0 {
		requiredBytes++
	}
	if requiredBytes > uint64(len(data)) {
		return 0, ErrShortBuffer
	}
	return start, nil
}

func AppendUint32(dst []byte, value uint32) []byte {
	return binary.LittleEndian.AppendUint32(dst, value)
}

func AppendUint64(dst []byte, value uint64) []byte {
	return binary.LittleEndian.AppendUint64(dst, value)
}

func Uint32(src []byte) (uint32, error) {
	if len(src) < 4 {
		return 0, ErrShortBuffer
	}
	return binary.LittleEndian.Uint32(src), nil
}

func Uint64(src []byte) (uint64, error) {
	if len(src) < 8 {
		return 0, ErrShortBuffer
	}
	return binary.LittleEndian.Uint64(src), nil
}
