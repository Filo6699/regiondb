package bitcodec

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"testing"
)

func TestCodecPropertyRoundTripMatchesBitOracle(t *testing.T) {
	t.Parallel()

	const count = 129
	for width := uint8(1); width <= 64; width++ {
		for _, order := range []Order{LSBFirst, MSBFirst} {
			name := fmt.Sprintf("width-%d/%s", width, orderName(order))
			t.Run(name, func(t *testing.T) {
				t.Parallel()

				codec, err := New(width, order)
				if err != nil {
					t.Fatal(err)
				}
				size, err := codec.PackedBytes(count)
				if err != nil {
					t.Fatal(err)
				}
				random := rand.New(rand.NewSource(int64(width)*2 + int64(order) + 0x5eed))
				got := make([]byte, size)
				if _, err := random.Read(got); err != nil {
					t.Fatal(err)
				}
				want := append([]byte(nil), got...)

				for index := uint64(0); index < count; index++ {
					value := random.Uint64()
					if width < 64 {
						value &= uint64(1)<<width - 1
					}
					if err := codec.Set(got, index, value); err != nil {
						t.Fatalf("Set(%d, %x): %v", index, value, err)
					}
					setBitsOracle(want, index, width, order, value)
					roundTrip, err := codec.Get(got, index)
					if err != nil {
						t.Fatalf("Get(%d): %v", index, err)
					}
					if roundTrip != value {
						t.Fatalf("Get(%d) = %x, want %x", index, roundTrip, value)
					}
				}
				if !bytes.Equal(got, want) {
					t.Fatalf("packed bytes = %x, oracle = %x", got, want)
				}
				for index := uint64(0); index < count; index++ {
					gotValue, err := codec.Get(got, index)
					if err != nil {
						t.Fatalf("Get(%d): %v", index, err)
					}
					wantValue := getBitsOracle(want, index, width, order)
					if gotValue != wantValue {
						t.Fatalf("Get(%d) = %x, oracle = %x", index, gotValue, wantValue)
					}
				}
			})
		}
	}
}

func setBitsOracle(data []byte, index uint64, width uint8, order Order, value uint64) {
	start := index * uint64(width)
	for fieldBit := uint64(0); fieldBit < uint64(width); fieldBit++ {
		streamBit := start + fieldBit
		byteIndex := streamBit / 8
		bitIndex := streamBit % 8
		valueBit := fieldBit
		if order == MSBFirst {
			bitIndex = 7 - bitIndex
			valueBit = uint64(width) - 1 - fieldBit
		}
		mask := byte(1 << bitIndex)
		if value&(uint64(1)<<valueBit) != 0 {
			data[byteIndex] |= mask
		} else {
			data[byteIndex] &^= mask
		}
	}
}

func getBitsOracle(data []byte, index uint64, width uint8, order Order) uint64 {
	start := index * uint64(width)
	var value uint64
	for fieldBit := uint64(0); fieldBit < uint64(width); fieldBit++ {
		streamBit := start + fieldBit
		bitIndex := streamBit % 8
		if order == MSBFirst {
			bitIndex = 7 - bitIndex
		}
		bit := (data[streamBit/8] >> bitIndex) & 1
		valueBit := fieldBit
		if order == MSBFirst {
			valueBit = uint64(width) - 1 - fieldBit
		}
		value |= uint64(bit) << valueBit
	}
	return value
}

func orderName(order Order) string {
	if order == LSBFirst {
		return "lsb"
	}
	return "msb"
}

func valueForWidth(width uint8) uint64 {
	if width == 64 {
		return math.MaxUint64
	}
	return uint64(1)<<width - 1
}
