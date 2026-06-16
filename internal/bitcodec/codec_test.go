package bitcodec

import (
	"errors"
	"math"
	"testing"
)

func TestCodecBoundaryBits(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		order     Order
		width     uint8
		values    []uint64
		wantBytes []byte
	}{
		{"lsb one bit", LSBFirst, 1, []uint64{1, 0, 1, 1, 0, 0, 0, 1}, []byte{0x8d}},
		{"msb one bit", MSBFirst, 1, []uint64{1, 0, 1, 1, 0, 0, 0, 1}, []byte{0xb1}},
		{"lsb crossing bytes", LSBFirst, 5, []uint64{0x01, 0x1f, 0x12}, []byte{0xe1, 0x4b}},
		{"msb crossing bytes", MSBFirst, 5, []uint64{0x01, 0x1f, 0x12}, []byte{0x0f, 0xe4}},
		{"full width", LSBFirst, 64, []uint64{math.MaxUint64}, []byte{
			0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			codec, err := New(test.width, test.order)
			if err != nil {
				t.Fatal(err)
			}
			size, err := codec.PackedBytes(uint64(len(test.values)))
			if err != nil {
				t.Fatal(err)
			}
			data := make([]byte, size)
			for index, value := range test.values {
				if err := codec.Set(data, uint64(index), value); err != nil {
					t.Fatalf("Set(%d): %v", index, err)
				}
			}
			if string(data) != string(test.wantBytes) {
				t.Fatalf("encoded bytes = %x, want %x", data, test.wantBytes)
			}
			for index, want := range test.values {
				got, err := codec.Get(data, uint64(index))
				if err != nil {
					t.Fatalf("Get(%d): %v", index, err)
				}
				if got != want {
					t.Fatalf("Get(%d) = %x, want %x", index, got, want)
				}
			}
		})
	}
}

func TestCodecRejectsInvalidOperations(t *testing.T) {
	t.Parallel()

	if _, err := New(0, LSBFirst); !errors.Is(err, ErrInvalidWidth) {
		t.Fatalf("New(0) error = %v", err)
	}
	if _, err := New(65, LSBFirst); !errors.Is(err, ErrInvalidWidth) {
		t.Fatalf("New(65) error = %v", err)
	}
	if _, err := New(1, Order(2)); !errors.Is(err, ErrInvalidOrder) {
		t.Fatalf("New(invalid order) error = %v", err)
	}

	codec, err := New(5, LSBFirst)
	if err != nil {
		t.Fatal(err)
	}
	if err := codec.Set(make([]byte, 1), 0, 32); !errors.Is(err, ErrValueTooWide) {
		t.Fatalf("Set(wide value) error = %v", err)
	}
	if _, err := codec.Get(make([]byte, 1), 1); !errors.Is(err, ErrShortBuffer) {
		t.Fatalf("Get(short buffer) error = %v", err)
	}
	if _, err := codec.PackedBytes(math.MaxUint64); !errors.Is(err, ErrOutOfRange) {
		t.Fatalf("PackedBytes(overflow) error = %v", err)
	}
}

func TestPackedBytesRespectsTargetIntWidth(t *testing.T) {
	t.Parallel()

	codec, err := New(1, LSBFirst)
	if err != nil {
		t.Fatal(err)
	}
	maxInt := targetMaxInt()
	if maxInt > math.MaxUint32 {
		if _, err := codec.PackedBytes(math.MaxUint64); err != nil {
			t.Fatalf("PackedBytes(MaxUint64) error = %v", err)
		}
		return
	}
	maxCount := maxInt * 8
	if got, err := codec.PackedBytes(maxCount); err != nil || got != math.MaxInt {
		t.Fatalf("PackedBytes(MaxInt) = %d, %v", got, err)
	}
	if _, err := codec.PackedBytes(maxCount + 1); !errors.Is(err, ErrOutOfRange) {
		t.Fatalf("PackedBytes(MaxInt + 1) error = %v", err)
	}
}

func targetMaxInt() uint64 {
	return uint64(math.MaxInt)
}

func TestLittleEndianHelpers(t *testing.T) {
	t.Parallel()

	data := AppendUint32(nil, 0x12345678)
	data = AppendUint64(data, 0x0102030405060708)
	if got := data; string(got) != string([]byte{
		0x78, 0x56, 0x34, 0x12,
		0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01,
	}) {
		t.Fatalf("encoded bytes = %x", got)
	}
	if got, err := Uint32(data); err != nil || got != 0x12345678 {
		t.Fatalf("Uint32() = %x, %v", got, err)
	}
	if got, err := Uint64(data[4:]); err != nil || got != 0x0102030405060708 {
		t.Fatalf("Uint64() = %x, %v", got, err)
	}
	if _, err := Uint32(data[:3]); !errors.Is(err, ErrShortBuffer) {
		t.Fatalf("Uint32(short) error = %v", err)
	}
	if _, err := Uint64(data[:7]); !errors.Is(err, ErrShortBuffer) {
		t.Fatalf("Uint64(short) error = %v", err)
	}
	high := []byte{
		0xff, 0xff, 0xff, 0xff,
		0xff, 0xff, 0xff, 0xff,
	}
	if got, err := Uint32(high); err != nil || got != math.MaxUint32 {
		t.Fatalf("Uint32(high bytes) = %x, %v", got, err)
	}
	if got, err := Uint64(high); err != nil || got != math.MaxUint64 {
		t.Fatalf("Uint64(high bytes) = %x, %v", got, err)
	}
}
