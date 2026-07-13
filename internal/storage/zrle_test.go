package storage

import (
	"bytes"
	"errors"
	"testing"
)

func TestZRLERoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		source []byte
		want   []byte
	}{
		{name: "empty", source: nil, want: nil},
		{name: "zeros", source: []byte{0, 0, 0}, want: []byte{0x82}},
		{name: "literal", source: []byte{1, 2, 3}, want: []byte{0x02, 1, 2, 3}},
		{
			name:   "mixed",
			source: []byte{1, 0, 0, 2, 3, 0},
			want:   []byte{0x00, 1, 0x81, 0x01, 2, 3, 0x80},
		},
		{
			name:   "split long runs",
			source: append(bytes.Repeat([]byte{0}, 129), bytes.Repeat([]byte{7}, 129)...),
			want: append(
				[]byte{0xff, 0x80, 0x7f},
				append(bytes.Repeat([]byte{7}, 128), 0x00, 7)...,
			),
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			encoded := EncodeZRLE(test.source)
			if !bytes.Equal(encoded, test.want) {
				t.Fatalf("EncodeZRLE() = %x, want %x", encoded, test.want)
			}
			decoded, err := DecodeZRLE(encoded, len(test.source))
			if err != nil {
				t.Fatalf("DecodeZRLE() error = %v", err)
			}
			if !bytes.Equal(decoded, test.source) {
				t.Fatalf("DecodeZRLE() = %x, want %x", decoded, test.source)
			}
		})
	}
}

func TestDecodeZRLERejectsMalformedInput(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name    string
		encoded []byte
		size    int
	}{
		{name: "negative size", size: -1},
		{name: "short output", encoded: []byte{0x80}, size: 2},
		{name: "zero overflow", encoded: []byte{0x81}, size: 1},
		{name: "truncated literal", encoded: []byte{0x01, 1}, size: 2},
		{name: "literal overflow", encoded: []byte{0x01, 1, 2}, size: 1},
		{name: "trailing run", encoded: []byte{0x80, 0x80}, size: 1},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := DecodeZRLE(test.encoded, test.size); !errors.Is(err, ErrInvalidZRLE) {
				t.Fatalf("DecodeZRLE(%x, %d) error = %v, want ErrInvalidZRLE", test.encoded, test.size, err)
			}
		})
	}
}

func BenchmarkZRLECodec(b *testing.B) {
	source := make([]byte, 64*64*2+64*64/8)
	for index := 0; index < len(source); index += 97 {
		source[index] = byte(index | 1)
	}
	encoded := EncodeZRLE(source)

	b.Run("encode_sparse_chunk", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(source)))
		for range b.N {
			if len(EncodeZRLE(source)) == 0 {
				b.Fatal("empty encoding")
			}
		}
	})
	b.Run("decode_sparse_chunk", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(source)))
		for range b.N {
			decoded, err := DecodeZRLE(encoded, len(source))
			if err != nil || len(decoded) != len(source) {
				b.Fatalf("DecodeZRLE() = %d bytes, %v", len(decoded), err)
			}
		}
	})
}
