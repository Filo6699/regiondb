package bitcodec

import (
	"fmt"
	"testing"
)

var benchmarkValue uint64

func BenchmarkCodecSetGet(b *testing.B) {
	for _, width := range []uint8{5, 8, 16, 24, 32, 64} {
		for _, order := range []Order{LSBFirst, MSBFirst} {
			b.Run(fmt.Sprintf("width-%d/%s", width, orderName(order)), func(b *testing.B) {
				codec, err := New(width, order)
				if err != nil {
					b.Fatal(err)
				}
				const fields = 1024
				size, err := codec.PackedBytes(fields)
				if err != nil {
					b.Fatal(err)
				}
				data := make([]byte, size)
				value := valueForWidth(width)

				b.ReportAllocs()
				b.ResetTimer()
				for iteration := range b.N {
					index := uint64(iteration % fields)
					if err := codec.Set(data, index, value); err != nil {
						b.Fatal(err)
					}
					got, err := codec.Get(data, index)
					if err != nil {
						b.Fatal(err)
					}
					benchmarkValue = got
				}
			})
		}
	}
}
