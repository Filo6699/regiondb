package fs_split

import (
	"fmt"
	"testing"

	"github.com/Filo6699/regiondb/internal/geometry"
)

func BenchmarkAppendWAL(b *testing.B) {
	g, err := geometry.New(geometry.Config{
		ChunkEdge:      1,
		LargeChunkEdge: 1,
		BlockBits:      8,
	})
	if err != nil {
		b.Fatal(err)
	}
	wal, err := openWAL(b.TempDir(), nil)
	if err != nil {
		b.Fatal(err)
	}
	store := &Store{
		geometry: g,
		options:  Options{Durability: DurabilityRelaxed},
		wal:      wal,
	}
	record := store.encodeWALRecord(geometry.Coord{X: -17, Y: 29}, []byte{0xa5})

	b.Cleanup(func() {
		if err := wal.Close(); err != nil {
			b.Errorf("close WAL: %v", err)
		}
	})
	b.SetBytes(int64(len(record)))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := store.appendWAL(record); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkChunkCacheEviction(b *testing.B) {
	g, err := geometry.New(geometry.Config{
		ChunkEdge:      1,
		LargeChunkEdge: 1,
		BlockBits:      8,
	})
	if err != nil {
		b.Fatal(err)
	}
	for _, residents := range []int{1, 1024, 65536} {
		b.Run(fmt.Sprintf("residents-%d", residents), func(b *testing.B) {
			cache := newChunkCache(g, residents)
			payload := []byte{0x5a}
			for index := range residents {
				coord := geometry.Coord{X: int64(index)}
				if err := cache.put(coord, payload); err != nil {
					b.Fatal(err)
				}
			}

			b.ReportAllocs()
			b.ResetTimer()
			for index := range b.N {
				coord := geometry.Coord{X: int64(residents + index)}
				if err := cache.put(coord, payload); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
