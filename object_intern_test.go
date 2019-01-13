package goi

import (
	"testing"

	"github.com/replay/go-generic-object-store"
)

var testBytes = [][]byte{
	[]byte("SmallString"),
	[]byte("LongerString"),
	[]byte("AnEvenLongerString"),
	[]byte("metric"),
	[]byte("AndTheLongestStringWeDealWithWithEvenASmallAmountOfSpaceMoreToGetUsOverTheGiganticLimitOfStuff"),
}

func TestAddOrGet(t *testing.T) {
	oi := NewObjectIntern(nil)
	testResults := make([]gos.ObjAddr, 0)

	for _, b := range testBytes {
		ret, err := oi.AddOrGet(b)
		if err != nil {
			t.Error("Failed to AddOrGet: ", b)
			return
		}
		testResults = append(testResults, ret)
	}

	for _, b := range testBytes {
		ret, err := oi.AddOrGet(b)
		if err != nil {
			t.Error("Failed to AddOrGet: ", b)
			return
		}
		testResults = append(testResults, ret)
	}
}

func TestCompressionDecompress(t *testing.T) {
	oi := NewObjectIntern(nil)
	testResults := make([][]byte, 0)

	for _, b := range testBytes {
		c := oi.Compress(b)
		d, err := oi.Decompress(c)
		if err != nil {
			t.Error("Decompression failed for: ", c)
			return
		}
		testResults = append(testResults, d)
	}

	for i, res := range testResults {
		for k, v := range res {
			if v != testBytes[i][k] {
				t.Error("Mismatched: ", v, " - ", testBytes[i][k])
				return
			}
		}
	}
}

func BenchmarkCompressShoco(b *testing.B) {
	cnf := NewConfig()
	cnf.CompressionType = SHOCO
	benchmarkCompress(b, cnf)
}

func BenchmarkDecompressShoco(b *testing.B) {
	cnf := NewConfig()
	cnf.CompressionType = SHOCO
	benchmarkDecompress(b, cnf)
}

func BenchmarkCompressNone(b *testing.B) {
	cnf := NewConfig()
	cnf.CompressionType = 0
	benchmarkCompress(b, cnf)
}

func BenchmarkDecompressNone(b *testing.B) {
	cnf := NewConfig()
	cnf.CompressionType = 0
	benchmarkDecompress(b, cnf)
}

func benchmarkCompress(b *testing.B, cnf *ObjectInternConfig) {
	oi := NewObjectIntern(cnf)
	data := []byte("HowTheWindBlowsThroughTheTrees")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		oi.Compress(data)
	}
}

func benchmarkDecompress(b *testing.B, cnf *ObjectInternConfig) {
	oi := NewObjectIntern(cnf)
	data := []byte("HowTheWindBlowsThroughTheTrees")
	comp := oi.Compress(data)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		oi.Decompress(comp)
	}
}
