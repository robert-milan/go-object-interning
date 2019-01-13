package goi

import (
	"bytes"
	"testing"

	"github.com/replay/go-generic-object-store"
)

var testBytes = [][]byte{
	[]byte("SmallString"),
	[]byte("LongerString"),
	[]byte("AnEvenLongerString"),
	[]byte("metric"),
	[]byte("root"),
	[]byte("server"),
	[]byte("servername1234"),
	[]byte("servername4321"),
	[]byte("servername91FFXX"),
	[]byte("AndTheLongestStringWeDealWithWithEvenASmallAmountOfSpaceMoreToGetUsOverTheGiganticLimitOfStuff"),
}

var testStrings = []string{
	string("SmallString"),
	string("LongerString"),
	string("AnEvenLongerString"),
	string("metric"),
	string("root"),
	string("server"),
	string("servername1234"),
	string("servername4321"),
	string("servername91FFXX"),
	string("AndTheLongestStringWeDealWithWithEvenASmallAmountOfSpaceMoreToGetUsOverTheGiganticLimitOfStuff"),
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

	// increase reference count to 2
	for _, b := range testBytes {
		_, err := oi.AddOrGet(b)
		if err != nil {
			t.Error("Failed to AddOrGet: ", b)
			return
		}
	}

	// increase reference count to 3
	for _, b := range testBytes {
		_, err := oi.AddOrGet(b)
		if err != nil {
			t.Error("Failed to AddOrGet: ", b)
			return
		}
	}
}

func TestObjBytes(t *testing.T) {
	oi := NewObjectIntern(nil)
	objAddrs := make([]uintptr, 0)

	for _, b := range testBytes {
		addr, err := oi.AddOrGet(b)
		if err != nil {
			t.Error("Failed to AddOrGet: ", b)
			return
		}
		objAddrs = append(objAddrs, addr)
	}

	for idx, addr := range objAddrs {
		valFromStore, err := oi.ObjBytes(addr)
		if err != nil {
			t.Error("Failed while getting ObjBytes")
			return
		}
		if !bytes.Equal(valFromStore, testBytes[idx]) {
			t.Error("Original and returned values do not match")
			return
		}
	}
}

func TestObjString(t *testing.T) {

}

func TestCompressDecompress(t *testing.T) {
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

func TestCompressSzDecompressSz(t *testing.T) {
	oi := NewObjectIntern(nil)
	testResults := make([]string, 0)

	for _, sz := range testStrings {
		cSz := oi.CompressSz(sz)
		dSz, err := oi.DecompressSz(cSz)
		if err != nil {
			t.Error("Decompression failed for: ", cSz)
			return
		}
		testResults = append(testResults, dSz)
	}

	for i, res := range testResults {
		if res != testStrings[i] {
			t.Error("Mismatched: ", res, " - ", testStrings[i])
			return
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
	cnf.CompressionType = NOCPRSN
	benchmarkCompress(b, cnf)
}

func BenchmarkDecompressNone(b *testing.B) {
	cnf := NewConfig()
	cnf.CompressionType = NOCPRSN
	benchmarkDecompress(b, cnf)
}

func benchmarkCompress(b *testing.B, cnf *ObjectInternConfig) {
	oi := NewObjectIntern(cnf)
	data := []byte("HowTheWindBlowsThroughTheTrees")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		oi.compress(data)
	}
}

func benchmarkDecompress(b *testing.B, cnf *ObjectInternConfig) {
	oi := NewObjectIntern(cnf)
	data := []byte("HowTheWindBlowsThroughTheTrees")
	comp := oi.compress(data)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		oi.decompress(comp)
	}
}

func BenchmarkCompressSzShoco(b *testing.B) {
	cnf := NewConfig()
	cnf.CompressionType = SHOCO
	benchmarkCompressSz(b, cnf, "testingString")
}

func BenchmarkDecompressSzShoco(b *testing.B) {
	cnf := NewConfig()
	cnf.CompressionType = SHOCO
	benchmarkDecompressSz(b, cnf, "testingString")
}

func BenchmarkCompressSzNone(b *testing.B) {
	cnf := NewConfig()
	cnf.CompressionType = NOCPRSN
	benchmarkCompressSz(b, cnf, "testingString")
}

func BenchmarkDecompressSzNone(b *testing.B) {
	cnf := NewConfig()
	cnf.CompressionType = NOCPRSN
	benchmarkDecompressSz(b, cnf, "testingString")
}

func benchmarkCompressSz(b *testing.B, cnf *ObjectInternConfig, sz string) {
	oi := NewObjectIntern(cnf)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		oi.CompressSz(sz)
	}
}

func benchmarkDecompressSz(b *testing.B, cnf *ObjectInternConfig, sz string) {
	oi := NewObjectIntern(cnf)
	comp := oi.CompressSz(sz)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		oi.DecompressSz(comp)
	}
}
