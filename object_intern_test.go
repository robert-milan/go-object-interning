package goi

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
	"time"
	"unsafe"
)

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

var src = rand.NewSource(time.Now().UnixNano())

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

func randStringBytesMaskImprSrc(n int) string {
	b := make([]byte, n)
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := n-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return string(b)
}

func TestAddOrGet(t *testing.T) {
	oi := NewObjectIntern(NewConfig())
	results := make(map[string]uintptr, 0)

	for _, b := range testBytes {
		ret, err := oi.AddOrGet(b, true)
		if err != nil {
			t.Error("Failed to AddOrGet: ", b)
			return
		}
		// need to compress b just as is done in the actual AddOrGet method
		results[string(oi.compress(b))] = ret
	}

	// increase reference count to 2
	for _, b := range testBytes {
		addr, err := oi.AddOrGet(b, true)
		if err != nil {
			t.Error("Failed to AddOrGet: ", b)
			return
		}
		refCnt := *(*uint32)(unsafe.Pointer(addr + uintptr(len(oi.compress(b)))))
		if refCnt != 2 {
			t.Errorf("Reference count should be 2, instead found %d\n", refCnt)
			return
		}
	}

	// increase reference count to 3
	for _, b := range testBytes {
		addr, err := oi.AddOrGet(b, true)
		if err != nil {
			t.Error("Failed to AddOrGet: ", b)
			return
		}
		refCnt := *(*uint32)(unsafe.Pointer(addr + uintptr(len(oi.compress(b)))))
		if refCnt != 3 {
			t.Errorf("Reference count should be 3, instead found %d\n", refCnt)
			return
		}
	}

	// make sure all of these keys exist in the index
	for k, v := range oi.objIndex {
		if v != results[k] {
			t.Error("Results not found in index")
			return
		}
	}
}

func TestRefCount(t *testing.T) {
	oi := NewObjectIntern(NewConfig())
	results := make(map[string]uintptr, 0)

	for _, b := range testBytes {
		ret, err := oi.AddOrGet(b, true)
		if err != nil {
			t.Error("Failed to AddOrGet: ", b)
			return
		}
		// need to compress b just as is done in the actual AddOrGet method
		results[string(oi.compress(b))] = ret
	}

	// increase reference count to 10
	for i := 0; i < 9; i++ {
		for _, b := range testBytes {
			_, err := oi.AddOrGet(b, true)
			if err != nil {
				t.Error("Failed to AddOrGet: ", b)
				return
			}
		}
	}

	for _, v := range results {
		rc, err := oi.RefCnt(v)
		if err != nil {
			t.Error("Failed to get reference count: ", rc)
			return
		}
		if rc != 10 {
			t.Error("Reference Count should be 10, instead we found ", rc)
			return
		}
	}
}

func TestAddOrGetAndDelete25(t *testing.T) {
	cnf := NewConfig()
	cnf.Compression = Shoco
	testAddOrGetAndDelete(t, 25, 501, cnf)
}

func TestAddOrGetAndDelete250(t *testing.T) {
	cnf := NewConfig()
	cnf.Compression = Shoco
	testAddOrGetAndDelete(t, 250, 501, cnf)
}

func TestAddOrGetAndDeleteNoCprsn25(t *testing.T) {
	cnf := NewConfig()
	cnf.Compression = None
	testAddOrGetAndDelete(t, 25, 501, cnf)
}

func TestAddOrGetAndDeleteNoCprsn250(t *testing.T) {
	cnf := NewConfig()
	cnf.Compression = None
	testAddOrGetAndDelete(t, 250, 501, cnf)
}

func testAddOrGetAndDelete(t *testing.T, keySize int, numKeys int, cnf ObjectInternConfig) {
	oi := NewObjectIntern(cnf)

	// slice to store addresses
	addrs := make([]uintptr, 0)
	// generate numKeys random strings of keySize length
	originalSzs := make([]string, 0)
	// also generate compressed versions stored in []byte
	compSzs := make([][]byte, 0)
	for i := 0; i < numKeys; i++ {
		sz := randStringBytesMaskImprSrc(keySize)
		originalSzs = append(originalSzs, sz)
		compSzs = append(compSzs, oi.compress([]byte(sz)))
	}

	// reference count should be 1 after this finishes
	for _, sz := range originalSzs {
		addr, err := oi.AddOrGet([]byte(sz), true)
		if err != nil {
			t.Error("Failed to AddOrGet: ", []byte(sz))
			return
		}
		// add addr to addrs
		addrs = append(addrs, addr)
	}

	// reference count should be 2 after this finishes
	for _, sz := range originalSzs {
		_, err := oi.AddOrGet([]byte(sz), true)
		if err != nil {
			t.Error("Failed to AddOrGet: ", []byte(sz))
			return
		}
	}

	// decrease reference count by 1, it should now be 1 again
	for _, addr := range addrs {
		ok, err := oi.Delete(addr)
		if err != nil {
			t.Error("Failed to delete object (possibly not found in the object store): ", addr)
			return
		}
		if ok {
			t.Error("Ok should be false since reference count is at 1 now")
			return
		}
	}

	// decrease reference count by 1, now objects should be deleted (slabs are deleted as well)
	for _, addr := range addrs {
		ok, err := oi.Delete(addr)
		if err != nil {
			t.Error("Failed to delete object (possibly not found in the object store): ", addr)
			return
		}
		if !ok {
			t.Error("Ok should be true since object should have been deleted")
			return
		}
	}

}

func TestJoinStringsCompressed(t *testing.T) {
	cnf := NewConfig()
	cnf.Compression = Shoco
	testJoinStrings(t, cnf)
}

func TestJoinStringsUncompressed(t *testing.T) {
	cnf := NewConfig()
	cnf.Compression = None
	testJoinStrings(t, cnf)
}

func testJoinStrings(t *testing.T, cnf ObjectInternConfig) {
	oi := NewObjectIntern(cnf)

	addrs := make([]uintptr, 0)
	for _, tmpBytes := range testBytes {
		addr, err := oi.AddOrGet(tmpBytes, true)
		if err != nil {
			t.Error("Failed to add object to object store")
		}
		addrs = append(addrs, addr)
	}

	expected := "SmallString.LongerString.AnEvenLongerString.metric.root.server.servername1234.servername4321.servername91FFXX.AndTheLongestStringWeDealWithWithEvenASmallAmountOfSpaceMoreToGetUsOverTheGiganticLimitOfStuff"

	joinedString, err := oi.JoinStrings(addrs, ".")
	if err != nil {
		t.Error(err)
	}
	if joinedString != expected {
		t.Errorf("Expected: %s\nActual: %s\n", expected, joinedString)
	}
}

func TestAddOrGetAndDeleteByVal25(t *testing.T) {
	cnf := NewConfig()
	cnf.Compression = Shoco
	testAddOrGetAndDeleteByVal(t, 25, 501, cnf)
}

func TestAddOrGetAndDeleteByVal250(t *testing.T) {
	cnf := NewConfig()
	cnf.Compression = Shoco
	testAddOrGetAndDeleteByVal(t, 250, 501, cnf)
}

func TestAddOrGetAndDeleteByValNoCprsn25(t *testing.T) {
	cnf := NewConfig()
	cnf.Compression = None
	testAddOrGetAndDeleteByVal(t, 25, 501, cnf)
}

func TestAddOrGetAndDeleteByValNoCprsn250(t *testing.T) {
	cnf := NewConfig()
	cnf.Compression = None
	testAddOrGetAndDeleteByVal(t, 250, 501, cnf)
}

func testAddOrGetAndDeleteByVal(t *testing.T, keySize int, numKeys int, cnf ObjectInternConfig) {
	oi := NewObjectIntern(cnf)

	// slice to store addresses
	addrs := make([]uintptr, 0)
	// generate numKeys random strings of keySize length
	originalSzs := make([]string, 0)
	// also generate compressed versions stored in []byte
	decompBytes := make([][]byte, 0)
	for i := 0; i < numKeys; i++ {
		sz := randStringBytesMaskImprSrc(keySize)
		originalSzs = append(originalSzs, sz)
		decompBytes = append(decompBytes, []byte(sz))
	}

	// reference count should be 1 after this finishes
	for _, sz := range originalSzs {
		addr, err := oi.AddOrGet([]byte(sz), true)
		if err != nil {
			t.Error("Failed to AddOrGet: ", []byte(sz))
			return
		}
		// add addr to addrs
		addrs = append(addrs, addr)
	}

	// reference count should be 2 after this finishes
	for _, sz := range originalSzs {
		_, err := oi.AddOrGet([]byte(sz), true)
		if err != nil {
			t.Error("Failed to AddOrGet: ", []byte(sz))
			return
		}
	}

	// decrease reference count by 1, it should now be 1 again
	for _, compObj := range decompBytes {
		ok, err := oi.DeleteByByte(compObj)
		if err != nil {
			t.Error("Failed to delete object (possibly not found in the object store): ", compObj)
			return
		}
		if ok {
			t.Error("Ok should be false since reference count is at 1 now")
			return
		}
	}

	// decrease reference count by 1, now objects should be deleted (slabs are deleted as well)
	for _, compObj := range decompBytes {
		ok, err := oi.DeleteByByte(compObj)
		if err != nil {
			t.Error("Failed to delete object (possibly not found in the object store): ", compObj)
			return
		}
		if !ok {
			t.Error("Ok should be true since object should have been deleted")
			return
		}
	}

}

func TestAddOrGetAndDeleteByValSz25(t *testing.T) {
	cnf := NewConfig()
	cnf.Compression = Shoco
	testAddOrGetAndDeleteByValSz(t, 25, 501, cnf)
}

func TestAddOrGetAndDeleteByValSz250(t *testing.T) {
	cnf := NewConfig()
	cnf.Compression = Shoco
	testAddOrGetAndDeleteByValSz(t, 250, 501, cnf)
}

func TestAddOrGetAndDeleteByValSzNoCprsn25(t *testing.T) {
	cnf := NewConfig()
	cnf.Compression = None
	testAddOrGetAndDeleteByValSz(t, 25, 501, cnf)
}

func TestAddOrGetAndDeleteByValSzNoCprsn250(t *testing.T) {
	cnf := NewConfig()
	cnf.Compression = None
	testAddOrGetAndDeleteByValSz(t, 250, 501, cnf)
}

func testAddOrGetAndDeleteByValSz(t *testing.T, keySize int, numKeys int, cnf ObjectInternConfig) {
	oi := NewObjectIntern(cnf)

	// slice to store addresses
	addrs := make([]uintptr, 0)
	// generate numKeys random strings of keySize length
	originalSzs := make([]string, 0)
	compSzs := make([]string, 0)
	for i := 0; i < numKeys; i++ {
		sz := randStringBytesMaskImprSrc(keySize)
		originalSzs = append(originalSzs, sz)
		compSzs = append(compSzs, string(oi.compress([]byte(sz))))
	}

	// reference count should be 1 after this finishes
	for _, sz := range originalSzs {
		addr, err := oi.AddOrGet([]byte(sz), true)
		if err != nil {
			t.Error("Failed to AddOrGet: ", []byte(sz))
			return
		}
		// add addr to addrs
		addrs = append(addrs, addr)
	}

	// reference count should be 2 after this finishes
	for _, sz := range originalSzs {
		_, err := oi.AddOrGet([]byte(sz), true)
		if err != nil {
			t.Error("Failed to AddOrGet: ", []byte(sz))
			return
		}
	}

	// decrease reference count by 1, it should now be 1 again
	for _, sz := range compSzs {
		ok, err := oi.DeleteByString(sz)
		if err != nil {
			t.Error("Failed to delete object (possibly not found in the object store): ", sz)
			return
		}
		if ok {
			t.Error("Ok should be false since reference count is at 1 now")
			return
		}
	}

	// decrease reference count by 1, now objects should be deleted (slabs are deleted as well)
	for _, sz := range compSzs {
		ok, err := oi.DeleteByString(sz)
		if err != nil {
			t.Error("Failed to delete object (possibly not found in the object store): ", sz)
			return
		}
		if !ok {
			t.Error("Ok should be true since object should have been deleted")
			return
		}
	}

}

func TestObjBytes(t *testing.T) {
	oi := NewObjectIntern(NewConfig())
	objAddrs := make([]uintptr, 0)

	for _, b := range testBytes {
		addr, err := oi.AddOrGet(b, true)
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
	oi := NewObjectIntern(NewConfig())
	objAddrs := make([]uintptr, 0)

	for _, b := range testBytes {
		addr, err := oi.AddOrGet(b, true)
		if err != nil {
			t.Error("Failed to AddOrGet: ", b)
			return
		}
		objAddrs = append(objAddrs, addr)
	}

	for idx, addr := range objAddrs {
		valFromStore, err := oi.ObjString(addr)
		if err != nil {
			t.Error("Failed while getting ObjString")
			return
		}
		if valFromStore != testStrings[idx] {
			t.Error("Original and returned values do not match")
			return
		}
	}
}

func TestCompressDecompress(t *testing.T) {
	oi := NewObjectIntern(NewConfig())
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
	oi := NewObjectIntern(NewConfig())
	testResults := make([]string, 0)

	for _, sz := range testStrings {
		cSz := oi.CompressString(sz)
		dSz, err := oi.DecompressString(cSz)
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

func BenchmarkMapSzLookup10(b *testing.B) {
	benchmarkMapSzLookup(b, 10)
}

func BenchmarkMapSzLookup100(b *testing.B) {
	benchmarkMapSzLookup(b, 100)
}

func BenchmarkMapSzLookup1000(b *testing.B) {
	benchmarkMapSzLookup(b, 1000)
}

func BenchmarkMapSzLookup10000(b *testing.B) {
	benchmarkMapSzLookup(b, 10000)
}

func BenchmarkMapSzLookup100000(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping " + b.Name() + " in short mode")
	}
	benchmarkMapSzLookup(b, 100000)
}

func BenchmarkMapSzLookup1000000(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping " + b.Name() + " in short mode")
	}
	benchmarkMapSzLookup(b, 1000000)
}

func BenchmarkMapSzLookup5000000(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping " + b.Name() + " in short mode")
	}
	benchmarkMapSzLookup(b, 5000000)
}

func benchmarkMapSzLookup(b *testing.B, num int) {
	index := make(map[string]uintptr)
	keys := make([]string, 0)
	vals := make([]uintptr, 0)
	for i := 0; i < num; i++ {
		key := fmt.Sprintf("%d", i)
		keys = append(keys, key)
		vals = append(vals, uintptr(i))
		index[key] = vals[i]

	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		for j, key := range keys {
			val := index[key]
			if val != vals[j] {
				b.Error("Value from map does not match value in slice")
				return
			}
		}
	}
}

func BenchmarkMapSzInsert10(b *testing.B) {
	benchmarkMapSzInsert(b, 10)
}

func BenchmarkMapSzInsert100(b *testing.B) {
	benchmarkMapSzInsert(b, 100)
}

func BenchmarkMapSzInsert1000(b *testing.B) {
	benchmarkMapSzInsert(b, 1000)
}

func BenchmarkMapSzInsert10000(b *testing.B) {
	benchmarkMapSzInsert(b, 10000)
}

func BenchmarkMapSzInsert100000(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping " + b.Name() + " in short mode")
	}
	benchmarkMapSzInsert(b, 100000)
}

func BenchmarkMapSzInsert1000000(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping " + b.Name() + " in short mode")
	}
	benchmarkMapSzInsert(b, 1000000)
}

func BenchmarkMapSzInsert5000000(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping " + b.Name() + " in short mode")
	}
	benchmarkMapSzInsert(b, 5000000)
}

func benchmarkMapSzInsert(b *testing.B, num int) {
	index := make(map[string]uintptr)
	keys := make([]string, 0)
	vals := make([]uintptr, 0)
	for i := 0; i < num; i++ {
		keys = append(keys, fmt.Sprintf("%d", i))
		vals = append(vals, uintptr(i))

	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		for j, key := range keys {
			index[key] = vals[j]
		}
	}
}

func BenchmarkMapIntLookup10(b *testing.B) {
	benchmarkMapIntLookup(b, 10)
}

func BenchmarkMapIntLookup100(b *testing.B) {
	benchmarkMapIntLookup(b, 100)
}

func BenchmarkMapIntLookup1000(b *testing.B) {
	benchmarkMapIntLookup(b, 1000)
}

func BenchmarkMapIntLookup10000(b *testing.B) {
	benchmarkMapIntLookup(b, 10000)
}

func BenchmarkMapIntLookup100000(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping " + b.Name() + " in short mode")
	}
	benchmarkMapIntLookup(b, 100000)
}

func BenchmarkMapIntLookup1000000(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping " + b.Name() + " in short mode")
	}
	benchmarkMapIntLookup(b, 1000000)
}

func BenchmarkMapIntLookup5000000(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping " + b.Name() + " in short mode")
	}
	benchmarkMapIntLookup(b, 5000000)
}

func benchmarkMapIntLookup(b *testing.B, num int) {
	index := make(map[uint32]uintptr)
	keys := make([]uint32, 0)
	vals := make([]uintptr, 0)
	for i := 0; i < num; i++ {
		keys = append(keys, uint32(i))
		vals = append(vals, uintptr(i))
		index[uint32(i)] = vals[i]

	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		for j, key := range keys {
			val := index[key]
			if val != vals[j] {
				b.Error("Value from map does not match value in slice")
				return
			}
		}
	}
}

func BenchmarkMapIntInsert10(b *testing.B) {
	benchmarkMapIntInsert(b, 10)
}

func BenchmarkMapIntInsert100(b *testing.B) {
	benchmarkMapIntInsert(b, 100)
}

func BenchmarkMapIntInsert1000(b *testing.B) {
	benchmarkMapIntInsert(b, 1000)
}

func BenchmarkMapIntInsert10000(b *testing.B) {
	benchmarkMapIntInsert(b, 10000)
}

func BenchmarkMapIntInsert100000(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping " + b.Name() + " in short mode")
	}
	benchmarkMapIntInsert(b, 100000)
}

func BenchmarkMapIntInsert1000000(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping " + b.Name() + " in short mode")
	}
	benchmarkMapIntInsert(b, 1000000)
}

func BenchmarkMapIntInsert5000000(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping " + b.Name() + " in short mode")
	}
	benchmarkMapIntInsert(b, 5000000)
}

func benchmarkMapIntInsert(b *testing.B, num int) {
	index := make(map[uint32]uintptr)
	keys := make([]uint32, 0)
	vals := make([]uintptr, 0)
	for i := 0; i < num; i++ {
		keys = append(keys, uint32(i))
		vals = append(vals, uintptr(i))

	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		for j, key := range keys {
			index[key] = vals[j]
		}
	}
}

func BenchmarkAddOrGet10(b *testing.B) {
	benchmarkAddOrGet(b, 10)
}

func BenchmarkAddOrGet100(b *testing.B) {
	benchmarkAddOrGet(b, 100)
}

func BenchmarkAddOrGet1000(b *testing.B) {
	benchmarkAddOrGet(b, 1000)
}

func BenchmarkAddOrGet10000(b *testing.B) {
	benchmarkAddOrGet(b, 10000)
}

func BenchmarkAddOrGet100000(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping " + b.Name() + " in short mode")
	}
	benchmarkAddOrGet(b, 100000)
}

func BenchmarkAddOrGet1000000(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping " + b.Name() + " in short mode")
	}
	benchmarkAddOrGet(b, 1000000)
}

func BenchmarkAddOrGet5000000(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping " + b.Name() + " in short mode")
	}
	benchmarkAddOrGet(b, 5000000)
}

func benchmarkAddOrGet(b *testing.B, num int) {
	oi := NewObjectIntern(NewConfig())
	data := make([][]byte, 0)
	for i := 0; i < num; i++ {
		data = append(data, []byte(fmt.Sprintf("%d", i)))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		for _, obj := range data {
			oi.AddOrGet(obj, true)
		}
	}
}

func BenchmarkAddOrGetString10(b *testing.B) {
	benchmarkAddOrGetString(b, 10)
}

func BenchmarkAddOrGetString100(b *testing.B) {
	benchmarkAddOrGetString(b, 100)
}

func BenchmarkAddOrGetString1000(b *testing.B) {
	benchmarkAddOrGetString(b, 1000)
}

func BenchmarkAddOrGetString10000(b *testing.B) {
	benchmarkAddOrGetString(b, 10000)
}

func BenchmarkAddOrGetString100000(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping " + b.Name() + " in short mode")
	}
	benchmarkAddOrGetString(b, 100000)
}

func BenchmarkAddOrGetString1000000(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping " + b.Name() + " in short mode")
	}
	benchmarkAddOrGetString(b, 1000000)
}

func BenchmarkAddOrGetString5000000(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping " + b.Name() + " in short mode")
	}
	benchmarkAddOrGetString(b, 5000000)
}

func benchmarkAddOrGetString(b *testing.B, num int) {
	oi := NewObjectIntern(NewConfig())
	data := make([][]byte, 0)
	for i := 0; i < num; i++ {
		data = append(data, []byte(fmt.Sprintf("%d", i)))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		for _, obj := range data {
			oi.AddOrGetString(obj, true)
		}
	}
}

func BenchmarkCompressShoco(b *testing.B) {
	cnf := NewConfig()
	cnf.Compression = Shoco
	benchmarkCompress(b, cnf)
}

func BenchmarkDecompressShoco(b *testing.B) {
	cnf := NewConfig()
	cnf.Compression = Shoco
	benchmarkDecompress(b, cnf)
}

func BenchmarkCompressNone(b *testing.B) {
	cnf := NewConfig()
	cnf.Compression = None
	benchmarkCompress(b, cnf)
}

func BenchmarkDecompressNone(b *testing.B) {
	cnf := NewConfig()
	cnf.Compression = None
	benchmarkDecompress(b, cnf)
}

func benchmarkCompress(b *testing.B, cnf ObjectInternConfig) {
	oi := NewObjectIntern(cnf)
	data := []byte("HowTheWindBlowsThroughTheTrees")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		oi.compress(data)
	}
}

func benchmarkDecompress(b *testing.B, cnf ObjectInternConfig) {
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
	cnf.Compression = Shoco
	benchmarkCompressSz(b, cnf, "testingString")
}

func BenchmarkDecompressSzShoco(b *testing.B) {
	cnf := NewConfig()
	cnf.Compression = Shoco
	benchmarkDecompressSz(b, cnf, "testingString")
}

func BenchmarkCompressSzNone(b *testing.B) {
	cnf := NewConfig()
	cnf.Compression = None
	benchmarkCompressSz(b, cnf, "testingString")
}

func BenchmarkDecompressSzNone(b *testing.B) {
	cnf := NewConfig()
	cnf.Compression = None
	benchmarkDecompressSz(b, cnf, "testingString")
}

func benchmarkCompressSz(b *testing.B, cnf ObjectInternConfig, sz string) {
	oi := NewObjectIntern(cnf)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		oi.CompressString(sz)
	}
}

func benchmarkDecompressSz(b *testing.B, cnf ObjectInternConfig, sz string) {
	oi := NewObjectIntern(cnf)
	comp := oi.CompressString(sz)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		oi.DecompressString(comp)
	}
}
