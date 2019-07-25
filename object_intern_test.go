package goi

import (
	"bytes"
	"fmt"
	"math/rand"
	"reflect"
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
	testAddOrGet(t, true, false)
}

func TestAddOrGetUnsafe(t *testing.T) {
	testAddOrGet(t, false, false)
}

func TestAddOrGetCompressed(t *testing.T) {
	testAddOrGet(t, true, true)
}

func testAddOrGet(t *testing.T, safe bool, compress bool) {
	c := NewConfig()
	if compress {
		c.Compression = Shoco
	}
	oi := NewObjectIntern(c)
	results := make(map[string]uintptr, 0)

	for _, b := range testBytes {
		ret, err := oi.AddOrGet(b, safe)
		if err != nil {
			t.Error("Failed to AddOrGet: ", b)
			return
		}
		// need to compress b just as is done in the actual AddOrGet method
		results[string(oi.compress(b))] = ret
	}

	// increase reference count to 2
	for _, b := range testBytes {
		addr, err := oi.AddOrGet(b, safe)
		if err != nil {
			t.Error("Failed to AddOrGet: ", b)
			return
		}
		refCnt := *(*uint32)(unsafe.Pointer(addr))
		if refCnt != 2 {
			t.Errorf("Reference count should be 2, instead found %d\n", refCnt)
			return
		}
	}

	// increase reference count to 3
	for _, b := range testBytes {
		addr, err := oi.AddOrGet(b, safe)
		if err != nil {
			t.Error("Failed to AddOrGet: ", b)
			return
		}
		refCnt := *(*uint32)(unsafe.Pointer(addr))
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

func TestAddOrGetString(t *testing.T) {
	testAddOrGetString(t, true, false)
}

func TestAddOrGetStringUnsafe(t *testing.T) {
	testAddOrGetString(t, false, false)
}

func TestAddOrGetStringCompressed(t *testing.T) {
	testAddOrGetString(t, true, true)
}

func testAddOrGetString(t *testing.T, safe bool, compress bool) {
	c := NewConfig()
	if compress {
		c.Compression = Shoco
	}
	oi := NewObjectIntern(c)
	results := make(map[string]uintptr, 0)
	results2 := make(map[string]uintptr, 0)
	resultStrings := make(map[string]string, 0)
	resultStrings2 := make(map[string]string, 0)

	for _, s := range testStrings {
		retStr, err := oi.AddOrGetString([]byte(s), safe)
		if err != nil {
			t.Error("Failed to AddOrGetString: ", s)
			return
		}
		resultStrings[s] = retStr

		addr, err := oi.GetPtrFromByte([]byte(s))
		if err != nil {
			t.Error("Failed to GetPtrFromByte: ", s)
			return
		}
		results[s] = addr
	}

	// increase reference count to 2
	for _, s := range testStrings {
		retStr, err := oi.AddOrGetString([]byte(s), safe)
		if err != nil {
			t.Error("Failed to AddOrGetString: ", s)
			return
		}
		resultStrings2[s] = retStr

		addr, err := oi.GetPtrFromByte([]byte(s))
		if err != nil {
			t.Error("Failed to GetPtrFromByte: ", s)
			return
		}

		results2[s] = addr

		refCnt := *(*uint32)(unsafe.Pointer(addr))
		if refCnt != 2 {
			t.Errorf("Reference count should be 2, instead found %d\n", refCnt)
			return
		}
	}

	// these should be equal
	if !reflect.DeepEqual(results, results2) {
		t.Error("Pointer addresses are not equal")
		return
	}

	// uncompressed version
	if !compress {

		// make sure they are in the object index
		for k, v := range oi.objIndex {
			if v != results[k] {
				t.Error("Results not found in index")
				return
			}
		}

		// now compare the string data pointers, they should match
		for k, v := range resultStrings {
			dataPointer := (*reflect.StringHeader)(unsafe.Pointer(&v)).Data

			str2 := resultStrings2[k]
			dataPointer2 := (*reflect.StringHeader)(unsafe.Pointer(&str2)).Data

			if dataPointer != dataPointer2 {
				t.Error("Uintptr mismatch for: ", k)
				return
			}

			if v != str2 {
				t.Error("String mismatch for: ", v)
				return
			}
		}

		// return so we don't run the compressed version checks
		return
	}

	// compressed version

	// make sure they are in the object index
	for k, v := range oi.objIndex {
		dcmp, err := oi.decompress([]byte(k))
		if err != nil {
			t.Error("Failed to decompress string")
			return
		}
		if v != results[string(dcmp)] {
			t.Error("Results not found in index")
			return
		}
	}

	// now compare the string data pointers, they should NOT match
	for k, v := range resultStrings {
		dataPointer := (*reflect.StringHeader)(unsafe.Pointer(&v)).Data

		str2 := resultStrings2[k]
		dataPointer2 := (*reflect.StringHeader)(unsafe.Pointer(&str2)).Data

		if dataPointer == dataPointer2 {
			t.Error("Uintptrs should not match for compressed data: ", k)
			return
		}

		if v != str2 {
			t.Error("String mismatch for: ", v)
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

func TestIncRefCount(t *testing.T) {
	oi := NewObjectIntern(NewConfig())
	results := make(map[string]uintptr, 0)

	for _, b := range testBytes {
		ret, err := oi.AddOrGet(b, true)
		if err != nil {
			t.Error("Failed to AddOrGet: ", b)
			return
		}
		results[string(b)] = ret
	}

	// increase reference count to 10
	for i := 0; i < 9; i++ {
		for _, v := range results {
			_, err := oi.IncRefCnt(v)
			if err != nil {
				t.Error("Failed to increase reference count: ", v)
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

func TestIncRefCountString(t *testing.T) {
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
		for k := range results {
			_, err := oi.IncRefCntByString(k)
			if err != nil {
				t.Error("Failed to increase reference count by string")
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

func TestIncRefCntBatch(t *testing.T) {
	oi := NewObjectIntern(NewConfig())
	results := make(map[string]uintptr, 0)
	ptrs := make([]uintptr, len(testBytes))

	for _, b := range testBytes {
		ret, err := oi.AddOrGet(b, true)
		if err != nil {
			t.Error("Failed to AddOrGet: ", b)
			return
		}
		results[string(b)] = ret
		ptrs = append(ptrs, ret)
	}

	// increase reference count to 10
	for i := 0; i < 9; i++ {
		oi.IncRefCntBatch(ptrs)
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
	for i := 0; i < numKeys; i++ {
		sz := randStringBytesMaskImprSrc(keySize)
		originalSzs = append(originalSzs, sz)
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

func TestBatchDelete501(t *testing.T) {
	cnf := NewConfig()
	cnf.Compression = None
	testBatchDelete(t, 30, 501, cnf)
}

func TestBatchDeleteNoCprsn(t *testing.T) {
	cnf := NewConfig()
	cnf.Compression = Shoco
	testBatchDelete(t, 30, 501, cnf)
}

func testBatchDelete(t *testing.T, keySize int, numKeys int, cnf ObjectInternConfig) {
	oi := NewObjectIntern(cnf)

	// slice to store addresses
	addrs := make([]uintptr, 0)
	// generate numKeys random strings of keySize length
	originalSzs := make([]string, 0)
	for i := 0; i < numKeys; i++ {
		sz := randStringBytesMaskImprSrc(keySize)
		originalSzs = append(originalSzs, sz)
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

	// reference count should be 10 after this finishes
	for i := 0; i < 9; i++ {
		for _, sz := range originalSzs {
			_, err := oi.AddOrGet([]byte(sz), true)
			if err != nil {
				t.Error("Failed to AddOrGet: ", []byte(sz))
				return
			}
		}
	}

	// use subslice to decrease and then delete a subslice of the original
	// added strings.
	subAddrs := addrs[:numKeys-5]
	for i := 0; i < 10; i++ {
		oi.DeleteBatch(subAddrs)
	}

	// check to make sure the last 5 still exist
	lastFive := addrs[numKeys-5:]
	for _, ptr := range lastFive {
		_, err := oi.GetStringFromPtr(ptr)
		if err != nil {
			t.Error("Could not find string in object store")
			return
		}
	}

	// delete everything
	for i := 0; i < 10; i++ {
		oi.DeleteBatch(lastFive)
	}

	// make sure none of the items exist in the store
	for _, ptr := range addrs {
		_, err := oi.GetStringFromPtr(ptr)
		if err == nil {
			t.Error("Object should not have been found in the store")
			return
		}
	}
}

func TestMemStatsPerPool(t *testing.T) {
	oi := NewObjectIntern(NewConfig())

	addrs := make([]uintptr, 0)
	for _, tmpBytes := range testBytes {
		addr, err := oi.AddOrGet(tmpBytes, true)
		if err != nil {
			t.Error("Failed to add object to object store")
		}
		addrs = append(addrs, addr)
	}

	m := oi.MemStatsPerPool()
	for _, s := range m {
		t.Logf("ObjectSize: %d\nMemUsed: %d\n\n", s.ObjSize, s.MemUsed)
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
		return
	}
	if joinedString != expected {
		t.Errorf("Expected: %s\nActual: %s\n", expected, joinedString)
		return
	}

	joinedString, err = oi.JoinStrings([]uintptr{}, ".")
	if err == nil {
		t.Error("We should have an error here")
		return
	}

	joinedString, err = oi.JoinStrings([]uintptr{addrs[0]}, ".")
	if err != nil {
		t.Error(err)
		return
	}
	if joinedString != string(testBytes[0]) {
		t.Errorf("Expected: %s\nActual: %s\n", string(testBytes[0]), joinedString)
		return
	}
}

func TestReset(t *testing.T) {
	c := NewConfig()
	oi := NewObjectIntern(c)

	data := make([][]byte, 0, 10000)
	rand.Seed(time.Now().UnixNano())
	l := len(testStrings)

	for i := 0; i < 10000; i++ {
		data = append(data, []byte(fmt.Sprintf(testStrings[rand.Intn(l)]+"%d", i)))
		oi.AddOrGet(data[i], false)
	}

	if len(oi.objIndex) != 10000 {
		t.Fatalf("Length of object index should be 10000, instead found: %d", len(oi.objIndex))
	}

	err := oi.Reset()
	if err != nil {
		t.Fatalf("Reset returned an error: %s", err)
	}

	if len(oi.objIndex) != 0 {
		t.Fatalf("Length of object index should be 0, instead found: %d", len(oi.objIndex))
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
	for i := 0; i < numKeys; i++ {
		sz := randStringBytesMaskImprSrc(keySize)
		originalSzs = append(originalSzs, sz)
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
	for _, sz := range originalSzs {
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
	for _, sz := range originalSzs {
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
	testObjBytes(t, false)
}

func TestObjBytesCompressed(t *testing.T) {
	testObjBytes(t, true)
}

func testObjBytes(t *testing.T, compress bool) {
	c := NewConfig()
	if compress {
		c.Compression = Shoco
	}
	oi := NewObjectIntern(c)

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
	testObjString(t, false)
}

func TestObjStringCompressed(t *testing.T) {
	testObjString(t, true)
}

func testObjString(t *testing.T, compress bool) {
	c := NewConfig()
	if compress {
		c.Compression = Shoco
	}
	oi := NewObjectIntern(c)

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

var globalPtr uintptr
var globalStr string

func BenchmarkAddOrGet(b *testing.B) {
	benchmarks := []struct {
		name        string
		num         int
		compression bool
		safe        bool
		dupe        bool
		short       bool
		stringTest  bool
	}{
		// AddOrGet
		{"CompressedUintptr-10", 10, true, true, false, false, false},
		{"CompressedUintptr-100", 100, true, true, false, false, false},
		{"CompressedUintptr-1000", 1000, true, true, false, false, false},
		{"CompressedUintptr-10000", 10000, true, true, false, false, false},
		// skip short
		{"CompressedUintptr-100000", 100000, true, true, false, true, false},
		{"CompressedUintptr-1000000", 1000000, true, true, false, true, false},
		{"CompressedUintptr-5000000", 5000000, true, true, false, true, false},

		// dupes
		{"CompressedDuplicatesUintptr-10", 10, true, true, true, false, false},
		{"CompressedDuplicatesUintptr-100", 100, true, true, true, false, false},
		{"CompressedDuplicatesUintptr-1000", 1000, true, true, true, false, false},
		{"CompressedDuplicatesUintptr-10000", 10000, true, true, true, false, false},
		// skip short
		{"CompressedDuplicatesUintptr-100000", 100000, true, true, true, true, false},
		{"CompressedDuplicatesUintptr-1000000", 1000000, true, true, true, true, false},
		{"CompressedDuplicatesUintptr-5000000", 5000000, true, true, true, true, false},

		{"UnsafeUintptr-10", 10, false, false, false, false, false},
		{"UnsafeUintptr-100", 100, false, false, false, false, false},
		{"UnsafeUintptr-1000", 1000, false, false, false, false, false},
		{"UnsafeUintptr-10000", 10000, false, false, false, false, false},
		// skip short
		{"UnsafeUintptr-100000", 100000, false, false, false, true, false},
		{"UnsafeUintptr-1000000", 1000000, false, false, false, true, false},
		{"UnsafeUintptr-5000000", 5000000, false, false, false, true, false},

		// dupes
		{"UnsafeDuplicatesUintptr-10", 10, false, false, true, false, false},
		{"UnsafeDuplicatesUintptr-100", 100, false, false, true, false, false},
		{"UnsafeDuplicatesUintptr-1000", 1000, false, false, true, false, false},
		{"UnsafeDuplicatesUintptr-10000", 10000, false, false, true, false, false},
		// skip short
		{"UnsafeDuplicatesUintptr-100000", 100000, false, false, true, true, false},
		{"UnsafeDuplicatesUintptr-1000000", 1000000, false, false, true, true, false},
		{"UnsafeDuplicatesUintptr-5000000", 5000000, false, false, true, true, false},

		{"SafeUintptr-10", 10, false, true, false, false, false},
		{"SafeUintptr-100", 100, false, true, false, false, false},
		{"SafeUintptr-1000", 1000, false, true, false, false, false},
		{"SafeUintptr-10000", 10000, false, true, false, false, false},
		// skip short
		{"SafeUintptr-100000", 100000, false, true, false, true, false},
		{"SafeUintptr-1000000", 1000000, false, true, false, true, false},
		{"SafeUintptr-5000000", 5000000, false, true, false, true, false},

		// dupes
		{"SafeDuplicatesUintptr-10", 10, false, true, true, false, false},
		{"SafeDuplicatesUintptr-100", 100, false, true, true, false, false},
		{"SafeDuplicatesUintptr-1000", 1000, false, true, true, false, false},
		{"SafeDuplicatesUintptr-10000", 10000, false, true, true, false, false},
		// skip short
		{"SafeDuplicatesUintptr-100000", 100000, false, true, true, true, false},
		{"SafeDuplicatesUintptr-1000000", 1000000, false, true, true, true, false},
		{"SafeDuplicatesUintptr-5000000", 5000000, false, true, true, true, false},

		// AddOrGetString
		{"CompressedString-10", 10, true, true, false, false, true},
		{"CompressedString-100", 100, true, true, false, false, true},
		{"CompressedString-1000", 1000, true, true, false, false, true},
		{"CompressedString-10000", 10000, true, true, false, false, true},
		// skip short
		{"CompressedString-100000", 100000, true, true, false, true, true},
		{"CompressedString-1000000", 1000000, true, true, false, true, true},
		{"CompressedString-5000000", 5000000, true, true, false, true, true},

		// dupes
		{"CompressedDuplicatesString-10", 10, true, true, true, false, true},
		{"CompressedDuplicatesString-100", 100, true, true, true, false, true},
		{"CompressedDuplicatesString-1000", 1000, true, true, true, false, true},
		{"CompressedDuplicatesString-10000", 10000, true, true, true, false, true},
		// skip short
		{"CompressedDuplicatesString-100000", 100000, true, true, true, true, true},
		{"CompressedDuplicatesString-1000000", 1000000, true, true, true, true, true},
		{"CompressedDuplicatesString-5000000", 5000000, true, true, true, true, true},

		{"UnsafeString-10", 10, false, false, false, false, true},
		{"UnsafeString-100", 100, false, false, false, false, true},
		{"UnsafeString-1000", 1000, false, false, false, false, true},
		{"UnsafeString-10000", 10000, false, false, false, false, true},
		// skip short
		{"UnsafeString-100000", 100000, false, false, false, true, true},
		{"UnsafeString-1000000", 1000000, false, false, false, true, true},
		{"UnsafeString-5000000", 5000000, false, false, false, true, true},

		// dupes
		{"UnsafeDuplicatesString-10", 10, false, false, true, false, true},
		{"UnsafeDuplicatesString-100", 100, false, false, true, false, true},
		{"UnsafeDuplicatesString-1000", 1000, false, false, true, false, true},
		{"UnsafeDuplicatesString-10000", 10000, false, false, true, false, true},
		// skip short
		{"UnsafeDuplicatesString-100000", 100000, false, false, true, true, true},
		{"UnsafeDuplicatesString-1000000", 1000000, false, false, true, true, true},
		{"UnsafeDuplicatesString-5000000", 5000000, false, false, true, true, true},

		{"SafeString-10", 10, false, true, false, false, true},
		{"SafeString-100", 100, false, true, false, false, true},
		{"SafeString-1000", 1000, false, true, false, false, true},
		{"SafeString-10000", 10000, false, true, false, false, true},
		// skip short
		{"SafeString-100000", 100000, false, true, false, true, true},
		{"SafeString-1000000", 1000000, false, true, false, true, true},
		{"SafeString-5000000", 5000000, false, true, false, true, true},

		// dupes
		{"SafeDuplicatesString-10", 10, false, true, true, false, true},
		{"SafeDuplicatesString-100", 100, false, true, true, false, true},
		{"SafeDuplicatesString-1000", 1000, false, true, true, false, true},
		{"SafeDuplicatesString-10000", 10000, false, true, true, false, true},
		// skip short
		{"SafeDuplicatesString-100000", 100000, false, true, true, true, true},
		{"SafeDuplicatesString-1000000", 1000000, false, true, true, true, true},
		{"SafeDuplicatesString-5000000", 5000000, false, true, true, true, true},
	}
	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			if testing.Short() && bm.short {
				b.Skip()
			}

			c := NewConfig()
			if bm.compression {
				c.Compression = Shoco
			}

			oi := NewObjectIntern(c)

			data := make([][]byte, 0, bm.num)
			for i := 0; i < bm.num; i++ {
				data = append(data, []byte(fmt.Sprintf("words%d", i)))
			}

			if bm.dupe {
				for i := 2; i < bm.num; i += 2 {
					data[i] = []byte(fmt.Sprintf("words%d", i-1))
				}
			}

			b.ResetTimer()
			b.ReportAllocs()

			if bm.stringTest {
				for i := 0; i < b.N; i++ {
					for _, obj := range data {
						globalStr, _ = oi.AddOrGetString(obj, bm.safe)
					}
				}
			} else {
				for i := 0; i < b.N; i++ {
					for _, obj := range data {
						globalPtr, _ = oi.AddOrGet(obj, bm.safe)
					}
				}
			}
		})
	}
}

// if you don't use the -short flag while running these benchmarks, they will take
// a very long time to complete
func BenchmarkDelete(b *testing.B) {
	benchmarks := []struct {
		name        string
		num         int
		compression bool
		byByte      bool
		byString    bool
		short       bool
	}{
		// Delete
		{"Uintptr-10", 10, false, false, false, false},
		{"Uintptr-100", 100, false, false, false, false},
		{"Uintptr-1000", 1000, false, false, false, false},
		{"Uintptr-10000", 10000, false, false, false, false},
		// skip short
		{"Uintptr-100000", 100000, false, false, false, true},
		{"Uintptr-1000000", 1000000, false, false, false, true},
		{"Uintptr-5000000", 5000000, false, false, false, true},

		// Delete By Byte
		{"Byte-10", 10, false, true, false, false},
		{"Byte-100", 100, false, true, false, false},
		{"Byte-1000", 1000, false, true, false, false},
		{"Byte-10000", 10000, false, true, false, false},
		// skip short
		{"Byte-100000", 100000, false, true, false, true},
		{"Byte-1000000", 1000000, false, true, false, true},
		{"Byte-5000000", 5000000, false, true, false, true},

		// Delete By Byte Compressed
		{"CompressedByte-10", 10, true, true, false, false},
		{"CompressedByte-100", 100, true, true, false, false},
		{"CompressedByte-1000", 1000, true, true, false, false},
		{"CompressedByte-10000", 10000, true, true, false, false},
		// skip short
		{"CompressedByte-100000", 100000, true, true, false, true},
		{"CompressedByte-1000000", 1000000, true, true, false, true},
		{"CompressedByte-5000000", 5000000, true, true, false, true},

		// Delete By String
		{"String-10", 10, false, false, true, false},
		{"String-100", 100, false, false, true, false},
		{"String-1000", 1000, false, false, true, false},
		{"String-10000", 10000, false, false, true, false},
		// skip short
		{"String-100000", 100000, false, false, true, true},
		{"String-1000000", 1000000, false, false, true, true},
		{"String-5000000", 5000000, false, false, true, true},

		// Delete By String Compressed
		{"CompressedString-10", 10, true, false, true, false},
		{"CompressedString-100", 100, true, false, true, false},
		{"CompressedString-1000", 1000, true, false, true, false},
		{"CompressedString-10000", 10000, true, false, true, false},
		// skip short
		{"CompressedString-100000", 100000, true, false, true, true},
		{"CompressedString-1000000", 1000000, true, false, true, true},
		{"CompressedString-5000000", 5000000, true, false, true, true},
	}
	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			if testing.Short() && bm.short {
				b.Skip()
			}

			c := NewConfig()
			if bm.compression {
				c.Compression = Shoco
			}

			oi := NewObjectIntern(c)

			var ok bool
			var err error

			b.ResetTimer()
			b.ReportAllocs()

			if bm.byByte {
				for i := 0; i < b.N; i++ {
					b.StopTimer()

					data := make([][]byte, 0, bm.num)
					rand.Seed(time.Now().UnixNano())
					l := len(testStrings)

					for i := 0; i < bm.num; i++ {
						data = append(data, []byte(fmt.Sprintf(testStrings[rand.Intn(l)]+"%d", i)))
						oi.AddOrGet(data[i], false)
					}

					b.StartTimer()
					for _, obj := range data {
						ok, err = oi.DeleteByByte(obj)
						if !ok {
							b.Fatalf("Failed to delete byte: %v -- %v", obj, err)
						}
					}
				}
			} else if bm.byString {
				for i := 0; i < b.N; i++ {
					b.StopTimer()

					strs := make([]string, 0, bm.num)
					data := make([][]byte, 0, bm.num)
					rand.Seed(time.Now().UnixNano())
					l := len(testStrings)

					for i := 0; i < bm.num; i++ {
						data = append(data, []byte(fmt.Sprintf(testStrings[rand.Intn(l)]+"%d", i)))
						strs = append(strs, string(data[i]))
						oi.AddOrGet(data[i], false)
					}

					b.StartTimer()
					for _, str := range strs {
						ok, err = oi.DeleteByString(str)
						if !ok {
							b.Fatalf("Failed to delete string: %s -- %v", str, err)
						}
					}
				}
			} else {
				for i := 0; i < b.N; i++ {
					b.StopTimer()

					ptrs := make([]uintptr, 0, bm.num)
					data := make([][]byte, 0, bm.num)
					rand.Seed(time.Now().UnixNano())
					l := len(testStrings)

					for i := 0; i < bm.num; i++ {
						data = append(data, []byte(fmt.Sprintf(testStrings[rand.Intn(l)]+"%d", i)))
						globalPtr, _ = oi.AddOrGet(data[i], false)
						ptrs = append(ptrs, globalPtr)
					}

					b.StartTimer()
					for _, ptr := range ptrs {
						ok, err = oi.Delete(ptr)
						if !ok {
							b.Fatalf("Failed to delete by uintptr: %d -- %v", ptr, err)
						}
					}
				}
			}
		})
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

var globalBSlice []byte

func benchmarkCompress(b *testing.B, cnf ObjectInternConfig) {
	oi := NewObjectIntern(cnf)
	data := []byte("HowTheWindBlowsThroughTheTrees")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		globalBSlice = oi.compress(data)
	}
}

func benchmarkDecompress(b *testing.B, cnf ObjectInternConfig) {
	oi := NewObjectIntern(cnf)
	data := []byte("HowTheWindBlowsThroughTheTrees")
	comp := oi.compress(data)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		globalBSlice, _ = oi.decompress(comp)
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
		globalStr = oi.CompressString(sz)
	}
}

func benchmarkDecompressSz(b *testing.B, cnf ObjectInternConfig, sz string) {
	oi := NewObjectIntern(cnf)
	comp := oi.CompressString(sz)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		globalStr, _ = oi.DecompressString(comp)
	}
}
