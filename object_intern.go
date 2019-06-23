package goi

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
	"unsafe"

	gos "github.com/grafana/go-generic-object-store"
	"github.com/tmthrgd/shoco"
)

// InitialRefCount is appended to all new objects inserted into the store
var InitialRefCount = []byte{0x1, 0x0, 0x0, 0x0}

// ObjectIntern stores a map of uintptrs to interned objects.
// The string key itself uses an interned object for its data pointer
type ObjectIntern struct {
	sync.RWMutex
	conf       ObjectInternConfig
	store      gos.ObjectStore
	objIndex   map[string]uintptr
	compress   func(in []byte) []byte
	decompress func(in []byte) ([]byte, error)
}

// NewObjectIntern returns a new ObjectIntern with the settings
// provided in the ObjectInternConfig.
func NewObjectIntern(c ObjectInternConfig) *ObjectIntern {
	oi := ObjectIntern{
		conf:     c,
		store:    gos.NewObjectStore(100),
		objIndex: make(map[string]uintptr),
	}

	// set compression and decompression functions
	switch oi.conf.Compression {
	case Shoco:
		oi.compress = shoco.Compress
		oi.decompress = shoco.Decompress
	case ShocoDict:
		panic("Compression ShocoDict not implemented yet")
	case None:
		oi.compress = func(in []byte) []byte { return in }
		oi.decompress = func(in []byte) ([]byte, error) { return in, nil }
	default:
		panic(fmt.Sprintf("Compression %d not recognized", oi.conf.Compression))
	}

	return &oi
}

// CompressionFunc returns the current compression func used by the library
func (oi *ObjectIntern) CompressionFunc() func(in []byte) []byte {
	return oi.compress
}

// DecompressionFunc returns the current decompression func used by the library
func (oi *ObjectIntern) DecompressionFunc() func(in []byte) ([]byte, error) {
	return oi.decompress
}

// Compress returns a compressed version of in as a []byte
// It is important to keep in mind that not all values can be compressed,
// so this may at times return the original value
func (oi *ObjectIntern) Compress(in []byte) []byte {
	return oi.compress(in)
}

// Decompress returns a decompressed version of in as a []byte and nil on success.
// On failure it returns an error
func (oi *ObjectIntern) Decompress(in []byte) ([]byte, error) {
	return oi.decompress(in)
}

// CompressString returns a compressed version of in as a string
// It is important to keep in mind that not all values can be compressed,
// so this may at times return the original value
func (oi *ObjectIntern) CompressString(in string) string {
	if oi.conf.Compression == None {
		return in
	}
	return string(oi.compress([]byte(in)))
}

// DecompressString returns a decompressed version of string as a string and nil upon success.
// On failure it returns in and an error.
func (oi *ObjectIntern) DecompressString(in string) (string, error) {
	if oi.conf.Compression == None {
		return in, nil
	}
	b, err := oi.decompress([]byte(in))
	return string(b), err
}

// AddOrGet finds or adds an object and returns its uintptr and nil upon success.
// This method takes a []byte of the object, and a bool. If safe is set to true
// then this method will create a copy of the []byte before performing any operations
// that might modify the backing array.
// On failure it returns 0 and an error
//
// If the object is found in the store its reference count is increased by 1.
// If the object is added to the store its reference count is set to 1.
func (oi *ObjectIntern) AddOrGet(obj []byte, safe bool) (uintptr, error) {

	// if either of these two terms is true then the rest of this block
	// requires a lot of allocations
	if (oi.conf.Compression != None) || (safe && oi.conf.Compression == None) {
		objComp := obj

		if oi.conf.Compression != None {
			// this returns a new byte slice, so we don't need to check for safe
			objComp = oi.compress(obj)
		}

		// the only case we need to handle specially is when compression
		// is turned off and the user has requested that the operation be safe
		if safe && oi.conf.Compression == None {
			// create a copy so we don't modify the original []byte
			objComp = make([]byte, len(obj))
			copy(objComp, obj)
		}

		// acquire lock
		oi.Lock()

		// try to find the object in the index
		addr, ok := oi.objIndex[string(objComp)]
		if ok {
			// increment reference count by 1
			(*(*uint32)(unsafe.Pointer(addr + uintptr(len(objComp)))))++
			oi.Unlock()
			return addr, nil
		}

		// The object is not in the index therefore it is not in the store.
		// We need to set its initial reference count to 1 before adding it.
		//
		// The object store backend has no knowledge of a reference count, so
		// we need to manage it at this layer. Here we add 4 bytes to be used
		// henceforth as the reference count for this object. Reference count is
		// always placed as the LAST 4 bytes of an object and is NEVER compressed.
		objComp = append(objComp, InitialRefCount...)
		addr, err := oi.store.Add(objComp)
		if err != nil {
			oi.Unlock()
			return 0, err
		}

		// remove ref count from end before converting to string
		objString := string(objComp[:len(objComp)-4])

		// set objString data to the object inside the object store
		((*reflect.StringHeader)(unsafe.Pointer(&objString))).Data = addr

		// add the object to the index
		oi.objIndex[objString] = addr

		oi.Unlock()
		return addr, nil
	}

	// if neither of those terms is true then we can avoid costly allocations
	// acquire lock
	oi.Lock()

	// try to find the object in the index
	addr, ok := oi.objIndex[string(obj)]
	if ok {
		// increment reference count by 1
		(*(*uint32)(unsafe.Pointer(addr + uintptr(len(obj)))))++
		oi.Unlock()
		return addr, nil
	}

	// The object is not in the index therefore it is not in the store.
	// We need to set its initial reference count to 1 before adding it.
	//
	// The object store backend has no knowledge of a reference count, so
	// we need to manage it at this layer. Here we add 4 bytes to be used
	// henceforth as the reference count for this object. Reference count is
	// always placed as the LAST 4 bytes of an object and is NEVER compressed.
	obj = append(obj, InitialRefCount...)
	addr, err := oi.store.Add(obj)
	if err != nil {
		oi.Unlock()
		return 0, err
	}

	// remove ref count from end before converting to string
	objString := string(obj[:len(obj)-4])

	// set objString data to the object inside the object store
	((*reflect.StringHeader)(unsafe.Pointer(&objString))).Data = addr

	// add the object to the index
	oi.objIndex[objString] = addr

	oi.Unlock()
	return addr, nil

}

// AddOrGetString finds or adds an object and then returns a string with its Data pointer set to the newly interned object and nil.
// This method takes a []byte of the object, and a bool. If safe is set to true
// then this method will create a copy of the []byte before performing any operations
// that might modify the backing array. If compression is turned on this method returns
// a decompressed version of the string, which means it does not use the interned data.
// On failure it returns an empty string and an error
//
// If the object is found in the store its reference count is increased by 1.
// If the object is added to the store its reference count is set to 1.
func (oi *ObjectIntern) AddOrGetString(obj []byte, safe bool) (string, error) {

	// if either of these two terms is true then the rest of this block
	// requires a lot of allocations
	if (oi.conf.Compression != None) || (safe && oi.conf.Compression == None) {
		objComp := obj

		if oi.conf.Compression != None {
			objComp = oi.compress(obj)
		}

		// the only case we need to handle specially is when compression
		// is turned off and the user has requested that the operation be safe
		if safe && oi.conf.Compression == None {
			// create a copy so we don't modify the original []byte
			objComp = make([]byte, len(obj))
			copy(objComp, obj)
		}

		// acquire lock
		oi.Lock()

		// try to find the object in the index
		addr, ok := oi.objIndex[string(objComp)]
		if ok {
			// increment reference count by 1
			(*(*uint32)(unsafe.Pointer(addr + uintptr(len(objComp)))))++
			if oi.conf.Compression == None {
				// create a StringHeader and set its values appropriately
				stringHeader := &reflect.StringHeader{
					Data: addr,
					Len:  len(objComp),
				}
				oi.Unlock()
				return (*(*string)(unsafe.Pointer(stringHeader))), nil
			}
			// don't want to return compressed data, so we create a string from the original object
			oi.Unlock()
			return string(obj), nil
		}

		// The object is not in the index therefore it is not in the store.
		// We need to set its initial reference count to 1 before adding it.
		//
		// The object store backend has no knowledge of a reference count, so
		// we need to manage it at this layer. Here we add 4 bytes to be used
		// henceforth as the reference count for this object. Reference count is
		// always placed as the LAST 4 bytes of an object and is NEVER compressed.
		objComp = append(objComp, InitialRefCount...)
		addr, err := oi.store.Add(objComp)
		if err != nil {
			oi.Unlock()
			return "", err
		}

		// remove ref count from end before converting to string
		objString := string(objComp[:len(objComp)-4])

		// set objString data to the object inside the object store
		(*reflect.StringHeader)(unsafe.Pointer(&objString)).Data = addr

		// add the object to the index
		oi.objIndex[objString] = addr

		oi.Unlock()
		if oi.conf.Compression != None {
			// don't want to return compressed data, so we create a string from the original object
			objString = string(obj)
		}

		// create a StringHeader and set its values appropriately
		stringHeader := &reflect.StringHeader{
			Data: addr,
			Len:  len(objComp),
		}
		return (*(*string)(unsafe.Pointer(stringHeader))), nil
	}

	// if neither of those terms is true then we can avoid costly allocations
	// acquire lock
	oi.Lock()

	// try to find the object in the index
	addr, ok := oi.objIndex[string(obj)]
	if ok {
		// increment reference count by 1
		(*(*uint32)(unsafe.Pointer(addr + uintptr(len(obj)))))++
		if oi.conf.Compression == None {
			// create a StringHeader and set its values appropriately
			stringHeader := &reflect.StringHeader{
				Data: addr,
				Len:  len(obj),
			}
			oi.Unlock()
			return (*(*string)(unsafe.Pointer(stringHeader))), nil
		}
		// don't want to return compressed data, so we create a string from the original object
		oi.Unlock()
		return string(obj), nil
	}

	// The object is not in the index therefore it is not in the store.
	// We need to set its initial reference count to 1 before adding it.
	//
	// The object store backend has no knowledge of a reference count, so
	// we need to manage it at this layer. Here we add 4 bytes to be used
	// henceforth as the reference count for this object. Reference count is
	// always placed as the LAST 4 bytes of an object and is NEVER compressed.
	obj = append(obj, InitialRefCount...)
	addr, err := oi.store.Add(obj)
	if err != nil {
		oi.Unlock()
		return "", err
	}

	// remove ref count from end before converting to string
	objString := string(obj[:len(obj)-4])

	// set objString data to the object inside the object store
	(*reflect.StringHeader)(unsafe.Pointer(&objString)).Data = addr

	// add the object to the index
	oi.objIndex[objString] = addr

	if oi.conf.Compression != None {
		oi.Unlock()
		// don't want to return compressed data, so we create a string from the original object
		return string(obj), nil
	}
	oi.Unlock()
	return objString, nil
}

// GetPtrFromByte finds an interned object and returns its address as a uintptr.
// Upon failure it returns 0 and an error.
//
// This method is designed specifically to be used with map keys that are interned,
// since the only way to retrieve the key itself is by iterating over the entire map.
// This method should be faster than iterating over a map (depending on the size of the map).
// This is usually called directly before deleting an interned map key from its map so that we
// can properly decrement the reference count of that interned object.
//
// This method does not increase the reference count of the interned object.
func (oi *ObjectIntern) GetPtrFromByte(obj []byte) (uintptr, error) {
	if oi.conf.Compression != None {
		oi.RLock()
		// try to find the compressed object in the index
		addr, ok := oi.objIndex[string(oi.compress(obj))]
		if ok {
			oi.RUnlock()
			return addr, nil
		}
	}

	oi.RLock()
	// try to find the object in the index
	addr, ok := oi.objIndex[string(obj)]
	if ok {
		oi.RUnlock()
		return addr, nil
	}

	oi.RUnlock()
	return 0, fmt.Errorf("Could not find object in store: %s", string(obj))
}

// GetStringFromPtr returns an interned version of a string stored at objAddr and nil.
// If compression is turned on it returns a non-interned string and nil.
// Upon failure it returns an empty string and an error.
//
// This method does not increase the reference count of the interned object.
func (oi *ObjectIntern) GetStringFromPtr(objAddr uintptr) (string, error) {
	oi.RLock()
	defer oi.RUnlock()

	b, err := oi.store.Get(objAddr)
	if err != nil {
		return "", err
	}

	if oi.conf.Compression != None {
		// get decompressed []byte after removing the trailing 4 bytes for the reference count
		b, err = oi.decompress(b[:len(b)-4])
		// because compression is turned on we can't just set string's Data to the address,
		// we need to actually create a new string from the decompressed []byte
		return string(b), err
	}

	// since compression is turned off, we can simply use the uncompressed interned data for the string
	var tmpString string
	(*reflect.StringHeader)(unsafe.Pointer(&tmpString)).Data = objAddr

	// remove 4 trailing bytes for reference count
	(*reflect.StringHeader)(unsafe.Pointer(&tmpString)).Len = len(b) - 4

	return tmpString, nil
}

// Delete decrements the reference count of an object identified by its address.
// Possible return values are as follows:
//
// true, nil - reference count reached 0 and the object was removed from both the index
// and the object store.
//
// false, nil - reference count was decremented by 1 and no further action was taken.
//
// false, error - the object was not found in the object store or could not be deleted
func (oi *ObjectIntern) Delete(objAddr uintptr) (bool, error) {
	var compObj []byte
	var err error

	// acquire write lock
	oi.Lock()

	// check if object exists in the object store
	compObj, err = oi.store.Get(objAddr)
	if err != nil {
		oi.Unlock()
		return false, err
	}

	// most likely case is that we will just decrement the reference count and return
	if *(*uint32)(unsafe.Pointer(objAddr + uintptr(len(compObj)-4))) > 1 {
		// decrement reference count by 1
		(*(*uint32)(unsafe.Pointer(objAddr + uintptr(len(compObj)-4))))--

		oi.Unlock()
		return false, nil
	}

	// if reference count is 1 or less, delete the object and remove it from index
	// If one of these operations fails it is still safe to perform the other
	// Once we get to this point we are just going to remove all traces of the object

	// delete object from index first
	// If you delete all of the objects in the slab then the slab will be deleted
	// When this happens the memory that the slab was using is MUnmapped, which is
	// the same memory pointed to by the key stored in the ObjIndex. When you try to
	// access the key to delete it from the ObjIndex you will get a SEGFAULT
	//
	// remove 4 trailing bytes for reference count since ObjIndex does not store reference count in the key
	delete(oi.objIndex, string(compObj[:len(compObj)-4]))

	// delete object from object store
	err = oi.store.Delete(objAddr)

	if err == nil {
		oi.Unlock()
		return true, nil
	}
	oi.Unlock()
	return false, err
}

// DeleteByByte decrements the reference count of an object identified by its value as a []byte.
// Possible return values are as follows:
//
// true, nil - reference count reached 0 and the object was removed from both the index
// and the object store.
//
// false, nil - reference count was decremented by 1 and no further action was taken.
//
// false, error - the object was not found in the object store or could not be deleted
func (oi *ObjectIntern) DeleteByByte(obj []byte) (bool, error) {

	if oi.conf.Compression != None {
		oi.RLock()
		// try to find the compressed object in the index
		addr, ok := oi.objIndex[string(oi.compress(obj))]
		if !ok {
			oi.RUnlock()
			return false, fmt.Errorf("Could not find object in store: %s", string(obj))
		}
		oi.RUnlock()
		return oi.Delete(addr)
	}

	oi.RLock()
	// try to find the object in the index
	addr, ok := oi.objIndex[string(obj)]
	if !ok {
		oi.RUnlock()
		return false, fmt.Errorf("Could not find object in store: %s", string(obj))
	}
	oi.RUnlock()
	return oi.Delete(addr)
}

// DeleteByString decrements the reference count of an object identified by its string representation.
//
// Possible return values are as follows:
//
// true, nil - reference count reached 0 and the object was removed from both the index
// and the object store.
//
// false, nil - reference count was decremented by 1 and no further action was taken.
//
// false, error - the object was not found in the object store or could not be deleted
func (oi *ObjectIntern) DeleteByString(obj string) (bool, error) {

	if oi.conf.Compression != None {
		oi.RLock()
		// try to find the compressed object in the index
		addr, ok := oi.objIndex[string(oi.compress([]byte(obj)))]
		if !ok {
			oi.RUnlock()
			return false, fmt.Errorf("Could not find object in store: %s", string(obj))
		}
		oi.RUnlock()
		return oi.Delete(addr)
	}

	oi.RLock()
	// try to find the object in the index
	addr, ok := oi.objIndex[obj]
	if !ok {
		oi.RUnlock()
		return false, fmt.Errorf("Could not find object in store: %s", obj)
	}
	oi.RUnlock()
	return oi.Delete(addr)
}

// RefCnt checks if the object identified by objAddr exists in the
// object store and returns its current reference count and nil on success.
// On failure it returns 0 and an error, which means the object was not found
// in the object store.
func (oi *ObjectIntern) RefCnt(objAddr uintptr) (uint32, error) {
	oi.RLock()
	defer oi.RUnlock()

	// check if object exists in the object store
	compObj, err := oi.store.Get(objAddr)
	if err != nil {
		return 0, err
	}

	// remove 4 trailing bytes for reference count
	return *(*uint32)(unsafe.Pointer(objAddr + uintptr(len(compObj)-4))), nil

}

// ObjBytes returns a []byte and nil on success.
// On failure it returns nil and an error.
//
// WARNING: This can be dangerous. You are able to directly modify the values stored
// in the object store after you retrieve an uncompressed []byte
//
// If compression is turned off, this will return a []byte slice with the backing array
// set to the interned data.
func (oi *ObjectIntern) ObjBytes(objAddr uintptr) ([]byte, error) {
	var err error

	oi.RLock()

	b, err := oi.store.Get(objAddr)
	if err != nil {
		oi.RUnlock()
		return nil, err
	}

	if oi.conf.Compression != None {
		// remove 4 trailing bytes for reference count and decompress
		b, err = oi.decompress(b[:len(b)-4])
		oi.RUnlock()
		return b, err
	}

	oi.RUnlock()
	// remove 4 trailing bytes for reference count
	return b[:len(b)-4], nil
}

// ObjString returns a string and nil on success.
// On failure it returns an empty string and an error.
//
// This method does not use the interned data to create a string,
// instead it allocates a new string.
func (oi *ObjectIntern) ObjString(objAddr uintptr) (string, error) {
	oi.RLock()
	defer oi.RUnlock()

	b, err := oi.store.Get(objAddr)
	if err != nil {
		return "", err
	}

	if oi.conf.Compression != None {
		// remove 4 trailing bytes for reference count and decompress
		b, err := oi.decompress(b[:len(b)-4])
		if err != nil {
			return "", err
		}
		return string(b), nil
	}

	return string(b[:len(b)-4]), nil
}

// Len takes a slice of object addresses, it assumes that compression is turned off.
// Upon success it returns a slice of the lengths of all of the interned objects - the 4 trailing bytes for reference count, and true.
// The returned slice indexes match the indexes of the slice of uintptrs.
// On failure it returns a possibly partial slice of the lengths, and false.
func (oi *ObjectIntern) Len(ptrs []uintptr) (retLn []int, all bool) {
	retLn = make([]int, len(ptrs))
	all = true

	oi.RLock()
	defer oi.RUnlock()

	for idx, ptr := range ptrs {
		b, err := oi.store.Get(ptr)
		if err != nil {
			return retLn, false
		}
		// remove 4 trailing bytes of reference count
		retLn[idx] = len(b) - 4
	}
	return
}

// JoinStrings takes a slice of uintptr and returns a reconstructed string using sep
// as the separator.
func (oi *ObjectIntern) JoinStrings(nodes []uintptr, sep string) (string, error) {
	if oi.conf.Compression != None {
		return oi.joinStringsCompressed(nodes, sep)
	}

	return oi.joinStringsUncompressed(nodes, sep)
}

func (oi *ObjectIntern) joinStringsCompressed(nodes []uintptr, sep string) (string, error) {
	switch len(nodes) {
	case 0:
		return "", fmt.Errorf("Cannot create string from 0 length slice")
	case 1:
		single, err := oi.GetStringFromPtr(nodes[0])
		return single, err
	}

	var bld strings.Builder

	first, err := oi.GetStringFromPtr(nodes[0])
	if err != nil {
		return "", err
	}
	bld.WriteString(first)

	for _, nodePtr := range nodes[1:] {
		tmpString, err := oi.GetStringFromPtr(nodePtr)
		if err != nil {
			return "", err
		}
		bld.WriteString(sep)
		bld.WriteString(tmpString)
	}

	return bld.String(), nil
}

func (oi *ObjectIntern) joinStringsUncompressed(nodes []uintptr, sep string) (string, error) {
	switch len(nodes) {
	case 0:
		return "", fmt.Errorf("Cannot create string from 0 length slice")
	case 1:
		single, err := oi.GetStringFromPtr(nodes[0])
		return single, err
	}

	lengths, complete := oi.Len(nodes)
	if !complete {
		return "", fmt.Errorf("Could not find object in store")
	}

	oi.RLock()
	totalSize := len(sep) * (len(nodes) - 1)
	for _, length := range lengths {
		totalSize += length
	}

	var tmpString string
	var bld strings.Builder
	bld.Grow(totalSize)

	stringHeader := (*reflect.StringHeader)(unsafe.Pointer(&tmpString))

	stringHeader.Data = nodes[0]
	stringHeader.Len = lengths[0]
	bld.WriteString(tmpString)

	for idx, nodePtr := range nodes[1:] {
		stringHeader.Data = nodePtr
		stringHeader.Len = lengths[idx+1]
		bld.WriteString(sep)
		bld.WriteString(tmpString)
	}

	oi.RUnlock()
	return bld.String(), nil
}

func (oi *ObjectIntern) FragStatsByObjSize(objSize uint8) (float32, error) {
	oi.RLock()
	defer oi.RUnlock()
	return oi.store.FragStatsByObjSize(objSize)
}

func (oi *ObjectIntern) FragStatsPerPool() []gos.FragStat {
	oi.RLock()
	defer oi.RUnlock()
	return oi.store.FragStatsPerPool()
}

func (oi *ObjectIntern) FragStatsTotal() (float32, error) {
	oi.RLock()
	defer oi.RUnlock()
	return oi.store.FragStatsTotal()
}

func (oi *ObjectIntern) MemStatsByObjSize(objSize uint8) (uint64, error) {
	oi.RLock()
	defer oi.RUnlock()
	return oi.store.MemStatsByObjSize(objSize)
}

func (oi *ObjectIntern) MemStatsPerPool() []gos.MemStat {
	oi.RLock()
	defer oi.RUnlock()
	return oi.store.MemStatsPerPool()
}

func (oi *ObjectIntern) MemStatsTotal() (uint64, error) {
	oi.RLock()
	defer oi.RUnlock()
	return oi.store.MemStatsTotal()
}
