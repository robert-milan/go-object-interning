package goi

import (
	"reflect"
	"sync"
	"unsafe"

	gos "github.com/replay/go-generic-object-store"
	"github.com/tmthrgd/shoco"
)

// ObjectIntern stores a map of slices to memory addresses of previously interned objects
type ObjectIntern struct {
	sync.RWMutex
	conf       *ObjectInternConfig
	Store      gos.ObjectStore
	ObjCache   map[string]uintptr
	compress   func(in []byte) []byte
	decompress func(in []byte) ([]byte, error)
}

// NewObjectIntern returns a new ObjectIntern. Pass in
// nil if you want to use the default configuration
func NewObjectIntern(c *ObjectInternConfig) *ObjectIntern {
	oi := &ObjectIntern{
		conf:     Config,
		Store:    gos.NewObjectStore(100),
		ObjCache: make(map[string]uintptr),
	}
	if c != nil {
		oi.conf = c
	}

	// set compression and decompression functions
	switch oi.conf.CompressionType {
	case SHOCO:
		oi.compress = func(in []byte) []byte {
			return shoco.Compress(in)
		}
		oi.decompress = func(in []byte) ([]byte, error) {
			b, err := shoco.Decompress(in)
			return b, err
		}
	default:
		oi.compress = func(in []byte) []byte {
			return in
		}
		oi.decompress = func(in []byte) ([]byte, error) {
			return in, nil
		}
	}

	return oi
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

// CompressSz returns a compressed version of in as a string
// It is important to keep in mind that not all values can be compressed,
// so this may at times return the original value
func (oi *ObjectIntern) CompressSz(in string) string {
	if oi.conf.CompressionType == NOCPRSN {
		return in
	}
	return string(oi.compress([]byte(in)))
}

// DecompressSz returns a decompressed version of string as a string and nil upon success.
// On failure it returns in and an error
func (oi *ObjectIntern) DecompressSz(in string) (string, error) {
	if oi.conf.CompressionType == NOCPRSN {
		return in, nil
	}
	b, err := oi.decompress([]byte(in))
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// AddOrGet finds or adds and then returns a uintptr to an object and nil.
// On failure it returns 0 and an error
//
// If the object is found in the store its reference count is increased by 1.
// If the object is added to the store its reference count is set to 1.
func (oi *ObjectIntern) AddOrGet(obj []byte) (uintptr, error) {
	var addr gos.ObjAddr
	var ok bool
	var err error
	var objSz string

	// compress the object before searching for it
	objComp := oi.compress(obj)
	objSz = string(objComp)

	// acquire read lock
	oi.RLock()

	// try to find the object in the cache
	addr, ok = oi.ObjCache[objSz]
	if ok {
		// increment reference count by 1
		(*(*uint32)(unsafe.Pointer(addr + uintptr(len(objComp)))))++
		oi.RUnlock()
		return addr, nil
	}

	// acquire write lock
	oi.RUnlock()
	oi.Lock()

	// check if object was added before we re-acquired the lock
	addr, ok = oi.ObjCache[objSz]
	if ok {
		// increment reference count by 1
		(*(*uint32)(unsafe.Pointer(addr + uintptr(len(objComp)))))++
		oi.Unlock()
		return addr, nil
	}

	// The object is not in the cache therefore it is not in the store.
	// We need to set its initial reference count to 1 before adding it
	objComp = append(objComp, []byte{0x1, 0x0, 0x0, 0x0}...)
	addr, err = oi.Store.Add(objComp)
	if err != nil {
		oi.Unlock()
		return 0, err
	}

	// set objSz data to the object inside the object store
	((*reflect.StringHeader)(unsafe.Pointer(&objSz))).Data = addr

	// add the object to the cache
	oi.ObjCache[objSz] = addr

	oi.Unlock()
	return addr, nil
}

// Delete decrements the reference count of an object identified by its address.
// Possible return values are as follows:
//
// true, nil - reference count reached 0 and the object was removed from both the cache
// and the object store.
//
// false, nil - reference count was decremented by 1 and no further action was taken.
//
// false, error - the object was not found in the object store or could not be deleted
func (oi *ObjectIntern) Delete(objAddr uintptr) (bool, error) {
	var compObj []byte
	var err error

	// TODO: think about removing the first check as it is highly unlikely
	// that the object won't exist and we may just be wasting cpu and adding
	// complexity here. Instead just go straight to a write lock.

	// acquire read lock
	oi.RLock()
	// check if object exists in the object store
	compObj, err = oi.Store.Get(objAddr)
	if err != nil {
		oi.RUnlock()
		return false, err
	}

	// acquire write lock
	oi.RUnlock()
	oi.Lock()

	// re-check if object still exists in the object store because
	// there is a small chance that it has changed while we were
	// re-acquiring the new lock
	compObj, err = oi.Store.Get(objAddr)
	if err != nil {
		oi.Unlock()
		return false, err
	}

	// if reference count is 1, delete the object and remove it from cache
	if *(*uint32)(unsafe.Pointer(objAddr + uintptr(len(compObj)-4))) == 1 {
		// If one of these operations fails it is still safe to perform the other
		// Once we get to this point we are just going to remove all traces of the object

		// TODO: check order of delete operations and how it affects
		// actual memory in the object store. Does this clear the data
		// in the store?

		// delete object from cache
		delete(oi.ObjCache, string(compObj[:len(compObj)-4]))

		// delete object from object store
		err := oi.Store.Delete(objAddr)
		if err == nil {
			oi.Unlock()
			return true, nil
		}
		oi.Unlock()
		return false, err
	}

	// decrement reference count by 1
	(*(*uint32)(unsafe.Pointer(objAddr + uintptr(len(compObj)-4))))--

	oi.Unlock()
	return false, nil
}

// RefCnt checks if the object identified by objAddr exists in the
// object store and returns its current reference count and nil on success.
// On failure it returns 0 and an error, which means the object was not found
// in the object store.
func (oi *ObjectIntern) RefCnt(objAddr uintptr) (uint32, error) {
	oi.RLock()
	defer oi.RUnlock()

	// check if object exists in the object store
	compObj, err := oi.Store.Get(objAddr)
	if err != nil {
		return 0, err
	}

	return *(*uint32)(unsafe.Pointer(objAddr + uintptr(len(compObj)))), nil

}

// ObjBytes returns a []byte and nil on success.
// On failure it returns nil and an error
func (oi *ObjectIntern) ObjBytes(objAddr uintptr) ([]byte, error) {
	oi.RLock()
	defer oi.RUnlock()

	b, err := oi.Store.Get(objAddr)
	if err != nil {
		return nil, err
	}
	// remove 4 trailing bytes for reference count and decompress
	objDecomp, err := oi.decompress(b[:len(b)-4])
	if err != nil {
		return nil, err
	}
	return objDecomp, nil
}

// ObjString returns a string and nil on success.
// On failure it returns an empty string and an error
func (oi *ObjectIntern) ObjString(objAddr uintptr) (string, error) {
	oi.RLock()
	defer oi.RUnlock()

	b, err := oi.Store.Get(objAddr)
	if err != nil {
		return "", err
	}
	// remove 4 trailing bytes for reference count and decompress
	objDecomp, err := oi.decompress(b[:len(b)-4])
	if err != nil {
		return "", err
	}
	return string(objDecomp), nil
}
