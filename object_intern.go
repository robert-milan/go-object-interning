package goi

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"

	gos "github.com/grafana/go-generic-object-store"
	"github.com/tmthrgd/shoco"
)

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
		store:    gos.NewObjectStore(c.SlabSize),
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

// getAndIncrement increments the reference count of an object in the
// index and returns its address and true.
//
// Upon failure it returns 0 and false.
//
// The caller is responsible for locking and unlocking.
func (oi *ObjectIntern) getAndIncrement(obj []byte) (uintptr, bool) {
	// try to find the object in the index
	addr, ok := oi.objIndex[string(obj)]
	if ok {
		// increment reference count by 1
		atomic.AddUint32((*uint32)(unsafe.Pointer(addr)), 1)
		return addr, true
	}
	return 0, false
}

// add sets the initial reference count for a new object and adds it to the store and index.
//
// Upon success it returns the address of the newly stored object and nil
//
// If this fails it returns 0 and an error
//
// The caller is responsible for locking and unlocking.
func (oi *ObjectIntern) add(obj []byte) (uintptr, error) {
	objString := string(obj)

	// We need to set its initial reference count to 1 before adding it.
	//
	// The object store backend has no knowledge of a reference count, so
	// we need to manage it at this layer. Here we add 4 bytes to be used
	// henceforth as the reference count for this object. Reference count is
	// always placed as the FIRST 4 bytes of an object and is NEVER compressed.
	obj = append([]byte{0x1, 0x0, 0x0, 0x0}, obj...)
	addr, err := oi.store.Add(obj)
	if err != nil {
		return 0, err
	}

	// set objString data to the object inside the object store
	// we need to add 4 at the beginning for the reference count
	((*reflect.StringHeader)(unsafe.Pointer(&objString))).Data = addr + 4

	// add the object to the index
	oi.objIndex[objString] = addr

	return addr, nil
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

		// if compression is turned off this is likely the least costly and most
		// probable path
		if oi.conf.Compression == None {
			oi.RLock()
			addr, ok := oi.getAndIncrement(obj)
			if ok {
				oi.RUnlock()
				return addr, nil
			}
			oi.RUnlock()
		}

		var objComp []byte

		if oi.conf.Compression != None {
			// this returns a new byte slice, so we don't need to check for safe
			objComp = oi.compress(obj)
		} else {
			// stay safe
			// create a copy so we don't modify the original []byte
			// we add 4 bytes to the capacity in case we need to append a reference count
			objComp = make([]byte, len(obj), len(obj)+4)
			copy(objComp, obj)
		}

		// acquire lock
		oi.RLock()

		addr, ok := oi.getAndIncrement(objComp)
		if ok {
			oi.RUnlock()
			return addr, nil
		}

		oi.RUnlock()

		oi.Lock()

		// re-check everything
		addr, ok = oi.getAndIncrement(objComp)
		if ok {
			oi.Unlock()
			return addr, nil
		}

		addr, err := oi.add(objComp)
		if err != nil {
			oi.Unlock()
			return 0, err
		}

		oi.Unlock()
		return addr, nil
	}

	// if neither of those terms is true then we can avoid costly allocations
	// acquire lock
	oi.RLock()

	addr, ok := oi.getAndIncrement(obj)
	if ok {
		oi.RUnlock()
		return addr, nil
	}

	oi.RUnlock()

	oi.Lock()

	// re-check everything
	addr, ok = oi.getAndIncrement(obj)
	if ok {
		oi.Unlock()
		return addr, nil
	}

	addr, err := oi.add(obj)
	if err != nil {
		oi.Unlock()
		return 0, err
	}

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

		// if compression is turned off this is likely the least costly and most
		// probable path
		if oi.conf.Compression == None {

			//acquire the lock
			oi.RLock()

			addr, ok := oi.getAndIncrement(obj)
			if ok {
				stringHeader := &reflect.StringHeader{
					// add 4 for reference count
					Data: addr + 4,
					Len:  len(obj),
				}
				oi.RUnlock()
				return (*(*string)(unsafe.Pointer(stringHeader))), nil
			}

			oi.RUnlock()
		}

		var objComp []byte

		if oi.conf.Compression != None {
			objComp = oi.compress(obj)
		} else {
			// stay safe
			// create a copy so we don't modify the original []byte
			// we add 4 bytes to the capacity in case we need to append a reference count
			objComp = make([]byte, len(obj), len(obj)+4)
			copy(objComp, obj)
		}

		// acquire lock
		oi.RLock()

		addr, ok := oi.getAndIncrement(objComp)
		if ok {
			if oi.conf.Compression == None {
				// create a StringHeader and set its values appropriately
				stringHeader := &reflect.StringHeader{
					// add 4 for reference count
					Data: addr + 4,
					Len:  len(objComp),
				}
				oi.RUnlock()
				return (*(*string)(unsafe.Pointer(stringHeader))), nil
			}
			// don't want to return compressed data, so we create a string from the original object
			oi.RUnlock()
			return string(obj), nil
		}

		oi.RUnlock()

		oi.Lock()

		// re-check everything
		addr, ok = oi.getAndIncrement(objComp)
		if ok {
			if oi.conf.Compression == None {
				// create a StringHeader and set its values appropriately
				stringHeader := &reflect.StringHeader{
					// add 4 for reference count
					Data: addr + 4,
					Len:  len(objComp),
				}
				oi.Unlock()
				return (*(*string)(unsafe.Pointer(stringHeader))), nil
			}
			// don't want to return compressed data, so we create a string from the original object
			oi.Unlock()
			return string(obj), nil
		}

		addr, err := oi.add(objComp)
		if err != nil {
			oi.Unlock()
			return "", err
		}

		oi.Unlock()
		if oi.conf.Compression != None {
			// don't want to return compressed data, so we create a string from the original object
			return string(obj), nil
		}

		// create a StringHeader and set its values appropriately
		stringHeader := &reflect.StringHeader{
			// add 4 for reference count
			Data: addr + 4,
			Len:  len(objComp),
		}
		return (*(*string)(unsafe.Pointer(stringHeader))), nil
	}

	// if neither of those terms is true then we can avoid costly allocations
	// acquire lock
	oi.RLock()

	addr, ok := oi.getAndIncrement(obj)
	if ok {
		// create a StringHeader and set its values appropriately
		stringHeader := &reflect.StringHeader{
			// add 4 for reference count
			Data: addr + 4,
			Len:  len(obj),
		}
		oi.RUnlock()
		return (*(*string)(unsafe.Pointer(stringHeader))), nil
	}

	oi.RUnlock()

	oi.Lock()

	// re-check everything
	addr, ok = oi.getAndIncrement(obj)
	if ok {
		// create a StringHeader and set its values appropriately
		stringHeader := &reflect.StringHeader{
			// add 4 for reference count
			Data: addr + 4,
			Len:  len(obj),
		}
		oi.Unlock()
		return (*(*string)(unsafe.Pointer(stringHeader))), nil
	}

	addr, err := oi.add(obj)
	if err != nil {
		oi.Unlock()
		return "", err
	}

	// create a StringHeader and set its values appropriately
	stringHeader := &reflect.StringHeader{
		// add 4 for reference count
		Data: addr + 4,
		Len:  len(obj),
	}

	oi.Unlock()
	return (*(*string)(unsafe.Pointer(stringHeader))), nil
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

		oi.RUnlock()
		return 0, fmt.Errorf("Could not find object in store: %s", string(obj))
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
		// get decompressed []byte after removing the leading 4 bytes for the reference count
		b, err = oi.decompress(b[4:])
		// because compression is turned on we can't just set string's Data to the address,
		// we need to actually create a new string from the decompressed []byte
		return string(b), err
	}

	// create a StringHeader and set its values appropriately
	stringHeader := &reflect.StringHeader{
		// add 4 for reference count
		Data: objAddr + 4,
		Len:  len(b) - 4,
	}
	return (*(*string)(unsafe.Pointer(stringHeader))), nil
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
	var obj []byte
	var err error

	// acquire write lock
	oi.RLock()

	// check if object exists in the object store
	obj, err = oi.store.Get(objAddr)
	if err != nil {
		oi.RUnlock()
		return false, err
	}

	// most likely case is that we will just decrement the reference count and return
	if atomic.LoadUint32((*uint32)(unsafe.Pointer(objAddr))) > 1 {
		// decrement reference count by 1
		atomic.AddUint32((*uint32)(unsafe.Pointer(objAddr)), ^uint32(0))

		oi.RUnlock()
		return false, nil
	}

	oi.RUnlock()

	oi.Lock()

	// re-check if object exists in the object store
	obj, err = oi.store.Get(objAddr)
	if err != nil {
		oi.Unlock()
		return false, err
	}

	// most likely case is that we will just decrement the reference count and return
	if atomic.LoadUint32((*uint32)(unsafe.Pointer(objAddr))) > 1 {
		// decrement reference count by 1
		atomic.AddUint32((*uint32)(unsafe.Pointer(objAddr)), ^uint32(0))

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
	// remove 4 leading bytes for reference count since ObjIndex does not store reference count in the key
	delete(oi.objIndex, string(obj[4:]))

	// delete object from object store
	err = oi.store.Delete(objAddr)

	oi.Unlock()

	if err == nil {
		return true, nil
	}
	return false, err
}

// DeleteBatch decrements the reference count or deletes the objects from the store
func (oi *ObjectIntern) DeleteBatch(ptrs []uintptr) {
	var obj []byte
	var err error

	// acquire lock
	oi.RLock()

	toDelete := ptrs[:0]

	for _, p := range ptrs {
		// check if object exists in the object store
		obj, err = oi.store.Get(p)
		if err != nil {
			continue
		}

		// most likely case is that we will just decrement the reference count and return
		if atomic.LoadUint32((*uint32)(unsafe.Pointer(p))) > 1 {
			// decrement reference count by 1
			atomic.AddUint32((*uint32)(unsafe.Pointer(p)), ^uint32(0))
			continue
		}

		toDelete = append(toDelete, p)
	}

	oi.RUnlock()

	if len(toDelete) > 0 {

		oi.Lock()

		for _, p := range toDelete {
			// re-check if object exists in the object store
			obj, err = oi.store.Get(p)
			if err != nil {
				continue
			}

			// most likely case is that we will just decrement the reference count and return
			if atomic.LoadUint32((*uint32)(unsafe.Pointer(p))) > 1 {
				// decrement reference count by 1
				atomic.AddUint32((*uint32)(unsafe.Pointer(p)), ^uint32(0))
				continue
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
			// remove 4 leading bytes for reference count since ObjIndex does not store reference count in the key
			delete(oi.objIndex, string(obj[4:]))

			// delete object from object store
			err = oi.store.Delete(p)
		}

		oi.Unlock()
	}
}

// DeleteBatchUnsafe does the same thing as DeleteBatch, but saves time by not acquiring
// read locks if the objects only need their reference count decremented. This is not safe, and it
// is up to the caller to ensure the objects actually exist in the store. If you are unsure, don't use this
// method.
func (oi *ObjectIntern) DeleteBatchUnsafe(ptrs []uintptr) {

	toDelete := ptrs[:0]

	for _, p := range ptrs {
		// most likely case is that we will just decrement the reference count and return
		if atomic.LoadUint32((*uint32)(unsafe.Pointer(p))) > 1 {
			// decrement reference count by 1
			atomic.AddUint32((*uint32)(unsafe.Pointer(p)), ^uint32(0))
			continue
		}

		toDelete = append(toDelete, p)
	}

	// this should happen infrequently in most cases
	if len(toDelete) > 0 {

		var obj []byte
		var err error

		oi.Lock()

		for _, p := range toDelete {
			// re-check if object exists in the object store
			obj, err = oi.store.Get(p)
			if err != nil {
				continue
			}

			// most likely case is that we will just decrement the reference count and return
			if atomic.LoadUint32((*uint32)(unsafe.Pointer(p))) > 1 {
				// decrement reference count by 1
				atomic.AddUint32((*uint32)(unsafe.Pointer(p)), ^uint32(0))
				continue
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
			// remove 4 leading bytes for reference count since ObjIndex does not store reference count in the key
			delete(oi.objIndex, string(obj[4:]))

			// delete object from object store
			err = oi.store.Delete(p)
		}

		oi.Unlock()
	}
}

// DeleteUnsafe is just like Delete but it doesn't acquire read locks or perform
// checks to ensure that the object at the address exists. This is a dangerous method and
// should only be used if you know what you are doing.
func (oi *ObjectIntern) DeleteUnsafe(objAddr uintptr) (bool, error) {
	// most likely case is that we will just decrement the reference count and return
	if atomic.LoadUint32((*uint32)(unsafe.Pointer(objAddr))) > 1 {
		// decrement reference count by 1
		atomic.AddUint32((*uint32)(unsafe.Pointer(objAddr)), ^uint32(0))
		return false, nil
	}

	oi.Lock()

	obj, err := oi.store.Get(objAddr)
	if err != nil {
		oi.Unlock()
		return false, err
	}

	// most likely case is that we will just decrement the reference count and return
	if atomic.LoadUint32((*uint32)(unsafe.Pointer(objAddr))) > 1 {
		// decrement reference count by 1
		atomic.AddUint32((*uint32)(unsafe.Pointer(objAddr)), ^uint32(0))

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
	// remove 4 leading bytes for reference count since ObjIndex does not store reference count in the key
	delete(oi.objIndex, string(obj[4:]))

	// delete object from object store
	err = oi.store.Delete(objAddr)

	oi.Unlock()

	if err == nil {
		return true, nil
	}
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
	_, err := oi.store.Get(objAddr)
	if err != nil {
		return 0, err
	}

	return atomic.LoadUint32((*uint32)(unsafe.Pointer(objAddr))), nil
}

// IncRefCnt increments the reference count of an object interned in the store.
// On failure it returns false and an error, on success it returns true and nil
func (oi *ObjectIntern) IncRefCnt(objAddr uintptr) (bool, error) {
	oi.RLock()
	_, err := oi.store.Get(objAddr)
	if err != nil {
		oi.RUnlock()
		return false, err
	}

	// increment reference count by 1
	atomic.AddUint32((*uint32)(unsafe.Pointer(objAddr)), 1)

	oi.RUnlock()
	return true, nil
}

// IncRefCntUnsafe increments the reference count of an object interned in the store.
// This method does not perform any safety checks and it is upon the user to ensure
// that the object actually exists in the store. There is no return value because
// if used improperly this will likely result in corrupt data or a panic. This method
// is dangerous, use at your own risk.
func (oi *ObjectIntern) IncRefCntUnsafe(objAddr uintptr) {
	// increment reference count by 1
	atomic.AddUint32((*uint32)(unsafe.Pointer(objAddr)), 1)
}

// IncRefCntByString increments the reference count of an object interned in the store.
// On failure it returns false and an error, on success it returns true and nil
func (oi *ObjectIntern) IncRefCntByString(obj string) (bool, error) {
	if oi.conf.Compression != None {
		obj = string(oi.compress([]byte(obj)))
	}

	// acquire read lock
	oi.RLock()

	// try to find the object in the index
	addr, ok := oi.objIndex[obj]
	if !ok {
		oi.RUnlock()
		return false, fmt.Errorf("Could not find object in store")
	}

	oi.RUnlock()
	return oi.IncRefCnt(addr)
}

// IncRefCntBatch increments the reference count of objects interned in the store.
func (oi *ObjectIntern) IncRefCntBatch(ptrs []uintptr) {
	oi.RLock()
	for _, p := range ptrs {

		_, err := oi.store.Get(p)
		if err != nil {
			continue
		}

		// increment reference count by 1
		atomic.AddUint32((*uint32)(unsafe.Pointer(p)), 1)

	}
	oi.RUnlock()
}

// IncRefCntBatchUnsafe increments the reference count of objects interned in the store.
// Since these operations are atomic we don't need to acquire any read locks, but it is
// up to the caller to ensure the objects actually exist. If you are not sure, use the safer method.
func (oi *ObjectIntern) IncRefCntBatchUnsafe(ptrs []uintptr) {
	for _, p := range ptrs {
		// increment reference count by 1
		atomic.AddUint32((*uint32)(unsafe.Pointer(p)), 1)
	}
}

// ObjBytes returns a []byte and nil on success.
// On failure it returns nil and an error.
//
// WARNING: This can be dangerous. You are able to directly modify the values stored
// in the object store after you retrieve an uncompressed []byte
//
// If compression is turned off, this will return a []byte slice with the backing array
// set to the interned data, otherwise it will return a new decompressed []byte
func (oi *ObjectIntern) ObjBytes(objAddr uintptr) ([]byte, error) {
	var err error

	oi.RLock()
	defer oi.RUnlock()

	b, err := oi.store.Get(objAddr)
	if err != nil {
		return nil, err
	}

	if oi.conf.Compression != None {
		// remove 4 leading bytes for reference count and decompress
		b, err = oi.decompress(b[4:])
		return b, err
	}

	// remove 4 leading bytes for reference count
	return b[4:], nil
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
		// remove 4 leading bytes for reference count and decompress
		b, err := oi.decompress(b[4:])
		if err != nil {
			return "", err
		}
		return string(b), nil
	}

	return string(b[4:]), nil
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
		// remove 4 leading bytes of reference count
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

	stringHeader.Data = nodes[0] + 4
	stringHeader.Len = lengths[0]
	bld.WriteString(tmpString)

	for idx, nodePtr := range nodes[1:] {
		stringHeader.Data = nodePtr + 4
		stringHeader.Len = lengths[idx+1]
		bld.WriteString(sep)
		bld.WriteString(tmpString)
	}

	oi.RUnlock()
	return bld.String(), nil
}

// Reset empties the object store and index and re-initializes them.
// This method should really only be used during testing, or if you
// are absolutely certain that no one is going to try to reference a
// previously interned object.
// Returns nil on success and an error on failure.
func (oi *ObjectIntern) Reset() error {
	var err error
	oi.Lock()
	for obj, addr := range oi.objIndex {
		// delete object from index first
		// If you delete all of the objects in the slab then the slab will be deleted
		// When this happens the memory that the slab was using is MUnmapped, which is
		// the same memory pointed to by the key stored in the ObjIndex. When you try to
		// access the key to delete it from the ObjIndex you will get a SEGFAULT
		delete(oi.objIndex, obj)

		// delete object from object store
		err = oi.store.Delete(addr)
		if err != nil {
			return err
		}
	}

	oi.store = gos.NewObjectStore(oi.conf.SlabSize)
	oi.objIndex = make(map[string]uintptr)

	oi.Unlock()
	return nil
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
