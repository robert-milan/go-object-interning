package goi

import (
	"unsafe"

	gos "github.com/replay/go-generic-object-store"
	"github.com/tmthrgd/shoco"
)

// ObjectIntern stores a map of slices to memory addresses of previously interned objects
type ObjectIntern struct {
	conf       *ObjectInternConfig
	Store      gos.ObjectStore
	Objects    map[uint8][]uintptr
	Compress   func(in []byte) []byte
	Decompress func(in []byte) ([]byte, error)
}

// NewObjectIntern returns a new ObjectIntern. Pass in
// nil if you want to use the default configuration
func NewObjectIntern(c *ObjectInternConfig) *ObjectIntern {
	oi := &ObjectIntern{
		conf:    Config,
		Store:   gos.NewObjectStore(100),
		Objects: make(map[uint8][]uintptr),
	}
	if c != nil {
		oi.conf = c
	}

	// set compression and decompression functions
	switch oi.conf.CompressionType {
	case SHOCO:
		oi.Compress = func(in []byte) []byte {
			return shoco.Compress(in)
		}
		oi.Decompress = func(in []byte) ([]byte, error) {
			b, err := shoco.Decompress(in)
			return b, err
		}
	default:
		oi.Compress = func(in []byte) []byte {
			return in
		}
		oi.Decompress = func(in []byte) ([]byte, error) {
			return in, nil
		}
	}

	return oi
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

	objComp := oi.Compress(obj)
	addr, ok = oi.Store.Search(objComp)
	if ok {
		// increment reference count by 1
		(*(*uint32)(unsafe.Pointer(addr + uintptr(len(objComp)))))++
		return addr, nil
	}

	objComp = append(objComp, []byte(uint32(1)))
	addr, err = oi.Store.Add(objComp)
	if err != nil {
		return 0, err
	}
	return addr, nil
}

// Object stores an object address and a reference count
type Object struct {
	addr   gos.ObjAddr
	refCnt uint32
}
