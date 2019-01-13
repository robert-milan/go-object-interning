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
	compress   func(in []byte) []byte
	decompress func(in []byte) ([]byte, error)
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
func (oi *ObjectIntern) Compress(in []byte) []byte {
	return oi.compress(in)
}

// Decompress returns a decompressed version of in as a []byte and nil on success.
// On failure it returns an error
func (oi *ObjectIntern) Decompress(in []byte) ([]byte, error) {
	return oi.decompress(in)
}

// CompressSz returns a compressed version of in as a string
func (oi *ObjectIntern) CompressSz(in string) string {
	return string(oi.compress([]byte(in)))
}

// DecompressSz returns a decompressed version of string as a string and nil upon success.
// On failure it returns an empty string and error
func (oi *ObjectIntern) DecompressSz(in string) (string, error) {
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

	// compress the object before searching for it
	objComp := oi.compress(obj)
	addr, ok = oi.Store.Search(objComp)
	if ok {
		// increment reference count by 1
		(*(*uint32)(unsafe.Pointer(addr + uintptr(len(objComp)))))++
		return addr, nil
	}

	// The object is not in the store so we need to set its initial
	// reference count to 1 before adding it
	objComp = append(objComp, []byte{0x1, 0x0, 0x0, 0x0}...)
	addr, err = oi.Store.Add(objComp)
	if err != nil {
		return 0, err
	}
	return addr, nil
}

// ObjBytes returns a []byte and nil on success.
// On failure it returns nil and an error
func (oi *ObjectIntern) ObjBytes(objAddr uintptr) ([]byte, error) {
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

// Object stores an object address and a reference count
type Object struct {
	addr   gos.ObjAddr
	refCnt uint32
}
