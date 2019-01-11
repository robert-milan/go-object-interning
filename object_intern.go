package goi

import (
	"fmt"

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

// AddOrGet finds or adds and then returns a uintptr to an object
func (oi *ObjectIntern) AddOrGet(obj []byte) uintptr {
	objComp := oi.Compress(obj)
	fmt.Println("Len: ", len(objComp))
	return 0
}
