package goi

// ObjectIntern stores a map of slices to memory addresses of previously interned objects
type ObjectIntern struct {
	Objects map[uint8][]uintptr
}

// NewObjectIntern returns a new ObjectIntern
func NewObjectIntern(c *ObjectInternConfig) *ObjectIntern {
	if c == nil {
		c = Config
	}
	return &ObjectIntern{
		Objects: make(map[uint8][]uintptr),
	}
}

// AddOrGet finds or adds and then returns a uintptr to an object
func (o *ObjectIntern) AddOrGet([]byte) uintptr {
	return 0
}
