package goi

//TODO: add explanation of each type of compression etc...
// Types of compression
const (
	NOCPRSN = iota
	SHOCO
	SHOCODICT
)

// Config provides a configuration with default settings
var Config = NewConfig()

// ObjectInternConfig holds a configuration to use when creating a new ObjectIntern
type ObjectInternConfig struct {
	CompressionType uint8
	Cache           bool
	MaxCacheSize    uint32
}

// NewConfig returns a new configuration with default settings
//
// CompressType: SHOCO,
// Cache: true,
// MasCacheSize: 157286400,
func NewConfig() *ObjectInternConfig {
	return &ObjectInternConfig{
		CompressionType: SHOCO,
		Cache:           true,
		MaxCacheSize:    157286400, // 150 MB
	}
}
