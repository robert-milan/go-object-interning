package goi

import (
	"github.com/replay/go-generic-object-store"
)

type Cache struct {
	Items []CacheItem
}

type CacheItem struct {
	Addr   gos.ObjAddr
	Data   []byte
	RefCnt uint32
	// free space due to alignment issues
	// use for something else
	paddedSpace uint32
}
