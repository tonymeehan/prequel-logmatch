package pool

import "sync"

const (
	MaxRecordSize = 4 << 20 // 4 Megabyte max record size
)

var bufferPool = sync.Pool{
	New: func() interface{} {
		v := make([]byte, MaxRecordSize)
		return &v
	},
}

func PoolAlloc() *[]byte {
	return bufferPool.Get().(*[]byte)
}

func PoolFree(ptr *[]byte) {
	*ptr = (*ptr)[:MaxRecordSize]
	bufferPool.Put(ptr)
}
