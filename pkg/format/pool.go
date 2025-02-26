package format

import "sync"

var fmtPool = sync.Pool{
	New: func() interface{} {
		v := make([]byte, MaxRecordSize)
		return &v
	},
}

func poolAlloc() *[]byte {
	return fmtPool.Get().(*[]byte)
}

func poolFree(ptr *[]byte) {
	*ptr = (*ptr)[:MaxRecordSize]
	fmtPool.Put(ptr)
}
