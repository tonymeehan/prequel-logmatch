package format

import "testing"

func BenchmarkPool(b *testing.B) {

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		poolSeq(b)
	}
}

func poolSeq(*testing.B) {
	v := poolAlloc()
	(*v)[0] = 1
	defer poolFree(v)
}
