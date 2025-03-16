package match

type bitMaskT uint64

func (m *bitMaskT) Set(slot int) {
	*m |= (1 << uint64(slot))
}

func (m *bitMaskT) Clr(slot int) {
	*m &= ^(1 << uint64(slot))
}

func (m *bitMaskT) Reset() {
	*m = 0
}

func (m bitMaskT) Zeros() bool {
	return m == 0
}

func (m bitMaskT) FirstN(n int) bool {
	mask := bitMaskT(1)<<n - 1
	return m&mask == mask
}

func (m bitMaskT) IsSet(slot int) bool {
	return (m & bitMaskT(1<<slot)) != 0
}
