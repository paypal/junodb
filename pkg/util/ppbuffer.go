package util

// embadding based inheritance does not work across package
// so copy of the Buffer class from bytes for now

type PPBuffer struct {
	Buffer 
}

func (b *PPBuffer) Resize(n int) {
	b.Reset()
	if (n > cap(b.buf)) {
		b.Grow(n)
	}
	
	b.buf = b.buf[b.off:n];
}

func NewPPBuffer(buf []byte) *PPBuffer { return &PPBuffer{Buffer{buf: buf}} }