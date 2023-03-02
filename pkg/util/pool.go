package util

type ChanPool struct {
	chPool chan interface {}
	New func() interface {}
}

func NewChanPool(chansize int, f func() interface{}) ( *ChanPool) {
	p := &ChanPool {
		chPool: make(chan interface{}, chansize),
		New: f,
	}
	
	return p
}

func (p *ChanPool) Get() (item interface{}) {
	select {
		case item = <- p.chPool:
		default:
			item = p.New()
	}
	
	return item 
}

func (p *ChanPool) Put(item interface{}) {
	select {
		case p.chPool <- item:
		default:
			// do nothing, will be gc
	}
}
	
	
	