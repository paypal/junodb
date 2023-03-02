package proc

import (
	"juno/cmd/proxy/stats"
	"juno/pkg/proto"
	"juno/pkg/util"
)

type ReqProcessorPool struct {
	procPool *util.ChanPool
	maxCount int32
	curCount *util.AtomicCounter
}

func NewRequestProcessorPool(chansize int32, maxsize int32, op proto.OpCode) *ReqProcessorPool {

	procPool := util.NewChanPool(int(chansize), func() interface{} {
		var p IRequestProcessor

		switch op {
		case proto.OpCodeCreate:
			p = NewCreateProcessor()
		case proto.OpCodeGet:
			p = NewGetProcessor()
		case proto.OpCodeUpdate:
			p = NewUpdateProcessor()
		case proto.OpCodeSet:
			p = NewSetProcessor()
		case proto.OpCodeDestroy:
			p = newDestroyRequestProcessor()
		case proto.OpCodeUDFGet:
			p = NewUDFGetProcessor()
		case proto.OpCodeUDFSet:
			p = NewSetProcessor()
			//p = NewUDFSetProcessor()
		default:
			return nil
		}
		p.Init()
		return p
	})

	var counter *util.AtomicCounter
	switch op {
	case proto.OpCodeCreate:
		counter = stats.GetActiveCreateCounter()
	case proto.OpCodeGet:
		counter = stats.GetActiveGetCounter()
	case proto.OpCodeUpdate:
		counter = stats.GetActiveUpdateCounter()
	case proto.OpCodeSet:
		counter = stats.GetActiveSetCounter()
	case proto.OpCodeDestroy:
		counter = stats.GetActiveDestroyCounter()
	case proto.OpCodeUDFGet:
		counter = stats.GetActiveUDFGetCounter()
	case proto.OpCodeUDFSet:
		counter = stats.GetActiveUDFSetCounter()
	default:
	}

	return &ReqProcessorPool{procPool, maxsize, counter}
}

func (p *ReqProcessorPool) GetProcessor() IRequestProcessor {

	// reached absolute max, should reject or queue request
	if p.GetCount() >= p.maxCount {
		return nil
	}

	if p.curCount != nil {
		p.curCount.Add(1)
	}
	return p.procPool.Get().(IRequestProcessor)
}

func (p *ReqProcessorPool) PutProcessor(proc IRequestProcessor) {
	proc.Init()
	p.procPool.Put(proc)
	if p.curCount != nil {
		p.curCount.Add(-1)
	}
}

func (p *ReqProcessorPool) DecreaseCount() {
	if p.curCount != nil {
		p.curCount.Add(-1)
	}
}

func (p *ReqProcessorPool) GetCount() int32 {
	if p.curCount != nil {
		return p.curCount.Get()
	} else {
		return 0
	}
}
