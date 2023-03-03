package proc

// SUCCESS: NoError, NoKey, MarkedDelete

var _ ITwoPhaseProcessor = (*UDFSetProcessor)(nil)

type UDFSetProcessor struct {
	SetProcessor
}

func NewUDFSetProcessor() *UDFSetProcessor {
	p := &UDFSetProcessor{
		SetProcessor: SetProcessor{},
	} //proto.OpCodeUDFSet
	p.self = p
	return p
}
