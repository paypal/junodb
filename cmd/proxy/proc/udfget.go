package proc

import (
	"juno/pkg/proto"
	"juno/pkg/udf"
	"juno/third_party/forked/golang/glog"
)

var _ IOnePhaseProcessor = (*UDFGetProcessor)(nil)

type UDFGetProcessor struct {
	GetProcessor
}

func NewUDFGetProcessor() *UDFGetProcessor {
	p := &UDFGetProcessor{
		GetProcessor: GetProcessor{
			OnePhaseProcessor: OnePhaseProcessor{
				ssRequestOpCode: proto.OpCodeRead,
			},
		},
	} //proto.OpCodeUDFGet
	p.self = p
	return p
}

func (p *UDFGetProcessor) Init() {
	p.GetProcessor.Init()
}

func (p *UDFGetProcessor) needApplyUDF() bool {
	return true
}
func (p *UDFGetProcessor) applyUDF(opmsg *proto.OperationalMessage) {
	mgr := udf.GetUDFManager()
	udfname := p.clientRequest.GetUDFName()

	if udf := mgr.GetUDF(string(udfname)); udf != nil {
		if res, err := udf.Call([]byte(""), opmsg.GetPayload().GetData(), p.clientRequest.GetPayload().GetData()); err == nil {
			opmsg.GetPayload().SetPayload(proto.PayloadTypeClear, res)
		} else {
			glog.Info("udf not exist")
		}
	}
}
