package proc

import (
	"juno/pkg/proto"
)

var _ IOnePhaseProcessor = (*DestroyProcessor)(nil)

type DestroyProcessor struct {
	OnePhaseProcessor
}

func newDestroyProcessor() *DestroyProcessor {
	p := &DestroyProcessor{
		OnePhaseProcessor: OnePhaseProcessor{
			ssRequestOpCode: proto.OpCodeDelete,
		},
	}
	p.self = p
	return p
}

func (p *DestroyProcessor) sendInitRequests() {
	raw := &p.request.raw
	raw.ShallowCopy(p.requestContext.GetMessage())
	proto.SetShardId(raw, p.shardId)
	if err := p.request.setOpCode(p.ssRequestOpCode); err != nil {
		p.replyStatusToClient(proto.OpStatusBadMsg) // TODO revisit
		return
	}
	for i := 0; i < p.ssGroup.numAvailableSSs; i++ {
		p.sendRequest()
	}
	if p.numSSRequestSent < confNumWrites {
		if p.request.numFailToSend == p.request.numFailToSendNoConn {
			p.replyStatusToClient(proto.OpStatusNoStorageServer)
		} else {
			p.replyStatusToClient(proto.OpStatusBusy)

		}
	}
}

func (p *DestroyProcessor) onSuccess(rc *SSRequestContext) {
	p.OnePhaseProcessor.onSuccess(rc)

	if p.succeeded() {
		var resp ResponseWrapper
		resp.ssRequest = rc
		m := rc.ssResponse.GetMessage()
		proto.SetOpStatus(m, proto.OpStatusNoError)
		p.replyToClient(&resp)
	}
}

func (p *DestroyProcessor) onFailure(rc *SSRequestContext) {
	p.request.onFailure(rc)
	if p.failed() {
		p.replyStatusToClient(p.errorResponseOpStatus())
	}
}

func (p *DestroyProcessor) OnResponseReceived(rc *SSRequestContext) {
	switch rc.ssResponseOpStatus {
	case proto.OpStatusNoError, proto.OpStatusNoKey:
		p.onSuccess(rc)
		return
	case proto.OpStatusBadParam:
		p.onFailure(rc)
	default:
		//glog.Warning("destory. error: ", proto.OpStatusNames[opStatus], "opCode: ", proto.OpCodeNameMap[opCode])
		p.onFailure(rc)
	}
}

func (p *DestroyProcessor) OnSSTimeout(rc *SSRequestContext) {
	p.onFailure(rc) /// TODO proto.OpStatusNoStorageServer)
}

func (p *DestroyProcessor) OnSSIOError(rc *SSRequestContext) {
	p.pendingResponses[rc.ssIndex] = nil
	p.onFailure(rc) ///TODO proto.OpStatusNoStorageServer)
}

func (p *DestroyProcessor) errorResponseOpStatus() (st proto.OpStatus) {
	if (p.request.getNumErrorResponse() == 0) || (p.request.getNumIOAndTimeout() >= confNumWrites) {
		if p.request.getNumNoStageErrors() >= confNumWrites {
			st = proto.OpStatusNoStorageServer
		} else {
			st = proto.OpStatusBusy
		}
		return
	}
	st = p.request.errorResponses[0].ssRequest.ssRespOpMsg.GetOpStatus()
	return
}
