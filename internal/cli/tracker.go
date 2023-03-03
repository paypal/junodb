package cli

import (
	"fmt"
	"time"

	"juno/third_party/forked/golang/glog"

	"juno/pkg/util"
)

type PendingRequest struct {
	reqCtx       *RequestContext
	sequence     uint32
	timeSent     time.Time
	timeToExpire time.Time
}

type PendingResponseMap map[uint32]*PendingRequest

type PendingTracker struct {
	mapRequestsSent PendingResponseMap
	pendingQueue    []*PendingRequest
	responseTimer   *util.TimerWrapper
	requestTimeout  time.Duration
}

func newPendingTracker(requestTimeout time.Duration) *PendingTracker {
	return &PendingTracker{
		mapRequestsSent: make(PendingResponseMap),
		responseTimer:   util.NewTimerWrapper(requestTimeout),
		requestTimeout:  requestTimeout,
	}
}

func (p *PendingTracker) GetTimeoutCh() <-chan time.Time {
	return p.responseTimer.GetTimeoutCh()
}

func (p *PendingTracker) OnRequestSent(reqCtx *RequestContext, sequence uint32) {
	now := time.Now()
	pending := &PendingRequest{reqCtx: reqCtx, sequence: sequence, timeSent: now, timeToExpire: now.Add(p.requestTimeout)}
	p.pendingQueue = append(p.pendingQueue, pending)
	if v, found := p.mapRequestsSent[sequence]; found {
		glog.Fatalf("wrong sequence: %v", v)
	}
	p.mapRequestsSent[sequence] = pending
	if p.responseTimer.IsStopped() {
		p.responseTimer.Reset(p.requestTimeout)
	}
}

func (p *PendingTracker) OnTimeout(now time.Time) {
	p.responseTimer.Stop() ///TODO
	sz := len(p.pendingQueue)
	if sz != 0 {
		queue := p.pendingQueue
		p.pendingQueue = p.pendingQueue[:0]

		var i int
		for i = 0; i < sz; i++ {
			pr := queue[i]
			if pr.reqCtx != nil {
				if pr.timeToExpire.After(now) {
					p.responseTimer.Reset(pr.timeToExpire.Sub(now))
					break
				} else {
					//				st.cancelFunc()
					seq := pr.sequence
					err := fmt.Errorf("request timeout")
					if _, found := p.mapRequestsSent[seq]; found {
						req := pr.reqCtx.request
						if req != nil {
							glog.Warningf("Timeout <- server: %s elapsed=%d,rid=%s",
								req.GetOpCode(), now.Sub(pr.timeSent), pr.reqCtx.request.GetRequestIDString())
						}
						pr.reqCtx.ReplyError(err)
						delete(p.mapRequestsSent, seq)
					}
				}
			}
		}
		if i != 0 {
			queue = queue[i:sz]
			p.pendingQueue = queue
		}
	}
}

func (p *PendingTracker) OnResonseReceived(readerResp *ReaderResponse) {
	if readerResp.err == nil {
		if readerResp.response == nil {
			glog.Fatal("both err and response are nil")
		}
		resp := readerResp.response
		connSequence := resp.GetOpaque()

		if pending, found := p.mapRequestsSent[connSequence]; found {
			delete(p.mapRequestsSent, connSequence)
			pending.reqCtx.Reply(resp)
			pending.reqCtx = nil
		} else {
			glog.Warningf("no pending response found. seq:%d,rid=%s\n", connSequence, resp.GetRequestIDString())
		}
	} else {
		p.responseTimer.Stop() ///TODO
		p.ClearOnError(readerResp.err)
		p.pendingQueue = p.pendingQueue[:0]
	}
}

func (p *PendingTracker) ClearOnError(err error) {
	glog.DebugDepth(1, err)
	for k, v := range p.mapRequestsSent {
		if v.reqCtx == nil {
			glog.Fatal("nil")
		} else {
			v.reqCtx.ReplyError(err)
		}
		delete(p.mapRequestsSent, k)
	}
}

func (p *PendingTracker) OnResponseReaderClosed() {
	p.responseTimer.Stop()
	p.ClearOnError(fmt.Errorf("reader closed"))
}
