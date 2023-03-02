package storage

import (
	"testing"
	"time"

	"juno/pkg/proto"
)

func BenchmarkSet(b *testing.B) {

	ct := uint32(time.Now().Unix())

	for i := 0; i < b.N; i++ {
		req := newDefaultSetRequest()
		resp, _ := processRequest(req)
		ttl := uint32(7200)
		commit := &proto.OperationalMessage{}
		commit.SetAsRequest()
		commit.SetOpCode(proto.OpCodeCommit)
		commit.SetKey(req.GetKey())
		commit.SetNamespace(req.GetNamespace())
		commit.SetRequestID(req.GetRequestID())
		commit.SetTimeToLive(ttl)
		if resp.GetCreationTime() == 0 {
			commit.SetCreationTime(ct)
		} else {
			commit.SetCreationTime(resp.GetCreationTime())
		}
		//commit.SetCreationTime(resp.GetCreationTime())
		commit.SetOriginatorRequestID(resp.GetOriginatorRequestID())
		commit.SetVersion(resp.GetVersion() + 1)
		resp, _ = processRequest(commit)
	}

}
