package storage

import (
	"os"
	"testing"
	"time"

	"juno/pkg/proto"
	"juno/pkg/util"
)

var (
	kSpecDelete_PrepReq specsT = specsT{
		kMust,      //	kMsgNamespace
		kMust,      //	kMsgKey
		kShouldNot, //	kMsgValue
		kShouldNot, //	kMsgTTLOrExpTime
		kShouldNot, //	kMsgVersion
		kShouldNot, //	kMsgCreationTime
		kShouldNot, //	kMsgLastMdificationTime
		kShouldNot, //	kMsgSrcInfo
		kMust,      //	kMsgRequestID
		kShouldNot, //	kMsgOriginatorRID
		kOptional,  //	kMsgCorrelationID
	}

	kSpecDelete_PrepReq_Rep specsT = specsT{
		kMust,      //	kMsgNamespace
		kMust,      //	kMsgKey
		kShouldNot, //	kMsgValue
		kMust,      //	kMsgTTLOrExpTime
		kMust,      //	kMsgVersion
		kMust,      //	kMsgCreationTime
		kMust,      //	kMsgLastMdificationTime
		kShouldNot, //	kMsgSrcInfo
		kMust,      //	kMsgRequestID
		kMust,      //	kMsgOriginatorRID
		kOptional,  //	kMsgCorrelationID
	}

	kSpecDelete_PrepResp_NoErr specsT = specsT{
		kMust,      //	kMsgNamespace
		kMust,      //	kMsgKey
		kShouldNot, //	kMsgValue
		kOptional,  //	kMsgTTLOrExpTime
		kMust,      //	kMsgVersion
		kMust,      //	kMsgCreationTime
		kMust,      //	kMsgLastMdificationTime
		kShouldNot, //	kMsgSrcInfo
		kMust,      //	kMsgRequestID
		kMust,      //	kMsgOriginatorRID
		kOptional,  //	kMsgCorrelationID
	}

	kSpecDelete_PrepResp_NoKey specsT = specsT{
		kMust,      //	kMsgNamespace
		kMust,      //	kMsgKey
		kShouldNot, //	kMsgValue
		kShouldNot, //	kMsgTTLOrExpTime
		kShouldNot, //	kMsgVersion
		kShouldNot, //	kMsgCreationTime
		kShouldNot, //	kMsgLastMdificationTime
		kShouldNot, //	kMsgSrcInfo
		kMust,      //	kMsgRequestID
		kShouldNot, //	kMsgOriginatorRID
		kOptional,  //	kMsgCorrelationID
	}

	kSpecDelete_CommitReq specsT = specsT{
		kMust,      //	kMsgNamespace
		kMust,      //	kMsgKey
		kShouldNot, //	kMsgValue
		kShouldNot, //	kMsgTTLOrExpTime
		kShouldNot, //	kMsgVersion
		kShouldNot, //	kMsgCreationTime
		kShouldNot, //	kMsgLastMdificationTime
		kShouldNot, //	kMsgSrcInfo
		kMust,      //	kMsgRequestID
		kShouldNot, //	kMsgOriginatorRID
		kOptional,  //	kMsgCorrelationID
	}
	kSpecDelete_CommitResp specsT = specsT{
		kMust,      //	kMsgNamespace
		kMust,      //	kMsgKey
		kShouldNot, //	kMsgValue
		kShouldNot, //	kMsgTTLOrExpTime
		kShouldNot, //	kMsgVersion
		kShouldNot, //	kMsgCreationTime
		kShouldNot, //	kMsgLastMdificationTime
		kShouldNot, //	kMsgSrcInfo
		kMust,      //	kMsgRequestID
		kShouldNot, //	kMsgOriginatorRID
		kShouldNot, //	kMsgCorrelationID
	}

	kSpecMarkDelete_Req specsT = specsT{
		kMust,        //	kMsgNamespace
		kMust,        //	kMsgKey
		kShouldNot,   //	kMsgValue
		kShould,      //	kMsgTTLOrExpTime
		kShould,      //	kMsgVersion
		kShould,      //	kMsgCreationTime
		kShould,      //	kMsgLastMdificationTime
		kUnspecified, //	kMsgSrcInfo
		kMust,        //	kMsgRequestID
		kShould,      //	kMsgOriginatorRID
		kOptional,    //	kMsgCorrelationID
	}

	kSpecMarkDelete_Resp specsT = specsT{
		kMust,      //	kMsgNamespace
		kMust,      //	kMsgKey
		kShouldNot, //	kMsgValue
		kShouldNot, //	kMsgTTLOrExpTime
		kShouldNot, //	kMsgVersion
		kShouldNot, //	kMsgCreationTime
		kShouldNot, //	kMsgLastMdificationTime
		kShouldNot, //	kMsgSrcInfo
		kMust,      //	kMsgRequestID
		kShouldNot, //	kMsgOriginatorRID
		kShouldNot, //	kMsgCorrelationID
	}
)

func newDefaultPrepDeleteRequest() *proto.OperationalMessage {
	req := &proto.OperationalMessage{}
	req.SetAsRequest()
	req.SetOpCode(proto.OpCodePrepareDelete)
	req.SetKey(testKey)
	req.SetNamespace(testNamespace)
	req.SetNewRequestID()
	return req
}

func newDefaultMarkDelete(rid proto.RequestId) *proto.OperationalMessage {
	req := &proto.OperationalMessage{}
	req.SetAsRequest()
	req.SetOpCode(proto.OpCodeMarkDelete)
	req.SetKey(testKey)
	req.SetNamespace(testNamespace)
	req.SetRequestID(rid)
	return req
}

func newDefaultMarkDeleteRequest() *proto.OperationalMessage {
	return nil
}

func TestDelete_PrepReq_validate(t *testing.T) {
	deleteRecord()
	req := newDefaultPrepDeleteRequest()
	req.UnSetRequestID()

	if dbStoreValidate(req) {
		t.Log("validation should fail")
		t.Fail()
	}
	resp, _ := processRequest(req)
	expectStatus(t, resp, proto.OpStatusBadParam)

	req.SetNewRequestID()
	if !dbStoreValidate(req) {
		t.Fail()
	}
	validateRequest(t, req, kSpecDelete_PrepReq)
	resp, _ = processRequest(req)
	expectStatus(t, resp, proto.OpStatusNoKey)
	validateResponse(t, req, resp, kSpecDelete_PrepResp_NoKey)
	abort(req)
}

func TestDelete_PrepCommit_NoErr(t *testing.T) {
	storeRecord(newDefaultRecord())

	req := newDefaultPrepDeleteRequest()
	validateRequest(t, req, kSpecDelete_PrepReq)
	resp, _ := processRequest(req)
	expectStatus(t, resp, proto.OpStatusNoError)

	validateResponse(t, req, resp, kSpecDelete_PrepResp_NoErr)

	commit := newDefaultCommitFromPrepReq(req)
	validateRequest(t, commit, kSpecDelete_CommitReq)
	resp, _ = processRequest(commit)
	expectStatus(t, resp, proto.OpStatusNoError)
	validateResponse(t, commit, resp, kSpecDelete_CommitResp)

	deleteRecord()
}

func TestDelete_PrepCommit_Rep_NoErr(t *testing.T) {
	rec := newDefaultRecord()
	storeRecord(rec)

	req := newDefaultPrepDeleteRequest()
	req.SetAsReplication()
	req.SetOriginatorRequestID(rec.OriginatorRequestId)
	req.SetCreationTime(rec.CreationTime)
	req.SetTimeToLive(util.GetTimeToLive(rec.ExpirationTime))
	req.SetLastModificationTime(rec.LastModificationTime)
	req.SetVersion(rec.Version)
	validateRequest(t, req, kSpecDelete_PrepReq_Rep)
	resp, _ := processRequest(req)
	expectStatus(t, resp, proto.OpStatusNoError)

	validateResponse(t, req, resp, kSpecDelete_PrepResp_NoErr)

	commit := newDefaultCommitFromPrepReq(req)
	commit.SetAsReplication()
	validateRequest(t, commit, kSpecDelete_CommitReq)
	resp, _ = processRequest(commit)
	expectStatus(t, resp, proto.OpStatusNoError)
	validateResponse(t, commit, resp, kSpecDelete_CommitResp)

	deleteRecord()
}

func TestDelete_PrepResp_NoKey(t *testing.T) {
	deleteRecord()
	req := newDefaultPrepDeleteRequest()
	resp, _ := processRequest(req)
	expectStatus(t, resp, proto.OpStatusNoKey)
	validateResponse(t, req, resp, kSpecDelete_PrepResp_NoKey)
	abort(req)
}

func TestMarkDelete(t *testing.T) {
	rec := newDefaultRecord()
	storeRecord(rec)

	req := newDefaultPrepDeleteRequest()
	validateRequest(t, req, kSpecDelete_PrepReq)
	resp, _ := processRequest(req)
	expectStatus(t, resp, proto.OpStatusNoError)

	validateResponse(t, req, resp, kSpecDelete_PrepResp_NoErr)

	markdelete := newDefaultMarkDelete(req.GetRequestID())
	markdelete.SetVersion(resp.GetVersion())
	markdelete.SetCreationTime(resp.GetCreationTime())
	markdelete.SetLastModificationTime(resp.GetLastModificationTime())
	markdelete.SetOriginatorRequestID(resp.GetOriginatorRequestID())
	markdelete.SetExpirationTime(rec.ExpirationTime)
	validateRequest(t, markdelete, kSpecMarkDelete_Req)
	resp, _ = processRequest(markdelete)
	expectStatus(t, resp, proto.OpStatusNoError)
	validateResponse(t, markdelete, resp, kSpecMarkDelete_Resp)

	deleteRecord()
}

func TestMarkDelete_noRecord(t *testing.T) {
	deleteRecord()

	rid := proto.RequestId{}
	rid.SetNewRequestId()
	req := newDefaultMarkDelete(rid)
	validateRequest(t, req, kSpecMarkDelete_Req)

	now := time.Now()

	req.SetVersion(10)
	req.SetCreationTime(uint32(now.Unix() - 1800))
	req.SetLastModificationTime(uint64(now.Add(-1800 * time.Second).UnixNano()))
	rid.SetNewRequestId()
	req.SetOriginatorRequestID(rid)
	req.SetExpirationTime(uint32(now.Unix() + 1800))
	validateRequest(t, req, kSpecMarkDelete_Req)
	resp, _ := processRequest(req)
	expectStatus(t, resp, proto.OpStatusNoError)
	validateResponse(t, req, resp, kSpecMarkDelete_Resp)

	rec, err := getTestRecord()
	if err == nil {
		if rec != nil {
			rec.PrettyPrint(os.Stdout)
		}
	}
	deleteRecord()
}
