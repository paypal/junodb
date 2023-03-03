package logging

import (
	"juno/pkg/logging/cal"
	"juno/pkg/proto"
)

type Status int

const (
	kStatusSuccess Status = Status(iota)
	kStatusFatal
	kStatusError
	kStatusWarning
	kNumStatus
)

var (
	calStatus []string = []string{
		cal.StatusSuccess,
		cal.StatusFatal,
		cal.StatusError,
		cal.StatusWarning,
	}

	opStatusToCalStatusMap map[proto.OpStatus]Status = map[proto.OpStatus]Status{
		proto.OpStatusNoError:          kStatusSuccess,
		proto.OpStatusNoKey:            kStatusSuccess,
		proto.OpStatusDupKey:           kStatusSuccess,
		proto.OpStatusRecordLocked:     kStatusSuccess,
		proto.OpStatusInserting:        kStatusSuccess,
		proto.OpStatusAlreadyFulfilled: kStatusSuccess,
		proto.OpStatusVersionConflict:  kStatusSuccess,
		proto.OpStatusInconsistent:     kStatusSuccess,

		proto.OpStatusBadMsg:             kStatusWarning,
		proto.OpStatusBadParam:           kStatusWarning,
		proto.OpStatusNoUncommitted:      kStatusWarning,
		proto.OpStatusSSReadTTLExtendErr: kStatusWarning,

		proto.OpStatusServiceDenied:   kStatusError,
		proto.OpStatusNoStorageServer: kStatusFatal,
		proto.OpStatusSSError:         kStatusError,
		proto.OpStatusSSOutofResource: kStatusError,
		proto.OpStatusReqProcTimeout:  kStatusError,
		proto.OpStatusCommitFailure:   kStatusError,
	}
)

func CalStatus(st proto.OpStatus) Status {
	if calstatus, ok := opStatusToCalStatusMap[st]; ok {
		return calstatus
	}
	return kStatusSuccess
}

func (s Status) CalStatus() string {
	if s < kNumStatus {
		return calStatus[int(s)]
	}
	return cal.StatusSuccess
}

func (s Status) NotSuccess() bool {
	return (s != kStatusSuccess)
}
