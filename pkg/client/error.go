package client

import (
	"juno/internal/cli"
	"juno/pkg/proto"
)

var (
	ErrNoKey              error
	ErrUniqueKeyViolation error
	ErrBadParam           error
	ErrConditionViolation error

	ErrBadMsg           error
	ErrNoStorage        error
	ErrRecordLocked     error
	ErrTTLExtendFailure error
	ErrBusy             error

	ErrWriteFailure   error
	ErrInternal       error
	ErrOpNotSupported error
)

var errorMapping map[proto.OpStatus]error

func init() {
	ErrNoKey = &cli.Error{"no key"}
	ErrUniqueKeyViolation = &cli.Error{"unique key violation"}
	ErrBadParam = &cli.Error{"bad parameter"}
	ErrConditionViolation = &cli.Error{"condition violation"} //version too old
	ErrTTLExtendFailure = &cli.Error{"fail to extend TTL"}

	ErrBadMsg = &cli.RetryableError{"bad message"}
	ErrNoStorage = &cli.RetryableError{"no storage"}
	ErrRecordLocked = &cli.RetryableError{"record locked"}
	ErrBusy = &cli.RetryableError{"server busy"}

	ErrWriteFailure = &cli.Error{"write failure"}
	ErrInternal = &cli.Error{"internal error"}
	ErrOpNotSupported = &cli.Error{"Op not supported"}

	errorMapping = map[proto.OpStatus]error{
		proto.OpStatusNoError:            nil,
		proto.OpStatusInconsistent:       nil,
		proto.OpStatusBadMsg:             ErrBadMsg,
		proto.OpStatusNoKey:              ErrNoKey,
		proto.OpStatusDupKey:             ErrUniqueKeyViolation,
		proto.OpStatusNoStorageServer:    ErrNoStorage,
		proto.OpStatusBadParam:           ErrBadParam,
		proto.OpStatusRecordLocked:       ErrRecordLocked,
		proto.OpStatusVersionConflict:    ErrConditionViolation,
		proto.OpStatusSSReadTTLExtendErr: ErrTTLExtendFailure,
		proto.OpStatusCommitFailure:      ErrWriteFailure,
		proto.OpStatusBusy:               ErrBusy,
		proto.OpStatusNotSupported:       ErrOpNotSupported,
	}
}
