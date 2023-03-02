package mayfly

import ()

var (
	MayflyMagic [4]byte = [4]byte{0xDE, 0xEF, 0xCA, 0xFE}
)

type (
	OpCode   uint16
	OpStatus uint16
	OpMode   uint8
)

const (
	OpCodeNOP OpCode = iota
	OpCodeCreate
	OpCodeGet
	OpCodeUpdate
	OpCodeDestroy
	OpCodeGetStats
	OpCodeCommitWrite
	OpCodeWriteData
	OpCodeReadData
	OpCodeAbortWrite
	OpCodeDeleteData
	OpCodeAppend
	OpCodeSet
	kNumOpCodes
)

const (
	OpStatusdNoError             OpStatus = iota
	OpStatusBadMsg                        // 1   network message errors
	OpStatusServiceDenied                 // 2   Service
	OpStatusNoKey                         // 3   Storage
	OpStatusDupKey                        // 4
	OpStatusDataExpired                   // 5
	OpStatusOutOfMem                      // 6
	OpStatusBadParam                      // 7   Client
	OpStatusRecordLocked                  // 8   storage
	OpStatusVersionTooOld                 // 9   storage
	OpStatusNoUncommitted                 // 10  storage
	OpStatusBadRequestID                  // 11  storage
	OpStatusNoStorageServer               // 12  Directory server
	OpStatusDuplicateRequest              // 13  Directory server
	OpStatusStorageServerTimeout          // 14  DEPRECATED, used in 2.0 DS
	OpStatusInserting                     // 15  storage
	OpStatusInvalidNamespace              // 16  DS detected invalid namespace
	OpStatusAlreadyFulfilled              // 17  storage server, write_data with last_write_request_id
	OpStatusNotSameRecord                 // 18  storage server, replicated "delete", create_time mis-match
	OpStatusVersionConflict               // 19  replicating data inconsistance
	OpStatusNotAppendable                 // 20  append op
	kNumOpStatus
)

const (
	OpModeNotSpecified   OpMode = iota
	OpModeIntendedInsert        // 1
	OpModeIntendedUpdate        // 2
	OpModeWriteThrough          // 3
	OpModeIntendedAppend        // 4
	kNumOpModes
)

const (
	kSenderTypeUnknown          uint16 = iota
	kSenderTypeClient                  // 1
	kSenderTypeDirectoryServer         // 2
	kSenderTypeStorageServer           // 3
	kSenderTypePersistentClient        // 4
	kNumSendTypes               uint16 = 5
	//SenderTypeAffinityDirectoryServer        // 5
)

const (
	kDataTagEndOfMsg   uint8 = 0
	kDataTagAppName    uint8 = 0xab
	kDataTagOpaqueData uint8 = 0xdd
)

const (
	kMaxAppNameLen   = 32
	kMaxNamespaceLen = 64
	kMaxKeyLen       = 256
)

const (
	kMessageTypeUnknown uint16 = iota
	kMessageTypeRequest
	kMessageTypeResponse
)

const (
	kHeaderLength           = 40
	kOpMsgHeaderLength      = 24
	kRecordInfoHeaderLength = 16
	kRequestIDLength        = 16
)

const (
	kDataTagEndOfOpMsg uint8 = iota
	kDataTagRecordInfo
	kDataTagRequestID
	kDataTagPayload
	kDataTagOptionalData
)

const (
	kDataTagEndOfRecordInfo uint8 = iota
	kDataTagNamespace
	kDataTagKey
)
