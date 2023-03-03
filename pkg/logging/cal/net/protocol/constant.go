package protocol

// CAL message Class field options
const (
	TxnStart  = 't'
	TxnEnd    = 'T'
	AtomicTxn = 'A'
	Event     = 'E'
	Heartbeat = 'H'
)

// maxMsgLen is the longest wire-formatted message length
// of CAL messages. Note that wire formatting adds bytes. This
// is exported only to document that messages will be truncated
// somewhere around a size of 4k.
const MaxMsgLen = 4096

const MaxMsgBufferSize = 10000

// MaxNamespaceLen is the longest allowed string for CAL
// namespace (Msg's Name or Type) fields. If a Name or Type is longer
// than MaxNamespaceLen, it will be truncated at (MaxNamespaceLen-1)
// bytes and have a "+" appended to it.
const MaxNamespaceLen = 127

// CAL logging type
const (
	CalTypeFile   string = "FILE"
	CalTypeSocket string = "SOCKET"
)
