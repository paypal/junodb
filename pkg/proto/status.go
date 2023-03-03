package proto

const (
	StatusOk			= 0
	StatusNoConn		= 1
	StatusCommErr		= 2
	StatusTimeout		= 3
	StatusBadRequest	= 4
	StatusBadResponse	= 5
	StatusNoCapacity	= 6
	StatusSSBusy		= 7	
	StatusRBCleanup		= 8
	StatusRBExpire		= 9
)

var statusText = map[int]string {
	StatusOk:	"OK",
	StatusNoConn: "not connected",
	StatusCommErr: "communication error",
	StatusTimeout: "timed out",
	StatusBadRequest: "bad request",
	StatusBadResponse: "bad response",
	StatusNoCapacity: "no capacity",
	StatusSSBusy: "SS busy",
	StatusRBCleanup: "RB cleanup", // target connection closed
	StatusRBExpire: "RB expire", // item in the rb expired
}

func StatusText(code int) string {
	return statusText[code]
}
