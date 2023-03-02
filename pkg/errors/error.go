package errors

import (
	"fmt"
)

var (
	ErrNoConnection = &Error{what: "no connection", errno: KErrNoConnection}
	ErrBusy         = &Error{what: "busy", errno: KErrBusy}
)

type Error struct {
	what  string
	errno uint32
}

func NewError(what string, errno uint32) *Error {
	return &Error{what: what, errno: errno}
}

func (e *Error) Error() string {
	return fmt.Sprintf("error: %s (%d) ", e.what, e.errno)
}

func (e *Error) ErrNo() uint32 {
	return e.errno
}
