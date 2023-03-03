package mayfly

import (
	"errors"
	"fmt"
)

var (
	errInvalidMagic           = errors.New("invalid magic")
	errInvalidBufferLength    = errors.New("invalid buffer length")
	errInvalidHeaderLength    = errors.New("invalid header length")
	ErrInvalidMessage         = errors.New("invalid message")
	errNotEoughBuffer         = errors.New("not enough buffer")
	errUnsupportedOpCode      = errors.New("unsupported opcode")
	errUnsupportedMessageType = errors.New("unsupported message type")
)

func invalidMessageError(err string) error {
	return fmt.Errorf("invalid message. %s", err)
}
