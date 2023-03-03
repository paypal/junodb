package sherlock

import "fmt"

// TimeoutError returned for Frontier timeout
// other errors are from websocket
type TimeoutError struct {
	id uint32
}

func newTimeoutError(id uint32) *TimeoutError {
	return &TimeoutError{id: id}
}

func (t *TimeoutError) Error() string {
	if t == nil {
		return "Timed out on Frontier send"
	}
	return fmt.Sprintf("Timed out on Frontier send %d", t.id)

}

// RejectedError returned for Frontier rejection
// not currently distinguishing fatal and retryable
type RejectedError struct {
	msg string
}

func newRejectedError(msg string) *RejectedError {
	return &RejectedError{msg: msg}
}

func (t *RejectedError) Error() string {
	if t == nil {
		return "Rejected on Frontier send"
	}
	return fmt.Sprintf("Rejected on Frontier send %s", t.msg)
}

type frontierCb func(e error)
