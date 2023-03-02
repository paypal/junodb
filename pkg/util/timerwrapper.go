package util

import (
    "time"
)

// This Wrapper class it to work around the issue with time.Timer.Reset(), mentioned below:
// https://github.com/golang/go/issues/11513
//
// timer.C is buffered, so if the timer has just expired,
// the newly reset timer can actually trigger immediately.
//
type TimerWrapper struct {
	t * time.Timer
	stopped bool
}

func NewTimerWrapper(d time.Duration) *TimerWrapper {
	t := &TimerWrapper{
		t: time.NewTimer(d),
		stopped: true,
	}
	
	t.t.Stop()
	return t
}

func (t *TimerWrapper) GetTimeoutCh() <-chan time.Time {
	if t.stopped {
		return nil
	} else {
		return t.t.C
	}
}

func (t *TimerWrapper) IsStopped() bool {
	return t.stopped
}

func (t *TimerWrapper) Stop() {
	if t.stopped {
		return
	}
	
	// To prevent the timer firing after a call to Stop,
	// check the return value and drain the channel.
	if !t.t.Stop() {
		select {
			case <-t.t.C:
			default:
		}
	}
	
	t.stopped = true
}

func (t *TimerWrapper) Reset(d time.Duration) {
	if !t.stopped {
		t.Stop()
	}
	
	t.t.Reset(d)
	t.stopped = false
}
