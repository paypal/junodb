//
//  Copyright 2023 PayPal Inc.
//
//  Licensed to the Apache Software Foundation (ASF) under one or more
//  contributor license agreements.  See the NOTICE file distributed with
//  this work for additional information regarding copyright ownership.
//  The ASF licenses this file to You under the Apache License, Version 2.0
//  (the "License"); you may not use this file except in compliance with
//  the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

package sherlock

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
	t       *time.Timer
	stopped bool
}

func NewTimerWrapper(d time.Duration) *TimerWrapper {
	t := &TimerWrapper{
		t:       time.NewTimer(d),
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
