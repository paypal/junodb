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
//  Package utility provides the utility interfaces for mux package
//  
package io

import "time"

type Backoff interface {
	// BackOff increases the expiration time.
	BackOff()

	// Expires returns the expiration time.
	// Expires returns zero time if BackOff has never been called.
	Expires() time.Time

	// C waits for the expiration time then sends
	// the current time on the returned channel.
	// If the expiration time is the zero time,
	// it returns a nil channel.
	// The channel returned by C is not updated
	// but future calls to BackOff or Reset.
	C() <-chan time.Time

	// Reset resets the expiration time.
	Reset()
}

// Exponential creates an exponential backoff tracker. A maxDelay <= 0 means no maximum.
func Exponential(initialDelay time.Duration, exponent float64, maxDelay time.Duration) Backoff {
	b := exponential{
		InitialDelay: initialDelay,
		Exponent:     exponent,
		MaxDelay:     maxDelay,
	}
	b.Reset()
	return &b
}

type exponential struct {
	InitialDelay time.Duration
	Exponent     float64
	MaxDelay     time.Duration
	// private
	currentDelay time.Duration
	expires      *time.Time
}

func (b *exponential) BackOff() {
	if b.expires == nil {
		b.currentDelay = b.InitialDelay
	} else {
		b.currentDelay = time.Duration(b.Exponent * float64(b.currentDelay))
	}
	if b.MaxDelay > 0 && b.currentDelay > b.MaxDelay {
		b.currentDelay = b.MaxDelay
	}
	newExpires := time.Now().Add(b.currentDelay)
	b.expires = &newExpires
}

func (b *exponential) Expires() time.Time {
	if b.expires != nil {
		return *b.expires
	}
	return time.Time{}
}

func (b *exponential) C() <-chan time.Time {
	if b.expires == nil {
		return nil
	}
	tte := b.expires.Sub(time.Now())
	return time.After(tte)
}

func (b *exponential) Reset() {
	b.expires = nil
	b.currentDelay = b.InitialDelay
}

// Do calls f up to attempts times, backing off according to b. f must
// be niladic and return some result and an error. f will be retried by BackoffDo
// iff f returns nil for result and a non-nil error. The return value from BackoffDo will
// be the last returned result and error from calling f.
func Do(b Backoff, attempts uint16, f func() (result interface{}, err error)) (interface{}, error) {
	var attempt uint16
	var result interface{}
	var err error
	for attempt = 0; attempt < attempts && result == nil; attempt++ {
		if attempt > 0 {
			sleep := b.Expires().Sub(time.Now())
			time.Sleep(sleep)
		}
		result, err = f()
		if err == nil {
			return result, nil
		}
		b.BackOff()
	}
	return result, err
}
