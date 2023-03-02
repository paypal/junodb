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
package redist

import (
	"juno/third_party/forked/golang/glog"
	"time"
)

// A variantion of token bucket algorithm
type RateLimiter struct {
	rate           int64         // bytes per interval
	token          int64         // remaining byte count for the current interval
	token_stime    time.Time     // token start time
	token_interval time.Duration // update token bukcet every token_interval milliseconds
}

func NewRateLimiter(rate int64, interval int64) *RateLimiter {
	if interval < 5 || interval > 1000 {
		interval = 100 // every 100 ms
	}

	r := &RateLimiter{
		rate:           rate * interval / 1000,
		token_interval: time.Duration(interval) * time.Millisecond,
	}

	r.Reset()
	return r
}

func (r *RateLimiter) Reset() {
	r.token = r.rate
	r.token_stime = time.Now()
}

func (r *RateLimiter) GetToken(size int64) {
	if r.token >= size {
		r.token -= size
		return
	}

	elapse := time.Since(r.token_stime)
	size -= r.token

	// release more token
	if elapse >= r.token_interval {
		r.Reset()
	} else {
		glog.Verbosef("RateLimiter: sleep %s", r.token_interval-elapse)
		time.Sleep(r.token_interval - elapse)
		r.Reset()
	}

	// handle extreem case where a request > bucket size
	for r.token < size {
		glog.Verbosef("RateLimiter: sleep %s", r.token_interval)
		time.Sleep(r.token_interval)
		r.token += r.rate
		r.token_stime = time.Now()
	}
	r.token -= size
	return
}
