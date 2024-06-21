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

package testutil

import (
	"time"

	"github.com/paypal/junodb/third_party/forked/golang/glog"
)

type timeKeeper struct {
	startTime time.Time
}

func NewTimeKeeper() *timeKeeper {
	t := &timeKeeper{startTime: time.Now()}
	return t
}

func (t *timeKeeper) SecondPassed() uint32 {
	return uint32(time.Now().Sub(t.startTime).Seconds())
}

func (t *timeKeeper) RemainingTTL(ttl uint32) uint32 {
	l := t.SecondPassed()
	if l > ttl {
		glog.Warningf("** TTL (%d)  for this test environment might be too short **", ttl)
		return 0
	}
	return ttl - l
}
