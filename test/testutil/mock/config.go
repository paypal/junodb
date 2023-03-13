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

package mock

import (
	"math"
	"time"

	"juno/pkg/io"
	"juno/pkg/service"
	"juno/pkg/util"
)

type SSConfig struct {
	service.Config
	MeanDelay   int
	StdDevDelay int
	ValueSize   int
	StdDevSize  int
	Inbound     io.InboundConfig
	LogLevel    string
}

var (
	DefaultSSConfig SSConfig = SSConfig{
		MeanDelay:   0,
		StdDevDelay: 0,
		ValueSize:   1024,
		StdDevSize:  100,
		Inbound: io.InboundConfig{
			IdleTimeout:    util.Duration{math.MaxUint32 * time.Second},
			ReadTimeout:    util.Duration{math.MaxUint32 * time.Millisecond},
			WriteTimeout:   util.Duration{math.MaxUint32 * time.Millisecond},
			RequestTimeout: util.Duration{600 * time.Millisecond},
		},
		LogLevel: "warning",
	}
)
