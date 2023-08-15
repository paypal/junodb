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

//go:build ignore
// +build ignore

package main

import (
	"math"
	"time"

	"github.com/paypal/junodb/third_party/forked/golang/glog"

	"github.com/BurntSushi/toml"

	"github.com/paypal/junodb/pkg/io"
	cal "github.com/paypal/junodb/pkg/logging/cal/config"
	"github.com/paypal/junodb/pkg/util"
)

var Conf = Config{

	Inbound: io.InboundConfig{
		IdleTimeout:          util.Duration{math.MaxUint32 * time.Second},
		ReadTimeout:          util.Duration{math.MaxUint32 * time.Millisecond},
		WriteTimeout:         util.Duration{math.MaxUint32 * time.Millisecond},
		RequestTimeout:       util.Duration{600 * time.Millisecond},
		ReqProcessorPoolSize: 5000,
		MaxNumReqProcessors:  20000,
	},
	CAL: cal.Config{
		Host:             "127.0.0.1",
		Port:             1118,
		Environment:      "PayPal",
		Poolname:         "fakess",
		MessageQueueSize: 10000,
		Enabled:          false,
		CalType:          "FILE",
		CalLogFile:       "cal.log",
	},
	LogLevel: "warning",
}

type Config struct {
	Inbound            io.InboundConfig
	MaxKeyLength       int
	MaxNamespaceLength int
	MaxPayloadLength   int
	MaxTimeToLive      int
	LogLevel           string
	CAL                cal.Config
}

func (c *Config) Dump() {

	glog.Infof("inbound: %%v", c.Inbound)
}

func LoadConfig(file string) error {
	if _, err := toml.DecodeFile(file, &Conf); err != nil {
		return err
	}

	// TODO: Config validation

	return nil
}
