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
  
package service

import (
	"fmt"
	"strings"
	"time"

	"juno/pkg/io"
	"juno/pkg/util"
)

const (
	kDefaultShutdownWaitTime    = 10 * time.Second
	kDefaultThrottlingDelayTime = 2 * time.Millisecond
)

var (
	DefaultListenerName = "default"
	DefaultConfig       = Config{
		ShutdownWaitTime:    util.Duration{kDefaultShutdownWaitTime},
		ThrottlingDelayTime: util.Duration{kDefaultThrottlingDelayTime},
		IO: io.InboundConfigMap{
			DefaultListenerName: io.DefaultInboundConfig,
		},
	}
)

type Config struct {
	Listener            []io.ListenerConfig
	ShutdownWaitTime    util.Duration
	ThrottlingDelayTime util.Duration
	IO                  io.InboundConfigMap
}

func (cfg *Config) IsSSLEnabled() bool {
	for _, ln := range cfg.Listener {
		if ln.SSLEnabled {
			return true
		}
	}
	return false
}

func (cfg *Config) SetDefaultIfNotDefined() {
	for _, ln := range cfg.Listener {
		ln.SetDefaultIfNotDefined()
	}
	if cfg.ShutdownWaitTime.Duration == 0 {
		cfg.ShutdownWaitTime.Duration = kDefaultShutdownWaitTime
	}
	if cfg.ThrottlingDelayTime.Duration == 0 {
		cfg.ThrottlingDelayTime.Duration = kDefaultThrottlingDelayTime
	}
	cfg.IO.SetDefaultIfNotDefined()
}

func (cfg *Config) SetListeners(values []string) {
	cfg.Listener = make([]io.ListenerConfig, len(values))
	for i, str := range values {
		str = strings.ToLower(str)
		lncfg := &cfg.Listener[i]
		if strings.HasPrefix(str, "ssl:") {
			str = strings.TrimPrefix(str, "ssl:")
			lncfg.SSLEnabled = true
		}
		if !strings.Contains(str, ":") {
			lncfg.Addr = ":" + str
		} else {
			lncfg.Addr = str
		}
	}
}

func (cfg *Config) GetIoConfig(lsnr *io.ListenerConfig) io.InboundConfig {
	if lsnr != nil {

		if c, ok := cfg.IO[lsnr.Name]; ok {
			return c
		} else {
			if c, ok = cfg.IO[DefaultListenerName]; ok {
				return c
			}
		}
	}
	return io.DefaultInboundConfig
}

func (cfg *Config) Validate() (err error) {
	if len(cfg.Listener) == 0 {
		err = fmt.Errorf("no listener defined")
	}
	return
}
