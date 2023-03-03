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
package etcd

import (
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"juno/pkg/util"
)

var (
	defaultConfig = Config{
		Config: clientv3.Config{
			DialTimeout: 1000 * time.Millisecond,
		},
		RequestTimeout:     util.Duration{1 * time.Second},
		MaxConnectAttempts: 5,
		MaxConnectBackoff:  10,
		CacheDir:           "./",
		CacheName:          "etcd_cache",
		EtcdKeyPrefix:      "juno.",
	}
)

type Config struct {
	clientv3.Config
	RequestTimeout     util.Duration
	MaxConnectAttempts int
	MaxConnectBackoff  int
	CacheDir           string
	CacheName          string
	EtcdKeyPrefix      string
}

func DefaultConfig() Config {
	return defaultConfig
}

func NewConfig(addrs ...string) (cfg *Config) {
	cfg = &Config{}
	*cfg = defaultConfig
	for _, addr := range addrs {
		cfg.Config.Endpoints = append(cfg.Config.Endpoints, addr)
	}
	return cfg
}
