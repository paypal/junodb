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

package sec

import (
	"fmt"
	"sync/atomic"

	"github.com/paypal/junodb/third_party/forked/golang/glog"
)

var (
	DefaultConfig = Config{
		AppName:    "junoserv",
		ClientAuth: true,
	}
	config    = DefaultConfig // xuli: to revisit
	secInited uint32          // xuli: to revisit
)

type Config struct {
	AppName          string
	CertPem          string
	KeyPem           string
	ClientAuth       bool
	KeyStoreFilePath string
	CertPemFilePath  string
	KeyPemFilePath   string
	CAFilePath       string
}

func InitSecConfig(conf *Config) error {
	if atomic.CompareAndSwapUint32(&secInited, 0, 1) {
		if conf != nil {
			config = *conf
		}
		config.Default()
		//TODO validate
	} else {
		return fmt.Errorf("sec config had been initialized before")
	}
	return nil
}

func (c *Config) Default() {
	c.Validate()
}

func (c *Config) Validate() {
	if len(c.AppName) <= 0 { ///TODO
		glog.Fatal("Error: AppName is required for KMS.")
	}
}

func (c *Config) Dump() {
	glog.Infof("KMS AppName : %s\n", c.AppName)
}
