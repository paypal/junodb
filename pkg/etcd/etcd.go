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

package etcd

import (
	"errors"
	"sync"

	"github.com/paypal/junodb/third_party/forked/golang/glog"
)

var (
	cli  *EtcdClient
	rw   *EtcdReadWriter
	once sync.Once
)

func Connect(cfg *Config, clsName string) (err error) {
	glog.Infof("Setting up etcd.")
	once.Do(func() {
		cli = NewEtcdClient(cfg, clsName)
		if cli != nil {
			rw = NewEtcdReadWriter(cli)
		}
	})

	if cli == nil {
		return errors.New("Failed to initialize etcd")
	}

	return nil
}

func Close() {
	glog.Infof("Closing etcd.")
}

func GetClsReadWriter() *EtcdReadWriter {
	return rw
}

func GetEtcdCli() *EtcdClient {
	return cli
}
