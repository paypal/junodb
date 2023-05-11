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
	"juno/third_party/forked/golang/glog"
	"os"
)

type localFileProtectedT struct {
	CertPem string
	KeyPem  string
}

func (p *localFileProtectedT) getCertAndKeyPemBlock(cfg *Config) (certPEMBlock []byte, keyPEMBlock []byte, err error) {
	certPEMBlock, err = os.ReadFile(cfg.CertPemFilePath)
	if err != nil {
		return
	}

	if cfg.ClientAuth {
		if _, err = os.Stat(cfg.CAFilePath); err == nil {
			var caPEMBlock []byte
			caPEMBlock, err = os.ReadFile(cfg.CAFilePath)
			if err != nil {
				glog.Errorln(err)
				return
			}
			str := string(certPEMBlock)
			str += string(caPEMBlock)
			certPEMBlock = []byte(str)
		} else {
			glog.Infof("os.Stat(cfg.CAFilePath) returns %v for filePath: %v", err, cfg.CAFilePath)
		}
	}

	keyPEMBlock, err = os.ReadFile(cfg.KeyPemFilePath)
	if err != nil {
		return
	}
	return
}
