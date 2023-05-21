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
	"os"
	"reflect"
	"testing"

	"github.com/paypal/junodb/third_party/forked/golang/glog"
)

// Sample secrets crt, not a real crt
var (
	server_crt_teststr = `SCERT`
	server_key_teststr = `SKEY`
	ca_teststr         = `CA`

	certPem_bytes = []byte{83, 67, 69, 82, 84}

	certCaPem_bytes = []byte{83, 67, 69, 82, 84, 67, 65}

	keyPem_byte = []byte{83, 75, 69, 89}
)

func createLocalSecretFile(secretfilename, secret_string string) {
	file, err := os.Create(secretfilename)
	if err != nil {
		glog.Errorln()
		return
	}
	_, err = file.Write([]byte(secret_string))
	if err != nil {
		glog.Errorln(err)
		return
	}
}

func removeLocalSecretFile(secretfilename string) {
	err := os.Remove(secretfilename)
	if err != nil {
		glog.Errorln(err)
		return
	}
}

func setCfgclientAuth(clientAuth bool, cfg *Config) *Config {
	cfg.ClientAuth = clientAuth
	return cfg
}

func Test_localFileProtectedT_getCertAndKeyPemBlock(t *testing.T) {
	type fields struct {
		CertPem string
		KeyPem  string
	}
	type args struct {
		cfg *Config
	}

	localServerCrtFile := "./server.crt"
	createLocalSecretFile(localServerCrtFile, server_crt_teststr)
	defer removeLocalSecretFile(localServerCrtFile)

	localServerKeyFile := "./server.pem"
	createLocalSecretFile(localServerKeyFile, server_key_teststr)
	defer removeLocalSecretFile(localServerKeyFile)

	localCaCrtFile := "./ca.crt"
	createLocalSecretFile(localCaCrtFile, ca_teststr)
	defer removeLocalSecretFile(localCaCrtFile)

	tcfg1 := &Config{
		AppName:         "junoserv",
		CertPemFilePath: localServerCrtFile,
		KeyPemFilePath:  localServerKeyFile,
		CAFilePath:      localCaCrtFile,
		ClientAuth:      false,
	}
	tcfg2 := &Config{
		AppName:         "junoserv",
		CertPemFilePath: localServerCrtFile,
		KeyPemFilePath:  localServerKeyFile,
		CAFilePath:      localCaCrtFile,
		ClientAuth:      true,
	}

	tests := []struct {
		name             string
		fields           fields
		args             args
		wantCertPEMBlock []byte
		wantKeyPEMBlock  []byte
		wantErr          bool
	}{
		// TODO: Add test cases.
		{"TestNoClientAuth", fields{"", ""}, args{tcfg1}, certPem_bytes, keyPem_byte, false},
		{"TestWithClientAuth", fields{"", ""}, args{tcfg2}, certCaPem_bytes, keyPem_byte, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &localFileProtectedT{
				CertPem: tt.fields.CertPem,
				KeyPem:  tt.fields.KeyPem,
			}
			gotCertPEMBlock, gotKeyPEMBlock, err := p.getCertAndKeyPemBlock(tt.args.cfg)
			if (err != nil) != tt.wantErr {
				t.Errorf("localFileProtectedT.getCertAndKeyPemBlock() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotCertPEMBlock, tt.wantCertPEMBlock) {
				t.Errorf("localFileProtectedT.getCertAndKeyPemBlock() gotCertPEMBlock = %v, want %v,  gotstr %v, wantstr %v", gotCertPEMBlock, tt.wantCertPEMBlock, string(gotCertPEMBlock), string(tt.wantCertPEMBlock))
			}
			if !reflect.DeepEqual(gotKeyPEMBlock, tt.wantKeyPEMBlock) {
				t.Errorf("localFileProtectedT.getCertAndKeyPemBlock() gotKeyPEMBlock = %v, want %v", gotKeyPEMBlock, tt.wantKeyPEMBlock)
			}
		})
	}
}
