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
	"reflect"
	"testing"
)

// Sample secrets crt, not a real crt
var (
	server_crt_teststr = `
-----BEGIN CERTIFICATE-----
MIIFGjCCAwICCQD30WAwBJQubzANBgkqhkiG9w0BAQsFADBPMRIwEAYDVQQKDAlK
dW5vIFRlc3QxFzAVBgNVBAMMDmNlcnQtYXV0aG9yaXR5MQswCQYDVQQGEwJVUzET
-----END CERTIFICATE-----
`
	server_key_teststr = `
-----BEGIN PRIVATE KEY-----
MIIJQwIBADANBgkqhkiG9w0BAQEFAASCCS0wggkpAgEAAoICAQDP9gNEfnn9Uay6
UggNSF1wmk4qKjhlThU2eiX0DQgwugHplRQEk0RcktmSTs6rF620HJMIKrgB9sP/
-----END PRIVATE KEY-----
`
	ca_teststr = `
-----BEGIN CERTIFICATE-----
MIIFGjCCAwICCQD30WAwBJQubzANBgkqhkiG9w0BAQsFADBPMRIwEAYDVQQKDAlK
dW5vIFRlc3QxFzAVBgNVBAMMDmNlcnQtYXV0aG9yaXR5MQswCQYDVQQGEwJVUzET
ihA+JnmsWFTHotRgeeo=
-----END CERTIFICATE-----
`

	certPem_bytes = []byte{10, 45, 45, 45, 45, 45, 66, 69, 71, 73, 78, 32, 67, 69, 82, 84, 73, 70, 73, 67, 65, 84, 69, 45, 45, 45, 45, 45, 10, 77, 73, 73, 70, 71, 106, 67, 67, 65, 119, 73, 67, 67, 81, 68, 51, 48, 87, 65, 119, 66, 74, 81, 117, 98, 122, 65, 78, 66, 103, 107, 113, 104, 107, 105, 71, 57, 119, 48, 66, 65, 81, 115, 70, 65, 68, 66, 80, 77, 82, 73, 119, 69, 65, 89, 68, 86, 81, 81, 75, 68, 65, 108, 75, 10, 100, 87, 53, 118, 73, 70, 82, 108, 99, 51, 81, 120, 70, 122, 65, 86, 66, 103, 78, 86, 66, 65, 77, 77, 68, 109, 78, 108, 99, 110, 81, 116, 89, 88, 86, 48, 97, 71, 57, 121, 97, 88, 82, 53, 77, 81, 115, 119, 67, 81, 89, 68, 86, 81, 81, 71, 69, 119, 74, 86, 85, 122, 69, 84, 10, 45, 45, 45, 45, 45, 69, 78, 68, 32, 67, 69, 82, 84, 73, 70, 73, 67, 65, 84, 69, 45, 45, 45, 45, 45, 10}

	certCaPem_bytes = []byte{10, 45, 45, 45, 45, 45, 66, 69, 71, 73, 78, 32, 67, 69, 82, 84, 73, 70, 73, 67, 65, 84, 69, 45, 45, 45, 45, 45, 10, 77, 73, 73, 70, 71, 106, 67, 67, 65, 119, 73, 67, 67, 81, 68, 51, 48, 87, 65, 119, 66, 74, 81, 117, 98, 122, 65, 78, 66, 103, 107, 113, 104, 107, 105, 71, 57, 119, 48, 66, 65, 81, 115, 70, 65, 68, 66, 80, 77, 82, 73, 119, 69, 65, 89, 68, 86, 81, 81, 75, 68, 65, 108, 75, 10, 100, 87, 53, 118, 73, 70, 82, 108, 99, 51, 81, 120, 70, 122, 65, 86, 66, 103, 78, 86, 66, 65, 77, 77, 68, 109, 78, 108, 99, 110, 81, 116, 89, 88, 86, 48, 97, 71, 57, 121, 97, 88, 82, 53, 77, 81, 115, 119, 67, 81, 89, 68, 86, 81, 81, 71, 69, 119, 74, 86, 85, 122, 69, 84, 10, 45, 45, 45, 45, 45, 69, 78, 68, 32, 67, 69, 82, 84, 73, 70, 73, 67, 65, 84, 69, 45, 45, 45, 45, 45, 10, 10, 45, 45, 45, 45, 45, 66, 69, 71, 73, 78, 32, 67, 69, 82, 84, 73, 70, 73, 67, 65, 84, 69, 45, 45, 45, 45, 45, 10, 77, 73, 73, 70, 71, 106, 67, 67, 65, 119, 73, 67, 67, 81, 68, 51, 48, 87, 65, 119, 66, 74, 81, 117, 98, 122, 65, 78, 66, 103, 107, 113, 104, 107, 105, 71, 57, 119, 48, 66, 65, 81, 115, 70, 65, 68, 66, 80, 77, 82, 73, 119, 69, 65, 89, 68, 86, 81, 81, 75, 68, 65, 108, 75, 10, 100, 87, 53, 118, 73, 70, 82, 108, 99, 51, 81, 120, 70, 122, 65, 86, 66, 103, 78, 86, 66, 65, 77, 77, 68, 109, 78, 108, 99, 110, 81, 116, 89, 88, 86, 48, 97, 71, 57, 121, 97, 88, 82, 53, 77, 81, 115, 119, 67, 81, 89, 68, 86, 81, 81, 71, 69, 119, 74, 86, 85, 122, 69, 84, 10, 105, 104, 65, 43, 74, 110, 109, 115, 87, 70, 84, 72, 111, 116, 82, 103, 101, 101, 111, 61, 10, 45, 45, 45, 45, 45, 69, 78, 68, 32, 67, 69, 82, 84, 73, 70, 73, 67, 65, 84, 69, 45, 45, 45, 45, 45, 10}

	keyPem_byte = []byte{10, 45, 45, 45, 45, 45, 66, 69, 71, 73, 78, 32, 80, 82, 73, 86, 65, 84, 69, 32, 75, 69, 89, 45, 45, 45, 45, 45, 10, 77, 73, 73, 74, 81, 119, 73, 66, 65, 68, 65, 78, 66, 103, 107, 113, 104, 107, 105, 71, 57, 119, 48, 66, 65, 81, 69, 70, 65, 65, 83, 67, 67, 83, 48, 119, 103, 103, 107, 112, 65, 103, 69, 65, 65, 111, 73, 67, 65, 81, 68, 80, 57, 103, 78, 69, 102, 110, 110, 57, 85, 97, 121, 54, 10, 85, 103, 103, 78, 83, 70, 49, 119, 109, 107, 52, 113, 75, 106, 104, 108, 84, 104, 85, 50, 101, 105, 88, 48, 68, 81, 103, 119, 117, 103, 72, 112, 108, 82, 81, 69, 107, 48, 82, 99, 107, 116, 109, 83, 84, 115, 54, 114, 70, 54, 50, 48, 72, 74, 77, 73, 75, 114, 103, 66, 57, 115, 80, 47, 10, 45, 45, 45, 45, 45, 69, 78, 68, 32, 80, 82, 73, 86, 65, 84, 69, 32, 75, 69, 89, 45, 45, 45, 45, 45, 10}
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
