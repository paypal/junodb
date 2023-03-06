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
	"juno/pkg/proto"
	"os"
	"reflect"
	"testing"
)

var tomlData = []byte(`# Sample Keystore
hexKeys=[
  "dbe438a35bc06a1a633e763e81973175dbca256c68c36e46206b091914969344",
  "fbee433c6745699db387f1190e8a39e8b447861ec4a2612f92c8b35b317a228f",
  "b9af8403f42a067e262af1c46f0cc5f3df831d11c0dfea9850aec5435af234f0",
]`)

func createLocalKeystoreFile(filename string) {
	file, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	_, err = file.Write(tomlData)
	if err != nil {
		panic(err)
	}
}

func removeLocalKeystoreFile(filename string) {
	err := os.Remove(filename)
	if err != nil {
		panic(err)
	}
}

func Test_initLocalFileStore(t *testing.T) {
	type args struct {
		cfg *Config
	}
	testcfg, err := loadFileCfg("./config.toml")
	if err != nil {
		panic(err)
	}

	localKsFile := "./localFileStore.toml"
	createLocalKeystoreFile(localKsFile)
	defer removeLocalKeystoreFile(localKsFile)
	testcfg.KeyStoreFilePath = localKsFile

	output := LocalFileStore{
		keys: [][]byte{
			{219, 228, 56, 163, 91, 192, 106, 26, 99, 62, 118, 62, 129, 151, 49, 117, 219, 202, 37, 108, 104, 195, 110, 70, 32, 107, 9, 25, 20, 150, 147, 68},
			{251, 238, 67, 60, 103, 69, 105, 157, 179, 135, 241, 25, 14, 138, 57, 232, 180, 71, 134, 30, 196, 162, 97, 47, 146, 200, 179, 91, 49, 122, 34, 143},
			{185, 175, 132, 3, 244, 42, 6, 126, 38, 42, 241, 196, 111, 12, 197, 243, 223, 131, 29, 17, 192, 223, 234, 152, 80, 174, 197, 67, 90, 242, 52, 240},
		},
		numKeys: 3,
	}

	tests := []struct {
		name    string
		args    args
		want    proto.IEncryptionKeyStore
		wantErr bool
	}{
		// TODO: Add test cases.
		{"KeyStoreTest", args{testcfg}, &output, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := initLocalFileStore(tt.args.cfg)
			if (err != nil) != tt.wantErr {
				t.Errorf("initLocalFileStore() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("initLocalFileStore() = %v, want %v", got, tt.want)
			}
		})
	}
}
