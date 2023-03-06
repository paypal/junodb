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
	"encoding/hex"
	"errors"
	"fmt"
	"juno/pkg/proto"
	"juno/third_party/forked/golang/glog"
	"time"

	"github.com/BurntSushi/toml"
)

var (
	ErrFailToGetEncryptionKey           = errors.New("Fail to get encryption key")
	ErrVersionMatchForEncryptionKey     = errors.New("Fail to get version encryption key")
	ErrNoEncryptionKeyFound             = errors.New("No encryption key found.")
	ErrMoreThanOneEnabledEncryptionKeys = errors.New("more than one enabled encryption keys found")
	ErrNoEnabledEncryptionKeys          = errors.New("no enabled encryption keys found")
)

type LocalFileStore struct {
	keys    [][]byte
	numKeys int
}

type localSecretsConfig struct {
	HexKeys []string `toml:"hexKeys"`
}

// Initialize a localFileStore
func initLocalFileStore(cfg *Config) (proto.IEncryptionKeyStore, error) {

	secretcfg := &localSecretsConfig{}
	if _, err := toml.DecodeFile(cfg.KeyStoreFilePath, secretcfg); err != nil {
		return nil, err
	}

	numKeys := len(secretcfg.HexKeys)
	if numKeys <= 0 {
		return nil, fmt.Errorf("No Keys Found in FileKeyStore")
	}

	ks := &LocalFileStore{
		keys:    make([][]byte, numKeys),
		numKeys: numKeys,
	}

	var err error
	for i, str := range secretcfg.HexKeys {
		ks.keys[i], err = hex.DecodeString(str)
		if err != nil {
			glog.Exitf("fail to generate keys for test encryption key store, exiting...")
		}
	}
	return ks, nil
}

func (ks *LocalFileStore) GetEncryptionKey() (key []byte, version uint32, err error) {
	version = uint32(int(time.Now().Unix()) % ks.numKeys)
	key = ks.keys[version]
	return
}

func (ks *LocalFileStore) GetDecryptionKey(version uint32) (key []byte, err error) {
	if int(version) >= ks.numKeys {
		err = ErrFailToGetEncryptionKey
		return
	}
	key = ks.keys[version]
	return
}

func (ks *LocalFileStore) NumKeys() int {
	return ks.numKeys
}
