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

package db

import (
	"bytes"
	"encoding/binary"

	"juno/third_party/forked/golang/glog"

	"juno/pkg/shard"
)

var enableMircoShardId bool = false // default false for backward compatibility

func SetEnableMircoShardId(flag bool) {
	enableMircoShardId = flag
}

type RecordID []byte

func (id *RecordID) GetShardID() shard.ID {
	return shard.ID(binary.BigEndian.Uint16((*id)[:2]))
}

func (id *RecordID) GetKeyWithoutShardID() []byte {
	if enableMircoShardId {
		return (*id)[3:]
	}
	return (*id)[2:]
}

func (id *RecordID) GetKey() []byte {
	return (*id)[:]
}

func NewRecordIDWithBuffer(buf *bytes.Buffer, shardId shard.ID, microShardId uint8,
	namespace []byte, key []byte) RecordID {
	szNamespace := len(namespace)
	szKey := len(key)
	szBuf := szNamespace + szKey + 1 + 2
	if enableMircoShardId {
		szBuf++
	}
	buf.Grow(szBuf)
	buf.Reset()

	var b [2]byte
	binary.BigEndian.PutUint16(b[:], shardId.Uint16())
	buf.Write(b[:])
	if enableMircoShardId {
		buf.WriteByte(uint8(microShardId))
	}
	buf.WriteByte(uint8(szNamespace))
	buf.Write(namespace)
	buf.Write(key)

	return RecordID(buf.Bytes())
}

func DecodeRecordKey(sskey []byte) ([]byte, []byte, error) {
	if enableMircoShardId {
		return DecodeRecordKeyNoShardID(sskey[3:])
	}

	return DecodeRecordKeyNoShardID(sskey[2:])
}

func DecodeRecordKeyNoShardID(storageKey []byte) ([]byte, []byte, error) {
	szStorageKey := len(storageKey)
	var szNamespace uint8 = storageKey[0]
	namespace := make([]byte, szNamespace)
	copy(namespace, storageKey[1:1+szNamespace])
	key := make([]byte, szStorageKey-int(szNamespace)-1)
	copy(key, storageKey[1+szNamespace:szStorageKey])
	glog.Verbosef("Decoding key:%X => namespace:%X, key:%X \n",
		key, namespace, key)
	return namespace, key, nil
}
