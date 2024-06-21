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
	"fmt"
	"testing"

	"github.com/paypal/junodb/pkg/shard"
)

func TestNoMicroShard(t *testing.T) {
	ns := "namespace"
	key := "testkey1"

	shardid := 129    // fake
	microshardid := 6 // fake

	// encode
	var buf bytes.Buffer
	recordid := NewRecordIDWithBuffer(&buf, shard.ID(shardid), uint8(microshardid), []byte(ns), []byte(key))

	// decode
	ns_d, key_d, _ := DecodeRecordKey([]byte(recordid))
	fmt.Printf("ns_d=%s, key_d=%s, len=%d", ns_d, key_d, len([]byte(recordid)))
	if string(ns_d) != ns {
		t.Error(fmt.Sprintf("passed wrong ns: %s", ns_d))
	}

	if string(key_d) != key {
		t.Error(fmt.Sprintf("passed wrong key: %s", key_d))
	}

	size := len(ns) + len(key) + 3
	if size != len([]byte(recordid)) {
		t.Error(fmt.Sprintf("wrong size %d", len([]byte(recordid))))
	}
}

func TestEnableMicroShard(t *testing.T) {
	SetEnableMircoShardId(true)
	ns := "namespace"
	key := "testkey1"

	shardid := 129    // fake
	microshardid := 6 // fake

	// encode
	var buf bytes.Buffer
	recordid := NewRecordIDWithBuffer(&buf, shard.ID(shardid), uint8(microshardid), []byte(ns), []byte(key))

	// decode
	ns_d, key_d, _ := DecodeRecordKey([]byte(recordid))
	fmt.Printf("ns_d=%s, key_d=%s, len=%d", ns_d, key_d, len([]byte(recordid)))
	if string(ns_d) != ns {
		t.Error(fmt.Sprintf("passed wrong ns: %s", ns_d))
	}

	if string(key_d) != key {
		t.Error(fmt.Sprintf("passed wrong key: %s", key_d))
	}

	size := len(ns) + len(key) + 4
	if size != len([]byte(recordid)) {
		t.Error(fmt.Sprintf("wrong size %d", len([]byte(recordid))))
	}
}
