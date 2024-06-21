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
	"io"
	"time"

	"github.com/paypal/junodb/third_party/forked/tecbot/gorocksdb"

	"github.com/paypal/junodb/cmd/storageserv/redist"
	"github.com/paypal/junodb/pkg/shard"
)

type IDBSharding interface {
	getDbInstanceAndKey(id RecordID) (dbInst *gorocksdb.DB, dbKey []byte)

	setupShards(dbnamePrefix string, shardMap shard.Map)

	shutdownShards([]shard.ID)
	shutdown()

	writeProperty(propKey string, w io.Writer)
	getIntProperty(propKey string) uint64

	decodeStorageKey(sskey []byte) ([]byte, []byte, error)
	duplicate() IDBSharding

	replicateSnapshot(shardId shard.ID, rb *redist.Replicator, mshardid int32) bool
}

type ShardingBase struct {
}

func (s *ShardingBase) waitForFinish(rb *redist.Replicator) bool {
	if rb.IsSnapShotDone() {
		return true
	}

	maxwait := redist.RedistConfig.MaxWaitTime * 1000 / 10

	// wait till the requests are all processed or max wait time reached
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	ts_passed := 0
	for {
		select {
		case <-ticker.C:
			if rb.IsSnapShotDone() {
				return true
			}

			ts_passed++
			if ts_passed > maxwait {
				return false
			}
		}
	}
}
