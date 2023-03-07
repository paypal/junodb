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

	"juno/cmd/storageserv/redist"
	"juno/pkg/shard"
)

type IDatabase interface {
	Setup()
	TruncateExpired()
	Shutdown()

	Put(id RecordID, value []byte) error
	Get(id RecordID, fetchExpired bool) (*Record, error)
	GetRecord(id RecordID, rec *Record) (recExists bool, err error)
	Delete(id RecordID) error

	IsPresent(id RecordID) (bool, error, *Record)
	IsRecordPresent(id RecordID, rec *Record) (bool, error)

	ReplicateSnapshot(shardId shard.ID, r *redist.Replicator, mshardid int32) bool
	ShardSupported(shardId shard.ID) bool
	UpdateRedistShards(shards shard.Map)
	UpdateShards(shards shard.Map)

	WriteProperty(propKey string, w io.Writer)
	GetIntProperty(propKey string) uint64
}
