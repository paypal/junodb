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
//  Package utility provides the utility interfaces for mux package
//  
package db

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync/atomic"
	"time"

	"juno/third_party/forked/golang/glog"
	"juno/third_party/forked/tecbot/gorocksdb"

	"juno/cmd/storageserv/redist"
	"juno/pkg/logging"
	"juno/pkg/logging/cal"
	"juno/pkg/proto"
	"juno/pkg/shard"
)

var (
	writeOptions *gorocksdb.WriteOptions = gorocksdb.NewDefaultWriteOptions()
	readOptions  *gorocksdb.ReadOptions  = gorocksdb.NewDefaultReadOptions()
)

var _ IDatabase = (*RocksDB)(nil)

type DBError struct {
	err error
}

func (e *DBError) Error() string {
	if e.err != nil {
		return "DBError: " + e.err.Error()
	}
	return "DBErr: "
}

func NewDBError(e error) *DBError {
	return &DBError{err: e}
}

var rocksdbIndex int32 = 0
var rocksdb [2]IDatabase

type RocksDB struct {
	zoneId       int
	nodeId       int
	numShards    int
	shards       shard.Map // from current shard map
	redistShards shard.Map // from redistribution, temporary/not commited
	sharding     IDBSharding
}

func GetDB() IDatabase {
	var index int32 = atomic.LoadInt32(&rocksdbIndex)
	return rocksdb[index]
}

func GetPrefixDB() *ShardingByPrefix {
	g := GetDB()
	if g == nil {
		return nil
	}

	if r, ok := g.(*RocksDB); ok {
		if d, ok := r.sharding.(*ShardingByPrefix); ok {
			return d
		}
	}

	return nil
}

///TODO xuli dbDir...
func newDBSharding(numShards int, numMicroShards int, numMicroShardGroups int, numPrefixDbs int, dbnamePrefix string) (sharding IDBSharding) {
	if numPrefixDbs > 0 { // Use prefix key
		shardFilters := make([]*ShardFilter, numPrefixDbs, numPrefixDbs)
		for i := 0; i < numPrefixDbs; i++ {
			shardFilters[i] = &ShardFilter{shardNum: -1}
		}
		sharding = &ShardingByPrefix{
			dbnamePrefix:        dbnamePrefix,
			dbs:                 make([]*gorocksdb.DB, numPrefixDbs, numPrefixDbs),
			shardFilters:        shardFilters,
			numMicroShards:      numMicroShards,
			numMicroShardGroups: numMicroShardGroups,
		}
	} else { // Not using prefix key
		sharding = &ShardingByInstance{
			dbnamePrefix: dbnamePrefix,
			dbs:          make([]*gorocksdb.DB, numShards, numShards),
		}
	}
	return
}

///TODO dbDir...
func newRocksDB(numShards int, numMicroShards int, numMicroShardGroups int, numPrefixDbs int, zoneId int, nodeId int, shardMap shard.Map) *RocksDB {
	db := &RocksDB{
		zoneId:    zoneId,
		nodeId:    nodeId,
		numShards: numShards,
		shards:    shardMap,
		sharding:  newDBSharding(numShards, numMicroShards, numMicroShardGroups, numPrefixDbs, fmt.Sprintf("%d-%d", zoneId, nodeId)),
	}
	db.Setup()
	return db
}

// only called once during start up
func Initialize(
	numShards int, numMicroShards int, numMicroShardGroups int,
	numPrefixDbs int, zoneId int, nodeId int, shardMap shard.Map, lruCacheSizeInMB int) {
	if numMicroShards > 0 {
		SetEnableMircoShardId(true)
		glog.Infof("Enable micro shards, NumMicroShards=%d, numMshardGroups=%d", numMicroShards, numMicroShardGroups)
	}

	if DBConfig.NewLRUCacheSizeInMB == 0 && lruCacheSizeInMB > 0 { // Use computed value
		DBConfig.NewLRUCacheSizeInMB = lruCacheSizeInMB
	}
	db := newRocksDB(numShards, numMicroShards, numMicroShardGroups, numPrefixDbs, zoneId, nodeId, shardMap)
	rocksdb[rocksdbIndex] = db
	// safe guard?
	rocksdb[(rocksdbIndex+1)%2] = db
}

func Finalize() {
	GetDB().Shutdown()
}

func fastDbFlush(db *gorocksdb.DB) {

	key := "disable_auto_compactions"
	val := "true"
	err := db.SetOption(key, val)
	if err != nil {
		glog.Errorf("SetOption error: %s", err.Error())
	}

	db.Flush(gorocksdb.NewDefaultFlushOptions())
}

func (r *RocksDB) Put(id RecordID, value []byte) error {
	db, key := r.sharding.getDbInstanceAndKey(id)

	if db == nil {
		glog.Errorf("no db for shard %d", id.GetShardID())
		return errors.New(fmt.Sprintf("no db for shard %d", id.GetShardID()))
	}

	var err error

	if cal.LogDebug() {
		start := time.Now()
		err = db.Put(writeOptions, key, value)
		defer r.LogCalTransaction(start, logging.CalMsgNameDbPut, err)
	} else {
		err = db.Put(writeOptions, key, value)
	}
	if err != nil {
		glog.Errorf("RocksDB error while Put: %s", err.Error())
		return NewDBError(err)
	}
	return nil
}

// Caller's responsibility to
// 1) zero'd rec before calling, and
// 2) free rec.holder if not nil afterwards
func (r *RocksDB) GetRecord(recId RecordID, rec *Record) (exist bool, err error) {
	db, key := r.sharding.getDbInstanceAndKey(recId)

	if db == nil {
		glog.Errorf("no db for shard %d", recId.GetShardID())
		err = errors.New(fmt.Sprintf("no db for shard %d", recId.GetShardID()))
		return
	}

	var value *gorocksdb.Slice
	var gerr error
	if cal.LogDebug() {
		start := time.Now()
		value, gerr = db.Get(readOptions, key)
		defer r.LogCalTransaction(start, logging.CalMsgNameDbGet, gerr)
	} else {
		value, gerr = db.Get(readOptions, key)
	}
	if gerr == nil {
		exist = value.Data() != nil
		if exist {
			gerr = rec.DecodeFrom(value)
			if gerr != nil {
				glog.Error(gerr)
				err = NewDBError(err)
			}
		}
	} else {
		glog.Errorf("%s, Key: %s", err, recId)
		err = NewDBError(err)
		return
	}

	return
}

func (r *RocksDB) LogCalTransaction(startT time.Time, name string, err error) {
	rht := time.Since(startT)
	if cal.IsEnabled() {
		status := cal.StatusSuccess
		if err != nil {
			status = cal.StatusError
		}
		cal.AtomicTransaction(logging.CalMsgTypeDb, name, status, rht, nil)
	}
}

func (r *RocksDB) Get(recId RecordID, fetchExpired bool) (*Record, error) {
	db, key := r.sharding.getDbInstanceAndKey(recId)

	if db == nil {
		glog.Errorf("no db for shard %d", recId.GetShardID())
		return nil, errors.New(fmt.Sprintf("no db for shard %d", recId.GetShardID()))
	}

	var value *gorocksdb.Slice
	var err error

	if cal.LogDebug() {
		start := time.Now()
		value, err = db.Get(readOptions, key)
		defer r.LogCalTransaction(start, "db_get", err)
	} else {
		value, err = db.Get(readOptions, key)
	}

	/* Data should be copied before defer executes */
	defer value.Free()

	if err != nil {
		glog.Errorf("%s, Key: %s", err, key)
		return nil, NewDBError(err)
	}

	// no key is not an error from db point of view
	// let caller handle the nokey
	if value.Data() == nil {
		return nil, nil
	}

	rec := new(Record)
	err = rec.Decode(value.Data())
	if err != nil {
		return rec, NewDBError(err)
	}

	// Let caller handle key expiration
	return rec, nil
}

// Caller's responsibility to
// 1) zero'd rec before calling, and
// 2) free rec.holder if not nil afterwards
func (r *RocksDB) IsRecordPresent(recId RecordID, rec *Record) (existAndNotExpired bool, err error) {
	if exist, gerr := r.GetRecord(recId, rec); gerr != nil {
		glog.Error(gerr)
		err = gerr
		return
	} else {
		existAndNotExpired = exist && (!rec.IsExpired())
	}
	return
}

func (r *RocksDB) IsPresent(key RecordID) (bool, error, *Record) {
	rec, err := r.Get(key, false)
	if err != nil {
		return false, err, nil
	}

	// nokey or expired
	if rec == nil || rec.IsExpired() {
		return false, nil, nil
	}

	return true, nil, rec
}

func (r *RocksDB) Delete(recId RecordID) error {
	db, key := r.sharding.getDbInstanceAndKey(recId)
	if db == nil {
		glog.Errorf("no db for shard %d", recId.GetShardID())
		return errors.New(fmt.Sprintf("no db for shard %d", recId.GetShardID()))
	}
	err := db.Delete(writeOptions, key)
	if err != nil {
		glog.Errorf("RocksDB Error while delete: %s", err.Error())
		return NewDBError(err)
	}
	return nil
}

// TODO: need lock?
func (r *RocksDB) Shutdown() {
	start := time.Now()
	glog.Debug("DB shutting down ...")

	//r.sharding.shutdownShards(r.shards.Keys())
	r.sharding.shutdown()

	glog.Infof("DB shutdown completed in %s", time.Since(start))
}

func (r *RocksDB) duplicate() *RocksDB {
	db := &RocksDB{
		zoneId:    r.zoneId,
		nodeId:    r.nodeId,
		numShards: r.numShards,
		shards:    r.shards,
		sharding:  r.sharding.duplicate(),
	}
	return db
}

func (r *RocksDB) setupShards(newShardMap shard.Map) (oldShards []shard.ID) {
	dbFileNamePrefix := fmt.Sprintf("%d-%d", r.zoneId, r.nodeId)
	r.sharding.setupShards(dbFileNamePrefix, newShardMap)
	for id, _ := range r.shards {
		_, ok := newShardMap[id]
		if !ok {
			oldShards = append(oldShards, id)
		}
	}
	r.shards = newShardMap
	return
}

func (r *RocksDB) setupRedistShards(newShardMap shard.Map) (oldShards []shard.ID) {
	dbFileNamePrefix := fmt.Sprintf("%d-%d", r.zoneId, r.nodeId)
	r.sharding.setupShards(dbFileNamePrefix, newShardMap)
	for id, _ := range r.shards {
		_, ok := newShardMap[id]
		if !ok {
			oldShards = append(oldShards, id)
		}
	}
	r.redistShards = newShardMap
	return
}

func (r *RocksDB) UpdateShards(shards shard.Map) {

	ndb := r.duplicate()

	rmshards := ndb.setupShards(shards)

	// set, next index
	var next int32 = (rocksdbIndex + 1) % 2
	rocksdb[next] = ndb
	glog.Infof("Update Index: %d", next)
	atomic.StoreInt32(&rocksdbIndex, next)

	if len(rmshards) > 0 {
		// close the rocksdb instance no longe needed.
		time.Sleep(1 * time.Second)
		glog.Infof("shards to be removed: %v", rmshards)
		r.sharding.shutdownShards(rmshards)
	}
}

// called by redist watcher to update the rocksb instances
func (r *RocksDB) UpdateRedistShards(shards shard.Map) {
	glog.Infof("Creating New DB with redist shards")
	defer glog.Infof("Creating New DB with redist shards done")

	if len(r.redistShards) > 0 && len(shards) > 0 {
		// two redistribution in a row,
		glog.Warningf("can't do two redistribution in a row")
		return
	}

	if len(shards) == 0 {
		glog.Debugf("no action needed")
		r.redistShards = nil
		return
	}

	glog.Infof("Create New DB with updated redist shards")

	// add new shards
	//	if _, err := os.Stat(r.dbDir); os.IsNotExist(err) {
	//		os.Mkdir(r.dbDir, 0777)
	//	}

	ndb := r.duplicate()

	ndb.setupRedistShards(shards)

	// set, next index
	var next int32 = (rocksdbIndex + 1) % 2
	glog.Infof("Set New DB - from Redist Udpate")

	rocksdb[next] = ndb
	glog.Infof("Update Index: %d", next)

	atomic.StoreInt32(&rocksdbIndex, next)
}

// initial set up
func (r *RocksDB) Setup() {

	if len(DBConfig.DbPaths) == 0 {
		glog.Exit("Error: DbPaths is not set in config.")
	}

	for _, dbpath := range DBConfig.DbPaths {
		if len(dbpath.Path) == 0 {
			glog.Exit("Error: DbPaths contains an empty string.")
		}
		if _, err := os.Stat(dbpath.Path); os.IsNotExist(err) {
			err = os.MkdirAll(dbpath.Path, 0777)
			if err != nil {
				glog.Exit("Error : ", err.Error())
			}
		}
	}

	dbFileNamePrefix := fmt.Sprintf("%d-%d", r.zoneId, r.nodeId)
	r.sharding.setupShards(dbFileNamePrefix, r.shards)
}

func (r *RocksDB) TruncateExpired() {
	//	for i, db := range r.dbs {
	//		glog.Infof("DB number is %d", i)
	//		db.CompactRange(gorocksdb.Range{nil, nil})
	//	}
}

func (r *RocksDB) WriteProperty(propKey string, w io.Writer) {
	r.sharding.writeProperty(propKey, w)
}

func (r *RocksDB) GetIntProperty(propKey string) uint64 {
	return r.sharding.getIntProperty(propKey)
}

// Run in a seperate go routine
// - can only have one go routine running per instance at a time
// - be able to abort
func (r *RocksDB) ReplicateSnapshot(shardId shard.ID, rb *redist.Replicator, mshardid int32) bool {
	if !redist.IsEnabled() {
		glog.Infof("Redistribute is not enabled, ignore replicating snapshot for shard %d", shardId)
		return false
	}

	return r.sharding.replicateSnapshot(shardId, rb, mshardid)
}

func sendRedistRep(shardId shard.ID, ns []byte, key []byte, rec *Record, rb *redist.Replicator) (err error) {

	var rowMsg proto.RawMessage
	rec.EncodeRedistMsg(shardId, ns, key, &rowMsg)

	maxtry := redist.RedistConfig.MaxWaitTime * 1000 / 20

	for i := 0; i < maxtry; i++ {
		err := rb.SendRequest(&rowMsg, false, false)
		if err == nil {
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}

	// one last try
	return rb.SendRequest(&rowMsg, false, true)
}

func (r *RocksDB) ShardSupported(shardId shard.ID) bool {
	if len(r.shards) > 0 {
		_, ok := r.shards[shardId]
		if ok {
			return ok
		}
	}

	if len(r.redistShards) > 0 {
		_, ok := r.redistShards[shardId]
		if ok {
			return ok
		}
	}

	return false
}
