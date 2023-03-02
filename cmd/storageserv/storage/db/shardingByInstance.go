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
	"fmt"
	"io"
	"sync"
	"time"

	"juno/third_party/forked/golang/glog"
	"juno/third_party/forked/tecbot/gorocksdb"

	"juno/cmd/storageserv/redist"
	"juno/pkg/shard"
	redistst "juno/pkg/stats/redist"
	"juno/pkg/util"
)

type ShardingByInstance struct {
	ShardingBase
	dbnamePrefix string
	dbs          []*gorocksdb.DB
}

func (r *ShardingByInstance) getDbInstanceAndKey(id RecordID) (dbInst *gorocksdb.DB, key []byte) {
	shardId := id.GetShardID()
	dbInst = r.dbs[shardId]
	if dbInst == nil {
		glog.Errorf("no db for shard %d", shardId)
		return
	}
	key = id.GetKeyWithoutShardID()
	return
}

func (s *ShardingByInstance) setupShards(dbnamePrefix string, shardMap shard.Map) {
	numShards := len(shardMap)

	if numShards == 0 {
		///LOG?
		return
	}

	glog.Infof("setup shards: %v", shardMap.Keys())

	shardsToBeSetup := make([]shard.ID, 0, numShards)
	for i, _ := range shardMap {
		handle := s.dbs[i]
		if handle != nil {
			fmt.Printf("i: %d	name: %s", i, handle.Name())
			// already exist
			s.dbs[i] = handle
		} else {
			shardsToBeSetup = append(shardsToBeSetup, i)
		}
	}

	num := len(shardsToBeSetup)

	if num != 0 {

		blockOpts := ConfigBlockCache()

		var paths = make([]string, len(DBConfig.DbPaths))
		var target_sizes = make([]uint64, len(DBConfig.DbPaths))

		dbNames := make([]string, num, num)
		options := make([]*gorocksdb.Options, num, num)
		for i, shardId := range shardsToBeSetup {
			options[i] = NewRocksDBptions()
			if !DBConfig.WriteDisableWAL && len(DBConfig.WalDir) > 0 {
				options[i].SetWalDir(fmt.Sprintf("%s/wal-%s-%d", DBConfig.WalDir, dbnamePrefix, shardId))
			}
			options[i].SetBlockBasedTableFactory(blockOpts)
			options[i].SetCompactionFilter(&compactionFilter{})

			fileName := fmt.Sprintf("%s-%d.db", dbnamePrefix, shardId)
			for k, dbpath := range DBConfig.DbPaths {
				paths[k] = fmt.Sprintf("%s/%s", dbpath.Path, fileName)
				target_sizes[k] = dbpath.TargetSize
				glog.Infof("dbName=%s", paths[k])
			}
			dbNames[i] = paths[0]

			if len(DBConfig.DbPaths) > 1 { // Add extra paths
				dbPaths := gorocksdb.NewDBPathsFromData(paths, target_sizes)
				options[i].SetDBPaths(dbPaths)
			}
		}

		wg := new(sync.WaitGroup)
		wg.Add(num)
		for i, shardId := range shardsToBeSetup {
			go func(sid shard.ID, dbopts *gorocksdb.Options, dbname string) {
				defer wg.Done()
				if db, err := gorocksdb.OpenDb(dbopts, dbname); err == nil {
					s.dbs[sid] = db
				} else {
					glog.Exitf("Error opening database:%s", err.Error())
				}

			}(shardId, options[i], dbNames[i])
		}

		glog.Infof("Waiting for all shards/dbs to be opened ...")
		wg.Wait()
		glog.Infof("all shards/dbs are opened")

		//LOG Alert in CAL?
	}
}

func (s *ShardingByInstance) shutdownShards(shards []shard.ID) {
	szDbInst := len(s.dbs)

	if len(shards) > 0 {
		wg := new(sync.WaitGroup)
		wg.Add(len(shards))

		for _, shardId := range shards {
			if int(shardId) >= szDbInst {
				glog.Errorf("shardId %d > len(s.dbs)", shardId)
				continue
			}
			go func(sid shard.ID) {
				defer wg.Done()
				if s.dbs[sid] == nil {
					return
				}

				// TODO: thread safe????
				db := s.dbs[sid]

				glog.Infof("Closing DB. shard id: %d", sid)
				fastDbFlush(db)
				db.Close()
				glog.Infof("DB closed. shard id: %d", sid)

				s.dbs[sid] = nil
			}(shardId)
		}
		wg.Wait()
	}
}

func (s *ShardingByInstance) shutdown() {
	var shards []shard.ID
	for i, db := range s.dbs {
		if db != nil {
			shards = append(shards, shard.ID(i))
		}
	}
	if len(shards) != 0 {
		s.shutdownShards(shards)
	}
}

func (s *ShardingByInstance) writeProperty(propKey string, w io.Writer) {
	key := "rocksdb." + propKey
	fmt.Fprintln(w, key)
	for i, db := range s.dbs {
		if db != nil {
			fmt.Fprintf(w, "\nDB %d (%s):\n", i, db.Name())
			stats := db.GetProperty(key)
			w.Write([]byte(stats))
		}
	}
}

// Get total count
func (s *ShardingByInstance) getIntProperty(propKey string) uint64 {
	key := "rocksdb." + propKey
	var valInt uint64
	for _, db := range s.dbs {
		if db != nil {
			valInt += db.GetIntProperty(key)
		}
	}
	return valInt
}

func (s *ShardingByInstance) duplicate() IDBSharding {
	dup := &ShardingByInstance{}
	numShards := len(s.dbs)
	dup.dbs = make([]*gorocksdb.DB, numShards, numShards)
	copy(dup.dbs, s.dbs)
	return dup
}

func (s *ShardingByInstance) replicateSnapshot(shardId shard.ID, rb *redist.Replicator, mshardid int32) bool {

	// Get Latest Sequence Number
	// get a dbstat before transfering
	glog.Debugf("db stats before sending snapshot for shard %d: %s",
		shardId, s.dbs[shardId].GetProperty("rocksdb.stats"))

	// get snapshot
	opts := gorocksdb.NewDefaultReadOptions()
	snapshot := s.dbs[shardId].NewSnapshot()
	opts.SetSnapshot(snapshot)
	iter := s.dbs[shardId].NewIterator(opts)
	defer iter.Close()

	// iterate through snapshot
	cnt := 0
	start := time.Now()
	rb.GetSnapshotStats().SetStatus(redistst.StatsFinish)
	defer rb.LogStats(start, true, false)
	// release snapshot
	defer s.dbs[shardId].ReleaseSnapshot(snapshot)

	rlconfig := redist.RedistConfig.SnapshotRateLimit
	if rb.GetRateLimit() > 0 {
		rlconfig = int64(rb.GetRateLimit())
	}
	ratelimit := redist.NewRateLimiter(rlconfig*1000, 200)

LOOP:
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		ns, key, err := s.decodeStorageKey(iter.Key().Data())
		if err != nil {
			continue LOOP
		}

		rec := new(Record)
		err = rec.Decode(iter.Value().Data())
		if err != nil {
			continue LOOP
		}

		// skip, if expired
		if rec.IsExpired() {
			glog.Verbosef("snapshot record expired, skip. ns=%s, key=%s", ns, util.ToPrintableAndHexString(key))
			continue LOOP
		}

		// throttle
		size := len(iter.Key().Data()) + len(iter.Value().Data())
		ratelimit.GetToken(int64(size))

		//glog.Verbosef("snapshot send ns=%s, key=%s, value=%s", ns, util.ToPrintableAndHexString(key), iter.Value().Data())
		err = sendRedistRep(shardId, ns, key, rec, rb)
		if err != nil {
			rb.LogStats(start, true, true)
			glog.Infof("target node is not available, abort the shard %d redistribution", shardId)
			return false
		}

		cnt++
		// throttling so it does not send too fast
		if !redist.IsEnabled() {
			// aborted, exit now
			glog.Infof("replicating snapshot for shard %d is aborted", shardId)
			return false
		}
	}

	elapsed := time.Since(start)
	glog.Verbosef("total %d records forwarded from shard %d snapshot in %s", cnt, shardId, elapsed)

	// wait till the requests are all processed (3 minutes max)
	return s.waitForFinish(rb)
}

func (s *ShardingByInstance) decodeStorageKey(sskey []byte) ([]byte, []byte, error) {
	return DecodeRecordKeyNoShardID(sskey)
}
