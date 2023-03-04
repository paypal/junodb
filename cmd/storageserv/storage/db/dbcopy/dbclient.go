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
package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"juno/third_party/forked/golang/glog"
	"juno/third_party/forked/tecbot/gorocksdb"

	"juno/cmd/storageserv/storage/db"
	"juno/pkg/util"
)

type DbClient struct {
	path      string
	db        *gorocksdb.DB
	prefixLen int

	// For target db
	tgtPath string
	tgtdb   *gorocksdb.DB
	ro      *gorocksdb.ReadOptions
	wo      *gorocksdb.WriteOptions
}

func NewDbInstance(dbpath string, prefixLen int, compact bool) (instance *gorocksdb.DB, err error) {

	blockOpts := db.ConfigBlockCache()
	opts := db.NewRocksDBptions()
	opts.SetBlockBasedTableFactory(blockOpts)

	glog.Infof("dbpath=%s prefix_len=%d", dbpath, prefixLen)
	opts.SetPrefixExtractor(gorocksdb.NewFixedPrefixTransform(prefixLen))
	opts.SetDisableAutoCompactions(true)

	if compact {
		opts.SetCompactionFilter(&CompactionFilter{})
	}

	instance, err = gorocksdb.OpenDb(opts, dbpath)
	if err != nil {
		glog.Errorf("[ERROR] dbpath=%s, Open failed: %s", dbpath, err)
		return nil, err
	}

	return instance, nil
}

func (d *DbClient) Close() {

	if d.db == nil {
		return
	}
	d.db.Flush(gorocksdb.NewDefaultFlushOptions())

	if d.tgtdb != nil {
		d.tgtdb.Close()
		d.tgtdb = nil
	}
	d.db.Close()
	d.db = nil
}

func (d *DbClient) DisplayStats() {

	d.db.Flush(gorocksdb.NewDefaultFlushOptions())
	fmt.Fprintf(os.Stderr, "db: %s\nestimate-num-keys: %d\n\n%s\n\n",
		d.path,
		d.db.GetIntProperty("rocksdb.estimate-num-keys"),
		d.db.GetProperty("rocksdb.levelstats"))

	if d.tgtdb != nil {
		d.tgtdb.Flush(gorocksdb.NewDefaultFlushOptions())
		fmt.Fprintf(os.Stderr, "target db: %s\nestimate-num-keys: %d\n\n%s\n\n",
			d.tgtPath,
			d.tgtdb.GetIntProperty("rocksdb.estimate-num-keys"),
			d.tgtdb.GetProperty("rocksdb.levelstats"))
	}
}

func (d *DbClient) Copy(key []byte, val []byte) (error, int) {

	var err error

	var newKey []byte
	if d.prefixLen == 2 {
		if len(key) <= 3 {
			return nil, 0 // bad key
		}
		newKey = make([]byte, len(key)+1)
		copy(newKey, key[0:2])

		// Add microshardId to key for target db
		// offset to skip prefix and namespace
		off := int(2 + 1 + uint8(key[2]))

		if len(key) <= off {
			return nil, 0 // bad key
		}

		newKey[2] = util.GetMicroShardId(key[off:], 256)

		copy(newKey[3:], key[2:])
	} else {
		newKey = make([]byte, len(key))
		copy(newKey[0:], key[0:])
	}

	for i := 0; i < 5; i++ {

		if err = d.tgtdb.Put(d.wo, newKey, val); err != nil {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		// succeeded
		return nil, 1
	}

	return err, 1
}

func (d *DbClient) ScanByShard(start int, stop int, tgtdbPath string) (err error) {

	var ro *gorocksdb.ReadOptions = gorocksdb.NewDefaultReadOptions()

	if stop > start {
		glog.Infof("Scan for shard range [%d:%d)", start, stop)
	}

	snapshot := d.db.NewSnapshot()
	defer d.db.ReleaseSnapshot(snapshot)
	ro.SetSnapshot(snapshot)

	iter := d.db.NewIterator(ro)
	defer iter.Close()

	sum := 0
	sumByNamespace := 0

	// Open target db
	if len(tgtdbPath) > 0 {
		d.tgtdb, err = NewDbInstance(tgtdbPath, 3, false)
		if err != nil {
			return err
		}
		defer d.tgtdb.Flush(gorocksdb.NewDefaultFlushOptions())

		d.tgtPath = tgtdbPath
		d.ro = gorocksdb.NewDefaultReadOptions()
		d.wo = gorocksdb.NewDefaultWriteOptions()
	}

	prefix := make([]byte, 2)
	glog.Infof("Scan db: %s", d.path)
	base := filepath.Base(d.path)

	keyCount := make([]int, stop-start) // one entry per shard

	for i := start; i < stop; i++ {
		binary.BigEndian.PutUint16(prefix[0:], uint16(i))
		count := 0
		matches := 0
		errCount := 0
		badKey := 0

		for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {

			key := iter.Key()
			val := iter.Value()
			if d.tgtdb == nil {
				count++

				if matchNamespace(key.Data(), false) {
					matches++
				}
				key.Free()
				val.Free()
				continue
			}

			// Copy to target db
			var incr int
			err, incr = d.Copy(iter.Key().Data(), iter.Value().Data())
			if err != nil && errCount < 10 {
				glog.Errorf("[ERROR] Write failed: %s", err)
				errCount++
			}

			if err == nil && incr == 0 && badKey < 5 {
				key := iter.Key().Data()
				if len(key) >= 6 {
					glog.Infof("Skip key=%v %s ...", key[0:4], string(key[4:6]))
					badKey++
				}
			}
			count += incr
			key.Free()
			val.Free()
		}

		sum += count
		sumByNamespace += matches
		keyCount[i-start] = count
		if count == 0 {
			continue
		}

		if matches > 0 {
			glog.Infof("db=%s shardId=%d keys=%d nsKeys=%d", base, i, count, matches)
		} else {
			glog.Infof("db=%s shardId=%d keys=%d", base, i, count)
		}

	}
	if sumByNamespace > 0 {
		glog.Infof("db=%s total_keys=%d total_nskeys=%d", base, sum, sumByNamespace)
	} else {
		glog.Infof("db=%s total_keys=%d", base, sum)
	}

	if d.tgtdb != nil {
		glog.Info("")
		glog.Infof("Scan target db: %s", tgtdbPath)

		// Validate keyCount
		err = d.ScanTargetByShard(start, stop, keyCount)
		return err
	}
	return nil
}

func (d *DbClient) ScanTargetByShard(start int, stop int, keyCount []int) error {

	var ro *gorocksdb.ReadOptions = gorocksdb.NewDefaultReadOptions()

	snapshot := d.tgtdb.NewSnapshot()
	defer d.tgtdb.ReleaseSnapshot(snapshot)
	ro.SetSnapshot(snapshot)

	iter := d.tgtdb.NewIterator(ro)
	defer iter.Close()

	prefix := make([]byte, 2)

	base := filepath.Base(d.tgtPath)
	sum := 0
	foundMismatch := false

	for i := start; i < stop; i++ {
		binary.BigEndian.PutUint16(prefix[0:], uint16(i))
		count := 0

		for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
			count++
		}

		sum += count
		tail := ""

		if count < keyCount[i-start] {
			tail = fmt.Sprintf("expected=%d", keyCount[i-start])
			foundMismatch = true
		}
		if count == 0 && len(tail) == 0 {
			continue
		}
		glog.Infof("tgt_db=%s shardId=%d keys=%d %s", base, i, count, tail)
	}

	glog.Infof("tgt_db=%s total_keys=%d", base, sum)
	if foundMismatch {
		glog.Errorf("db=%s [ERROR] Key count mismatches in some shards.", base)
		return errors.New("key count mismatches.")
	}

	glog.Infof("db=%s Copy completed. Key count matches.", base)
	return nil
}

func (d *DbClient) CompactRange(start, stop int) error {

	if stop > start {
		glog.Infof("Compact for shard range [%d:%d)", start, stop)
	}

	if stop == start+1 {
		SetShardidFilter(d.getPrefixKey(uint16(start)))
	}

	compactOpts := gorocksdb.NewDefaultCompactOptions()

	// No auto compaction.
	compactOpts.SetExclusiveManual(true)

	keyRange := d.getKeyRange(uint16(start), uint16(stop))

	err := d.db.CompactRangeOptions(compactOpts, keyRange)
	if err != nil {
		glog.Errorf("Compact failed: %s", err.Error())
		return err
	}

	count := GetMatchCount()
	if count == 0 {
		glog.Infof("Compact completed.")
		return nil
	}
	if keep {
		glog.Infof("Compact completed: ns_keys=%d", count)
	} else {
		glog.Infof("Compact completed: deleted_keys=%d", count)
	}

	return nil
}

func (d *DbClient) getPrefixKey(shardId uint16) []byte {

	prefix := make([]byte, 2)

	binary.BigEndian.PutUint16(prefix[0:], shardId)
	return prefix
}

func (d *DbClient) getKeyRange(startId, stopId uint16) gorocksdb.Range {

	beginKey := d.getPrefixKey(startId)

	var endKey []byte = nil
	if stopId > 0 {
		endKey = d.getPrefixKey(stopId)
	}
	keyRange := gorocksdb.Range{
		Start: beginKey,
		Limit: endKey,
	}
	return keyRange
}
