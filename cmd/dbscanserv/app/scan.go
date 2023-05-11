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

package app

import (
	"encoding/binary"
	"sync/atomic"
	"time"

	"juno/third_party/forked/golang/glog"
	"juno/third_party/forked/tecbot/gorocksdb"

	"juno/cmd/dbscanserv/prime"
	"juno/cmd/storageserv/storage/db"
)

// Map from zoneid, shardid to Scanner
type ScannerMap map[int]*Scanner

type Scanner struct {
	zoneid     int
	shardid    int
	nodeid     int
	serverAddr string
	local      bool

	state int32
	ch    chan *prime.MessageBlock

	front    *prime.MessageBlock
	beginTag *prime.MessageBlock
	endTag   *prime.MessageBlock

	opData prime.OpData
}

type ShardMap map[int]*ShardInfo

type ShardInfo struct {
	nodeid     int
	serverAddr string
}

const (
	MAX_RANGES = 256
	CHAN_LEN   = 4
)

const (
	Idle = iota
	Running
	Finished
)

var (
	// One scanner per zone, shard
	scannerMap = make(ScannerMap, 5*1024)

	// One entry per zone, shard
	shardMap = make(ShardMap, 5*1024)
)

// One scanner per zoneid, shardid.
func GetScanner(zoneid, shardid int) *Scanner {
	key := prime.GenMapKey(zoneid, shardid)
	return scannerMap[key]
}

func AddScanner(zoneid, shardid, nodeid int, ip string) {
	key := prime.GenMapKey(zoneid, shardid)
	isLocal := IsLocalAddress(ip, zoneid)
	var q chan *prime.MessageBlock

	if isLocal {
		q = make(chan *prime.MessageBlock, CHAN_LEN)
	}

	beginTag := prime.NewMessageBlock(shardid, -1, -1)
	scannerMap[key] = &Scanner{
		zoneid:     zoneid,
		shardid:    shardid,
		nodeid:     nodeid,
		serverAddr: GenRemoteAddr(ip, zoneid),
		local:      isLocal,

		state: Idle,
		ch:    q,

		front:    beginTag,
		beginTag: beginTag,
		endTag:   prime.NewMessageBlock(shardid, MAX_RANGES, 0),
	}
}

func GetShardInfo(zoneid, shardid int) (info *ShardInfo, found bool) {
	key := prime.GenMapKey(zoneid, shardid)
	info, found = shardMap[key]
	return
}

func AddShardInfo(zoneid, shardid, nodeid int, ip string) {
	key := prime.GenMapKey(zoneid, shardid)
	shardMap[key] = &ShardInfo{
		nodeid:     nodeid,
		serverAddr: GenRemoteAddr(ip, zoneid),
	}
}

func ResetScanners(startid, stopid int) bool {

	for _, scanner := range scannerMap {

		if scanner.shardid < startid || scanner.shardid >= stopid {
			continue
		}

		if scanner.local && atomic.LoadInt32(&scanner.state) == Running {
			return false // Not ready
		}
	}

	for _, scanner := range scannerMap {

		if scanner.shardid < startid || scanner.shardid >= stopid {
			continue
		}

		atomic.CompareAndSwapInt32(&scanner.state, Finished, Idle)
		scanner.replaceFront(scanner.beginTag)
	}

	return true
}

func (s *Scanner) ScanAndMerge(rangeid int, jm *prime.JoinMap, rs *prime.Result) error {

	if s.local {
		s.startThread(rangeid)
	}

	err := s.merge(rangeid, jm, rs)
	if err != nil {
		return err
	}

	return nil
}

func (s *Scanner) GetNext(rangeid int, keyList prime.KeyList) *prime.MessageBlock {

	s.startThread(rangeid)

	return s.localNext(rangeid, keyList)
}

// Start a scanner thread if in Idle state.
func (s *Scanner) startThread(rangeid int) bool {

	if !atomic.CompareAndSwapInt32(&s.state, Idle, Running) {
		// Not in Idle state
		return false
	}

	s.rewind()

	go func(rangeid int) {
		s.run(rangeid)
		atomic.StoreInt32(&s.state, Finished)

	}(rangeid)

	return true
}

func (s *Scanner) merge(rangeid int, jm *prime.JoinMap, rs *prime.Result) (err error) {

	var mb *prime.MessageBlock

	if s.local {
		mb = s.localNext(rangeid, rs.KeysByZone[s.zoneid])
	} else {
		mb, err = s.remoteNext(rangeid, rs.KeysByZone[s.zoneid])

		if err != nil { // No merge for this range
			return err
		}
	}

	if mb.Rangeid != rangeid || mb.IsEmpty() || jm == nil ||
		rangeid >= prime.GetRangeCount() {
		return nil
	}

	for k := range mb.Data {
		jm.Insert(s.zoneid, mb.Data[k].Key, mb.Data[k].Val[0:])
	}
	return nil
}

func (s *Scanner) rewind() {

	s.replaceFront(s.beginTag)
	s.opData.Reset()

	if s.local {
		close(s.ch)
		s.ch = make(chan *prime.MessageBlock, CHAN_LEN)
	}
}

func getPrefixKey(shardid uint16) []byte {

	prefix := make([]byte, 2)

	binary.BigEndian.PutUint16(prefix[0:], shardid)
	return prefix
}

func (s *Scanner) run(rangeid int) {

	handle := prime.GetDbHandle(s.zoneid, s.nodeid)
	snapshot := handle.NewSnapshot()
	defer handle.ReleaseSnapshot(snapshot)

	ro := gorocksdb.NewDefaultReadOptions()
	ro.SetSnapshot(snapshot)

	it := handle.NewIterator(ro)
	defer it.Close()

	curr := 0
	mb := prime.NewMessageBlock(s.shardid, curr, -1)

	// Set prefix
	prefix := getPrefixKey(uint16(s.shardid))
	count := 0

	var rec db.Record
	prime.UpdateDbAccessTime()
	nsCopy := prime.IsCopyNamespace()

	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {

		key := it.Key()
		value := it.Value()

		if nsCopy &&
			!prime.MatchNamespace(s.zoneid, key.Data()) {
			key.Free()
			value.Free()
			continue
		}

		next := prime.GetRangeidFromKey(key.Data())
		if next < rangeid {
			key.Free()
			value.Free()
			continue
		}
		if next != curr {
			count += mb.NumKeysThisBlock()
			if mb.NotEmpty() {
				// Push current block
				s.enqueue(mb)
			}

			if IsCancel() {
				return
			}
			curr = next
			mb = prime.NewMessageBlock(s.shardid, curr, -1)
		}

		err := rec.Decode(value.Data())
		if err == nil {
			mb.AppendData(key.Data(), &rec, nsCopy)
		} else {
			glog.Errorf("[ERROR] Decode error=%s", err)
		}

		key.Free()
		value.Free()
	}

	count += mb.NumKeysThisBlock()
	if mb.NotEmpty() {
		mb.TotalKeys = count
		s.enqueue(mb)
	}

	mb = s.endTag
	mb.TotalKeys = count
	s.enqueue(mb)

	glog.Infof("  zoneid=%d shardid=%d rangeid=%d count=%d",
		s.zoneid, mb.Shardid, mb.Rangeid, count)
}

// Push to channel
func (s *Scanner) enqueue(mb *prime.MessageBlock) {

	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	prime.UpdateDbAccessTime()
	for {
		select {
		case s.ch <- mb:
			return
		case <-ticker.C:
			prime.UpdateDbAccessTime()
			if IsCancel() {
				return
			}
		}
	}
}

// Fetch next message block from remote.
func (s *Scanner) remoteNext(rangeid int, keyList prime.KeyList) (*prime.MessageBlock, error) {

	if s.front.Rangeid >= rangeid {
		return s.front, nil
	}

	client := GetRpcClient(s.serverAddr)

	req := &Request{
		Zoneid:  s.zoneid,
		Shardid: s.shardid,
		Rangeid: rangeid,
		KeyList: keyList,
	}

	var mb = &prime.MessageBlock{}

	err := client.Invoke("Remote.GetNext", *req, mb)
	if err != nil {
		glog.Errorf("[ERROR] shardid=%d Remote.GetNext: %s", s.shardid, err)
		return s.front, err
	}

	s.replaceFront(mb)

	return s.front, nil
}

// Fetch next message block from local.
func (s *Scanner) localNext(rangeid int, keyList prime.KeyList) *prime.MessageBlock {

	s.opData.FindAndReplicate(s.zoneid, rangeid, keyList)

	currRangeid := s.front.Rangeid
	var mb *prime.MessageBlock
	var valid bool
	for currRangeid < rangeid {
		mb, valid = <-s.ch

		if !valid {
			s.replaceFront(s.endTag)
			return s.endTag
		}
		currRangeid = mb.Rangeid
	}

	if mb != nil {
		s.opData.SetData(s.zoneid, s.shardid, mb)
		s.replaceFront(mb)
	}
	return s.front
}

func (s *Scanner) replaceFront(mb *prime.MessageBlock) {
	s.front.Release()
	s.front = mb
}
