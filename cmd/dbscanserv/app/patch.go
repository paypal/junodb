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
	"bytes"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"juno/third_party/forked/golang/glog"
	"juno/third_party/forked/tecbot/gorocksdb"

	"juno/cmd/dbscanserv/config"
	"juno/cmd/dbscanserv/prime"
	"juno/cmd/storageserv/storage/db"
	"juno/pkg/proto"
	"juno/pkg/util"
)

type CmdUpdatePatch struct {
	Key   []byte
	Value []byte
}

var (
	writeOptions *gorocksdb.WriteOptions = gorocksdb.NewDefaultWriteOptions()
	dbclient     *gorocksdb.DB
	client       *RpcClient

	namespaces string
	nsList     [][]byte

	relayEnabled bool
	patchDbPath  string
	patchTTL     = 86400 * 8 // Default to 8 days
	debug        bool

	rwMutex    sync.RWMutex
	E9         = uint64(time.Second)
	TEST_PATCH = "__test_patch"
)

// Called by storageserv
func InitPatch(cfg *config.DbScan) {

	namespaces = cfg.ReplicationNamespaces
	names := strings.Split(namespaces, "|")
	nsList = make([][]byte, len(names))
	for i := range names {
		nsList[i] = append(nsList[i], names[i]...)
	}

	if len(cfg.ReplicationAddr) == 0 {
		return
	}
	if len(names) > 0 {
		relayEnabled = true
		if cfg.PatchTTL > 0 {
			patchTTL = cfg.PatchTTL
		}
	}

	debug = cfg.Debug
	glog.Infof("Init %v", *cfg)
	client = NewRpcClient(cfg.ListenPort)
}

// Called by dbscanserv
func InitPatchDb(rootPath string, cfg *config.DbScan, readOnly bool) {

	if dbclient != nil {
		return
	}
	relayEnabled = (len(cfg.ReplicationAddr) > 0 &&
		len(cfg.ReplicationNamespaces) > 0)
	if !relayEnabled {
		return
	}
	if cfg.PatchTTL > 0 {
		patchTTL = cfg.PatchTTL
	}

	name := cfg.PatchDbName
	if len(name) == 0 {
		name = "patch.db"
	}
	patchDbPath = fmt.Sprintf("%s/%s", rootPath, name)
	dbclient = prime.NewDbHandle(0, patchDbPath, readOnly)
}

func matchNamespace(ns []byte) bool {
	for i := range nsList {
		if bytes.Compare(ns, nsList[i]) == 0 {
			return true
		}
	}
	return false
}

// Used by storageserv
// Bump Version
// Keep same CreationTime
// Clear payload
// key is recordId.GetKey()
func RelayDelete(ns []byte, key []byte, rec *db.Record) error {

	var reply CmdReply
	var buf bytes.Buffer

	if !relayEnabled || !matchNamespace(ns) {
		return nil
	}
	ns2, appkey, _ := prime.GetNamespaceAndKey(key)
	tmp := *rec
	tmp.LastModificationTime = uint64(time.Now().UnixNano())
	tmp.Version++
	et := util.GetExpirationTime(uint32(patchTTL))
	if et > tmp.ExpirationTime {
		et = tmp.ExpirationTime
	}
	tmp.ExpirationTime = et
	tmp.Payload.Clear()
	tmp.ClearMarkedDelete()
	buf.Grow(tmp.EncodingSize())
	tmp.EncodeToBuffer(&buf)

	if debug || ns2 == TEST_PATCH {
		now := uint32(time.Now().Unix())
		msg := ""
		if len(appkey) != 16 {
			msg = "key=" + string(appkey)
		}

		glog.Infof("Relay: ct=(%d, %d) ver=(%d, %d) ttl=(%d, %d) mt=(%d, %d) md=%v %s",
			tmp.CreationTime, rec.CreationTime,
			tmp.Version, rec.Version,
			tmp.ExpirationTime-now, rec.ExpirationTime-now,
			tmp.LastModificationTime/E9, rec.LastModificationTime/E9,
			tmp.IsMarkedDelete(),
			msg)
	}

	req := CmdUpdatePatch{
		Key:   key,
		Value: buf.Bytes(),
	}
	err := client.Invoke("Remote.UpdatePatch", req, &reply)
	if err != nil {
		glog.Errorf("[ERROR] err=%s", err)
		return err
	}

	return nil
}

func ClosePatchDb() {
	if dbclient == nil {
		return
	}

	rwMutex.Lock()
	dbclient.SetOption("disable_auto_compactions", "true")
	dbclient.Flush(gorocksdb.NewDefaultFlushOptions())
	dbclient.Close()
	rwMutex.Unlock()
}

func UpdatePatch(key, value []byte) error {
	if dbclient == nil {
		return nil
	}

	var rec db.Record
	err := rec.Decode(value)
	if err != nil {
		glog.Errorf("%s", err)
	}

	if dbscanConfig.Debug {
		ns, appkey, ok := prime.GetNamespaceAndKey(key)
		msg := ""
		if len(appkey) != 16 {
			msg = "key=" + string(appkey)
		}
		glog.Infof("ns=%s, %s ok=%v ttl=%d", ns, msg,
			ok, rec.ExpirationTime-uint32(time.Now().Unix()))
	}
	rwMutex.RLock()
	err = dbclient.Put(writeOptions, key, value)
	rwMutex.RUnlock()
	if err != nil {
		glog.Errorf("%s", err)
	}
	return err
}

func (r *Remote) UpdatePatch(req CmdUpdatePatch, reply *CmdReply) error {

	return UpdatePatch(req.Key, req.Value)
}

// Patch records in target pool
func DoPatch(namespace string) int {

	if dbclient == nil {
		return 0
	}
	rwMutex.Lock()
	dbclient.SetOption("disable_auto_compactions", "true")
	dbclient.Flush(gorocksdb.NewDefaultFlushOptions())
	dbclient.SetOption("disable_auto_compactions", "false")
	rwMutex.Unlock()

	snapshot := dbclient.NewSnapshot()
	defer dbclient.ReleaseSnapshot(snapshot)

	ro := gorocksdb.NewDefaultReadOptions()
	ro.SetSnapshot(snapshot)

	it := dbclient.NewIterator(ro)
	defer it.Close()
	count := 0
	expired := 0

	// Scan
	prefix := make([]byte, 1)
	time.Sleep(time.Duration(rand.Intn(5000)) * time.Millisecond)
	for id := 0; id < 4; id++ {
		prefix[0] = byte(id)

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {

			key := it.Key()
			value := it.Value()

			var rec db.Record
			err := rec.Decode(value.Data())
			if err != nil {
				key.Free()
				value.Free()
				continue
			}

			count++
			if (count%1000) == 0 && IsCancel() {
				return count
			}
			now := uint32(time.Now().Unix())
			if rec.ExpirationTime < now {
				expired++
				if dbscanConfig.Debug {
					ns, appkey, _ := prime.GetNamespaceAndKey(key.Data())
					if len(namespace) > 0 && ns != namespace {
						key.Free()
						value.Free()
						continue
					}
					glog.Infof("Patch expired: ns=%s ct=%d ver=%d ttl=-%d mt=%d key=%s",
						ns, rec.CreationTime,
						rec.Version,
						now-rec.ExpirationTime,
						rec.LastModificationTime/E9,
						string(appkey))
				}
				key.Free()
				value.Free()
				continue
			}

			var req proto.OperationalMessage
			req.SetAsRequest()
			req.SetAsDeleteReplication()
			req.SetOpCode(proto.OpCodeUpdate)
			ns, appkey, ok := prime.GetNamespaceAndKey(key.Data())
			if !ok {
				key.Free()
				value.Free()
				continue
			}
			req.SetKey(appkey)
			req.SetNamespace([]byte(ns))
			req.SetLastModificationTime(rec.LastModificationTime)
			req.SetCreationTime(rec.CreationTime)
			req.SetVersion(rec.Version)
			et := util.GetExpirationTime(uint32(600))
			req.SetExpirationTime(et)
			req.SetNewRequestID()
			req.SetOriginatorRequestID(rec.OriginatorRequestId)

			if ns == TEST_PATCH || dbscanConfig.Debug {
				msg := ""
				if len(appkey) != 16 {
					msg = "key=" + string(appkey)
				}
				glog.Infof("Patch: ns=%s ct=(%d, %d) ver=(%d, %d) ttl=(%d, %d)"+
					" mt=(%d, %d) %s rid=%s",
					ns, req.GetCreationTime(), rec.CreationTime,
					req.GetVersion(), rec.Version,
					req.GetExpirationTime()-now, rec.ExpirationTime-now,
					req.GetLastModificationTime()/E9, rec.LastModificationTime/E9,
					msg, rec.OriginatorRequestId)
			}

			prime.ReplicateRecord(&req)
			key.Free()
			value.Free()
		}
	}
	glog.Infof("Patch: totalKeys=%d expiredKeys=%d\n", count, expired)

	return count
}
