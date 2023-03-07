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
  
package patch

import (
	"time"

	"juno/third_party/forked/golang/glog"

	"juno/cmd/dbscanserv/app"
	"juno/cmd/dbscanserv/config"
	"juno/cmd/storageserv/storage/db"
	"juno/pkg/proto"
)

// Called by storageserv.
func Init(cfg *config.DbScan) {
	app.InitPatch(cfg)
}

// Called by storageserv.
// key is recordId.GetKey().
func RelayDelete(ns []byte, key []byte, rec *db.Record) error {
	return app.RelayDelete(ns, key, rec)
}

// Called by storageserv.
func DeleteNeeded(op *proto.OperationalMessage, rec *db.Record) bool {
	var result bool
	var tail string
	if op.GetCreationTime() >= rec.CreationTime {
		result = true
		tail = " >> deleted"
	}
	if glog.LOG_DEBUG || string(op.GetNamespace()) == "__test_patch" {
		now := uint32(time.Now().Unix())
		key := string(op.GetKey())
		if len(key) > 8 {
			key = ""
		}
		glog.Infof("ct=(%d, %d) ver=(%d, %d) ttl=(%d, %d) mt=(%d, %d) key=%s %s",
			op.GetCreationTime(), rec.CreationTime,
			op.GetVersion(), rec.Version,
			op.GetExpirationTime()-now, rec.ExpirationTime-now,
			op.GetLastModificationTime()/app.E9, rec.LastModificationTime/app.E9,
			key, tail)
	}

	return result
}
