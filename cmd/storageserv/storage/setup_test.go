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

package storage

import (
	"flag"
	"os"
	"testing"

	"juno/third_party/forked/golang/glog"

	"juno/cmd/storageserv/config"
	"juno/cmd/storageserv/storage/db"
	"juno/pkg/shard"
)

func testSetup() {
	config.ServerConfig().ClusterInfo.NumShards = 1
	shardMap = shard.NewMap()
	shardMap[shard.ID(0)] = struct{}{}

	config.ServerConfig().DB.DbPaths = []db.DbPath{
		db.DbPath{"./test.db", 0}}
	db.Initialize(1, 1, 0, 0, 0, 0, shardMap)
	InitializeCMap(1)
	//	Setup()
}

func testTeardown() {
	db.Finalize()
	os.RemoveAll("./test.db")
}

func TestMain(m *testing.M) {

	var logLevel string
	flag.StringVar(&logLevel, "log_level", "error", "specify log level")
	flag.Parse()
	glog.InitLogging(logLevel, " [st] ")

	testSetup()
	rc := m.Run()
	testTeardown()
	os.Exit(rc)
}
