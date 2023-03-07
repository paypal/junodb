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
  
package main

import (
	"fmt"

	"juno/third_party/forked/golang/glog"
	"juno/third_party/forked/tecbot/gorocksdb"

	stor "juno/cmd/storageserv/storage/db"
)

func PrintDbStats(name string) {

	if name == "" {
		return
	}

	blockOpts := stor.ConfigBlockCache()
	opts := stor.NewRocksDBptions()
	opts.SetBlockBasedTableFactory(blockOpts)

	db, err := gorocksdb.OpenDbForReadOnly(opts, name, true)
	if err != nil {
		glog.Errorf("%s", err)
		return
	}

	fmt.Printf("\n%s\n%s\n\n",
		db.GetProperty("rocksdb.levelstats"),
		db.GetProperty("rocksdb.aggregated-table-properties"))

	db.Close()
	return
}
