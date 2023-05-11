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
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/BurntSushi/toml"

	"juno/cmd/storageserv/config"
	"juno/cmd/storageserv/storage/db"
	"juno/third_party/forked/golang/glog"
)

var cfg = config.Config{
	NumPrefixDbs:        1,
	NumMicroShards:      0,
	NumMicroShardGroups: 0,
	MicroShardsEnabled:  true,
	DB:                  &db.DBConfig,
}

func LoadConfig(file string) string {
	var path string
	var err error
	if path, err = filepath.Abs(file); err != nil {
		path = file
	}
	glog.Infof("Load config: %s", path)

	if _, err = toml.DecodeFile(file, &cfg); err != nil {
		glog.Exitf("[ERROR] Failed to load local config file %s. %s", file, err)
	}

	if cfg.NumPrefixDbs != 1 {
		glog.Exitf("[ERROR] NumPrefixDbs must be 1 in config.")
	}

	if !cfg.MicroShardsEnabled {
		glog.Exitf("[ERROR] config: MicroShardEnabled is false.")
	}

	if len(cfg.DB.DbPaths) > 1 {
		glog.Exitf("[ERROR] Multiple DB.DbPaths in config is not supported.")
	}

	if len(cfg.DB.DbPaths) == 0 ||
		cfg.DB.DbPaths[0].Path == "" {
		glog.Exitf("[ERROR] DbPaths.Path entry is missing in config.")
	}

	dbPath := cfg.DB.DbPaths[0].Path
	if !cfg.MicroShardsEnabled {
		tagFile := fmt.Sprintf("%s/microshard_enabled.txt", dbPath)
		_, err := os.Stat(tagFile)
		if errors.Is(err, fs.ErrNotExist) {
			// db was converted by dbcopy tool.
			cfg.MicroShardsEnabled = true
			cfg.NumMicroShards = 256
		}
	}

	return dbPath
}
