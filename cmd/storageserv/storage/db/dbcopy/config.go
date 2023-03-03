package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/BurntSushi/toml"

	"juno/third_party/forked/golang/glog"
	"juno/cmd/storageserv/config"
	"juno/cmd/storageserv/storage/db"
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
		if !os.IsNotExist(err) {
			// db was converted by dbcopy tool.
			cfg.MicroShardsEnabled = true
			cfg.NumMicroShards = 256
		}
	}

	return dbPath
}
