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
