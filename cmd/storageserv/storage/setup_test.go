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
