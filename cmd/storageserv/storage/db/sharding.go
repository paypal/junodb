package db

import (
	"io"
	"time"

	"juno/third_party/forked/tecbot/gorocksdb"

	"juno/cmd/storageserv/redist"
	"juno/pkg/shard"
)

type IDBSharding interface {
	getDbInstanceAndKey(id RecordID) (dbInst *gorocksdb.DB, dbKey []byte)

	setupShards(dbnamePrefix string, shardMap shard.Map)

	shutdownShards([]shard.ID)
	shutdown()

	writeProperty(propKey string, w io.Writer)
	getIntProperty(propKey string) uint64

	decodeStorageKey(sskey []byte) ([]byte, []byte, error)
	duplicate() IDBSharding

	replicateSnapshot(shardId shard.ID, rb *redist.Replicator, mshardid int32) bool
}

type ShardingBase struct {
}

func (s *ShardingBase) waitForFinish(rb *redist.Replicator) bool {
	if rb.IsSnapShotDone() {
		return true
	}

	maxwait := redist.RedistConfig.MaxWaitTime * 1000 / 10

	// wait till the requests are all processed or max wait time reached
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	ts_passed := 0
	for {
		select {
		case <-ticker.C:
			if rb.IsSnapShotDone() {
				return true
			}

			ts_passed++
			if ts_passed > maxwait {
				return false
			}
		}
	}
	return true
}
