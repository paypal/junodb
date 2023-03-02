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
