package compact

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"juno/cmd/storageserv/storage/db"
	"juno/pkg/service"
	"juno/third_party/forked/golang/glog"
	"juno/third_party/forked/tecbot/gorocksdb"
)

type DbClient struct {
	DbPath        string
	PrefixLen     int
	FilterEnabled bool
	ReadOnly      bool

	db *gorocksdb.DB
}

func fileExist(name string) bool {
	files, _ := filepath.Glob(name)
	return len(files) > 0
}

func Watch(zoneid, nodeid int, suspend service.SuspendFunc) {

	dir := "events"
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		os.Mkdir(dir, 0777)
	}

	eventFile := fmt.Sprintf("%s/zone_%d", dir, zoneid)
	start := fmt.Sprintf("%s/zone_%d_start_%d", dir, zoneid, nodeid)
	pass := fmt.Sprintf("%s/zone_%d_done_%d_pass", dir, zoneid, nodeid)
	fail := fmt.Sprintf("%s/zone_%d_done_%d_fail", dir, zoneid, nodeid)
	done := fmt.Sprintf("%s/zone_%d_done_%d_*", dir, zoneid, nodeid)

	time.Sleep(5 * time.Second)
	if fileExist(start) { // Session was left over.
		err := os.Rename(start, fail)
		if err == nil {
			glog.Errorf("Compact failed.")
		}
	}

	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {

		case <-ticker.C:

			if !fileExist(eventFile) || fileExist(done) {
				break
			}
			event := NewEventConfig(eventFile)
			if event == nil || !event.isNamespace() {
				break
			}

			initFilter(event)
			// Suspend service
			suspend(true)
			os.Create(start)
			time.Sleep(3 * time.Second)
			db.GetDB().Shutdown()

			ok := CompactDb()
			if ok {
				os.Rename(start, pass)
			} else {
				os.Rename(start, fail)
			}

			db.GetDB().Setup()
			// Resume service
			suspend(false)
		}
	}
}

func CompactDb() bool {

	d := db.GetPrefixDB()
	if d == nil {
		return false
	}

	ok := true
	for _, name := range d.DbNames {
		dbClient := &DbClient{
			DbPath:        name,
			PrefixLen:     d.PrefixBytes,
			FilterEnabled: true,
		}
		if dbClient.init() == nil {
			continue
		}

		if err := dbClient.compactNamespace(); err != nil {
			ok = false
		}
		dbClient.displayStats()
		dbClient.close()
	}

	return ok
}

func (d *DbClient) init() *DbClient {

	blockOpts := db.ConfigBlockCache()
	opts := db.NewRocksDBptions()
	opts.SetBlockBasedTableFactory(blockOpts)

	glog.Infof("dbpath=%s prefix_len=%d", d.DbPath, d.PrefixLen)
	opts.SetPrefixExtractor(gorocksdb.NewFixedPrefixTransform(d.PrefixLen))
	opts.SetDisableAutoCompactions(true)

	if d.FilterEnabled {
		opts.SetCompactionFilter(&namespaceFilter{})
	}

	var err error
	if d.ReadOnly {
		d.db, err = gorocksdb.OpenDbForReadOnly(opts, d.DbPath, true)
	} else {
		d.db, err = gorocksdb.OpenDb(opts, d.DbPath)
	}
	if err != nil {
		glog.Errorf("[ERROR] dbpath=%s, Open failed: %s", d.DbPath, err)
		return nil
	}

	return d
}

func (d *DbClient) close() {

	if d.db == nil {
		return
	}
	d.db.Flush(gorocksdb.NewDefaultFlushOptions())

	d.db.Close()
	d.db = nil
}

func (d *DbClient) displayStats() {

	d.db.Flush(gorocksdb.NewDefaultFlushOptions())
	fmt.Fprintf(os.Stderr, "db: %s\nestimate-num-keys: %d\n\n%s\n\n",
		d.DbPath,
		d.db.GetIntProperty("rocksdb.estimate-num-keys"),
		d.db.GetProperty("rocksdb.levelstats"))

}

func (d *DbClient) compactNamespace() error {

	compactOpts := gorocksdb.NewDefaultCompactOptions()

	// No auto compaction.
	compactOpts.SetExclusiveManual(true)

	keyRange := d.getKeyRange(0)

	glog.Infof("Compact started ...")
	err := d.db.CompactRangeOptions(compactOpts, keyRange)
	if err != nil {
		glog.Errorf("Compact failed: %s", err.Error())
		return err
	}

	count := getMatchCount()
	if count == 0 {
		glog.Infof("Compact completed.")
		return nil
	}

	glog.Infof("Compact completed: delete_count=%d", count)
	return nil
}

func (d *DbClient) getPrefixKey(shardId uint16) []byte {

	prefix := make([]byte, 2)

	binary.BigEndian.PutUint16(prefix[0:], shardId)
	return prefix
}

func (d *DbClient) getKeyRange(startId uint16) gorocksdb.Range {

	beginKey := d.getPrefixKey(startId)

	keyRange := gorocksdb.Range{
		Start: beginKey,
		Limit: nil,
	}
	return keyRange
}
