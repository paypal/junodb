package db

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"juno/third_party/forked/golang/glog"
	"juno/third_party/forked/tecbot/gorocksdb"

	"juno/cmd/storageserv/redist"
	"juno/pkg/shard"
	redistst "juno/pkg/stats/redist"
	"juno/pkg/util"
)

type ShardFilter struct {
	shardNum int32
}

func (s *ShardFilter) SetCompactionFilter(opts *gorocksdb.Options, enable bool) {
	if enable {
		opts.SetCompactionFilter(&compactionFilter{shardFilter: s})
	} else {
		opts.SetCompactionFilter(&compactionFilter{})
	}
}

func (s *ShardFilter) matchShardNum(key []byte) bool {
	expected := atomic.LoadInt32(&s.shardNum)
	if expected < 0 {
		return false
	}
	actual := int32(binary.BigEndian.Uint16(key[0:]))
	return expected == actual
}

func (s *ShardFilter) SetShardNum(shardNum int32) {
	atomic.StoreInt32(&s.shardNum, shardNum)
}

func (s *ShardFilter) Disable() {
	atomic.StoreInt32(&s.shardNum, -1)
}

type ShardingByPrefix struct {
	ShardingBase
	DbNames     []string
	PrefixBytes int

	dbnamePrefix        string
	dbs                 []*gorocksdb.DB
	shardFilters        []*ShardFilter // For ComactRangeByShard
	numMicroShards      int
	numMicroShardGroups int
}

func (s *ShardingByPrefix) getDbInstanceAndKey(id RecordID) (dbInst *gorocksdb.DB, key []byte) {

	numDbs := len(s.dbs)
	shardId := id.GetShardID()
	dbInst = s.dbs[int(shardId)%numDbs]
	if dbInst == nil {
		glog.Errorf("no db for shard %d", shardId)
		return
	}
	key = id

	return
}

func (s *ShardingByPrefix) setupShards(dbnamePrefix string, shardMap shard.Map) {

	numShards := len(shardMap)
	numDbs := len(s.dbs)

	if numShards == 0 || numDbs == 0 {
		return
	}

	newDbs := 0
	for i := 0; i < numDbs; i++ {
		if s.dbs[i] == nil {
			newDbs++
		}
	}
	if newDbs == 0 {
		return
	}

	blockOpts := ConfigBlockCache()

	var paths = make([]string, len(DBConfig.DbPaths))
	var target_sizes = make([]uint64, len(DBConfig.DbPaths))

	s.DbNames = make([]string, numDbs)
	options := make([]*gorocksdb.Options, numDbs)

	for i := 0; i < numDbs; i++ {
		if s.dbs[i] != nil {
			continue
		}

		options[i] = NewRocksDBptions()

		if !DBConfig.WriteDisableWAL && len(DBConfig.WalDir) > 0 {
			options[i].SetWalDir(fmt.Sprintf("%s/wal%s-%d", DBConfig.WalDir, dbnamePrefix, i))
		}

		options[i].SetBlockBasedTableFactory(blockOpts)
		s.PrefixBytes = 2
		if s.numMicroShards > 0 {
			s.PrefixBytes = 3
		}
		options[i].SetPrefixExtractor(gorocksdb.NewFixedPrefixTransform(s.PrefixBytes))

		options[i].SetCompactionFilter(&compactionFilter{shardFilter: s.shardFilters[i]})

		fileName := fmt.Sprintf("%s-%d.db", dbnamePrefix, i)
		for k, dbpath := range DBConfig.DbPaths {
			paths[k] = fmt.Sprintf("%s/%s", dbpath.Path, fileName)
			target_sizes[k] = dbpath.TargetSize
			glog.Debugf("DbName=%s", paths[k])
		}
		s.DbNames[i] = paths[0]

		if len(DBConfig.DbPaths) > 1 { // Add extra paths
			dbPaths := gorocksdb.NewDBPathsFromData(paths, target_sizes)
			options[i].SetDBPaths(dbPaths)
		}
	}

	wg := new(sync.WaitGroup)
	wg.Add(numDbs)
	for i := 0; i < numDbs; i++ {
		if s.dbs[i] != nil {
			wg.Done()
			continue
		}
		go func(ix int, option *gorocksdb.Options, dbname string) {
			defer wg.Done()
			var err error
			if s.dbs[ix], err = gorocksdb.OpenDb(option, dbname); err != nil {
				glog.Exitf("failed to open %s err: %s", dbname, err)
			}
			glog.Debugf("%s opened", dbname)

		}(i, options[i], s.DbNames[i])
	}

	glog.Debugf("waiting for all dbs to be opened ...")
	wg.Wait()
	glog.Infof("%v opened", s.DbNames)

	//LOG Alert in CAL?
}

func (s *ShardingByPrefix) shutdownShards(shards []shard.ID) {

	ok := 0
	for _, shardId := range shards {
		if err := s.DeleteFilesByShard(shardId); err == nil {
			ok++
		}
	}
	glog.Infof("DeleteFilesByShard succeeded for %d shards.", ok)

	ok = 0
	for _, shardId := range shards {
		start := time.Now()
		if err := s.CompactRangeByShard(shardId); err == nil {
			ok++
		}

		// Throttling.  Pause for 0.3 * elapsed per shard
		t := time.Duration(time.Since(start).Seconds() * 300)
		time.Sleep(t * time.Millisecond)
	}
	glog.Infof("CompactRangeByShards succeeded for %d shards.", ok)
}

func (s *ShardingByPrefix) shutdown() {

	if len(s.dbs) == 0 {
		return
	}

	wg := new(sync.WaitGroup)
	wg.Add(len(s.dbs))

	for i, db := range s.dbs {
		if db == nil {
			wg.Done()
			continue
		}
		go func(ix int) {
			defer wg.Done()

			glog.Debugf("Closing DB. db index: %d", ix)
			fastDbFlush(s.dbs[ix])
			s.dbs[ix].Close()
			s.dbs[ix] = nil
			glog.Debugf("DB closed. db index: %d", ix)

		}(i)
	}
	wg.Wait()
}

func (s *ShardingByPrefix) writeProperty(propKey string, w io.Writer) {
	key := "rocksdb." + propKey
	fmt.Fprintln(w, key)
	for _, db := range s.dbs {
		if db != nil {
			fmt.Fprintf(w, "\nDB (%s):\n", db.Name())
			stats := db.GetProperty(key)
			w.Write([]byte(stats))
		}
	}
}

// Get total count
func (s *ShardingByPrefix) getIntProperty(propKey string) uint64 {
	key := "rocksdb." + propKey
	var valInt uint64
	for _, db := range s.dbs {
		if db != nil {
			valInt += db.GetIntProperty(key)
		}
	}
	return valInt
}

type MicroShardGroupStats struct {
	start_id   uint8
	end_id     uint8
	cnt_keys   uint32
	cnt_exp    uint32
	cnt_err    uint32
	start_time time.Time
	mshards    string
	lastgrp    bool
}

func (m *MicroShardGroupStats) reset(numMicroShards int, numMShardsPerGroup int, curGroupNum int) {

	m.lastgrp = true
	if numMicroShards > 0 {
		m.start_id = uint8(curGroupNum * numMShardsPerGroup)
		m.end_id = uint8(int(m.start_id) + numMShardsPerGroup - 1)

		if numMicroShards-1-int(m.end_id) < numMShardsPerGroup { // last group, may have extras
			m.end_id = uint8(numMicroShards - 1)
			m.lastgrp = true
		} else {
			m.lastgrp = false
		}

		m.mshards = fmt.Sprintf(", micro shards (%d-%d) ", m.start_id, m.end_id)
	}
	m.cnt_keys = 0
	m.cnt_exp = 0
	m.cnt_err = 0
	m.start_time = time.Now()
	//glog.Infof("reset group: %d, %d, %d, %d, %d\n", m.start_id, m.end_id, curGroupNum, numMicroShards, numMShardsPerGroup)
}

func (m *MicroShardGroupStats) logStats(shardId shard.ID, rb *redist.Replicator) bool {
	elapsed := time.Since(m.start_time)
	glog.Infof("total %d records forwarded from shard %d%s in %s, excluding expired_cnt=%d, decode_err_cnt=%d",
		m.cnt_keys, shardId, m.mshards, elapsed, m.cnt_exp, m.cnt_err)

	rediststat := rb.GetSnapshotStats()
	rediststat.SetMShardId(int32(m.end_id))
	if m.lastgrp {
		rediststat.SetStatus(redistst.StatsFinish)
	} else {
		rediststat.SetStatus(redistst.StatsInProgress)
	}
	return rb.LogStats(m.start_time, true, false)

}

func (s *ShardingByPrefix) replicateSnapshot(shardId shard.ID, rb *redist.Replicator, mshardid int32) bool {

	numDbs := len(s.dbs)
	dbInst := s.dbs[int(shardId)%numDbs]
	if dbInst == nil {
		glog.Errorf("no db for shard %d", shardId)
		return false
	}

	// Get Latest Sequence Number
	// get a dbstat before transfering
	glog.Infof("db stats before sending snapshot for shard %d: %s",
		shardId, dbInst.GetProperty("rocksdb.stats"))

	// get snapshot
	opts := gorocksdb.NewDefaultReadOptions()
	snapshot := dbInst.NewSnapshot()
	// release snapshot
	defer dbInst.ReleaseSnapshot(snapshot)

	opts.SetSnapshot(snapshot)
	iter := dbInst.NewIterator(opts)
	defer iter.Close()

	// iterate through snapshot
	start := time.Now()
	//defer rb.LogStats(start, true)

	prefix := s.getPrefixKey(shardId)
	numMShardsPerGroup := s.numMicroShards
	if s.numMicroShardGroups > 0 {
		numMShardsPerGroup = s.numMicroShards / s.numMicroShardGroups
	}
	groupnum := 0

	var msgroup MicroShardGroupStats
	msgroup.reset(s.numMicroShards, numMShardsPerGroup, groupnum)

	rlconfig := redist.RedistConfig.SnapshotRateLimit
	if rb.GetRateLimit() > 0 {
		rlconfig = int64(rb.GetRateLimit())
	}
	ratelimit := redist.NewRateLimiter(rlconfig*1000, 200)

LOOP:
	for iter.Seek(prefix); iter.ValidForPrefix(prefix[0:2]); iter.Next() {

		if s.numMicroShards > 0 { // micro shards enabled
			cur_mshardid := int(iter.Key().Data()[2])

			if cur_mshardid < int(mshardid) {
				continue LOOP
			}
			for cur_mshardid > int(msgroup.end_id) {
				//end of last micro shard group, waiting for this group to finish
				s.waitForFinish(rb)
				abort := msgroup.logStats(shardId, rb)
				if abort {
					return false
				}
				groupnum++
				msgroup.reset(s.numMicroShards, numMShardsPerGroup, groupnum)
			}
		}

		ns, key, err := s.decodeStorageKey(iter.Key().Data())
		if err != nil {
			msgroup.cnt_err++
			continue LOOP
		}

		rec := new(Record)
		err = rec.Decode(iter.Value().Data())
		if err != nil {
			msgroup.cnt_err++
			continue LOOP
		}

		// skip, if expired
		if rec.IsExpired() {
			glog.Verbosef("snapshot record expired, skip. ns=%s, key=%s", ns, util.ToPrintableAndHexString(key))
			msgroup.cnt_exp++
			continue LOOP
		}

		// throttle
		size := len(iter.Key().Data()) + len(iter.Value().Data())
		ratelimit.GetToken(int64(size))

		//glog.Verbosef("snapshot send ns=%s, key=%s, value=%s", ns, util.ToPrintableAndHexString(key), rec.Value)
		err = sendRedistRep(shardId, ns, key, rec, rb)
		if err != nil {
			rb.LogStats(start, true, true)
			glog.Infof("target node is not available, abort the shard %d redistribution", shardId)
			return false
		}

		msgroup.cnt_keys++

		if !redist.IsEnabled() {
			// aborted, exit now
			glog.Infof("replicating snapshot for shard %d is aborted", shardId)
			return false
		}
	}

	// log last group stats
	s.waitForFinish(rb)
	abort := msgroup.logStats(shardId, rb)

	if abort {
		return false
	}
	for groupnum < s.numMicroShardGroups-1 {
		// for non-primary shards, we still log stats
		groupnum++
		msgroup.reset(s.numMicroShards, numMShardsPerGroup, groupnum)
		msgroup.logStats(shardId, rb)
	}
	return true
}

func (s *ShardingByPrefix) duplicate() IDBSharding {
	dup := &ShardingByPrefix{}
	dup.dbnamePrefix = s.dbnamePrefix
	dup.dbs = make([]*gorocksdb.DB, len(s.dbs), len(s.dbs))
	copy(dup.dbs, s.dbs)

	dup.shardFilters = make([]*ShardFilter, len(s.shardFilters), len(s.shardFilters))
	copy(dup.shardFilters, s.shardFilters)
	dup.numMicroShards = s.numMicroShards
	dup.numMicroShardGroups = s.numMicroShardGroups
	return dup
}

func (s *ShardingByPrefix) getPrefixKey(shardId shard.ID) []byte {

	prefix := make([]byte, 2)

	binary.BigEndian.PutUint16(prefix[0:], shardId.Uint16())
	return prefix
}

func (s *ShardingByPrefix) getKeyRange(shardId shard.ID) gorocksdb.Range {

	beginKey := s.getPrefixKey(shardId)
	nextShardId := shard.ID(shardId.Uint16() + 1)
	endKey := s.getPrefixKey(nextShardId)
	if nextShardId.Uint16() == 0 {
		endKey = nil
	} // shardId is the last shard.

	keyRange := gorocksdb.Range{
		Start: beginKey,
		Limit: endKey,
	}
	return keyRange
}

func (s *ShardingByPrefix) DeleteFilesByShard(shardId shard.ID) error {

	dbInst := s.dbs[int(shardId)%len(s.dbs)]
	if dbInst == nil {
		msg := fmt.Sprintf("no db for shard %d", shardId)
		return errors.New(msg)
	}

	glog.Debugf("DeleteFilesByShard started for shardId=%d", shardId)
	keyRange := s.getKeyRange(shardId)

	err := dbInst.DeleteFilesInRange(keyRange)
	if err != nil {
		glog.Errorf("DeleteFilesByShard failed for shardId=%d error=%s", shardId, err.Error())
	} else {
		glog.Debugf("DeleteFilesByShard completed for shardId=%d", shardId)
	}

	return err
}

func (s *ShardingByPrefix) CompactRangeByShard(shardId shard.ID) error {

	dbInst := s.dbs[int(shardId)%len(s.dbs)]
	if dbInst == nil {
		msg := fmt.Sprintf("no db for shard %d", shardId)
		return errors.New(msg)
	}

	glog.Debugf("CompactRangeByShard started for shardId=%d", shardId)
	keyRange := s.getKeyRange(shardId)

	compactOpts := gorocksdb.NewDefaultCompactOptions()
	// Allow scheduling of auto compactions.
	compactOpts.SetExclusiveManual(false)

	if s.shardFilters == nil || len(s.shardFilters) == 0 {
		msg := fmt.Sprintf("no shardFilters array")
		return errors.New(msg)
	}

	shardFilter := s.shardFilters[int(shardId)%len(s.shardFilters)]
	if shardFilter == nil {
		msg := fmt.Sprintf("no shardFilter for shard %d", shardId)
		return errors.New(msg)
	}

	shardFilter.SetShardNum(int32(shardId))
	defer shardFilter.Disable()
	err := dbInst.CompactRangeOptions(compactOpts, keyRange)
	if err != nil {
		glog.Errorf("CompactRangeByShard failed for shardId=%d error=%s", shardId, err.Error())
	} else {
		glog.Debugf("CompactRangeByShard completed for shardId=%d", shardId)
	}

	return err
}

func (s *ShardingByPrefix) decodeStorageKey(sskey []byte) ([]byte, []byte, error) {
	return DecodeRecordKey(sskey)
}
