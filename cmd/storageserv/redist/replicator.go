package redist

import (
	"errors"
	"sync"
	"time"

	cerr "juno/pkg/errors"
	"juno/pkg/etcd"
	"juno/pkg/io"
	"juno/pkg/proto"
	"juno/pkg/shard"
	redistst "juno/pkg/stats/redist"

	"juno/third_party/forked/golang/glog"
)

type Replicator struct {
	shardId       shard.ID
	processor     *io.OutboundProcessor
	wg            *sync.WaitGroup
	snapshotStats redistst.Stats
	realtimeStats redistst.Stats
	statskey      string
	ratelimit     int
	etcdcli       *etcd.EtcdClient
}

func NewBalancer(shardId shard.ID, processor *io.OutboundProcessor, wg *sync.WaitGroup, key string, ratelimit int, cli *etcd.EtcdClient) (r *Replicator) {
	r = &Replicator{
		shardId:   shardId,
		processor: processor,
		wg:        wg,
		statskey:  key,
		ratelimit: ratelimit,
		etcdcli:   cli,
	}

	//glog.Infof("create new balancer %p, %v", r, r)
	return r
}

func (r *Replicator) SendRequest(msg *proto.RawMessage, params ...bool) error {
	glog.Verbosef("redist:ReplicateRequest: proc=%v, rb=%v", r.processor, r)
	if r.processor == nil {
		return errors.New("outbound processor is not available")
	}

	//default
	realtime := true
	cntOnFailure := true
	if len(params) > 0 {
		realtime = params[0]
	}

	if len(params) > 1 {
		cntOnFailure = params[1]
	}

	var stats *redistst.Stats = &r.snapshotStats
	if realtime {
		stats = &r.realtimeStats
	}

	reqctx := NewRedistRequestContext(msg, r.processor.GetRequestCh(), stats)
	var err *cerr.Error

	if realtime {
		err = r.processor.SendRequest(reqctx)
	} else {
		err = r.processor.SendRequestLowPriority(reqctx)
	}

	if err == nil {
		stats.IncreaseTotalCnt()
		return nil
	}

	// forwarding queue is full or not ready
	if cntOnFailure {
		stats.IncreaseTotalCnt()
		stats.IncreaseDropCnt()
	}
	return errors.New("Forwarding queue is either full or not ready, drop req")
}

func (r *Replicator) GetShardId() shard.ID {
	return r.shardId
}

func (r *Replicator) RestoretSnapShotState(s *redistst.Stats) {
	r.snapshotStats.Restore(s)
}

func (r *Replicator) IsSnapShotDone() bool {
	return r.snapshotStats.HasOutstandingReqs()
}

func (r *Replicator) GetSnapshotStats() *redistst.Stats {
	return &r.snapshotStats
}

func (r *Replicator) LogStats(start time.Time, etcd bool, forceabort bool) (abort bool) {

	abort = false
	if r.snapshotStats.ShouldAbort(RedistConfig.DropThreshold, RedistConfig.ErrThreshold) ||
		r.realtimeStats.ShouldAbort(RedistConfig.DropThresholdRealtime, RedistConfig.ErrThresholdRealtime) {
		r.snapshotStats.SetStatus(redistst.StatsAbort)
		abort = true
	}
	if !abort && forceabort {
		r.snapshotStats.SetStatus(redistst.StatsAbort)
		abort = true
	}

	if abort {
		r.snapshotStats.RestoreFromCheckPoint()
	} else {
		r.snapshotStats.SaveCheckPoint()
	}
	statstr := r.snapshotStats.GetStatsStr(start)
	if etcd {
		r.etcdcli.PutValue(r.statskey, statstr, 5, 5)
	}
	glog.Infof("redistribute shard %d, snapshot stats: %s, realtime stats: %s", r.shardId, statstr, r.realtimeStats.GetStatsStr(start))
	return
}

func (r *Replicator) GetRateLimit() int {
	return r.ratelimit
}

func (r *Replicator) SetRateLimit(limit int) {
	r.ratelimit = limit
}
