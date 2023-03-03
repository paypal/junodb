package stats

import (
	"fmt"
	"net/http/pprof"

	"juno/third_party/forked/golang/glog"

	"juno/cmd/storageserv/config"
	"juno/cmd/storageserv/stats/shmstats"
	"juno/pkg/debug"
)

func InitForManager(numChildren int) (err error) {
	return shmstats.InitForManager(numChildren)
}

func initForWorker(isChild bool, workerId int, zoneId uint32, machineIndex uint32) (err error) {
	cfg := config.ServerConfig()
	if cfg == nil {
		err = fmt.Errorf("nil config")
		return
	}

	if err = shmstats.InitForWorker(isChild, workerId, zoneId, machineIndex); err != nil {
		return
	}
	htmlstats.ClusterName = cfg.ClusterName
	htmlstats.AddSection(&htmlSectServerInfoT{})
	if cfg.StateLogEnabled {
		htmlstats.AddSection(&htmlSectReqProcStatsT{})
	}
	workerIdString = fmt.Sprintf("%d", workerId)
	HttpServerMux.HandleFunc("/", indexHandler)

	initDbIndexTemplate(workerIdString)
	initPprofIndexTemplate(workerIdString)

	addPage("/stats", httpStatsHandler)

	addPage("/debug/pprof/", debugPprofHandler)
	addPage("/debug/dbstats/", httpDebugDbStatsHandler)
	addPage("/debug/config", debugConfigHandler)
	addPage("/debug/pprof/profile", pprof.Profile)
	addPage("/debug/pprof/symbol", pprof.Symbol)
	addPage("/debug/pprof/trace", pprof.Trace)

	if debug.DEBUG {
		addPage("/debug/memstats", debugMemStatsHandler)
	}
	return
}

func InitForWorker(args ...interface{}) (err error) {
	sz := len(args)
	if sz < 4 {
		err = fmt.Errorf("4 argument expected")
		glog.Error(err)
		return
	}
	var (
		isChild      bool
		workerId     int
		zoneId       uint32
		machineIndex uint32
		ok           bool
	)
	if isChild, ok = args[0].(bool); !ok {
		err = fmt.Errorf("wrong argument 0 type, bool expected")
		glog.Error(err)
		return
	}
	if workerId, ok = args[1].(int); !ok {
		err = fmt.Errorf("wrong argument 1 type, int expected")
		glog.Error(err)
		return
	}
	if zoneId, ok = args[2].(uint32); !ok {
		err = fmt.Errorf("wrong argument 2 type, uint expected")
		glog.Error(err)
		return
	}
	if machineIndex, ok = args[3].(uint32); !ok {
		err = fmt.Errorf("wrong argument 3 type, uint expected")
		glog.Error(err)
		return
	}

	return initForWorker(isChild, workerId, zoneId, machineIndex)
}

func Finalize() {
	shmstats.Finalize()
}
