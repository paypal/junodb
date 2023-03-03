package app

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"juno/third_party/forked/golang/glog"

	"juno/cmd/proxy/config"
	"juno/cmd/proxy/handler"
	"juno/cmd/proxy/replication"
	"juno/cmd/proxy/stats"
	"juno/cmd/proxy/stats/shmstats"
	"juno/cmd/proxy/watcher"
	"juno/pkg/cluster"
	"juno/pkg/etcd"
	"juno/pkg/initmgr"
	"juno/pkg/logging"
	"juno/pkg/logging/cal"
	"juno/pkg/sec"
	"juno/pkg/service"
	"juno/pkg/udf"
	"juno/pkg/util"
)

type (
	Worker struct {
		CmdProxyCommon
		optWorkerId        uint
		optListenAddresses util.StringListFlags
		optIsChild         bool
		optHttpMonAddr     string
	}
	acceptLimiterT struct {
		acceptDelayTime time.Duration
	}
)

func (c *Worker) Init(name string, desc string) {
	c.CmdProxyCommon.Init(name, desc)
	c.UintOption(&c.optWorkerId, "id|worker-id", 0, "specify the ID of the worker")
	c.ValueOption(&c.optListenAddresses, "listen", "specify listening address. Override Listener in config file")
	c.BoolOption(&c.optIsChild, "child", false, "specify if the worker was started by a parent process")
	c.StringOption(&c.optHttpMonAddr, "mon-addr|monitoring-address", "", "specify the http monitoring address. \n\toverride HttpMonAddr in config file")
}

func (c *Worker) Parse(args []string) (err error) {
	if err = c.CmdProxyCommon.Parse(args); err != nil {
		return
	}
	return
}

func (c *Worker) Exec() {
	appName := fmt.Sprintf("[proxy %d] ", c.optWorkerId)
	numInheritedFDs := util.GetNumOpenFDs()

	cfg := &config.Conf

	initmgr.Register(config.Initializer, c.optConfigFile)
	initmgr.Init() //initalize config first as others depend on it

	if len(cfg.LogLevel) == 0 || c.optLogLevel != kDefaultLogLevel {
		cfg.LogLevel = c.optLogLevel
	}
	if len(c.optListenAddresses) != 0 {
		cfg.SetListeners(c.optListenAddresses)
	}
	if len(c.optHttpMonAddr) != 0 {
		cfg.HttpMonAddr = c.optHttpMonAddr
	}

	if _, err := strconv.Atoi(cfg.HttpMonAddr); err == nil {
		cfg.HttpMonAddr = ":" + cfg.HttpMonAddr
	}

	initmgr.RegisterWithFuncs(glog.Initialize, glog.Finalize, cfg.LogLevel, appName)

	var chWatch chan int
	var etcdReader cluster.IReader
	if cfg.EtcdEnabled {
		chWatch = etcd.WatchForProxy()
		etcdReader = etcd.GetClsReadWriter()
	}
	cacheFile := filepath.Join(cfg.Etcd.CacheDir, cfg.Etcd.CacheName)

	initmgr.RegisterWithFuncs(cal.Initialize, nil, &cfg.CAL)

	initmgr.RegisterWithFuncs(cluster.Initialize, cluster.Finalize, &cluster.ClusterInfo[0],
		&cfg.Outbound, chWatch, etcdReader, cacheFile, &cfg.ClusterStats)
	//
	if c.optIsChild {
		initmgr.RegisterWithFuncs(stats.Initialize, nil, stats.KTypeWorker, int(c.optWorkerId))
	} else {
		initmgr.RegisterWithFuncs(stats.Initialize, stats.Finalize, stats.KTypeStandAloneWorker)
	}

	initmgr.Register(sec.Initializer, &cfg.Sec, cfg.GetSecFlag())
	initmgr.RegisterWithFuncs(replication.Initialize, replication.Finalize, &cfg.Replication)
	if cfg.EtcdEnabled {
		initmgr.RegisterWithFuncs(watcher.Initialize, watcher.Finalize, cfg.ClusterName, etcd.GetEtcdCli(), &cfg.Etcd)
	}
	udf.Init("")

	initmgr.Init()

	stats.RunCollector()

	logging.LogWorkerStart(int(c.optWorkerId))
	defer logging.LogWorkerExit(int(c.optWorkerId))

	var service *service.Service
	httpEnabled := len(cfg.HttpMonAddr) != 0

	if c.optIsChild {
		numListeners := len(cfg.Listener)

		if numInheritedFDs >= numListeners+3 {
			var fds []*os.File

			for i := 0; i < numListeners; i++ {
				if f := os.NewFile(uintptr(3+i), ""); f != nil && util.IsSocket(f) {
					fds = append(fds, f)
				} else {
					glog.Exitf("fd not validate")
				}
			}
			if httpEnabled {
				if numInheritedFDs > numListeners+3 {
					f := os.NewFile(uintptr(numListeners+3), "")
					if f != nil && util.IsSocket(f) {
						if httpListener, err := net.FileListener(f); err == nil {
							shmstats.SetHttpPort(httpListener.Addr().String())
							go func() {
								http.Serve(httpListener, &stats.HttpServerMux)
							}()
						}
					}
				}
			}

			service = handler.NewProxyServiceWithListenFd(&config.Conf, &acceptLimiterT{acceptDelayTime: cfg.Config.ThrottlingDelayTime.Duration}, fds...)
		}

	} else {
		if httpEnabled {
			go func() {
				glog.Infof("to serve HTTP on %s", cfg.HttpMonAddr)
				if err := http.ListenAndServe(cfg.HttpMonAddr, &stats.HttpServerMux); err != nil {
					glog.Warningf("fail to serve HTTP on %s, err: %s", cfg.HttpMonAddr, err)
				}
			}()

		}
		service = handler.NewProxyService(cfg)
	}
	service.Run()
}

func (l *acceptLimiterT) LimitReached() bool {
	return !shmstats.CurrentWorkerHasTheLeastInboundConnections()
}

func (l *acceptLimiterT) Throttle() {
	if l.acceptDelayTime != 0 {
		time.Sleep(l.acceptDelayTime)
	}
}
