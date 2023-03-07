//  
//  Copyright 2023 PayPal Inc.
//  
//  Licensed to the Apache Software Foundation (ASF) under one or more
//  contributor license agreements.  See the NOTICE file distributed with
//  this work for additional information regarding copyright ownership.
//  The ASF licenses this file to You under the Apache License, Version 2.0
//  (the "License"); you may not use this file except in compliance with
//  the License.  You may obtain a copy of the License at
//  
//     http://www.apache.org/licenses/LICENSE-2.0
//  
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//  
  
package stats

import (
	"bytes"
	"fmt"
	goio "io"
	"os"
	"path/filepath"
	"strconv"
	"time"

	//	"juno/third_party/forked/golang/glog"

	"juno/cmd/storageserv/config"
	"juno/cmd/storageserv/stats/shmstats"
	"juno/pkg/logging/cal"
	calconfig "juno/pkg/logging/cal/config"
	"juno/pkg/logging/sherlock"
	"juno/pkg/stats"
)

var (
	statslogger = statsLoggerT{chDone: make(chan bool)}

	_ stats.IStatesWriter = (*statsFileWriterT)(nil)
	_ stats.IStatesWriter = (*statsCalWriterT)(nil)
	_ stats.IStatesWriter = (*statsSherlockWriterT)(nil)
)

type (
	workerStatsWrapperT struct {
		stats        []stats.IState
		zoneId       stats.IState
		machineIndex stats.IState
	}
	statsLoggerT struct {
		writers []stats.IStatesWriter
		//		stats       shmstats.ReqProcStats
		workerStats []workerStatsWrapperT
		chDone      chan bool
	}

	statsFileWriterT struct {
		cnt    int
		header string
		writer goio.WriteCloser
	}
	statsCalWriterT struct {
	}
	statsSherlockWriterT struct {
		dimensions []sherlock.Dims
		count      uint32
	}
)

func InitializeForMonitor(args ...interface{}) (err error) {
	shmstats.InitForMonitor()
	statslogger.Init()
	return
}

func FinalizeForMonitor() {
	shmstats.Finalize()
}

func (w *workerStatsWrapperT) getWorkerId() string {
	return fmt.Sprintf("%s-%s", w.zoneId.State(), w.machineIndex.State())
}

func (l *statsLoggerT) Init() {
	//shmstats must have been initialized

	srvStats := shmstats.GetServerStats()
	numWorkers := int(srvStats.NumWorkers)
	l.workerStats = make([]workerStatsWrapperT, numWorkers)
	if numWorkers != 0 {
		for wi := 0; wi < numWorkers; wi++ {
			mgr := shmstats.GetWorkerStatsManager(wi)
			if mgr == nil {
				continue
			}
			st := mgr.GetWorkerStatsPtr()
			if st != nil {
				l.workerStats[wi].zoneId = stats.NewUint32State(&st.ZoneId, "", "")
				l.workerStats[wi].machineIndex = stats.NewUint32State(&st.MachineIndex, "", "")
				l.workerStats[wi].stats = append(l.workerStats[wi].stats,
					[]stats.IState{
						stats.NewUint64State(&st.StorageStats.Free, "free", "Free Storage Space (mbytes)"),
						stats.NewUint64State(&st.StorageStats.Used, "used", "Used Storage Space (mbytes)"),
						stats.NewUint64State(&st.NumRequests, "req", "Number of Requests"),
						stats.NewUint32State(&st.AvgReqProcTime, "apt", "Average Request Process time(us)"),
						stats.NewUint64State(&st.NumReads, "Read", "Number of Read"),
						stats.NewUint64State(&st.NumDeletes, "D", "Number of Delete"),
						stats.NewUint64State(&st.NumCommits, "C", "Number of Commit"),
						stats.NewUint64State(&st.NumAborts, "A", "Number of Abort"),
						stats.NewUint64State(&st.NumRepairs, "RR", "Number of Repair"),
						stats.NewUint64State(&st.StorageStats.NumKeys, "keys", "Number of Keys"),
						stats.NewUint32State(&st.StorageStats.MaxDBLevel, "LN", "Max LN Level in Rocksdb"),
						stats.NewUint32State(&st.StorageStats.CompSecByInterval, "compSec", "Compaction Sec"),
						stats.NewUint32State(&st.StorageStats.CompCountByInterval, "compCount", "Compaction Count"),
						stats.NewUint64State(&st.StorageStats.PendingCompKBytes, "pCompKB", "Pending Compaction KBytes"),
						stats.NewUint64State(&st.StorageStats.DelayedWriteRate, "stall", "Actural Delayed Write Rate"),
						stats.NewFloat32State(&st.ProcCpuUsage, "pCPU", "Process CPU usage percentage", 1),
						stats.NewFloat32State(&st.MachCpuUsage, "mCPU", "Machine CPU usage percentage", 1),
					}...)
			}
		}
		cfg := config.ServerConfig()
		if cfg.StateLogEnabled {
			if _, err := os.Stat(cfg.StateLogDir); os.IsNotExist(err) {
				os.Mkdir(cfg.StateLogDir, 0777)
			}
		}
		statelogName := filepath.Join(cfg.StateLogDir, "state.log")

		l.writers = nil
		if file, err := os.OpenFile(statelogName, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644); err == nil {
			var buf bytes.Buffer
			for _, i := range statslogger.workerStats[0].stats {
				format := fmt.Sprintf("%%%ds ", i.Width())
				fmt.Fprintf(&buf, format, i.Header())
			}

			l.writers = append(l.writers, &statsFileWriterT{
				writer: file,
				header: fmt.Sprintf("%8s %s", "id", string(buf.Bytes())),
			})
		} else {
			return
		}
		if cal.IsEnabled() && cfg.StateLogEnabled {
			l.writers = append(l.writers, &statsCalWriterT{})
		}

		if sherlock.IsEnabled() {
			sw := statsSherlockWriterT{}
			sw.dimensions = make([]sherlock.Dims, numWorkers)
			for i := 0; i < numWorkers; i++ {
				sw.dimensions[i] = sherlock.Dims{sherlock.GetDimName(): calconfig.CalConfig.Poolname, "id": fmt.Sprintf("%d", i)}
			}
			l.writers = append(l.writers, &sw)
		}
	}
}

func (l *statsLoggerT) DoWrite() {
	ticker := time.NewTicker(1 * time.Second)
	defer func() {
		ticker.Stop()
		for _, w := range l.writers {
			w.Close()
		}
	}()
	for {
		select {
		case <-l.chDone:
			return
		case now := <-ticker.C:
			for _, w := range l.writers {
				w.Write(now)
			}
		}
	}
}

func (w *statsFileWriterT) Write(now time.Time) error {
	numWorkers := len(statslogger.workerStats)

	if numWorkers != 0 {
		for wi := 0; wi < numWorkers; wi++ {
			var buf bytes.Buffer
			for _, i := range statslogger.workerStats[wi].stats {
				format := fmt.Sprintf("%%%ds ", i.Width())
				fmt.Fprintf(&buf, format, i.State())
			}
			if w.cnt%23 == 0 {
				fmt.Fprintf(w.writer, "%s %s\n", now.Format("01-02 15:04:05"), w.header)
			}
			fmt.Fprintf(w.writer, "%s %8s %s\n", now.Format("01-02 15:04:05"), statslogger.workerStats[wi].getWorkerId(), string(buf.Bytes()))
			w.cnt++
		}
	}
	return nil
}

func (w *statsFileWriterT) Close() error {
	if w.writer != nil {
		return w.writer.Close()
	}
	return nil
}

func (w *statsCalWriterT) Write(now time.Time) error {
	if cal.IsEnabled() {
		numWorkers := len(statslogger.workerStats)
		for wi := 0; wi < numWorkers; wi++ {
			var buf bytes.Buffer
			for i, v := range statslogger.workerStats[wi].stats {
				if i != 0 {
					buf.WriteByte('&')
				}
				fmt.Fprintf(&buf, "%s=%s", v.Header(), v.State())
			}
			cal.StateLog(fmt.Sprintf("%s", statslogger.workerStats[wi].getWorkerId()), buf.Bytes())
		}
	}
	return nil

}
func (w *statsCalWriterT) Close() error {
	return nil
}

var sherlockHeaderKeyMap = map[string]string{
	"free":       "free_mb_storage_space",
	"used":       "storage_used_mb",
	"req":        "requestCount",
	"apt":        "latency_avg_us",
	"Read":       "read_count",
	"PC":         "prepare_create_count",
	"PU":         "prepare_update_count",
	"PS":         "prepare_set_count",
	"D":          "delete_count",
	"PD":         "prepare_delete_count",
	"C":          "commit_count",
	"A":          "abort_count",
	"RR":         "repair_count",
	"keys":       "key_count",
	"LN":         "LN_level",
	"compSec":    "comp_sec",
	"compCount":  "comp_count",
	"pCompKB":    "pending_comp_kbytes",
	"stall":      "stall_write_rate",
	"tps":        "requestCountPerSec",
	"eps":        "errorPerSec",
	"nRead":      "read_count",
	"nWrite":     "write_count",
	"nBadShds":   "shard_bad_count",
	"nWarnShds":  "shard_warning_count",
	"nAlertShds": "shard_alert_count",
	"ssl_conns":  "conns_ssl_count",
	"conns":      "conns_count",
	"pCPU":       "cpu_usage",
	"mCPU":       "machine_cpu_usage",
}

func (w *statsSherlockWriterT) Write(now time.Time) error {
	if sherlock.IsEnabled() {
		if w.count%sherlock.ShrLockConfig.Resolution == 0 {
			numWorkers := len(statslogger.workerStats)
			for wi := 0; wi < numWorkers; wi++ {
				for _, v := range statslogger.workerStats[wi].stats {
					if fl, err := strconv.ParseFloat(v.State(), 64); err == nil {
						w.sendMetricsData(wi, v.Header(), fl, now)
					}

				}
			}
		}
		w.count++
	}
	return nil
}

func (w *statsSherlockWriterT) sendMetricsData(wid int, key string, value float64, now time.Time) {
	var data [1]sherlock.FrontierData
	headerKey, ok := sherlockHeaderKeyMap[key]
	if !ok {
		headerKey = key
	}
	data[0].Name = headerKey
	data[0].Value = value
	data[0].MetricType = sherlock.Gauge
	sherlock.SherlockClient.SendMetric(w.dimensions[wid], data[:1], now)
}

func (w *statsSherlockWriterT) Close() error {
	return nil
}

func RunMonitorLogger() {
	go statslogger.DoWrite()
}
