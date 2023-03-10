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
	"time"

	"juno/cmd/proxy/config"
	"juno/cmd/proxy/stats/shmstats"
	"juno/pkg/io"
	"juno/pkg/logging/cal"
	"juno/pkg/logging/otel"
	"juno/pkg/stats"
)

var (
	statslogger = statsLoggerT{chDone: make(chan bool)}

	_ stats.IStatesWriter = (*statsFileWriterT)(nil)
	_ stats.IStatesWriter = (*statsCalWriterT)(nil)
)

type (
	statsLoggerT struct {
		writers []stats.IStatesWriter
		//		stats       shmstats.ReqProcStats
		workerStats [][]stats.IState
		chDone      chan bool
	}

	statsFileWriterT struct {
		cnt    int
		header string
		writer goio.WriteCloser
	}
	statsCalWriterT struct {
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

func (l *statsLoggerT) Init() {
	//shmstats must have been initialized

	srvStats := shmstats.GetServerStats()
	numWorkers := int(srvStats.NumWorkers)
	l.workerStats = make([][]stats.IState, numWorkers)
	repTargets := shmstats.GetReplicationTargetStats()
	numTargets := len(repTargets)

	if numWorkers != 0 {
		lsnrs := shmstats.GetListenerStats()
		for i := 0; i < numWorkers; i++ {
			mgr := shmstats.GetWorkerStatsManager(i)
			st := mgr.GetWorkerStatsPtr()

			conns := mgr.GetInboundConnStatsPtr()
			if len(conns) == len(lsnrs) {
				for li, lsnr := range lsnrs {
					var name string
					if io.ListenerType(lsnr.Type) == io.ListenerTypeTCPwSSL {
						name = "ssl_conns"
					} else {
						name = "conns"
					}
					l.workerStats[i] = append(l.workerStats[i], stats.NewUint32State(&conns[li].NumConnections, name, ""))
				}
			}
			if st != nil {
				l.workerStats[i] = append(l.workerStats[i],
					[]stats.IState{
						stats.NewUint32State(&st.RequestsPerSecond, "tps", "number of transactions per second"),
						stats.NewUint32State(&st.AvgReqProcTime, "apt", "average processing time"),
						stats.NewUint32State(&st.ReqProcErrsPerSecond, "eps", "number of errors per second"),
						stats.NewUint32State(&st.NumReads, "nRead", "number of active read requests"),
						stats.NewUint32State(&st.NumWrites, "nWrite", "number of active write requests"),
						stats.NewUint16State(&st.NumBadShards, "nBShd", "number of bad shards"),
						stats.NewUint16State(&st.NumAlertShards, "nAShd", "number of shards with no redundancy"),
						stats.NewUint16State(&st.NumWarnShards, "nWShd", "number of shards with bad SS"),
						stats.NewFloat32State(&st.ProcCpuUsage, "pCPU", "Process CPU usage percentage", 1),
						stats.NewFloat32State(&st.MachCpuUsage, "mCPU", "Machine CPU usage percentage", 1),
					}...)
			}

			// replication stats
			for t := 0; t < numTargets; t++ {
				repStats := mgr.GetReplicatorStatsPtr(t)
				tgtName := string(repTargets[t].Name[:repTargets[t].LenName])
				repconn := fmt.Sprintf("%s_c", tgtName)
				repdrop := fmt.Sprintf("%s_d", tgtName)
				reperr := fmt.Sprintf("%s_e", tgtName)
				l.workerStats[i] = append(l.workerStats[i],
					[]stats.IState{
						stats.NewUint16State(&repStats.NumConnections, repconn, "replication connection count"),
						stats.NewUint64DeltaState(&repStats.NumDrops, repdrop,
							"replication requests drop count", uint16(10)),
						stats.NewUint64DeltaState(&repStats.NumErrors, reperr,
							"replication requests error count", uint16(10)),
					}...)
			}
		}
		cfg := &config.Conf
		if cfg.StateLogEnabled {
			if _, err := os.Stat(cfg.StateLogDir); os.IsNotExist(err) {
				os.Mkdir(cfg.StateLogDir, 0777)
			}
		}

		statelogName := filepath.Join(cfg.StateLogDir, "state.log")
		l.writers = nil
		if file, err := os.OpenFile(statelogName, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644); err == nil {
			var buf bytes.Buffer
			for _, i := range statslogger.workerStats[0] {
				format := fmt.Sprintf("%%%ds ", i.Width())
				fmt.Fprintf(&buf, format, i.Header())
			}

			l.writers = append(l.writers, &statsFileWriterT{
				writer: file,
				header: fmt.Sprintf("%3s %s", "id", string(buf.Bytes())),
			})
		} else {
			return
		}
		if cal.IsEnabled() {
			l.writers = append(l.writers, &statsCalWriterT{})
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
			for _, i := range statslogger.workerStats[wi] {
				format := fmt.Sprintf("%%%ds ", i.Width())
				fmt.Fprintf(&buf, format, i.State())
			}
			if w.cnt%23 == 0 {
				fmt.Fprintf(w.writer, "%s %s\n", now.Format("01-02 15:04:05"), w.header)
			}
			fmt.Fprintf(w.writer, "%s %3d %s\n", now.Format("01-02 15:04:05"), wi, string(buf.Bytes()))
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
			for i, v := range statslogger.workerStats[wi] {
				if i != 0 {
					buf.WriteByte('&')
				}
				fmt.Fprintf(&buf, "%s=%s", v.Header(), v.State())
			}
			cal.StateLog(fmt.Sprintf("%d", wi), buf.Bytes())
		}
	}
	return nil

}
func (w *statsCalWriterT) Close() error {
	return nil
}

func RunMonitorLogger() {
	go statslogger.DoWrite()
	if otel.IsEnabled() {
		otel.InitSystemMetrics(otel.SvrTypeProxy, statslogger.workerStats)
	}
}
