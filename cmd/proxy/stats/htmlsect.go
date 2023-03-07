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
	"html/template"
	"time"

	"juno/cmd/proxy/config"
	"juno/cmd/proxy/stats/shmstats"
	"juno/pkg/cluster"
	"juno/pkg/stats"
)

type (
	htmlSectServerInfoT       struct{}
	htmlSectStorageInfoT      struct{}
	htmlSectReqProcStatsT     struct{}
	htmlSectShardMgrStatsT    struct{}
	htmlSectReplicationStatsT struct{}
	htmlSectClientStatsT      struct{}
	htmlSectLimitsConfigT     struct{}
)

func (s *htmlSectServerInfoT) Title() template.HTML {
	return template.HTML("Server Info")
}

func (s *htmlSectServerInfoT) Body() template.HTML {
	var buf bytes.Buffer
	buf.WriteString(
		`<div id="id-server-info"><table title="server-info">
<tr><th>Start Time</th><th>PID</th><th>Zones</th><th>Shards</th></tr>`)
	stats := shmstats.GetServerStats()
	startTime := time.Unix(0, stats.StartTimestampNs)
	fmt.Fprintf(&buf, "<tr><td>%s</td><td>%d</td><td>%d</td><td>%d</td></tr></table>",
		startTime.Format("2006-01-02 15:04:05"), stats.Pid, stats.NumZones, stats.NumShards)

	return template.HTML(buf.String())
}

func (s *htmlSectStorageInfoT) Title() template.HTML {
	return "Storage Servers"
}

func (s *htmlSectStorageInfoT) Body() template.HTML {
	var buf bytes.Buffer
	if cluster.GetShardMgr() != nil {
		cluster.GetShardMgr().WriteProcessorsStats(&buf)
	}

	return template.HTML(buf.String())
}

func (s *htmlSectReqProcStatsT) Title() template.HTML {
	return "Request Processing"
}

func (s *htmlSectReqProcStatsT) Body() template.HTML {
	var buf bytes.Buffer
	buf.WriteString(`<div id="id-req-proc"><table title="req-proc">`)

	buf.WriteString(`<table title="Inbound Request"><tr><th>Listener</<th><th>Connections</th><th>Requests</th><th>Throughput</th><th>Average Request Process Time</th><th>Errors/s</th><th>Cal Drops</th></tr>`)

	listeners := shmstats.GetListenerStats()
	numListeners := len(listeners)
	var td string
	if numListeners > 1 {
		td = fmt.Sprintf("<td rowspan=\"%d\">", numListeners)
	} else {
		td = "<td>"
	}

	if mgr := shmstats.GetCurrentWorkerStatsManager(); mgr != nil {
		reqStats := mgr.GetReqProcStats()
		inbConnStats := mgr.GetInboundConnStats()

		if len(inbConnStats) == numListeners {
			for i, _ := range listeners {
				if i == 0 {
					var avgStr string
					if reqStats.AvgReqProcTime != 0 {
						avgStr = stats.HtmlDurationEscapeString(time.Duration(reqStats.AvgReqProcTime * 1000))
					}
					fmt.Fprintf(&buf, "<tr><td>%s</td><td>%d</td>%s%d</td>%s%d</td>%s%s</td>%s%d</td>%s%d</td></tr>",
						listeners[i].GetListenAddress(), inbConnStats[i].NumConnections, td, reqStats.NumRequests, td, reqStats.RequestsPerSecond,
						td, avgStr, td, reqStats.ReqProcErrsPerSecond, td, mgr.GetWorkerStats().CalDropCount)
				} else {
					fmt.Fprintf(&buf, "<tr><td>%s</td><td>%d</td></tr>",
						listeners[i].GetListenAddress(), inbConnStats[i].NumConnections)
				}
			}
		}
	}

	buf.WriteString("</table></div>")

	return template.HTML(buf.String())
}

func (s *htmlSectReplicationStatsT) Title() template.HTML {
	return "Replication"
}

func (s *htmlSectReplicationStatsT) Body() template.HTML {
	var buf bytes.Buffer

	targets := shmstats.GetReplicationTargetStats()
	worker := shmstats.GetCurrentWorkerStatsManager()
	repStats := worker.GetReplicatorStats()

	numTargets := len(targets)
	if numTargets != 0 && len(repStats) == numTargets {
		fmt.Fprint(&buf, `<div id="id-replicator-info"><table title="replicator-info">`)
		fmt.Fprint(&buf, "<tr><th>Target</th><th>Connections</th><th>Queue Size</th><th>Max Queue Size</th><th>Drop Count</th><th>Error Count</th></tr>\n")
		for i := 0; i < numTargets; i++ {
			fmt.Fprintf(&buf, "<tr>")
			if repStats[i].NumConnections != 0 {
				fmt.Fprintf(&buf, "<td>%s</td>", targets[i].GetListenAddress())
			} else {
				fmt.Fprintf(&buf, "<td style=\"background-color:#F29A38\">%s</td>", targets[i].GetListenAddress())
			}
			fmt.Fprintf(&buf, "<td>%d</td>", repStats[i].NumConnections)
			szQueue := repStats[i].SzQueue
			if szQueue*2 < targets[i].CapQueue {
				fmt.Fprintf(&buf, "<td>%d</td>", szQueue)
			} else {
				fmt.Fprintf(&buf, "<td style=\"background-color:#F29A38\">%d</td>", szQueue)
			}
			fmt.Fprintf(&buf, "<td>%d</td>", repStats[i].MaxSzQueue)
			fmt.Fprintf(&buf, "<td>%d</td>", repStats[i].NumDrops)
			fmt.Fprintf(&buf, "<td>%d</td></tr>\n", repStats[i].NumErrors)
		}
		fmt.Fprint(&buf, "</table></div>")
	}
	return template.HTML(buf.String())
}

func (s *htmlSectShardMgrStatsT) Title() template.HTML {
	return "Shard Map"
}

func (s *htmlSectShardMgrStatsT) Body() template.HTML {
	var buf bytes.Buffer
	if cluster.GetShardMgr() != nil {
		cluster.GetShardMgr().WriteProcessorsStatsByShards(&buf)
	}

	return template.HTML(buf.String())
}

func (s *htmlSectClientStatsT) Title() template.HTML {
	return "Client Statistics"
}

func (s *htmlSectClientStatsT) Body() template.HTML {
	var buf bytes.Buffer
	worker := shmstats.GetCurrentWorkerStatsManager()
	stmap := worker.GetAppNsStatsMap()
	if stmap != nil {
		fmt.Fprint(&buf, `<div id="id-client-stats"><table title="client-stats">`)
		fmt.Fprint(&buf, "<tr><th rowspan=\"2\">Namespace</th><th rowspan=\"2\">Application</th><th colspan=\"2\">Payload</th><th colspan=\"2\">TTL</th></tr>\n")
		fmt.Fprint(&buf, "<tr><th>Max</th><th>Average</th><th>Max</th><th>Average</th></tr>\n")
		for ns, v := range stmap {
			for app, st := range v {
				if ns != config.JunoInternalNamespace() {
					fmt.Fprintf(&buf, "<tr><td>%s</td><td>%s</td><td>%d</td><td>%d</td><td>%d</td><td>%d</td></tr>",
						ns, app, st.MaxPayloadLen, st.AvgPayloadLen, st.MaxTimeToLive, st.AvgTimeToLive)
				}
			}
		}
	}
	fmt.Fprint(&buf, "</table></div>")
	return template.HTML(buf.String())
}

func (s *htmlSectLimitsConfigT) Title() template.HTML {
	return "Limits Configuration"
}

func (s *htmlSectLimitsConfigT) Body() template.HTML {
	conf := config.GetCopyOfLimitsConfig()
	var buf bytes.Buffer
	fmt.Fprint(&buf, `<div id="id-limits-config"><table title="limits-config">`)
	fmt.Fprintf(&buf, "<tr><th>Namespace</th><th>Max Key Length</th><th>Max Payload length</th><th>Max Time to Live</th></tr>\n")
	fmt.Fprintf(&buf, "<tr><td></td><td>%d</td><td>%d</td><td>%d</td></tr>\n",
		conf.MaxKeyLength, conf.MaxPayloadLength, conf.MaxTimeToLive)
	for k, v := range conf.Namespace {
		if k != config.JunoInternalNamespace() {
			fmt.Fprintf(&buf, "<tr><td>%s</td><td>%d</td><td>%d</td><td>%d</td></tr>\n",
				k, v.MaxKeyLength, v.MaxPayloadLength, v.MaxTimeToLive)
		}
	}
	fmt.Fprint(&buf, "</table></div>")
	return template.HTML(buf.String())
}
