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
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"juno/third_party/forked/golang/glog"

	"juno/cmd/storageserv/config"
	"juno/cmd/storageserv/stats/shmstats"
	"juno/pkg/stats"
	"juno/pkg/version"
)

type (
	workerInfoT struct {
		id   int
		addr string
	}
	htmlSectWorkerInfoT struct {
		title   string
		workers []workerInfoT
	}

	HttpHandlerForMonitor struct {
		isChild            bool
		htmlSectWorkerInfo htmlSectWorkerInfoT
	}
)

func (w *htmlSectWorkerInfoT) GetNumWorkers() int {
	return len(w.workers)
}

func (w *htmlSectWorkerInfoT) GetWorkerUrl(i int) string {
	if i >= w.GetNumWorkers() {
		return ""
	}
	return "http://" + w.workers[i].addr
}

func (h *HttpHandlerForMonitor) GetWorkerUrl(i int) string {
	return h.htmlSectWorkerInfo.GetWorkerUrl(i)
}

func (h *HttpHandlerForMonitor) GetNumWorkers() int {
	return len(h.htmlSectWorkerInfo.workers)
}

func (h *HttpHandlerForMonitor) Init(isChild bool, addrs []string) {
	h.isChild = isChild
	htmlstats.Title = "Juno Storage Server Monitor"
	htmlstats.ClusterName = config.ServerConfig().ClusterName
	if len(addrs) != 0 {
		if len(addrs) == 1 {
			h.htmlSectWorkerInfo.title = "Worker"
		} else {
			h.htmlSectWorkerInfo.title = "Workers"
		}

		htmlstats.AddSection(&htmlSectServerInfoT{})
		htmlstats.AddSection(&h.htmlSectWorkerInfo)
		for i, addr := range addrs {
			glog.Debugf("addr: %s", addr)
			h.htmlSectWorkerInfo.workers = append(h.htmlSectWorkerInfo.workers, workerInfoT{id: i, addr: addr})
		}
	}
	HttpServerMux.HandleFunc("/", h.httpHandler)
	HttpServerMux.HandleFunc("/stats/json", h.httpJsonStatsHandler)
	HttpServerMux.HandleFunc("/stats/text", h.httpTextStatsHandler)
	HttpServerMux.HandleFunc("/version", version.HttpHandler)
}

func (c *HttpHandlerForMonitor) getFromWorkerWithWorkerId(urlPath string, query url.Values, workerId int) (body []byte, err error) {
	if workerId >= c.GetNumWorkers() {
		err = fmt.Errorf("invalid worker id %d. should < %d ", workerId, c.GetNumWorkers())
		return
	}
	var resp *http.Response
	url := c.GetWorkerUrl(workerId) + urlPath
	qstr := query.Encode()
	if qstr != "" {
		url += "?" + qstr
	}
	if resp, err = http.Get(url); err == nil {
		body, err = io.ReadAll(resp.Body)
		resp.Body.Close()
	} else {
		glog.Errorln(err)
	}
	return
}

func (c *HttpHandlerForMonitor) getFromWorker(urlPath string, query url.Values) (body []byte, err error) {
	wid := query.Get("wid")
	if wid != "" {
		var id int
		if id, err = strconv.Atoi(wid); err == nil {
			query.Del("wid")
			body, err = c.getFromWorkerWithWorkerId(urlPath, query, id)
		} else {
			err = fmt.Errorf("invalid wid %s", wid)
		}
	} else {
		err = fmt.Errorf("wid not found in query")
	}

	return
}

func (c *HttpHandlerForMonitor) httpJsonStatsHandler(w http.ResponseWriter, r *http.Request) {
	var indent bool
	workerId := "*"
	if values := r.URL.Query(); len(values) != 0 {
		if values.Get("indent") != "" {
			indent = true
		}
		workerId = values.Get("wid")
	}

	shmstats.WriteStatsInJson(w, workerId, indent)
}

func (c *HttpHandlerForMonitor) httpTextStatsHandler(w http.ResponseWriter, r *http.Request) {
	workerId := "*"

	if values := r.URL.Query(); len(values) != 0 {
		workerId = values.Get("wid")
	}
	shmstats.PrettyPrint(w, workerId)
}

func (c *HttpHandlerForMonitor) httpHandler(w http.ResponseWriter, r *http.Request) {
	values := r.URL.Query()
	if values.Get("wid") != "" {
		if body, err := c.getFromWorker(r.URL.Path, values); err == nil {
			w.Write(body)
			return
		} else {
			glog.Errorln(err)

		}
	} else if values.Get(kQueryElemKey) == kQueryElemValueMain {
		stats.HtmlSectionsTmpl.Execute(w, &htmlstats)
	} else {
		stats.HtmlStatsTmpl.Execute(w, &htmlstats)
	}
}

func (s *htmlSectWorkerInfoT) Title() template.HTML {
	return template.HTML(s.title)
}

func (s *htmlSectWorkerInfoT) Body() template.HTML {
	var buf bytes.Buffer
	buf.WriteString(
		`<div id="id-worker-info"><table title="worker-info">
<tr><th>ID</th><th>PID</th><th>Listen on</th><th>Monitor Address</th>
<th>Requests</th><th>Throughput</th><th>Average Request Process Time</th></tr>`)

	for i, w := range s.workers {
		ws := shmstats.GetWorkerStatsManager(i).GetWorkerStats()

		fmt.Fprintf(&buf, "<tr><td>%d</td><td>%d</td><td>%d</td><td><a href=\"%s\">%s</a></td><td>%d</td><td>%d</td>",
			w.id, ws.Pid, ws.Port, fmt.Sprintf("/?wid=%d", w.id), w.addr, ws.NumRequests, ws.RequestsPerSecond)
		if ws.AvgReqProcTime == 0 {
			fmt.Fprintf(&buf, "<td></td>")
		} else {
			fmt.Fprintf(&buf, "<td>%s</td>", stats.HtmlDurationEscapeString(time.Duration(ws.AvgReqProcTime*1000)))
		}
		fmt.Fprintf(&buf, "</tr>")
	}
	fmt.Fprintf(&buf, "</table>")

	return template.HTML(buf.String())
}
