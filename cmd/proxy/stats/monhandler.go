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
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"juno/third_party/forked/golang/glog"

	"juno/cmd/proxy/config"
	"juno/cmd/proxy/stats/shmstats"
	"juno/pkg/stats"
	"juno/pkg/version"
)

var ()

type (
	HandlerForMonitor struct {
		htmlSectWorkerInfo htmlSectWorkerInfoT
		isChild            bool
	}
	workerInfoT struct {
		id   int
		addr string
	}
	htmlSectWorkerInfoT struct {
		title   string
		workers []workerInfoT
	}
	htmlSectAggReqProcStatsT struct{}
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

func (h *HandlerForMonitor) GetWorkerUrl(i int) string {
	return h.htmlSectWorkerInfo.GetWorkerUrl(i)
}

func (h *HandlerForMonitor) GetNumWorkers() int {
	return len(h.htmlSectWorkerInfo.workers)
}

func (h *HandlerForMonitor) Init(isChild bool, addrs []string) {
	htmlstats.Title = "Juno Proxy Monitor"
	h.isChild = isChild

	htmlstats.ClusterName = config.Conf.ClusterName
	htmlstats.AddSection(&htmlSectServerInfoT{})
	htmlstats.AddSection(&htmlSectAggReqProcStatsT{})
	if len(addrs) != 0 {
		if len(addrs) == 1 {
			h.htmlSectWorkerInfo.title = "Worker"
		} else {
			h.htmlSectWorkerInfo.title = "Workers"
		}

		htmlstats.AddSection(&h.htmlSectWorkerInfo)

		for i, addr := range addrs {
			glog.Debugf("addr: %s", addr)
			h.htmlSectWorkerInfo.workers = append(h.htmlSectWorkerInfo.workers, workerInfoT{id: i, addr: addr})
		}
	}
	initClusterConsole(config.Conf.ClusterName)
}

func (h *HandlerForMonitor) ListenAndServe(addr string) error {
	HttpServerMux.HandleFunc("/", h.httpHandler)
	HttpServerMux.HandleFunc("/stats/json", h.httpJsonStatsHandler)
	HttpServerMux.HandleFunc("/stats/text", h.httpTextStatsHandler)
	HttpServerMux.HandleFunc("/debug/pprof/", h.debugPprofHandler)
	HttpServerMux.HandleFunc("/version", version.HttpHandler)

	HttpServerMux.HandleFunc("/cluster/", h.httpClusterConsoleHandler)
	HttpServerMux.HandleFunc("/cluster/admin", h.dummyHandler)
	HttpServerMux.HandleFunc("/cluster/stats", h.dummyHandler)
	HttpServerMux.HandleFunc("/cluster/shardmap", h.httpClusterConsoleShardMapHandler)
	HttpServerMux.HandleFunc("/cluster/tool", h.dummyHandler)
	HttpServerMux.HandleFunc("/cluster/login", h.dummyHandler)

	glog.Infof("to serve HTTP on %s", addr)
	return http.ListenAndServe(addr, &HttpServerMux)
}

func (s *htmlSectWorkerInfoT) Title() template.HTML {
	return template.HTML(s.title)
}

func (s *htmlSectWorkerInfoT) Body() template.HTML {
	var buf bytes.Buffer
	buf.WriteString(
		`<div id="id-worker-info"><table title="worker-info">
<tr><th>ID</th><th>PID</th><th>Monitor Address</th><th>Connections</th><th>Requests</th><th>Throughput</th><th>Average Request Process Time</th></tr>`)

	for i, w := range s.workers {
		ws := shmstats.GetWorkerStatsManager(i).GetWorkerStats()
		fmt.Fprintf(&buf, "<tr><td>%d</td><td>%d</td><td><a href=\"%s\">%s</a></td><td>%d</td><td>%d</td><td>%d</td>",
			w.id, ws.Pid, fmt.Sprintf("?wid=%d", w.id), w.addr, ws.TotalNumConnections, ws.NumRequests, ws.RequestsPerSecond)
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

func (h *HandlerForMonitor) getFromWorkerWithWorkerIdByPost(r *http.Request, workerId int) (body []byte, err error) {
	glog.Infof("getFromWorkerWithWorkerIdByPost: content-type: %v, method: %v\n", r.Header["Content-Type"], r.Method)
	if workerId >= h.GetNumWorkers() {
		err = fmt.Errorf("invalid worker id %d. should < %d ", workerId, h.GetNumWorkers())
		return
	}
	var resp *http.Response
	url := h.GetWorkerUrl(workerId) + r.URL.Path
	headers := r.Header["Content-Type"]
	var contents string = ""
	for i := range headers {
		contents += headers[i]
		if (i + 1) < len(headers) {
			contents += "/"
		}
		glog.Infof("Content-Type: %v\n", contents)
	}
	if resp, err = http.Post(url, contents, r.Body); err == nil {
		body, err = ioutil.ReadAll(resp.Body)
		resp.Body.Close()
	} else {
		glog.Errorln(err)
	}
	return
}

func (h *HandlerForMonitor) debugPprofHandler(w http.ResponseWriter, r *http.Request) {
	values := r.URL.Query()
	if len(values) != 0 {
		if values.Get("wid") != "" {
			if body, err := h.getFromWorker(r.URL.Path, values); err == nil {
				w.Write(body)
			} else {
				glog.Errorln(err)
			}
			return
		}
	}
	debugPprofHandler(w, r)
}

func (h *HandlerForMonitor) httpJsonStatsHandler(w http.ResponseWriter, r *http.Request) {
	var indent bool
	workerId := ""
	if values := r.URL.Query(); len(values) != 0 {
		if values.Get("indent") != "" {
			indent = true
		}
		workerId = values.Get("wid")
	}

	shmstats.WriteStatsInJson(w, workerId, indent)
}

func (c *HandlerForMonitor) httpTextStatsHandler(w http.ResponseWriter, r *http.Request) {
	workerId := "all"
	if values := r.URL.Query(); len(values) != 0 {
		workerId = values.Get("wid")
	}
	shmstats.PrettyPrint(w, workerId)
}

func (h *HandlerForMonitor) httpHandler(w http.ResponseWriter, r *http.Request) {
	values := r.URL.Query()
	if values.Get("wid") != "" {
		if body, err := h.getFromWorker(r.URL.Path, values); err == nil {
			w.Write(body)
		} else {
			glog.Errorln(err)
		}
	} else if values.Get("info") != "" {
		h.handleQuery(w, r.URL.Path, values)
	} else {
		if values.Get(kQueryElemKey) == kQueryElemValueMain {
			stats.HtmlSectionsTmpl.Execute(w, &htmlstats)
		} else {
			stats.HtmlStatsTmpl.Execute(w, &htmlstats)
		}
	}
}

func (h *HandlerForMonitor) getFromWorkerWithWorkerId(urlPath string, query url.Values, workerId int) (body []byte, err error) {
	if workerId >= h.GetNumWorkers() {
		err = fmt.Errorf("invalid worker id %d. should < %d ", workerId, h.GetNumWorkers())
		return
	}
	var resp *http.Response
	url := h.GetWorkerUrl(workerId) + urlPath
	qstr := query.Encode()
	if qstr != "" {
		url += "?" + qstr
	}
	if resp, err = http.Get(url); err == nil {
		body, err = ioutil.ReadAll(resp.Body)
		resp.Body.Close()
	} else {
		glog.Errorln(err)
	}
	return
}

func (h *HandlerForMonitor) getFromWorker(urlPath string, query url.Values) (body []byte, err error) {
	wid := query.Get("wid")
	if wid != "" {
		var id int
		if id, err = strconv.Atoi(wid); err == nil {
			body, err = h.getFromWorkerWithWorkerId(urlPath, query, id)
		} else {
			err = fmt.Errorf("invalid wid %s", wid)
		}
	} else {
		err = fmt.Errorf("wid not found in query")
	}

	return
}

func (h *HandlerForMonitor) handleQuery(w http.ResponseWriter, urlPath string, values url.Values) {
	if keys, ok := values["info"]; ok && len(keys) != 0 {
		numWorkers := h.GetNumWorkers()
		key := keys[0]
		switch key {
		case "get_pid":

			var pids []string
			if h.isChild {
				pids = append(pids, fmt.Sprintf("%d", os.Getppid()))
			}
			pids = append(pids, fmt.Sprintf("%d", os.Getpid()))
			for i := 0; i < numWorkers; i++ {
				if body, err := h.getFromWorkerWithWorkerId(urlPath, values, i); err == nil {
					pids = append(pids, string(body))
				} else {
					glog.Errorln(err)
				}
			}
			w.Write([]byte(strings.Join(pids, ",")))
		case "ss_connected":
			connected := "true"
			for i := 0; i < numWorkers; i++ {
				if body, err := h.getFromWorkerWithWorkerId(urlPath, values, i); err == nil {
					if strings.ToLower(string(body)) == "false" {
						connected = "false"
						break
					}
				} else {
					glog.Errorln(err)
				}
			}
			w.Write([]byte(connected))
		case "ss_not_connected":
			notConnected := "false"
			for i := 0; i < numWorkers; i++ {
				if body, err := h.getFromWorkerWithWorkerId(urlPath, values, i); err == nil {
					if strings.ToLower(string(body)) == "true" {
						notConnected = "true"
						break
					}
				} else {
					glog.Errorln(err)
				}
			}
			w.Write([]byte(notConnected))
		case "ss_bad_shard_hosts":
			hosts := make(map[string]int)
			for i := 0; i < numWorkers; i++ {
				if body, err := h.getFromWorkerWithWorkerId(urlPath, values, i); err == nil {
					if len(body) == 0 {
						continue
					}
					parts := strings.Split(string(body), ",")
					for i := range parts {
						hosts[parts[i]] = 0
					}
				} else {
					glog.Errorln(err)
				}
			}
			str := ""
			if len(hosts) != 0 {
				for key := range hosts {
					str += key + ","
				}
				str = strings.Trim(str, ",")
			}
			w.Write([]byte(str))
		default:
			w.Write([]byte(fmt.Sprintf("key: %s not supported", key)))
		}
	} else {
		w.Write([]byte("query not supported"))
	}
}

func (h *htmlSectAggReqProcStatsT) Title() template.HTML {
	return "Request Processing"
}

func (h *htmlSectAggReqProcStatsT) Body() template.HTML {
	var buf bytes.Buffer
	buf.WriteString(`<div id="id-req-proc"><table title="req-proc">`)

	buf.WriteString(`<table title="Inbound Request"><tr><th>Listener</<th><th>Connections</th><th>Requests</th><th>Throughput</th><th>Average Request Process Time</th><th>Errors/s</th></tr>`)

	reqStats := shmstats.GetAggregatedReqProcStats()

	inbConnStats := shmstats.GetAggregatedInboundConnStats()

	listeners := shmstats.GetListenerStats()

	numListeners := len(listeners)
	var td string
	if numListeners > 1 {
		td = fmt.Sprintf("<td rowspan=\"%d\">", numListeners)
	} else {
		td = "<td>"
	}

	if len(inbConnStats) == numListeners {
		for i, _ := range listeners {
			if i == 0 {
				var avgStr string
				if reqStats.AvgReqProcTime != 0 {
					avgStr = stats.HtmlDurationEscapeString(time.Duration(reqStats.AvgReqProcTime * 1000))
				}
				fmt.Fprintf(&buf, "<tr><td>%s</td><td>%d</td>%s%d</td>%s%d</td>%s%s</td>%s%d</td></tr>",
					listeners[i].GetListenAddress(), inbConnStats[i].NumConnections, td, reqStats.NumRequests, td, reqStats.RequestsPerSecond,
					td, avgStr, td, reqStats.ReqProcErrsPerSecond)
			} else {
				fmt.Fprintf(&buf, "<tr><td>%s</td><td>%d</td></tr>",
					listeners[i].GetListenAddress(), inbConnStats[i].NumConnections)
			}
		}
	}

	buf.WriteString("</table></div>")

	return template.HTML(buf.String())
}
