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
	"fmt"
	"net/http"
	"net/http/pprof"
	rpprof "runtime/pprof"
	"strings"

	//"juno/third_party/forked/golang/glog"
	"github.com/BurntSushi/toml"

	"juno/cmd/proxy/config"
	"juno/cmd/proxy/stats/qry"
	"juno/pkg/stats"
	"juno/pkg/version"
)

var (
	indexPage stats.IndexPage
	htmlstats stats.HtmlStats = stats.HtmlStats{
		Title:   "Juno Proxy Statistics",
		Version: version.OnelineVersionString(),
	}
	htmlShardMgrStats stats.HtmlStats = htmlstats
	HttpServerMux     http.ServeMux
	workerIdString    string
)

const (
	kQueryElemKey       = "elem"
	kQueryElemValueMain = "main"
)

func indexHandler(w http.ResponseWriter, r *http.Request) {
	tmpl := stats.IndexPageTmpl

	if r.URL.Query().Get(kQueryElemKey) == kQueryElemValueMain {
		tmpl = stats.IndexPageMainTmpl
	}
	if err := tmpl.Execute(w, &indexPage); err != nil {
		fmt.Fprint(w, err)
	}
}

func addPage(path string, handler func(w http.ResponseWriter, r *http.Request)) {
	HttpServerMux.HandleFunc(path, handler)
	if workerIdString != "" {
		indexPage.AddLink(path+"?wid="+workerIdString, path)
	} else {
		indexPage.AddLink(path, path)
	}
}

func httpStatsHandler(w http.ResponseWriter, r *http.Request) {
	values := r.URL.Query()
	if values.Get("info") != "" {
		qry.InfoQuery(w, values)
	} else if values.Get(kQueryElemKey) == kQueryElemValueMain {
		stats.HtmlSectionsTmpl.Execute(w, &htmlstats)
	} else {
		stats.HtmlStatsTmpl.Execute(w, &htmlstats)
	}
}

func debugConfigHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	encoder := toml.NewEncoder(w)
	encoder.Encode(&config.Conf)
}

func debugShardManagerStatsHandler(w http.ResponseWriter, r *http.Request) {
	if r.URL.Query().Get(kQueryElemKey) == kQueryElemValueMain {
		stats.HtmlSectionsTmpl.Execute(w, &htmlShardMgrStats)
	} else {
		stats.HtmlStatsTmpl.Execute(w, &htmlShardMgrStats)
	}
}

func debugPprofHandler(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/debug/pprof/") {
		name := strings.TrimPrefix(r.URL.Path, "/debug/pprof/")
		if name != "" {
			pprof.Handler(name).ServeHTTP(w, r)
			return
		}
	}

	profiles := rpprof.Profiles()
	tmpl := pprofIndexTmpl
	if r.URL.Query().Get(kQueryElemKey) == kQueryElemValueMain {
		tmpl = pprofIndexMainTmpl
	}
	if err := tmpl.Execute(w, profiles); err != nil {
		fmt.Fprint(w, err)
	}
}

func initStatsForWorker(workerId int) {

	htmlShardMgrStats.AddSection(&htmlSectShardMgrStatsT{})

	htmlstats.ClusterName = config.Conf.ClusterName
	htmlstats.AddSection(&htmlSectServerInfoT{})

	htmlstats.AddSection(&htmlSectStorageInfoT{})

	if config.Conf.StateLogEnabled {
		htmlstats.AddSection(&htmlSectReqProcStatsT{})
	}
	if len(config.Conf.Replication.Targets) != 0 {
		htmlstats.AddSection(&htmlSectReplicationStatsT{})
	}
	htmlstats.AddSection(&htmlSectLimitsConfigT{})
	htmlstats.AddSection(&htmlSectClientStatsT{})

	workerIdString = fmt.Sprintf("%d", workerId)
	initPprofIndexTemplate()

	HttpServerMux.HandleFunc("/", indexHandler)

	addPage("/stats", httpStatsHandler)
	addPage("/debug/pprof/", debugPprofHandler)
	addPage("/debug/shardmgr", debugShardManagerStatsHandler)
	addPage("/debug/config", debugConfigHandler)
	addPage("/debug/pprof/profile", pprof.Profile)
	addPage("/debug/pprof/symbol", pprof.Symbol)
	addPage("/debug/pprof/trace", pprof.Trace)
}
