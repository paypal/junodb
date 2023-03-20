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
	"html/template"
	"os"

	"juno/pkg/stats"
)

var (
	rockdbProperties = []string{
		"stats",
		"sstables",
		"cfstats",
		"cfstats-no-file-histogram",
		"cf-file-histogram",
		"dbstats",
		"levelstats",
		"num-immutable-mem-table",
		"num-immutable-mem-table-flushed",
		"mem-table-flush-pending",
		"num-running-flushes",
		"compaction-pending",
		"num-running-compactions",
		"background-errors",
		"cur-size-active-mem-table",
		"cur-size-all-mem-tables",
		"size-all-mem-tables",
		"num-entries-active-mem-table",
		"num-entries-imm-mem-tables",
		"num-deletes-active-mem-table",
		"num-deletes-imm-mem-tables",
		"estimate-num-keys",
		"estimate-table-readers-mem",
		"is-file-deletions-enabled",
		"num-snapshots",
		"oldest-snapshot-time",
		"num-live-versions",
		"current-super-version-number",
		"estimate-live-data-size",
		"min-log-number-to-keep",
		"total-sst-files-size",
		"base-level",
		"estimate-pending-compaction-bytes",
		"aggregated-table-properties",
		"aggregated-table-properties-at-level0",
		"aggregated-table-properties-at-level1",
		"aggregated-table-properties-at-level2",
		"aggregated-table-properties-at-level3",
		"aggregated-table-properties-at-level4",
		"aggregated-table-properties-at-level5",
		"aggregated-table-properties-at-level6",
		"actual-delayed-write-rate",
		"is-write-stopped",
	}

	dbIndexTmpl        *template.Template
	dbIndexMainTmpl    *template.Template
	pprofIndexTmpl     *template.Template
	pprofIndexMainTmpl *template.Template
)

func initDbIndexTemplate(workerId string) {
	var queryString string

	if len(workerId) != 0 {
		queryString = "&wid=" + workerId
	}
	main := fmt.Sprintf(`
/debug/dbstats/<br>
<br>
DB properties:<br>
<table>
{{range $prop := .}}
<tr><td align=right><td><a href="/?prop={{$prop}}%s">rocksdb.{{$prop}}</a>
{{end}}
</table>
<br>
`, queryString)

	page := fmt.Sprintf(`<html>
<head>
<title>/debug/dbstats/</title>
`+stats.HtmlElemScript(stats.KScriptHrefClickWithElemPath)+`
</head>
<body>
%s
</body>
</html>
`, main)
	dbIndexTmpl = template.Must(template.New("dbindex").Parse(page))
	dbIndexMainTmpl = template.Must(template.New("dbindexmain").Parse(main))
}

/*
& - %26
= - %3D
? - %3F

*/
func initPprofIndexTemplate(workerId string) {
	queryString := ""
	workerInfo := ""

	if workerId != "" {
		queryString = "&" + "wid=" + workerId
		workerInfo = "worker: " + workerId + ","
	}
	workerInfo += fmt.Sprintf("  pid: %d", os.Getpid())

	main := fmt.Sprintf(`
/debug/pprof/ &nbsp;&nbsp;&nbsp;&nbsp; (%s)
<br>
<br>
profiles:<br>
<table>
{{range .}}
<tr><td align=right>{{.Count}}<td><a href="/{{.Name}}?debug=1%s">{{.Name}}</a>
{{end}}
</table>
<br>
<a href="/goroutine?debug=2%s">full goroutine stack dump</a><br>
`, workerInfo, queryString, queryString)

	page := fmt.Sprintf(`<html>
<head>
<title>pprof</title>
`+stats.HtmlElemScript(stats.KScriptHrefClickWithElemPath)+`
</head>
<body>
%s
</body>
</html>
`, main)
	pprofIndexTmpl = template.Must(template.New("pprofindex").Parse(page))
	pprofIndexMainTmpl = template.Must(template.New("pprofindexmain").Parse(main))
}
