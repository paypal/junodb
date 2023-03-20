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
	pprofIndexTmpl     *template.Template
	pprofIndexMainTmpl *template.Template
)

func initPprofIndexTemplate() {
	queryString := ""
	workerInfo := ""

	if workerIdString != "" {
		queryString = "&" + "wid=" + workerIdString
		workerInfo = "worker: " + workerIdString + ","
	}
	workerInfo += fmt.Sprintf("  pid: %d", os.Getpid())

	mainSect := fmt.Sprintf(`
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
<a href="goroutine?debug=2%s">full goroutine stack dump</a><br>
`, workerInfo, queryString, queryString)

	page := fmt.Sprintf(`<html>
<head>
<title>/debug/pprof/</title>
`+stats.HtmlElemScript(stats.KScriptHrefClickWithElemPath)+`
</head>
<body>
%s
</body>
</html>
`, mainSect)
	pprofIndexTmpl = template.Must(template.New("pprofindex").Parse(page))
	pprofIndexMainTmpl = template.Must(template.New("pprofindexMain").Parse(mainSect))
}
