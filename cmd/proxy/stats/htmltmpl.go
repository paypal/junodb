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
