package stats

import (
	"bytes"
	"html/template"
	"strings"
	"time"
)

const (
	KScriptHrefClick = `
  document.onclick = function (e) {
    e = e ||  window.event;
    var element = e.target || e.srcElement;

    if (element.tagName == 'A') {
      var loc = window.location.href.replace(/\/$/, '');
      loc = loc.replace(/\?/, '?');
	  location.href = loc + encodeURI(element.search)
      return false; 
    }
  };	
`
	KScriptHrefClickWithElemPath = `
  document.onclick = function (e) {
    e = e ||  window.event;
    var element = e.target || e.srcElement;

    if (element.tagName == 'A') {
  	  var str = window.location.href
	  var pos = str.lastIndexOf("?")
	  var base = str.substr(0, pos)
	  var pathname = element.pathname.replace(/\/$/, '');
	
	  location.href = base + pathname + encodeURI(element.search)
      return false; 
    }
  };
`
)

var (
	kHtmlDefaultCSS = HtmlElem("style", `
body {
  font-family: 'lato', sans-serif;
  font-size: 12px;
}
h1 {
    font-size: 32px;
    font-weight: 400;
}
h2 {
    font-size: 24px;
    font-weight: 500;
    color: #375EAB;
}
table {
    border-collapse: collapse;
}

th, td {
    text-align: left;
    padding: 8px;
}

tr:nth-child(even){background-color: #E6EAF2}
tr:nth-child(odd){background-color: #EBF5FB}

th {
    background-color: #2F72B1;
    color: white;
}
div.container {
    width: 100%;
    border: 1px solid gray;
}

header, footer {
    padding: 1em;
    color: white;
    background-color:  #2C5893;
    clear: left;
    text-align: center;
}
`)

	HtmlStatsTmpl = template.Must(template.New("html-stats-page").Parse(`<!DOCTYPE html>
<html>

<head>
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{{ .Title }}</title>
` + kHtmlDefaultCSS + HtmlElemScript(KScriptHrefClick) + `
</head>
<body>

<div class="container">

<header>
<h1> {{.Title }} </h1>
<p style="text-align:left;"><font size="2"> {{.ClusterName}} 
<span style="float:right;">{{ .Version }} </font></span> 
</p>
</header>
  
{{range .Sections}}
		<h2> {{.Title}} </h2>
		{{.Body}}
   {{end}}
   <br><br>
<footer>Copyright &copy; PayPal</footer>
</div>

</body>
</html>`))

	HtmlSectionsTmpl = template.Must(template.New("html-sections").Parse(`
{{range .Sections}}
		<h2> {{.Title}} </h2>
		{{.Body}}
   {{end}}
   <br><br>
`))

	IndexPageTmpl = template.Must(template.New("index").Parse(`<html>
<head>
<title>{{.Title}}</title>
` + HtmlElemScript(KScriptHrefClickWithElemPath) + `
</head>
<body>
<br>
<table>
{{range $prop := .Links}}
<tr><td align=right><td><a href="{{$prop.HRef}}">{{$prop.Text}}</a>
{{end}}
</table>
<br>
</body>
</html>
`))

	IndexPageMainTmpl = template.Must(template.New("index").Parse(`
<br>
<table>
{{range $prop := .Links}}
<tr><td align=right><td><a href="{{$prop.HRef}}">{{$prop.Text}}</a>
{{end}}
</table>
<br>
`))
)

func HtmlElemScript(code string) string {
	return HtmlElem("script", code)
}

func HtmlElem(tagName string, code string) string {
	var buf bytes.Buffer
	buf.WriteByte('<')
	buf.WriteString(tagName)
	buf.WriteString(">\n")
	buf.WriteString(code)
	buf.WriteString("</")
	buf.WriteString(tagName)
	buf.WriteString(">\n")
	return buf.String()
}

func HtmlDurationEscapeString(d time.Duration) string {
	return strings.Replace(d.String(), "µs", "&#181;s", 1)
}
