package cmd

import (
	"text/template"
)

var usageTemplate = template.Must(template.New("cmd-usage").Parse(`
NAME
	{{.GetName}}{{if .GetDesc}} - {{.GetDesc}}{{end}}

SYNOPSIS
	{{.GetName}} {{if .GetSynopsis}}{{.GetSynopsis}}{{else}}[<args>]{{end}}
{{if .GetOptionDesc}}
OPTION
{{.GetOptionDesc}}
{{end}}
{{if .GetDetails}}DESCRIPTION
{{.GetDetails}}
{{end}}
{{if .GetExample}}EXAMPLE
{{.GetExample}}
{{end}}
`))
