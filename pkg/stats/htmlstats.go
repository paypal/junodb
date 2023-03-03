package stats

import (
	"bytes"
	"fmt"
	"html/template"
	"os"
	"time"
)

type (
	IHtmlStatsSection interface {
		Title() template.HTML
		Body() template.HTML
	}

	HtmlStats struct {
		Title       string
		Version     string
		ClusterName string
		Sections    []IHtmlStatsSection
	}

	ServerInfo struct {
		NumZones  uint32
		NumShards uint32
		StartTime time.Time
	}
	IndexPage struct {
		Title string
		Links []Hyperlink
	}
	Hyperlink struct {
		Text string
		HRef string
	}
)

func (s *ServerInfo) Title() template.HTML {
	return template.HTML("Server Info")
}

func (s *ServerInfo) Body() template.HTML {
	var buf bytes.Buffer
	buf.WriteString(
		`<div id="id-server-info"><table title="server-info">
<tr><th>Start Time</th><th>Process ID</th><th>Number of Zones</th><th>Number of Shards</th></tr>`)

	fmt.Fprintf(&buf, "<tr><td>%s</td><td>%d</td><td>%d</td><td>%d</td></tr></table>",
		s.StartTime.Format("2006-01-02 15:04:05"), os.Getpid(), s.NumZones, s.NumShards)

	return template.HTML(buf.String())
}

func (s *HtmlStats) AddSection(sec IHtmlStatsSection) {
	s.Sections = append(s.Sections, sec)
}

func (p *IndexPage) AddLink(href string, text string) {
	if len(text) == 0 {
		text = href
	}
	p.Links = append(p.Links, Hyperlink{Text: text, HRef: href})
}
