package stats

import (
	"bytes"
	"fmt"
	"html/template"
	"time"

	"juno/cmd/storageserv/stats/shmstats"
	"juno/pkg/stats"
)

type (
	htmlSectReqProcStatsT struct{}
	htmlSectServerInfoT   struct{}
)

func (s *htmlSectReqProcStatsT) Title() template.HTML {
	return "Request Processing"
}

func (s *htmlSectReqProcStatsT) Body() template.HTML {
	var buf bytes.Buffer
	buf.WriteString(`<div id="id-req-proc"><table title="req-proc">`)

	buf.WriteString(`<table title="Request Processing">`)
	mgr := shmstats.GetCurrentWorkerStatsManager()
	wstats := mgr.GetWorkerStats()

	buf.WriteString("<tr>")
	buf.WriteString("<th>Requests</th><th>Throughtput</th><th>Average Request Process Time</th>")
	buf.WriteString("<th>Reads</th>")
	buf.WriteString("<th>Commits</th><th>Aborts</th><th>Repairs</th><th>MarkDeletes</th>")
	buf.WriteString("</tr><tr>")
	fmt.Fprintf(&buf, "<td>%d</td>", wstats.NumRequests)
	fmt.Fprintf(&buf, "<td>%d</td>", wstats.RequestsPerSecond)
	fmt.Fprintf(&buf, "<td>%s</td>", stats.HtmlDurationEscapeString(time.Duration(wstats.AvgReqProcTime*1000)))
	fmt.Fprintf(&buf, "<td>%d</td>", wstats.NumReads)
	fmt.Fprintf(&buf, "<td>%d</td>", wstats.NumCommits)
	fmt.Fprintf(&buf, "<td>%d</td>", wstats.NumAborts)
	fmt.Fprintf(&buf, "<td>%d</td>", wstats.NumRepairs)
	fmt.Fprintf(&buf, "<td>%d</td>", wstats.NumMarkDeletes)

	buf.WriteString("</tr>")
	buf.WriteString("</table></div>")

	return template.HTML(buf.String())
}

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
