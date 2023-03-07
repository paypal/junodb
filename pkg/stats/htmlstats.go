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
