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

	"github.com/paypal/junodb/pkg/version"
)

var (
	htmlClusterImpl *template.Template
)

type mainSectionT struct {
	Body template.HTML
}

func initClusterPageTemplate(clusterName string) {
	var buf bytes.Buffer

	fmt.Fprintf(&buf, `<nav id="navbar">
<font size="6">&nbsp;&nbsp;Juno</font><font size="1"> %s %s </font>
<div id="navbar-right">
  <a href="/cluster/admin">Admin</a>
  <a href="/cluster/stats">Stats</a>
  <a href="/cluster/shardmap">Shard Map</a>
  <a href="/cluster/tool">Toolkit</a>
  <a href="/cluster/login">Login</a>
</div>
</nav>
`, clusterName, version.OnelineVersionString())

	htmlClusterImpl = template.Must(template.New("ClusterPage").Parse(` 
<!DOCTYPE html>
<html>
<head>
<style>
` + htmlCSS +
		`
</style>
</head> 
<body>
` + buf.String() +
		`
<div class="main"> 
{{.Body}} 
</div>
</body>
</html>
`))
}

const (
	htmlCSS = `
#navbar {
  top: 0;
  overflow: hidden;
  background-color: #FFFFFF;
  background: linear-gradient(to right, #159CDE, #19478F);
  color: white;
  vertical-align:middle;
 vertical-align: middle
  padding: 14px 14px;
  position: fixed; /* Set the navbar to fixed position */
  top: 0; /* Position the navbar at the top of the page */
  width: 100%; /* Full width */
  text-shadow: 4px 4px 6px #000000;
}

#navbar a {
  float: left;
  display: block;
  color: #f2f2f2;
  text-align: center;
  padding: 14px;
  text-decoration: none;
}

#navbar a:hover {
  background-color: #ddd;
  color: black;
}

#navbar a.active {
  background-color: #4CAF50;
  color: white;
}

#navbar-right {
  float: right;
  font-weight: 600;
}

.content {
  padding: 16px;
}

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

.main {
  margin-top: 50px; 
}
`
)
