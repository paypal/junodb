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
//  Package utility provides the utility interfaces for mux package
//  
package stats

import (
	"fmt"
	"html/template"
	"net/http"
)

func initClusterConsole(clusterName string) {
	initClusterPageTemplate(clusterName)
}

func (h *HandlerForMonitor) httpClusterConsoleHandler(w http.ResponseWriter, r *http.Request) {
	if htmlClusterImpl != nil {
		htmlClusterImpl.Execute(w, mainSectionT{`
<div style="display: flex; justify-content: center;align-items: center; 
height: 200px;font-size: 32px;color: #1861A7; 
text-shadow: 10px 10px 16px #000000;font-weight: 900;
font-style: italic;">
Cluster Console</div>  
`})
	}
}

func (h *HandlerForMonitor) dummyHandler(w http.ResponseWriter, r *http.Request) {
	if htmlClusterImpl != nil {
		htmlClusterImpl.Execute(w, mainSectionT{"<br><h2>&nbsp;&nbsp;To be implementated</h2>"})
	}
}

func (h *HandlerForMonitor) httpClusterConsoleShardMapHandler(w http.ResponseWriter, r *http.Request) {
	values := r.URL.Query()
	values.Set(kQueryElemKey, kQueryElemValueMain)
	values.Set("wid", "0")

	body, err := h.getFromWorker("/debug/shardmgr", values)
	if err != nil {
		fmt.Fprint(w, err.Error())
		return
	}
	if htmlClusterImpl != nil {
		htmlClusterImpl.Execute(w, mainSectionT{template.HTML(body)})
	}
}
