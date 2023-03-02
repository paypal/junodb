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
