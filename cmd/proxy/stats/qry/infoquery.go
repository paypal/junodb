package qry

import (
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"

	"juno/pkg/cluster"
)

var (
	infoQueryFuncMap      = map[string]func(w http.ResponseWriter, v url.Values){}
	kQryCmdSsConnected    = "ss_connected"
	kQryCmdSsNotConnected = "ss_not_connected"
	kQryCmdSsGetPid       = "get_pid"
	kQryCmdBadShardHosts  = "ss_bad_shard_hosts"
)

func InfoQuery(w http.ResponseWriter, values url.Values) {
	keys, ok := values["info"]
	if ok && len(keys) != 0 {
		cmd := keys[0]
		f, ok := infoQueryFuncMap[cmd]
		if ok {
			f(w, values)
		}
	}
}

func getPid(w http.ResponseWriter, values url.Values) {
	fmt.Fprintf(w, "%v", os.Getpid())
}

func querySsConnected(w http.ResponseWriter, values url.Values) {
	connected := true
	if cluster.GetShardMgr() != nil {

		nodes, found := values["node"]
		if found {
			for _, n := range nodes {
				v := strings.Split(n, ",")
				if len(v) >= 2 {

					zone, err := strconv.Atoi(v[0])
					if err == nil {
						if zoneIndex, err := strconv.Atoi(v[1]); err == nil {
							if cluster.GetShardMgr().IsConnected(zone, zoneIndex) == false {
								connected = false
								break
							}
						}
					}
				}
			}
		} else {
			connStates := cluster.GetShardMgr().GetConnectivity()
		loop:
			for i := range connStates {
				for v := range connStates[i] {
					if connStates[i][v] == 0 {
						connected = false
						break loop
					}
				}
			}
		}
	}
	fmt.Fprintf(w, "%v", connected)
}

func querySsNotConnected(w http.ResponseWriter, values url.Values) {
	notconnected := true
	if cluster.GetShardMgr() != nil {

		nodes, found := values["node"]
		if found {
			for _, n := range nodes {
				v := strings.Split(n, ",")
				if len(v) >= 2 {

					zone, err := strconv.Atoi(v[0])
					if err == nil {
						if zoneIndex, err := strconv.Atoi(v[1]); err == nil {
							if cluster.GetShardMgr().IsConnected(zone, zoneIndex) == true {
								notconnected = false
								break
							}
						}
					}
				}
			}
		} else {
			connStates := cluster.GetShardMgr().GetConnectivity()
		loop:
			for i := range connStates {
				for v := range connStates[i] {
					if connStates[i][v] == 1 {
						notconnected = false
						break loop
					}
				}
			}
		}
	}
	fmt.Fprintf(w, "%v", notconnected)
}

func queryBadShardHosts(w http.ResponseWriter, values url.Values) {
	mgr := cluster.GetShardMgr()
	if mgr != nil {
		v, found := values["status"]
		c := uint32(0)
		if found {
			switch v[0] {
			case "warning":
				c = 2
			case "alert":
				c = 1
			case "fatal":
			default:
				c = 0
			}
		}
		hosts := mgr.GetBadShardHosts(c)
		fmt.Fprintf(w, "%v", hosts)
	} else {
		fmt.Fprintf(w, "%v", "Unable to get cluster manager")
	}
}

func init() {
	infoQueryFuncMap[kQryCmdSsGetPid] = getPid
	infoQueryFuncMap[kQryCmdSsConnected] = querySsConnected
	infoQueryFuncMap[kQryCmdSsNotConnected] = querySsNotConnected
	infoQueryFuncMap[kQryCmdBadShardHosts] = queryBadShardHosts
}
