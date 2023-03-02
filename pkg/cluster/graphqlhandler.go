package cluster

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"mime"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"juno/third_party/forked/golang/glog"

	"github.com/graphql-go/graphql"

	"juno/pkg/logging/cal"
)

// Host state
const (
	InvalidHost = iota
	NotReachable
	InvalidAppServiceID
	VersionIsCurrent
	RestartNotOk
	RestartOk
	DeployNotOk
	DeployOk
	HostLocked
	QueryHostLockedErr
)

const (
	HostLockedStr         = "HostLocked"
	QueryHostLockedErrStr = "QueryHostLockedErr"
)

func stateToString(n int) string {
	var rt string = "unknown"
	switch n {
	case InvalidHost:
		rt = "InvalidHost"
	case NotReachable:
		rt = "NotReachable"
	case InvalidAppServiceID:
		rt = "InvalidAppServiceID"
	case VersionIsCurrent:
		rt = "VersionIsCurrent"
	case RestartNotOk:
		rt = "RestartNotOk"
	case RestartOk:
		rt = "RestartOk"
	case DeployNotOk:
		rt = "DeployNotOk"
	case DeployOk:
		rt = "DeployOk"
	case HostLocked:
		rt = HostLockedStr
	case QueryHostLockedErr:
		rt = QueryHostLockedErrStr
	}
	return rt
}

// IsGood check state
func IsGood(state string) bool {
	if state == "RestartOk" || state == "DeployOk" {
		return true
	}
	return false
}

// HostRestartInfo is the return type
type HostRestartInfo struct {
	Info  RestartInfo //`json:"info"`
	Error string      //`json:"error"`
}

// RestartInfo is a type for return
type RestartInfo struct {
	Fqdn      string //`json:"FQDN"`
	HostState string //`json:"HostState"`
}

// HostDeployInfo is the return type
type HostDeployInfo struct {
	Info  DeployInfo //`json:"info"`
	Error string     //`json:"error"`
}

// DeployInfo is a type for return
type DeployInfo struct {
	Fqdn      string //`json:"FQDN"`
	HostState string //`json:"HostState"`
	Version   string //`json:"Version"`
}

var HostRestartInfoType = graphql.NewObject(
	graphql.ObjectConfig{
		Name: "HostRestartInfo",
		Fields: graphql.Fields{
			"info": &graphql.Field{
				Type: restartInfoType,
			},
			"error": &graphql.Field{
				Type: graphql.String,
			},
		},
	},
)

var restartInfoType = graphql.NewObject(
	graphql.ObjectConfig{
		Name: "RestartInfo",
		Fields: graphql.Fields{
			"fqdn": &graphql.Field{
				Type: graphql.String,
			},
			"hostState": &graphql.Field{
				Type: graphql.String,
			},
		},
	},
)

var HostDeployInfoType = graphql.NewObject(
	graphql.ObjectConfig{
		Name: "HostDeployInfoType",
		Fields: graphql.Fields{
			"info": &graphql.Field{
				Type: deployInfoType,
			},
			"error": &graphql.Field{
				Type: graphql.String,
			},
		},
	},
)

var deployInfoType = graphql.NewObject(
	graphql.ObjectConfig{
		Name: "DeployInfoType",
		Fields: graphql.Fields{
			"fqdn": &graphql.Field{
				Type: graphql.String,
			},
			"hostState": &graphql.Field{
				Type: graphql.String,
			},
			"version": &graphql.Field{
				Type: graphql.Int,
			},
		},
	},
)

var GetHostListInfoType = graphql.NewObject(
	graphql.ObjectConfig{
		Name: "HostListInfoType",
		Fields: graphql.Fields{
			"fqnds": &graphql.Field{
				Type: graphql.NewList(graphql.String),
			},
			"error": &graphql.Field{
				Type: graphql.String,
			},
		},
	},
)

// This is no
// var getHostListInfoType

// RestartConfigArgument shares with GetHostList
var RestartConfigArgument = graphql.FieldConfigArgument{
	"requestParam": &graphql.ArgumentConfig{
		Type: graphql.NewInputObject(
			graphql.InputObjectConfig{
				Name: "hostRestartParam",
				Fields: graphql.InputObjectConfigFieldMap{
					"fqdns": &graphql.InputObjectFieldConfig{
						Type: graphql.NewList(graphql.NewNonNull(graphql.String)),
					},
					"applicationServiceID": &graphql.InputObjectFieldConfig{
						Type: graphql.NewNonNull(graphql.String),
					},
					"colo": &graphql.InputObjectFieldConfig{
						Type: graphql.NewNonNull(graphql.String),
					},
				},
			},
		),
	},
}

var DeployConfigArgument = graphql.FieldConfigArgument{
	"requestParam": &graphql.ArgumentConfig{
		Type: graphql.NewInputObject(
			graphql.InputObjectConfig{
				Name: "hostDeployParam",
				Fields: graphql.InputObjectConfigFieldMap{
					"fqdns": &graphql.InputObjectFieldConfig{
						Type: graphql.NewList(graphql.NewNonNull(graphql.String)),
					},
					"applicationServiceID": &graphql.InputObjectFieldConfig{
						Type: graphql.NewNonNull(graphql.String),
					},
					"colo": &graphql.InputObjectFieldConfig{
						Type: graphql.NewNonNull(graphql.String),
					},
					"desiredVersion": &graphql.InputObjectFieldConfig{
						Type: graphql.NewNonNull(graphql.Int),
					},
				},
			},
		),
	},
}

var restartResolver = func(_params graphql.ResolveParams) (interface{}, error) {
	glog.Infoln("restartResolver")
	// Check empty, others are done by schema
	hostInfo, err := getHostInfo(_params)
	rt := make([]HostRestartInfo, 0)
	if err != nil {
		glog.Errorln(err)
		rt = append(rt, HostRestartInfo{
			Error: err.Error(),
		})
	} else {
		state := hostState{notOk: RestartNotOk, Ok: RestartOk}
		err := state.getHostsState(hostInfo)
		if err == nil {
			glog.Infoln(hostInfo)
			for key, val := range hostInfo {
				rt = append(rt, HostRestartInfo{
					Info:  RestartInfo{Fqdn: key, HostState: stateToString(val)},
					Error: "",
				})
			}
		} else {
			glog.Errorln(err)
			rt = append(rt, HostRestartInfo{
				Error: err.Error(),
			})
		}
	}
	glog.Infoln("restartResolver ", rt)
	return rt, nil
}

var deployResolver = func(_params graphql.ResolveParams) (interface{}, error) {
	glog.Infoln("deployResolver")
	hostInfo, err := getHostInfo(_params)
	rt := make([]HostDeployInfo, 0)
	if err != nil {
		glog.Errorln(err)
		rt = append(rt, HostDeployInfo{
			Error: err.Error(),
		})
	} else {
		state := hostState{notOk: DeployNotOk, Ok: DeployOk}
		err := state.getHostsState(hostInfo)
		glog.Infoln(hostInfo)
		if err == nil {
			for key, val := range hostInfo {
				// TBD: Not allow to deployment, revisit later
				val = DeployNotOk

				rt = append(rt, HostDeployInfo{
					Info:  DeployInfo{Fqdn: key, HostState: stateToString(val), Version: "-1"},
					Error: "",
				})
			}
		} else {
			glog.Errorln(err)
			rt = append(rt, HostDeployInfo{
				Error: err.Error(),
			})
		}
	}
	glog.Infoln("deployResolver ", rt)
	return rt, nil
}

var getHostListResolver = func(_params graphql.ResolveParams) (interface{}, error) {
	glog.Infoln("getHostListResolver")
	hostInfo, err := getHostInfo(_params)
	rt := make([]HostRestartInfo, 0)
	if err != nil {
		glog.Errorln("hostListResolver ", err)
		rt = append(rt, HostRestartInfo{Error: err.Error()})
	} else {
		hostList, err := getHostListInQuorumByShards(hostInfo)
		if err != nil {
			glog.Errorln("hostListResolver ", err)
			rt = append(rt, HostRestartInfo{Error: err.Error()})
		} else {
			for host := range hostList {
				hosts := hostList[host]
				if hosts == nil {
					// Alg < 2, hosts in hostInfo should be in the same zone,
					// then only one host should get all hosts in quorum
					continue
				}
				for key := range hosts {
					rt = append(rt, HostRestartInfo{
						Info:  RestartInfo{Fqdn: key, HostState: "NotUsed"},
						Error: "",
					})
				}
			}
		}
	}
	glog.Infoln("getHostListResolver ", rt)
	return rt, err
}

func getHostInfo(_params graphql.ResolveParams) (map[string]int, error) {
	glog.Infoln("getHostInfo")
	params, found := _params.Args["requestParam"].(map[string]interface{})
	if !found || len(params) == 0 {
		return nil, errors.New("requestParam cannot be empty")
	}
	if err := validateAppID(params); err != nil {
		return nil, err
	}

	list := params["fqdns"].([]interface{})
	return buildHostInfo(list)
}

func buildHostInfo(list []interface{}) (map[string]int, error) {
	glog.Infoln("buildHostInfo")
	hostInfo := map[string]int{}
	for r := range list {
		fqdn := list[r].(string)
		if len(fqdn) == 0 {
			return nil, errors.New("fqdn cannot be empty")
		}
		hostInfo[fqdn] = InvalidHost
	}
	glog.Infoln(hostInfo)
	return hostInfo, nil
}

func validateAppID(params map[string]interface{}) error {
	glog.Infoln("validateAppID")
	// Get current working directory
	appID := params["applicationServiceID"].(string)
	if len(appID) == 0 {
		return errors.New("applicationServiceID cannot be empty")
	}

	dir, err := os.Getwd()
	if err != nil {
		return err
	}
	glog.Infoln(dir)

	_, fileName := filepath.Split(dir)
	if len(fileName) == 0 {
		return errors.New("Fatal: Cannot get service name")
	}

	parts := strings.Split(appID, ":")
	if len(parts) != 3 {
		s := fmt.Sprintf("Invalid applicationServiceID %s 1", appID)
		return errors.New(s)
	}

	appPrefix := strings.Split(parts[2], "-")
	if appPrefix[0] != "junoserv" && appPrefix[0] != "junostorageserv" && appPrefix[0] != "junoclusterserv" {
		s := fmt.Sprintf("Invalid applicationServiceID %s 2", appID)
		return errors.New(s)
	}
	filePrefix := strings.Split(fileName, "-")
	if len(filePrefix) != len(appPrefix) {
		s := fmt.Sprintf("Invalid applicationServiceID %s 3", appID)
		return errors.New(s)
	}

	if len(filePrefix) > 1 && filePrefix[1] != appPrefix[1] {
		s := fmt.Sprintf("Invalid applicationServiceID %s 4", appID)
		return errors.New(s)
	}
	return nil
}

type hostState struct {
	Ok    int
	notOk int
}

func (h *hostState) getHostsState(hostInfo map[string]int) error {
	glog.Infoln("getHostsState")
	p := GetShardMgr()
	if p == nil {
		return errors.New("Fatal: failed to obtain ShardMgr")
	}
	m := p.GetShardMap()
	if m != nil && m.cluster != nil {
		glog.Info("getHostsState: Start loop through shardmap")
		numZones := int(m.cluster.Config.NumZones)
		numShards := int(m.cluster.Config.NumShards)
		for sid := 0; sid < numShards; sid++ {
			numOK := 0
			inHostNames := map[string]int{}
			// Check each zone in this shard
			for zid := 0; zid < numZones; zid++ {
				hostname, o := getHostName(p, m, sid, zid)
				// Logic:
				// If the input host is in this zone, increase the bad count
				// If the input host is not in this zone, but connection is bad, increase the bad count
				_, found := hostInfo[hostname]
				if found {
					inHostNames[hostname] = 1
					numOK++
				} else if o.GetIsConnected() == 0 {
					numOK++
				}

				// If the host in this zone and bad count is greater than 1, mark this host NotOk
				// If the host in this zone and its state did not changed, mark this host Ok
				if len(inHostNames) > 0 && numOK > 1 {
					for k := range inHostNames {
						hostInfo[k] = h.notOk
					}
				} else if len(inHostNames) > 0 && numOK == 1 {
					for k := range inHostNames {
						if hostInfo[k] == InvalidHost {
							hostInfo[k] = h.Ok
						}
					}
				}
				// if inHost {
				// 	glog.Infoln(hostInfo[hostname])
				// }
			}
		}
		return nil
	} else {
		return errors.New("fatal: unenable to get shard map")
	}
}

func getHostListInQuorumByShards(hostInfo map[string]int) (map[string]map[string]int, error) {
	glog.Infoln("getHostListInQuorumByShards")
	p := GetShardMgr()
	if p == nil {
		return nil, errors.New("Fatal: failed to obtain ShardMgr")
	}
	m := p.GetShardMap()
	if m != nil && m.cluster != nil {
		hostShards, pos := GetShardsByName(hostInfo)
		if hostShards == nil || pos == nil {
			return nil, errors.New("Fatal: Invalid hosts")
		}
		hostlist := map[string]map[string]int{}
		for host := range hostShards { // loop host list
			glog.Infoln("getHostListInQuorumByShards ", host)
			hosts := map[string]int{}
			glog.Infoln("getHostListInQuorumByShards #sid", len(hostShards[host]))
			for sid := range hostShards[host] { // loop shards of one of the hosts
				for i := 0; i < int(m.cluster.Config.NumZones); i++ { // find hosts in the same shard in other zone
					if i == pos[host][0] {
						// escape the zone of the input host
						continue
					}
					name, _ := getHostName(p, m, int(sid), i)
					hosts[name] = 1
				}
			}
			hostlist[host] = hosts
		}
		return hostlist, nil
	}
	return nil, errors.New("Invalid hosts")
}

// GetShardsByName export for testing
func GetShardsByName(hostInfo map[string]int) (map[string]map[uint32]int, map[string][2]int) {
	ssInstCount := getSSCount()
	pos := lookupHostPositionInternal(hostInfo, ssInstCount)
	if len(pos) == 0 {
		return nil, nil
	}

	shards := map[string]map[uint32]int{}
	for host := range pos {
		zid := pos[host][0]
		nid := pos[host][1]
		sub := map[uint32]int{}
		for i := nid; i < (nid + ssInstCount); i++ {
			tmps := ClusterInfo[0].Zones[zid].Nodes[i].GetShards()
			for i := range tmps {
				sub[tmps[i]] = 1
			}
		}
		shards[host] = sub
	}
	return shards, pos
}

func getHostName(p *ShardManager, m *ShardMap, sid int, zid int) (string, *OutboundSSProcessor) {
	node := m.shards[sid][zid]
	o := p.processors[zid][node.nodeid]
	endpoint := o.GetConnInfo()
	parts := strings.Split(endpoint, ":")
	return parts[0], o
}

func getSSCount() int {
	var host string = ""
	var count int = 0
	// Loop all instances in zone 0 only because
	// From SSHost[][]string, the instances on each ss will be the same
	// From ConnInfo, you can have difference instances on each ss, but
	// you will need to dup instance/instances in order to make ConnInfo
	// such as, ss b, it has only one instance, but 2 for others
	// a1 b1 c1 d1 e1
	// a2 b1 c2 d2 e2
	for i := range ClusterInfo[0].ConnInfo[0] {
		parts := strings.Split(ClusterInfo[0].ConnInfo[0][i], ":")
		if len(host) == 0 {
			host = parts[0]
		} else if host == parts[0] {
			count++
		}
	}
	if count == 0 {
		return len(ClusterInfo[0].SSPorts)
	}
	return count + 1
}

// LookupHostPosition export for testing
func LookupHostPosition(hostInfo map[string]int) map[string][2]int {
	ssInstCount := getSSCount()
	return lookupHostPositionInternal(hostInfo, ssInstCount)
}

func lookupHostPositionInternal(hostInfo map[string]int, ssInstCount int) map[string][2]int {
	rt := make(map[string][2]int, 0)
	for j := 0; j < int(ClusterInfo[0].NumZones); j++ { // Zones
		for i := 0; i < len(ClusterInfo[0].ConnInfo[j]); i += int(ssInstCount) { // row
			parts := strings.Split(ClusterInfo[0].ConnInfo[j][i], ":")
			//fmt.Println(parts[0])
			if _, found := hostInfo[parts[0]]; found {
				rt[parts[0]] = [2]int{j, i}
				delete(hostInfo, parts[0])
				if len(hostInfo) == 0 {
					return rt
				}
			}
		}
	}
	return rt
}

var queryType = graphql.NewObject(
	graphql.ObjectConfig{
		Name: "Query",
		Fields: graphql.Fields{
			/* Get (read) restart request list
			   http://10.176.19.204:8080/graphql?query={restart(requestParam:{fqdns:["127.0.0.1","abc"],applicationServiceID:"tr:prod:junoserv"}){info{fqdn,hostState},error}}
			*/
			"restart": &graphql.Field{
				Type:        graphql.NewList(HostRestartInfoType),
				Description: "Get product list by hostInfos",
				Args:        RestartConfigArgument,
				Resolve:     restartResolver,
			},
			"deploy": &graphql.Field{
				/* Get (read) deploy request list
				 * http://10.176.19.204:8080/graphql?query={deploy(requestParam:{fqdns:["127.0.0.1","abc"],applicationServiceID:"tr:prod:junoserv",desiredVersion:10}){info{fqdn,hostState},error}}
				 */
				Type:        graphql.NewList(HostDeployInfoType),
				Description: "Get product list by hostInfos",
				Args:        DeployConfigArgument,
				Resolve:     deployResolver,
			},
			"hostlist": &graphql.Field{
				/* Get (read) deploy request list
				 * http://10.176.19.204:8080/graphql?query={deploy(requestParam:{fqdns:["127.0.0.1","abc"],applicationServiceID:"tr:prod:junoserv",desiredVersion:10}){info{fqdn,hostState},error}}
				 */
				//Type:        graphql.NewNonNull(GetHostListInfoType),
				Type:        graphql.NewList(HostDeployInfoType),
				Description: "Get host list",
				Args:        RestartConfigArgument, // use the same input as restart
				Resolve:     getHostListResolver,
			},
		},
	})

var schema, _ = graphql.NewSchema(
	graphql.SchemaConfig{
		Query: queryType,
	},
)

func executeQuery(query string, schema graphql.Schema) *graphql.Result {
	result := graphql.Do(graphql.Params{
		Schema:        schema,
		RequestString: query,
	})
	if len(result.Errors) > 0 {
		glog.Errorf("errors: %v", result.Errors)
	}
	return result
}

// RegisterGraphql register here in web
func RegisterGraphql() {
	http.DefaultServeMux.HandleFunc("/graphql", GraphqlHandler)
}

// GraphqlHandler handler graphql query
func GraphqlHandler(w http.ResponseWriter, r *http.Request) {
	glog.Infoln("GraphqlHandler")
	qStr := GetQueryString(r)
	result := executeQuery(qStr, schema)
	s := fmt.Sprintln(result)
	cal.Event("Graphql", "request", cal.StatusSuccess, []byte(s))
	json.NewEncoder(w).Encode(result)
}

func GetQueryString(r *http.Request) string {
	glog.Info("getQueryString: " + r.Method)
	var s string = r.URL.Query().Get("query")
	if r.Method == "POST" {
		glog.Infoln("getQueryString -> POST")
		if mediaType, params, err := mime.ParseMediaType(r.Header.Get("Content-Type")); err == nil {
			glog.Infoln("mediaType: %v, params: %v\n", mediaType, params)
			if strings.Contains(mediaType, "multipart") {
				mr := multipart.NewReader(r.Body, params["boundary"])
				buf := []byte{}
				for {
					p, err := mr.NextPart()
					if err == io.EOF {
						break
					}
					if err != nil {
						glog.Errorln(err)
						break
					}
					slurp, err := ioutil.ReadAll(p)
					if err != nil {
						glog.Errorln(err)
						break
					}
					buf = append(buf, slurp...)
				}
				s = bytes.NewBuffer(buf).String()
			} else {
				if buf, err := ioutil.ReadAll(r.Body); err == nil {
					s = bytes.NewBuffer(buf).String()
				} else {
					glog.Errorln(err)
				}
			}
		} else {
			// Let resovler to handle it
			glog.Errorln(err)
		}
	}

	return s
}
