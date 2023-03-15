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

package cluster

import (
	"errors"
	"fmt"
	goio "io"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"juno/third_party/forked/golang/glog"

	"juno/pkg/io"
	"juno/pkg/logging"
	"juno/pkg/logging/cal"
	"juno/pkg/logging/otel"
	"juno/pkg/shard"
	"juno/pkg/util"
)

var (
	Version uint32 = 0

	shardMgrIndex int32 = 0
	ClusterInfo   [2]Cluster
	shardMgrPair  [2]*ShardManager
	etcdReader    IReader
	cacheFile     string
	isWatching    bool
	//zonemarkdown  int32
	markdownobj ZoneMarkDown
)

type (
	ZoneMarkDown struct {
		markdownid int32
	}

	OutboundSSProcessor struct {
		io.OutboundProcessor
		zoneId      int
		indexInZone int
	}
	ShardManager struct {
		AlgVersion uint32 // default
		shardMap   ShardMap
		connInfo   [][]string
		ssconfig   *io.OutboundConfig
		processors [][]*OutboundSSProcessor
		stats      *ClusterStats
	}
)

func Initialize(args ...interface{}) (err error) {
	markdownobj.Reset()

	//	ccfg *cluster.Config, conf *ioconfig.OutboundConfig, chWatch, etcdReader, cacheFile, cfg.ClusterStats) {
	sz := len(args)
	if sz < 6 {
		err = fmt.Errorf("six arguments expected")
		glog.Error(err)
		return
	}
	var ccfg *Cluster
	var ok bool
	var iocfg *io.OutboundConfig
	var statscfg *StatsConfig

	ccfg, ok = args[0].(*Cluster)
	if !ok {
		err = fmt.Errorf("wrong type of the first argument")
		glog.Error(err)
		return
	}
	iocfg, ok = args[1].(*io.OutboundConfig)
	if !ok {
		err = fmt.Errorf("wrong type of the second argument")
		glog.Error(err)
		return
	}

	if args[5] != nil {
		statscfg, ok = args[5].(*StatsConfig)
		if !ok {
			statscfg = nil
			glog.Exitln("wrong type of the sixth argument")
		}
	}

	if err = InitShardMgr(ccfg, iocfg, statscfg); err != nil {
		glog.Error(err)
		return
	}

	if args[2] != nil && args[3] != nil {
		etcdReader, ok = args[3].(IReader)
		if !ok {
			glog.Exitln("wrong type of the third argument")
		}

		if len(args) >= 5 && args[4] != nil {
			cacheFile, ok = args[4].(string)
			if !ok {
				glog.Exitln("wrong type of the fourth argument")
			}
		}
		var chWatch chan int
		chWatch, ok = args[2].(chan int)
		if ok {
			if !isWatching {
				isWatching = true
				go watchAndResetShardMgr(iocfg, chWatch, statscfg)
			}
		}
	}
	waitForSSConnsToComplete(GetShardMgr(), time.Duration(100*time.Millisecond), iocfg.ConnectTimeout.Duration*2)
	return
}

func waitForSSConnsToComplete(mgr *ShardManager, checkinterval time.Duration, waitDuration time.Duration) {
	timer := time.NewTimer(waitDuration)
	ticker := time.NewTicker(checkinterval)
	defer func() {
		timer.Stop()
		ticker.Stop()
	}()
	for {
		select {
		case <-timer.C:
			return
		case <-ticker.C:
			if mgr != nil && mgr.IsConnectivityOk() {
				return
			}
		}
	}
}

func Finalize() {
	GetShardMgr().Shutdown(nil)
}

func GetMarkDownObj() (obj *ZoneMarkDown) {
	return &markdownobj
}

// connInfo is a 2D slices with
// each row represents one zone's nodes connection info
// total rows: num of zones
// total columns: num of nodes in each zones
func newShardManager(ccfg *Cluster, conf *io.OutboundConfig, statscfg *StatsConfig, curMgr *ShardManager) (m *ShardManager, err error) {
	connInfo := ccfg.ConnInfo

	numRows := len(connInfo)
	if numRows == 0 {
		return nil, errors.New("bad Shard config")
	}

	mgr := &ShardManager{
		AlgVersion: 1,
		connInfo:   connInfo,
		ssconfig:   conf,
		processors: make([][]*OutboundSSProcessor, numRows),
	}

	mgr.AlgVersion = ccfg.AlgVersion
	mgr.shardMap.Populate(ccfg)
	if statscfg != nil && (statscfg.TimeoutStatsEnabled || statscfg.RespTimeStatsEnabled) {
		mgr.stats = NewClusterStats(ccfg.NumZones, uint32(ccfg.GetMaxNumHostsPerZone()), statscfg)
	}
	mgr.init(curMgr)

	glog.Infof("shard manager initialized (#Zones: %d, #ConnPerSS: %d)",
		numRows, conf.NumConnsPerTarget)
	return mgr, nil
}

func InitShardMgr(ccfg *Cluster, conf *io.OutboundConfig, statscfg *StatsConfig) (err error) {
	if ccfg == nil {
		err = fmt.Errorf("nil cluster config")
		glog.Error(err)
		return
	}

	if err = ccfg.Validate(); err != nil {
		glog.Error(err)
		return
	}

	//ccfg.Dump(true)

	var shardMgr *ShardManager
	shardMgr, err = newShardManager(ccfg, conf, statscfg, nil)
	if err != nil {
		err = fmt.Errorf("Cannot initialize Shard Manager %v", err)
		glog.Error(err)
		return
	}

	shardMgrPair[0] = shardMgr
	return
}

func watchAndResetShardMgr(conf *io.OutboundConfig, chWatch chan int, statscfg *StatsConfig) {
	for {
		select {
		case val, ok := <-chWatch:
			if !ok {
				return
			}
			glog.Infof("etcd: new version=%d", val)
			if Version < uint32(val) {
				ret := initStandbyShardMgr(conf, uint32(val), statscfg)
				if ret {
					Version = uint32(val)
				}
			}
			glog.Infof("shard mgr updated with new version=%d", val)
		}
	}
}

func initStandbyShardMgr(conf *io.OutboundConfig, val uint32, statscfg *StatsConfig) bool {

	glog.Infof("init standby shardmgr")
	var next int32 = (shardMgrIndex + 1) % 2
	c := ClusterInfo[next]
	_, err := c.ReadWithRetry(etcdReader, cacheFile, val)
	if err != nil {
		glog.Exitf("etcd error reading cluster info")
	}

	curMgr := GetShardMgr()

	standbyMgr, err := newShardManager(&c, conf, statscfg, curMgr)
	if err != nil {
		glog.Fatalf("Cannot initialize standby Shard Manager %v", err)
		return false
	}

	// give some time for all connection to be established
	waitForSSConnsToComplete(standbyMgr, time.Duration(1*time.Second), time.Duration(20*time.Second))

	// Make it active.
	shardMgrPair[next] = standbyMgr
	atomic.StoreInt32(&shardMgrIndex, next)

	// Shutdown the old one
	shardMgrPair[(next+1)%2].Shutdown(standbyMgr)
	return true
}

// Return shardmgr that is active.
func GetShardMgr() *ShardManager {
	var ix int32 = atomic.LoadInt32(&shardMgrIndex)
	return shardMgrPair[ix]
}

func (p *ShardManager) init(curMgr *ShardManager) {

	// Add connections to the downstream storage servers
	for i := 0; i < int(p.shardMap.cluster.NumZones); i++ {
		limit := len(p.connInfo[i])

		p.processors[i] = make([]*OutboundSSProcessor, limit /*nodes*/)

		glog.Debugf("Add %d connections to ss", limit)

		for j := 0; j < limit; j++ {
			if curMgr != nil {
				ss := curMgr.GetSSProcessor(i, j)
				if ss != nil && ss.GetConnInfo() == p.connInfo[i][j] {
					glog.Debugf("reuse existing connection to ss (zone=%d, node=%d, ip=%s)", i, j, p.connInfo[i][j])
					p.processors[i][j] = ss
					continue
				}
			}
			glog.Debugf("creating connection to ss (zone=%d, node=%d, ip=%s)", i, j, p.connInfo[i][j])
			p.processors[i][j] = p.newAndStartSSProcessor(i, j, true)
		}
	}

	if p.stats != nil {
		p.stats.Run()
	}
}

func (p *ShardManager) newAndStartSSProcessor(zoneId int, indexInZone int, enableBounce bool) *OutboundSSProcessor {
	proc := &OutboundSSProcessor{zoneId: zoneId, indexInZone: indexInZone}
	proc.Init(io.ServiceEndpoint{Addr: p.connInfo[zoneId][indexInZone]}, p.ssconfig, enableBounce)
	proc.SetConnEventHandler(proc)
	proc.Start()
	return proc
}

func (p *ShardManager) StatsEnabled() bool {
	return p.stats != nil
}

func (p *ShardManager) SendStats(zoneid int, hostid int, timeout bool, proctime int64) {
	if p.stats == nil {
		return
	}

	ps := &ProcStat{
		zoneid:   uint32(zoneid),
		nodeid:   uint32(hostid),
		timeout:  timeout,
		procTime: int32(proctime),
	}
	p.stats.SendNodeProcState(ps)
}

func (p *ShardManager) GetSSProcessor(zoneId int, nodeId int) *OutboundSSProcessor {
	// check boundary
	if zoneId >= int(p.shardMap.cluster.NumZones) || nodeId >= int(len(p.connInfo[zoneId])) {
		return nil
	}
	return p.processors[zoneId][nodeId]
}

//obsoleted
//func (p *ShardManager) GetRequestChsByKey(key []byte) ([]chan io.IRequestContext, error) {
//	partId := util.GetPartitionId(key, uint32(p.shardMap.cluster.NumShards))
//	return p.GetRequestChs(shard.ID(partId))
//}
//
//func (p *ShardManager) GetRequestChs(partId shard.ID) ([]chan io.IRequestContext, error) {
//
//	zones, nodes, err := p.shardMap.GetNodes(uint32(partId))
//	if err != nil {
//		return nil, err
//	}
//
//	reqChs := make([]chan io.IRequestContext, len(zones))
//	for i := range zones {
//		s := p.processors[int(zones[i])][nodes[i]]
//		reqChs[i] = s.GetRequestCh()
//	}
//
//	return reqChs, nil
//}

//used by request processor
//the caller's responsibility to make sure
// cap(procs) >= numZones and cap(pos) >= numZones
func (p *ShardManager) GetSSProcessors(key []byte, confNumWrites int, procs []*OutboundSSProcessor, pos []int) (shardId shard.ID, numProcs int) {

	shardid, start_zoneid := util.GetShardInfoByKey(key, uint32(p.shardMap.cluster.NumShards), uint32(p.shardMap.cluster.NumZones), p.AlgVersion)
	shardId = shard.ID(shardid)
	zones, nodes, err := p.shardMap.GetNodes(uint32(shardid), start_zoneid)

	if err != nil {
		return
	}

	zonemarkdown, zoneid := markdownobj.CheckMarkDown()
	if zonemarkdown {
		// if available processors can not form a quorum, we'll disable the markdown
		upcnt := 0
		for i := range zones {
			s := p.processors[int(zones[i])][nodes[i]]
			if (s.GetIsConnected() != 0) && (p.stats == nil || !p.stats.IsMarkeddown(zones[i], nodes[i])) {
				upcnt++
			}
		}
		if upcnt <= confNumWrites {
			zonemarkdown = false
		}
	}

	// soft-markdown list
	markdown_cnt := 0
	markown_list := make([]int, len(zones))
	for i := range zones {

		// skip the zone that is marked down via ETCD
		if zonemarkdown && int32(zones[i]) == zoneid {
			continue
		}

		// handle save soft markdown
		if p.stats != nil && p.stats.IsMarkeddown(zones[i], nodes[i]) {
			markown_list[markdown_cnt] = i
			markdown_cnt++
			continue
		}

		s := p.processors[int(zones[i])][nodes[i]]
		if s.GetIsConnected() != 0 {
			procs[numProcs] = s
			pos[numProcs] = i
			numProcs++
		}
	}

	// append soft markdown list at the end
	for i := 0; i < markdown_cnt; i++ {
		idx := markown_list[i]
		s := p.processors[int(zones[idx])][nodes[idx]]
		if s.GetIsConnected() != 0 {
			procs[numProcs] = s
			pos[numProcs] = idx
			numProcs++
		}
	}
	return
}

// Used by admin worker
func (p *ShardManager) GetProcessorsByKey(key []byte) (shardId shard.ID, procs []*OutboundSSProcessor, err error) {
	shardId = shard.ID(util.GetPartitionId(key, uint32(p.shardMap.cluster.NumShards)))
	procs, err = p.GetProcessors(shardId)
	return
}

// Used by admin worker
func (p *ShardManager) GetProcessors(partId shard.ID) ([]*OutboundSSProcessor, error) {

	// for testing connectivity, order does not matter, so always use 1 for now
	zones, nodes, err := p.shardMap.GetNodes(uint32(partId), 1)
	if err != nil {
		return nil, err
	}

	//glog.Verbosef("partid=%d, zones=%v, nodes=%v", partId, zones, nodes)

	procs := make([]*OutboundSSProcessor, len(zones))
	for i := range zones {
		s := p.processors[int(zones[i])][nodes[i]]
		procs[i] = s
	}

	return procs, nil
}

func (p *ShardManager) GetShardInfoByKey(key []byte) ([]uint32, []uint32, error) {
	shardid, start_zoneid := util.GetShardInfoByKey(key, uint32(p.shardMap.cluster.NumShards),
		uint32(p.shardMap.cluster.NumZones), p.AlgVersion)
	return p.shardMap.GetNodes(uint32(shardid), start_zoneid)
}

func (p *ShardManager) GetShardMap() *ShardMap {
	return &p.shardMap
}

func (p *ShardManager) DumpShardMap() {
	p.shardMap.Dump()
}

func (p *ShardManager) GetConnectivity() (connState [][]int) {
	// both should not be 0
	numRows := len(p.connInfo)
	connState = make([][]int, numRows)

	for x := range connState {
		connState[x] = make([]int, len(p.connInfo[x]))
	}

	for i := 0; i < numRows; i++ {
		for j := 0; j < len(p.connInfo[i]); j++ {
			if p.processors[i][j] == nil {
				connState[i][j] = 0
			} else {
				connState[i][j] = p.processors[i][j].GetIsConnected()
			}
		}
	}

	return
}

func (p *ShardManager) GetBadShardHosts(level uint32) (hosts string) {
	localHosts := make(map[string]int)
	m := p.GetShardMap()
	numWrites := (m.cluster.NumZones + 1) / 2
	numWrites += level

	for sid := uint32(0); sid < m.cluster.NumShards; sid++ {
		badHosts := make(map[string]int)
		for zid := uint32(0); zid < m.cluster.NumZones; zid++ {
			node := m.shards[sid][zid]
			proc := p.processors[zid][node.nodeid]
			if proc.GetIsConnected() == 0 {
				name, _ := getHostName(p, m, int(sid), int(zid))
				name = fmt.Sprintf("%d:%s", zid, name)
				badHosts[name] = 0
			}
		}

		if (m.cluster.NumZones - uint32(len(badHosts))) < numWrites {
			for key := range badHosts {
				localHosts[key] = 0
			}
		}
	}
	hosts = ""
	if len(localHosts) != 0 {
		for key := range localHosts {
			hosts += key + " "
		}
		hosts = strings.Trim(hosts, " ")
	}
	return
}

func (p *ShardManager) IsConnected(zone int, zoneIndex int) bool {
	numZones := len(p.processors)
	if numZones == 0 {
		return false
	}
	if zone >= numZones {
		return false
	}
	if zoneIndex >= len(p.processors[zone]) {
		return false
	}
	return p.processors[zone][zoneIndex].GetIsConnected() == 1
}

func (p *ShardManager) IsConnectivityOk() bool {

	numZones := len(p.processors)

	if numZones == 0 {
		return false
	}
	numWrites := (numZones + 1) / 2

	cluster := p.shardMap.cluster
	if cluster == nil {
		glog.Fatal("nil Cluster")
	}
	numShards := cluster.NumShards

	var i uint32

	for i = 0; i < numShards; i++ {
		numOk := 0
		procs, err := p.GetProcessors(shard.ID(i))
		if err == nil {
			for _, proc := range procs {
				if (proc != nil) && (proc.GetIsConnected() == 1) {
					numOk++
				} else {
					glog.Debugf("Not connected to %s\n", proc.GetConnInfo())
				}

			}
		}
		if numOk < numWrites {
			return false
		}
	}
	return true
}

// this function is a little costly, don't call it frequently !!!
func (p *ShardManager) GetSSConnectivityStats() (numOkShards uint32, numBadShards uint32, numWarnShards uint32, numAlertShards uint32) {
	numOkShards = 0
	numBadShards = 0
	numWarnShards = 0
	numAlertShards = 0

	numZones := len(p.processors)
	if numZones == 0 {
		return
	}
	numWrites := (numZones + 1) / 2

	cluster := p.shardMap.cluster
	if cluster == nil {
		glog.Fatal("nil Cluster")
	}
	numShards := cluster.NumShards

	var i uint32
	for i = 0; i < numShards; i++ {
		numOk := 0
		procs, err := p.GetProcessors(shard.ID(i))
		if err == nil {
			for _, proc := range procs {
				if (proc != nil) && (proc.GetIsConnected() == 1) {
					numOk++
				}
			}
		}

		if numOk == numZones {
			numOkShards++
		} else if numOk < numWrites {
			numBadShards++
		} else if numOk == numWrites {
			numAlertShards++
		} else {
			numWarnShards++
		}
	}

	return
}

func (p *ShardManager) Shutdown(curMgr *ShardManager) {
	numRows := len(p.connInfo)

	for i := 0; i < numRows; i++ {
		for j := 0; j < len(p.connInfo[i]); j++ {
			if p.processors[i][j] != nil {
				if curMgr != nil {
					ss := curMgr.GetSSProcessor(i, j)
					if ss != nil && ss.GetConnInfo() == p.processors[i][j].GetConnInfo() {
						glog.Debugf("keep connection to ss (zone=%d, node=%d, ip=%s)", i, j, p.connInfo[i][j])
						p.processors[i][j] = nil
						continue
					}
				}
				glog.Debugf("close connection to ss (zone=%d, node=%d, ip=%s)", i, j, p.connInfo[i][j])
				p.processors[i][j].Shutdown()
			}
		}
	}

	for i := 0; i < numRows; i++ {
		for j := 0; j < len(p.connInfo[i]); j++ {
			if p.processors[i][j] != nil {
				p.processors[i][j].WaitShutdown()
				p.processors[i][j] = nil
			}
		}
	}

	if p.stats != nil {
		p.stats.Quit()
	}
	glog.Infof("closed unused outbound connections")
}

const (
	kCellOk      = "<td>"
	kCellWarning = "<td style=\"background-color:#F29837\">"
	kCellError   = "<td style=\"background-color:#E32F37\">"
)

func (p *ShardManager) WriteProcessorsStats(w goio.Writer) {
	type ssHostInfoT struct {
		addr         string
		numInstances int
		numConnected int
	}
	type zoneT struct {
		numSSInstances int
		numSSHosts     int
		sshosts        []*ssHostInfoT
	}

	fmt.Fprint(w, `<div id="id-ss-info"><table title="ss-info">`)
	numZones := len(p.processors)

	//	numHostsPerZone := make([]int, numZones, numZones)
	zoneInfo := make([]zoneT, numZones, numZones)

	numRows := 0
	fmt.Fprint(w, "<tr>")
	for i := 0; i < numZones; i++ {
		addrMap := make(map[string]*ssHostInfoT)
		num := len(p.processors[i])
		zoneInfo[i].numSSInstances = num
		for j := 0; j < num; j++ {
			processor := p.processors[i][j]
			if host, _, err := net.SplitHostPort(processor.GetConnInfo()); err == nil {
				var info *ssHostInfoT
				var ok bool
				if info, ok = addrMap[host]; !ok {
					info = &ssHostInfoT{addr: host}
					addrMap[host] = info
					zoneInfo[i].numSSHosts++
					zoneInfo[i].sshosts = append(zoneInfo[i].sshosts, info)
				}
				info.numInstances++
				info.numConnected += processor.GetIsConnected()
			}
		}

		if zoneInfo[i].numSSHosts > numRows {
			numRows = zoneInfo[i].numSSHosts
		}
		fmt.Fprintf(w, "<th>Zone %d</th>", i)
	}
	fmt.Fprintf(w, "<th></th></tr>")
	for r := 0; r < numRows; r++ {
		fmt.Fprintf(w, "<tr>")
		for i := 0; i < numZones; i++ {
			if r < zoneInfo[i].numSSHosts {
				ssHost := zoneInfo[i].sshosts[r]
				if ssHost.numConnected == 0 {
					fmt.Fprint(w, kCellError)
				} else if ssHost.numConnected == ssHost.numInstances {
					fmt.Fprint(w, kCellOk)
				} else {
					fmt.Fprint(w, kCellWarning)
				}
				fmt.Fprintf(w, ssHost.addr)
			} else {
				fmt.Fprint(w, "<td>")
			}
			fmt.Fprint(w, "</td>")
		}
		fmt.Fprintf(w, "<td>%d</td></tr>", r)
	}
	fmt.Fprintf(w, "</table>")
}

func (p *ShardManager) WriteProcessorsStatsByShards(w goio.Writer) {
	fmt.Fprint(w, `<div id="shard-map-info"><table title="shard-map-info">`)
	m := p.GetShardMap()

	if m != nil && m.cluster != nil {
		numZones := int(m.cluster.Config.NumZones)
		numShards := int(m.cluster.Config.NumShards)
		numWrites := (numZones + 1) / 2

		fmt.Fprintf(w, "<tr><th>Shard ID</th>")
		for zid := 0; zid < numZones; zid++ {
			fmt.Fprintf(w, "<th>Zone%d</th>", zid)
		}
		fmt.Fprintf(w, "<th>Status</th></tr>\n")
		for sid := 0; sid < numShards; sid++ {
			fmt.Fprintf(w, "<tr><td>%d</td>", sid)
			numOK := 0
			for zid := 0; zid < numZones; zid++ {
				node := m.shards[sid][zid]
				p := p.processors[zid][node.nodeid]
				if p.GetIsConnected() != 0 {
					numOK++
					fmt.Fprintf(w, kCellOk)
				} else {
					fmt.Fprintf(w, kCellWarning)
				}
				if node.isPrimary {
					fmt.Fprintf(w, "<strong>%s</strong></td>", p.GetConnInfo())
				} else {
					fmt.Fprintf(w, "%s</td>", p.GetConnInfo())
				}
			}
			if numOK == numZones {
				fmt.Fprintf(w, "<td>&#9989;</td>")
			} else if numOK < numWrites {
				fmt.Fprintf(w, "<td>&#10060;</td>")
			} else {
				fmt.Fprintf(w, "<td>&#10004;</td>")
			}
			fmt.Fprintf(w, "</tr>\n")
		}
		fmt.Fprintf(w, "</table>")
	}
}

func (p *OutboundSSProcessor) Name() string {
	return fmt.Sprintf("%d-%d", p.zoneId, p.indexInZone)
}

func (p *OutboundSSProcessor) GetNodeInfo() (zoneid int, hostid int) {
	return p.zoneId, p.indexInZone
}

func (p *OutboundSSProcessor) OnConnectSuccess(conn io.Conn, connector *io.OutboundConnector, timeTaken time.Duration) {
	if cal.IsEnabled() {
		b := logging.NewKVBuffer()
		netConn := conn.GetNetConn()

		b.Add([]byte("raddr"), netConn.RemoteAddr().String())
		b.Add([]byte("laddr"), netConn.LocalAddr().String())
		if conn.IsTLS() {
			b.Add([]byte("ssl"), conn.GetStateString())
		}

		cal.AtomicTransaction(logging.CalMsgTypeSSConnect, p.Name(), cal.StatusSuccess, timeTaken, b.Bytes())
	}
	if otel.IsEnabled() {
		netConn := conn.GetNetConn()
		otel.RecordSSConnection(netConn.RemoteAddr().String(), otel.StatusSuccess, timeTaken.Milliseconds())
	}
}

func (p *OutboundSSProcessor) OnConnectError(timeTaken time.Duration, connStr string, err error) {
	if cal.IsEnabled() {
		b := logging.NewKVBuffer()
		b.Add([]byte("raddr"), connStr)
		b.Add([]byte("err"), err.Error())
		cal.AtomicTransaction(logging.CalMsgTypeSSConnectError, p.Name(), cal.StatusSuccess, timeTaken, b.Bytes())
	}
	if otel.IsEnabled() {
		otel.RecordSSConnection(connStr, otel.StatusError, timeTaken.Milliseconds())
	}
}

func (m *ZoneMarkDown) MarkDown(zoneid int32) {
	atomic.StoreInt32(&m.markdownid, zoneid)
}

func (m *ZoneMarkDown) Reset() {
	atomic.StoreInt32(&m.markdownid, -1)
}

func (m *ZoneMarkDown) CheckMarkDown() (markdown bool, zoneid int32) {
	markdown = false
	zoneid = atomic.LoadInt32(&m.markdownid)
	if zoneid != -1 {
		markdown = true
	}
	return
}
