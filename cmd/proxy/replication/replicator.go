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

package replication

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"juno/third_party/forked/golang/glog"

	repconfig "juno/cmd/proxy/replication/config"
	proxystats "juno/cmd/proxy/stats"
	"juno/cmd/proxy/stats/shmstats"
	"juno/pkg/io"
	"juno/pkg/logging"
	"juno/pkg/logging/cal"
	"juno/pkg/proto"
	"juno/pkg/util"
)

var (
	// The singleton
	TheReplicator *Replicator
	initOnce      sync.Once
	enabled       bool = false
)

type (
	ReplicattionHandshaker struct {
		ping      io.IRequestContext
		pingIP    string
		byPassLTM bool
	}

	Replicator struct {
		conf       *repconfig.Config
		processors []*replicationProcessorT
	}
	repReqCtxCreatorI interface {
		newRequestContext(recExpirationTime uint32, msg *proto.RawMessage, reqCh chan io.IRequestContext,
			dropCnt *util.AtomicShareCounter, errCnt *util.AtomicShareCounter) io.IRequestContext
		newKeepAliveRequestContext() io.IRequestContext
	}

	replicationProcessorT struct {
		io.OutboundProcessor
		reqCtxCreator repReqCtxCreatorI
		specNsMap     map[string]bool
		byPassLTM     bool
	}
)

func (h *ReplicattionHandshaker) GetHandshakeTimeout() util.Duration {
	return util.Duration{1000 * time.Millisecond}
}

func (h *ReplicattionHandshaker) GetPingRequest() io.IRequestContext {
	return h.ping
}

func (h *ReplicattionHandshaker) OnPingResponse(resp io.IResponseContext) bool {
	if resp.GetStatus() != proto.StatusOk {
		return false
	}
	opstatus, _ := proto.GetOpStatus(resp.GetMessage())
	if opstatus != proto.OpStatusNoError {
		return false
	}

	if h.byPassLTM {
		var opResp proto.OperationalMessage
		opResp.Decode(resp.GetMessage())
		//opResp.PrettyPrint(os.Stderr)
		if opResp.GetSrcIP() != nil {
			h.pingIP = opResp.GetSrcIP().String()
		}
	}

	h.GetPingRequest().OnComplete()
	resp.OnComplete()
	return true
}

func (h *ReplicattionHandshaker) ExpectResponse() bool {
	return true
}

func (h *ReplicattionHandshaker) GetName() string {
	return "rep"
}

func (h *ReplicattionHandshaker) GetPingIP() string {
	return h.pingIP
}

func Enabled() bool {
	return enabled
}

func Initialize(args ...interface{}) (err error) {
	sz := len(args)
	if sz == 0 {
		err = fmt.Errorf("replication config expected")
		glog.Error(err)
		return
	}
	conf, ok := args[0].(*repconfig.Config)
	if !ok {
		err = fmt.Errorf("wrong argument type")
		glog.Error(err)
		return
	}
	err = Init(conf)
	return
}

func Finalize() {
	if enabled && TheReplicator != nil {
		TheReplicator.Shutdown()
	}
}

func Init(conf *repconfig.Config) (err error) {
	initOnce.Do(func() {
		if len(conf.Targets) == 0 {
			enabled = false
			glog.Info("replication disabled")
			return
		} else {
			enabled = true
		}
		TheReplicator, err = newReplicator(conf)
		if err != nil {
			glog.Errorf("Cannot initialize replication Manager: %s", err)
			return
		}
		proxystats.SetRepStatsCallBack(&repStateCBImpl{})
	})
	return
}

func newReplicator(conf *repconfig.Config) (r *Replicator, err error) {
	num := len(conf.Targets)
	if num == 0 {
		return nil, errors.New("bad replication config")
	}

	r = &Replicator{
		conf:       conf,
		processors: make([]*replicationProcessorT, num),
	}

	for i, target := range conf.Targets {
		r.processors[i] = newReplicationProcessor(&target, conf.GetIoConfig(&target))
	}

	return r, nil
}

func (r *Replicator) SendRequest(opMsg *proto.OperationalMessage) { //expirationTime uint32, msg *proto.RawMessage) {
	var msg proto.RawMessage
	opMsg.Encode(&msg)

	mgr := shmstats.GetCurrentWorkerStatsManager()
	expirationTime := opMsg.GetExpirationTime()

	for i, processor := range r.processors {
		dropCnt := mgr.GetReplicatorDropCounter(i)
		errCnt := mgr.GetReplicatorErrorCounter(i)
		if processor.IsReplicable(opMsg) {
			// deep copy for each replication destination
			processor.replicate(expirationTime, &msg, dropCnt, errCnt)
		}
	}
	msg.ReleaseBuffer()
}

func (r *Replicator) Shutdown() {
	for _, processor := range r.processors {
		processor.Shutdown()
	}

	for _, processor := range r.processors {
		processor.WaitShutdown()
	}
}

func newReplicationProcessor(target *repconfig.ReplicationTarget, iocfg *io.OutboundConfig) *replicationProcessorT {
	var reqCtxCreator repReqCtxCreatorI
	if target.UseMayflyProtocol {
		var ipUint32 uint32
		var port uint16

		if host, portstr, err := net.SplitHostPort(target.ServiceEndpoint.Addr); err == nil {
			if ips, e := net.LookupIP(host); e == nil {
				for _, addr := range ips {
					if ip := addr.To4(); ip != nil {
						ipUint32 = binary.BigEndian.Uint32(ip)
						break
					}
				}
			}
			if p, e1 := strconv.Atoi(portstr); e1 == nil {
				port = uint16(p)
			}
		}
		if ipUint32 != 0 && port != 0 {
			reqCtxCreator = &mayflyRepReqCreatorT{targetId: target.Name, ip: ipUint32, port: port}
		} else {
			glog.Error("invalid ip and/or port")
		}
	} else {
		reqCtxCreator = &repReqCreatorT{targetId: target.Name}
	}

	var nsMap map[string]bool
	if len(target.Namespaces) != 0 {
		nsMap = make(map[string]bool)
		for _, ns := range target.Namespaces {
			nsMap[ns] = true
		}
	}

	p := &replicationProcessorT{
		reqCtxCreator: reqCtxCreator,
		specNsMap:     nsMap,
	}
	p.Init(target.ServiceEndpoint, iocfg, false)
	p.SetConnEventHandler(p)
	p.byPassLTM = target.BypassLTMEnabled
	p.Start()

	return p
}

func (r *replicationProcessorT) IsReplicable(opMsg *proto.OperationalMessage) bool {

	if len(r.specNsMap) != 0 {
		if _, ok := r.specNsMap[string(opMsg.GetNamespace())]; ok {
			return true
		}
		return false
	}
	return true
}

func (r *replicationProcessorT) replicate(recExpirationTime uint32, msg *proto.RawMessage,
	dropCnt *util.AtomicShareCounter, errCnt *util.AtomicShareCounter) {
	req := r.reqCtxCreator.newRequestContext(recExpirationTime, msg, r.GetRequestCh(), dropCnt, errCnt)
	glog.Verbosef("send replication request")

	if err := r.SendRequest(req); err != nil {
		glog.Infof("target %s, drop the req", err.Error())
		if cal.IsEnabled() {
			var request proto.OperationalMessage
			request.Decode(msg)
			buf := logging.NewKVBuffer()
			buf.AddOpRequest(&request)
			//TODO to revisit
			cal.Event("RR_Drop_QueueFull",
				request.GetOpCodeText(),
				cal.StatusWarning,
				buf.Bytes())
		}
		dropCnt.Add(1)
		req.OnComplete()
	}
}

func (r *replicationProcessorT) OnConnectSuccess(conn io.Conn, connector *io.OutboundConnector, timeTaken time.Duration) {
	r.OutboundProcessor.OnConnectSuccess(conn, connector, timeTaken)
	if connector != nil {
		ping := r.reqCtxCreator.newKeepAliveRequestContext()
		shaker := &ReplicattionHandshaker{ping: ping, byPassLTM: r.byPassLTM}
		connector.SetHandshaker(shaker)
	}
}

func (r *replicationProcessorT) OnConnectError(timeTaken time.Duration, connStr string, err error) {
	r.OutboundProcessor.OnConnectError(timeTaken, connStr, err)
}

//TODO tempory
func (r *Replicator) GetProcessors() []*replicationProcessorT {
	return r.processors
}

type repStateCBImpl struct {
}

func (cb *repStateCBImpl) Call() {
	mgr := shmstats.GetCurrentWorkerStatsManager()

	if TheReplicator != nil {
		repProcs := TheReplicator.GetProcessors()
		for i, proc := range repProcs {
			mgr.SetReplicatorStats(i, uint16(proc.GetNumConnections()), uint16(len(proc.GetRequestCh())))
		}
	}
}
