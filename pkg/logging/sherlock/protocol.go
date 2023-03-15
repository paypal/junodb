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

package sherlock

import (
	"strings"
	"time"

	"juno/third_party/forked/golang/glog"

	proto "github.com/golang/protobuf/proto"

	frontier "juno/pkg/logging/sherlock/sherlockproto"
)

var frontierTimeout = 30 * time.Second

// Gauge is a metric that is like a speedometer - up and down independently
var Gauge = frontier.MetricProto_GAUGE

// Counter is a metric that is like a odometer - always increasing till new car
var Counter = frontier.MetricProto_COUNTER

// CounterReplaceWithGauge is a metric that is like a odometer - but converted to velocity
var CounterReplaceWithGauge = frontier.MetricProto_COUNTER_REPLACE_WITH_GAUGE

// CounterCreateGauge is a metric that is like a odometer - and also converted to velocity
var CounterCreateGauge = frontier.MetricProto_COUNTER_CREATE_GAUGE

func (m *FrontierClient) doRedirect(s *frontier.RedirectSessionProto) {
	// called by something that has the mutex
	m.host = s.GetSocketAddress().GetAddress()
	if !strings.Contains(m.host, ".") {
		m.host = m.host + ".qa.paypal.com"
	}
	m.port = s.GetSocketAddress().GetPort()
	glog.Infof("About to Connect %s %d\n", m.host, m.port)
	err := m.connectWs()
	if err != nil {
		glog.Errorln("Got an error connecting ", err)
		time.Sleep(time.Second * 5)
		return
	}
	glog.Infof("Connected to host : %s port : %v", m.host, m.port)
	gs := m.generateSessionRequest()
	err = m.wsConn.writeMsg(m.clientID, gs)
	if err != nil {
		glog.Errorln("Got an error connecting ", err)
		return
	}
}

func (m *FrontierClient) handle(msg *frontier.ServerMessageProto) error {
	if msg.GetType() == frontier.ServerMessageProto_NOTIFICATION {
		glog.Infoln("Got a notification", msg)
		return nil
	}
	if msg.GetType() == frontier.ServerMessageProto_RESPONSE {
		serverResp := msg.GetServerResponse()
		responseType := serverResp.GetType()
		status := serverResp.GetStatus()
		if status != frontier.ServerResponseProto_SUCCESS {
			glog.Info("Non-success status", msg)
			return newRejectedError(serverResp.GetErrMsg())
		}
		switch responseType {
		case frontier.ReqRespTypeProto_GET_SESSION:
			sessResp := serverResp.GetSessionResponse
			sessStatus := sessResp.GetStatus()
			if sessStatus == frontier.GetSessionRespProto_SUCCESS {
				m.setSession(sessResp.GetSessionInfo().GetId())
				glog.Info("Connection up and ready for sending metrics.")
				m.gotConnect <- true
				return nil
			}
			if sessStatus == frontier.GetSessionRespProto_REDIRECT {
				m.lock()
				m.doRedirect(sessResp.GetRedirectSession())
				m.unlock()
				return nil
			}
		case frontier.ReqRespTypeProto_POST_UNREGISTERED:
			f := m.getAndClearCb(serverResp.GetId())
			if serverResp.GetStatus() == frontier.ServerResponseProto_SUCCESS {
				if f != nil {
					f(nil)
				}
				//calEvent("FRONTIER", "POST_OK", nil)
				//glog.Info("Frontier response ok")
				return nil
			}
			if f != nil {
				e := newRejectedError(serverResp.GetErrMsg())
				if f != nil {
					f(e)
				}
			}
			//calEvent("FRONTIER", "POST_FAILURE", nil)
			glog.Error("Frontier post failure ")
			if strings.Contains(serverResp.GetErrMsg(), "nvalid") {
				m.Restart()
				//m.doConnect <- true
				return newRejectedError(serverResp.GetErrMsg())
			}
			if strings.Contains(serverResp.GetErrMsg(), "Timed out while") {
				m.Restart()
				//m.doConnect <- true
				return newRejectedError(serverResp.GetErrMsg())
			}
		case frontier.ReqRespTypeProto_REGISTER:
		case frontier.ReqRespTypeProto_POST_REGISTERED:
		default:
			//calEvent("FRONTIER", "UNKNOWN_TYPE", nil)
			glog.Error("Frontier response Unknown Type ")
		}

	}
	return nil
}

func (m *FrontierClient) resetCb(msg proto.Message) {
	t, ok := msg.(*frontier.ClientRequestProto)
	if ok {
		id := t.GetId()
		m.getAndClearCb(id)
	}
}

func (m *FrontierClient) metricToMsg(metric frontierMessage) proto.Message {
	cr := &frontier.ClientRequestProto{}
	id := m.id
	colo := m.colo
	m.setCb(metric.cb, id)
	select {
	case m.timeOuts <- timeout{id, time.Now().Add(frontierTimeout)}:
	default:
		glog.Info("Error saving cb data") // oh well
	}
	cr.Id = &id
	t := frontier.ReqRespTypeProto_POST_UNREGISTERED
	cr.Type = &t
	m.lock()
	m.id++
	m.unlock()
	msg := frontier.PostUnregisteredReqProto{}
	cr.PostUnregisteredRequest = &msg
	msg.SessionId = m.getSession()
	timeT := uint32(metric.when.Unix())
	msg.EpochSecond = &timeT
	ack := frontier.PostConcernProto_ACKNOWLEDGED
	msg.PostConcern = &ack
	metricSet := &frontier.MetricSetProto{}
	metricSet.ResolutionSeconds = &metric.resolution
	metricSet.Profile = &m.profile
	// some default dims for EVPS
	if _, ok := metric.dim["environment"]; !ok {
		dimName := "environment"
		dimValue := "P"
		dim := frontier.PairProto{&dimName, &dimValue, nil}
		metricSet.Dimension = append(metricSet.Dimension, &dim)
	}
	if _, ok := metric.dim["host"]; !ok {
		dimName := "host"
		dimValue := hostName
		dim := frontier.PairProto{&dimName, &dimValue, nil}
		metricSet.Dimension = append(metricSet.Dimension, &dim)
	}
	if _, ok := metric.dim["colo"]; !ok {
		dimName := "colo"
		dimValue := colo
		dim := frontier.PairProto{&dimName, &dimValue, nil}
		metricSet.Dimension = append(metricSet.Dimension, &dim)
	}

	for k, v := range metric.dim {
		dimName := k
		dimValue := v
		dim := frontier.PairProto{&dimName, &dimValue, nil}
		metricSet.Dimension = append(metricSet.Dimension, &dim)
	}
	for _, d := range metric.data {
		name := d.Name
		value := d.Value
		metricType := d.MetricType
		valueType := frontier.MetricProto_DOUBLE
		var metricValue frontier.MetricValueProto

		switch metricType {
		case frontier.MetricProto_COUNTER_REPLACE_WITH_GAUGE:
			valueType = frontier.MetricProto_LONG
			intvalue := int64(value)
			metricValue = frontier.MetricValueProto{nil, &intvalue, nil, nil, nil}
		case frontier.MetricProto_COUNTER:
			valueType = frontier.MetricProto_LONG
			intvalue := int64(value)
			metricValue = frontier.MetricValueProto{nil, &intvalue, nil, nil, nil}
		case frontier.MetricProto_COUNTER_CREATE_GAUGE:
			valueType = frontier.MetricProto_LONG
			intvalue := int64(value)
			metricValue = frontier.MetricValueProto{nil, &intvalue, nil, nil, nil}
		case frontier.MetricProto_GAUGE:
			valueType = frontier.MetricProto_DOUBLE
			metricValue = frontier.MetricValueProto{nil, nil, nil, &value, nil}
		default:
			valueType = frontier.MetricProto_DOUBLE
			metricValue = frontier.MetricValueProto{nil, nil, nil, &value, nil}

		}
		data := frontier.MetricProto{&name,
			&metricType,
			&valueType,
			&metricValue,
			nil}
		metricSet.Metric = append(metricSet.Metric, &data)
	}
	msg.MetricSet = metricSet
	return cr
}
