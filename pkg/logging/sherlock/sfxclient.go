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
package sherlock

import (
	"context"
	"errors"
	"math/rand"
	"time"

	frontier "juno/pkg/logging/sherlock/sherlockproto"
	"juno/third_party/forked/golang/glog"

	"github.com/signalfx/golib/v3/datapoint"
	"github.com/signalfx/golib/v3/sfxclient"
)

type internalSfxClient struct {
	metrics chan sfxMessage
	// Create the retryMetrics to
	// 1. isolate SendMetric from retry logic.
	// 2. the real failure of sending metric is in implementation of specific
	//    targets. So it'd better handle in implementation side
	// 3. If retry in metrics, it will block it right away.
	retryMetrics chan retrySfxMessage
	// The number of time to retry a metrics if send failed
	retryCount uint32
	finished   chan bool
	// Channel to send rmCount below, has to be length of 1
	ignoreRetryCount chan uint32
	// If send failed, a metric push into retry channel also
	// failed, how many metrics we will remove from retry channel
	rmCount     uint32
	clientID    uint32
	client      *sfxclient.HTTPSink
	retryClient *sfxclient.HTTPSink
	// This is the base time to wait for each retry for back-off
	backoff    int64
	maxBackoff time.Duration
	profile    string
}

type sfxMessage struct {
	msgs []*datapoint.Datapoint
	cb   frontierCb
}

type retrySfxMessage struct {
	retryCount uint32
	msgs       []*datapoint.Datapoint
	cb         frontierCb
}

func createSfxClient(datapointEndpoint string, eventEndpoint string, timeout time.Duration) *sfxclient.HTTPSink {
	client := sfxclient.NewHTTPSink()
	// client.DatapointEndpoint = "https://ingest-sfx-observability.us-central1.gcp.dev.paypalinc.com/v2/datapoint"
	// client.EventEndpoint = "https://ingest-sfx-observability.us-central1.gcp.dev.paypalinc.com/v2/event"
	// client.TraceEndpoint = "https://ingest.{REALM}.signalfx.com/v1/trace"

	// Should read from protected package?
	client.DatapointEndpoint = datapointEndpoint
	client.EventEndpoint = eventEndpoint
	client.AuthToken = "9HM5CH5kr1_P7fiQ1HVOww"
	client.AdditionalHeaders = map[string]string{
		"Connection": "keep-alive",
	}
	client.Client.Timeout = timeout
	return client
}

func newInternalSfxClient(conf *Config) (*internalSfxClient, error) {
	clientIDMutex.Lock()
	clientID = clientID + 1
	myInternalSfxClient := &internalSfxClient{
		make(chan sfxMessage, conf.MainWriteQueueSize),
		make(chan retrySfxMessage, conf.RetryWriteQueueSize),
		conf.RetryCount,
		make(chan bool, 1),
		make(chan uint32, 1), // has to be 1
		conf.RmCount,
		clientID,
		createSfxClient(conf.DatapointEndpoint, conf.EventEndpoint, 500*time.Millisecond),
		createSfxClient(conf.DatapointEndpoint, conf.EventEndpoint, 500*time.Millisecond),
		0,
		conf.MaxBackoff,
		conf.SherlockProfile}
	clientIDMutex.Unlock()
	rand.Seed(time.Now().UTC().UnixNano())
	go myInternalSfxClient.mainLoopWrite()
	go myInternalSfxClient.retryLoopWrite()

	return myInternalSfxClient, nil
}

func initSfxWithConfig(conf *Config) {
	if conf != nil {
		ShrLockConfig = conf
		ShrLockConfig.Default()
		ShrLockConfig.Validate()
		if !ShrLockConfig.Enabled {
			return
		}
		var err error
		SherlockClient, err = newInternalSfxClient(conf)
		if err != nil {
			glog.Errorln("failed to make Sherlock client", err)
			//TODO : cal event throw ERROR
		}
	}
}

func (m *internalSfxClient) retryLoopWrite() {
	glog.Debugln("retryLoopWrite")
	ctx := context.Background()
	var localIgnoreCount uint32 = 0
	for {
		select {
		case <-m.finished:
			glog.Infoln("retryLoopWrite Done - exiting")
			return
		case ignoreCount := <-m.ignoreRetryCount:
			glog.Infoln("m.ignoreRetryCount is recieved ", ignoreCount)
			if localIgnoreCount == 0 {
				localIgnoreCount = ignoreCount
			}
		case metric := <-m.retryMetrics:
			glog.Debugln("m.retryMetrics ", metric)
			if localIgnoreCount > 0 {
				localIgnoreCount--
				continue
			}
			// back-off
			select {
			case <-time.After(m.getBackoff()):
			}
			glog.Debugln("m.retryMetrics start processing ", metric)
			metric.retryCount--
			err := m.retryClient.AddDatapoints(ctx, metric.msgs)
			if err != nil {
				m.retryEnRetryQueue(metric)
			} else {
				glog.Debugln("datapoint sent")
				m.resetBackoff()
			}
		case <-time.After(time.Second * 300):
			// Our metrics are 10s
			glog.Debugln("No writes for 5 minutes")
			continue
		}
	}
}

func (m *internalSfxClient) retryEnRetryQueue(retry retrySfxMessage) error {
	glog.Debugln("retryEnretryQueue")
	return m.enRetryQueue(retry, true)
}

func (m *internalSfxClient) resetBackoff() {
	m.backoff = 0
}

func (m *internalSfxClient) getBackoff() time.Duration {
	// here we want random part significant less than exponential part
	// we also do not want exponential grow up too fast
	// and there is no need to make them configurable
	// we also need to count timeout for http.Sink
	b := time.Duration((m.backoff*100 + rand.Int63n(30))) * time.Millisecond
	if m.backoff == 0 {
		m.backoff = 1
	}
	m.backoff = m.backoff << 1

	if b > m.maxBackoff {
		b = m.maxBackoff
	}
	return b
}

func (m *internalSfxClient) mainLoopWrite() {
	glog.Debugln("mainLoopWrite")
	ctx := context.Background()
	for {
		select {
		case <-m.finished:
			glog.Infoln("mainLoopWrite Done - exiting")
			return
		case metric := <-m.metrics:
			glog.Debugln("m.metrics ", metric.msgs[0])
			m.AddMoreDim(metric.msgs)
			err := m.client.AddDatapoints(ctx, metric.msgs)
			if err != nil {
				glog.Errorln("Could not send datapoints")
				m.mainEnRetryQueue(metric)
			} else {
				glog.Debugln("datapoint sent")
			}
		case <-time.After(time.Second * 60):
			// Our metrics are 10s
			glog.Debugln("No writes for 1 minute")
			continue
		}
	}
}

// Do not know why dim host added here
func (m *internalSfxClient) AddMoreDim(msgs []*datapoint.Datapoint) {
	for i := range msgs {
		msgs[i].Dimensions["host"] = hostName
		// Will have environment through sfx-agent
		//msgs[i].Dimensions["environment"] = "p"
	}
}

func (m *internalSfxClient) mainEnRetryQueue(metric sfxMessage) error {
	glog.Debugln("retryEnretryQueue")
	retry := retrySfxMessage{m.retryCount, metric.msgs, metric.cb}
	return m.enRetryQueue(retry, false)
}

func (m *internalSfxClient) enRetryQueue(retry retrySfxMessage, byRetry bool) error {
	if retry.retryCount > 0 {
		// Non blocking enqueue for retry object
		select {
		case m.retryMetrics <- retry:
			return nil
		default:
			// Here is the logic fo FIFO queue
			//
			// if a metric comes from m.metrics, any of them should be later than all
			// metrics in m.retryMetrics. So we'd like retry all of them. If
			// there is no room in m.retryMetrics, we removing some older metric
			// out of m.retryMetrics, see retryLoopWrite()
			//
			// If a metric comes from m.retryMetrics, it will be the oldest metric
			// so we just drop it if no more room in the m.retryMetrics
			if !byRetry {
				// Here is the logic. m.ignoreRetryCount chan length is 1
				//
				// Ignore first m.rmCount metrics in the retry chan, see m.retryMetrics
				// If failed, there is a required in the chan waiting for process.
				// If last request just accepted, m.retryMetrics should has not yet make
				// a room from last request, we overwtite ignoreCount in retryLoopWrite()
				// should have no effect.
				// It is possible that in a very busy use case, we can clean up
				// metrics in the retry chan.
				glog.Infoln("enRetryQueue try to make a room ", m.rmCount)
				select {
				case m.ignoreRetryCount <- m.rmCount:
				default:
					glog.Errorln("Failed to make room m.rmCount")
				}
			}
			// log error, ignore current one
			err := m.logError("retry msg in queue failed, channel", retry.cb)
			return err
		}
	} else {
		return m.logError("msg retried reaches the retry limit", retry.cb)
	}
}

func (m *internalSfxClient) logError(msg string, cb frontierCb) error {
	err := errors.New(msg)
	if cb != nil {
		cb(err)
	} else {
		glog.Errorln(err)
	}
	return err
}

func toSfxType(m frontier.MetricProto_MetricTypeProto) datapoint.MetricType {
	switch m {
	case frontier.MetricProto_GAUGE:
		return datapoint.Gauge
	case frontier.MetricProto_COUNTER:
		return datapoint.Counter
	// case frontier.MetricProto_COUNTER_CREATE_GAUGE:
	// case frontier.MetricProto_COUNTER_REPLACE_WITH_GAUGE:
	default:
		return datapoint.Gauge
	}
}

// SendWithCb  will enqueue dim, data at time when; never blocks; err means msg dropped
// f is called when message is acked/nacked from sfx.
func (m *internalSfxClient) SendWithCb(dim map[string]string,
	data []FrontierData,
	when time.Time,
	resolution uint32,
	f frontierCb) error {
	mtcs := make([]*datapoint.Datapoint, 0)
	profile := m.profile
	if len(profile) != 0 {
		profile += "."
	}
	for i := range data {
		rt := &datapoint.Datapoint{
			Metric:     profile + data[i].Name,
			Dimensions: dim,
			Value:      datapoint.NewFloatValue(data[i].Value),
			MetricType: toSfxType(data[i].MetricType),
			Timestamp:  when,
		}
		mtcs = append(mtcs, rt)
	}
	msg := sfxMessage{mtcs, f}
	select {
	case m.metrics <- msg:
	default:
		err := errors.New("sfx msg buffer full while calling API")
		if f != nil {
			f(err)
		} else {
			glog.Debugln(err, dim, data)
		}
		return err
	}

	return nil
}

// Send will enqueue dim, data at time when; never blocks; err means msg dropped
func (m *internalSfxClient) SendMetric(dim map[string]string,
	data []FrontierData,
	when time.Time) error {
	return m.SendWithCb(dim, data, when, ShrLockConfig.Resolution, nil)
}

func (m *internalSfxClient) Stop() {
	close(m.finished)
}
