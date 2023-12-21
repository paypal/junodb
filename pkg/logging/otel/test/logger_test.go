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

// This test uses a mock to validate the metrics and the sends the metric to
// QA OTEL collector for verifying the results in Sfx UI.
package otel

import (
	"fmt"
	"juno/pkg/logging/otel"
	config "juno/pkg/logging/otel/config"
	"juno/pkg/proto"
	"juno/pkg/stats"
	"testing"
	"time"
)

var exportinterval int = 10

var SfxConfig = config.Config{
	Host:       "localhost",
	Port:       4318,
	Enabled:    true,
	Resolution: 3,
}

func TestJunoOperation(t *testing.T) {
	mc := runMockCollector(t, mockCollectorConfig{
		Port:    4318,
		WithTLS: false,
	})
	defer mc.MustStop(t)

	otel.InitMetricProvider(&SfxConfig)

	time.Sleep(time.Duration(1) * time.Second)

	otel.RecordOperation("Create", proto.OpStatusNoError, 2000)
	otel.RecordOperation("Get", proto.OpStatusNoError, 1000)
	otel.RecordOperation("Update", proto.OpStatusNoError, 3000)
	otel.RecordOperation("Destroy", proto.OpStatusNoError, 500)
	otel.RecordOperation("Set", proto.OpStatusNoError, 2500)

	otel.RecordOperation("Create", proto.OpStatusBadParam, 2000)

	time.Sleep(time.Duration(SfxConfig.Resolution) * time.Second)

	v1m := mc.GetMetrics()

	if len(v1m) == 0 {
		fmt.Println(" V1m length is 0")
		t.Errorf("Test fail")
	} else {

		for i := 0; i < len(v1m); i++ {
			dp := v1m[i].GetHistogram().GetDataPoints()
			for j := 0; j < len(dp); j++ {
				// fmt.Println(" Latencey :", dp[j].GetAttributes(), dp[j].GetSum())
				sum := dp[j].GetSum()
				attr := dp[j].GetAttributes()
				for _, kv := range attr {
					switch kv.Value.GetStringValue() {
					case "Create":
						if sum != 2000 {
							t.Errorf("Create Sum does not match")
						}
					case "Get":
						if sum != 1000 {
							t.Errorf("Get Sum does not match")
						}
					case "Update":
						if sum != 3000 {
							t.Errorf("Update Sum does not match")
						}
					case "Destory":
						if sum != 500 {
							t.Errorf("Destory Sum does not match")
						}
					case "Set":
						if sum != 2500 {
							t.Errorf("Destory Sum does not match")
						}
					default:
						//do nothing
					}
				}
			}
		}

		if count := len(v1m[0].GetHistogram().GetDataPoints()); count != 6 {
			t.Errorf("Count is not correct: %d", count)
		}
	}
}

func TestReplication(t *testing.T) {
	mc := runMockCollector(t, mockCollectorConfig{
		Port:    4318,
		WithTLS: false,
	})
	defer mc.MustStop(t)

	otel.InitMetricProvider(&SfxConfig)

	time.Sleep(time.Duration(1) * time.Second)

	otel.RecordReplication("Create", "SUCCESS", "Replicate_Region1", 2000)
	otel.RecordReplication("Get", "SUCCESS", "Replicate_Region1", 1000)
	otel.RecordReplication("Update", "SUCCESS", "Replicate_Region1", 3000)
	otel.RecordReplication("Destroy", "SUCCESS", "Replicate_Region1", 500)
	otel.RecordReplication("Set", "SUCCESS", "Replicate_Region1", 2500)

	otel.RecordReplication("Create", "ERROR", "Replicate_Region1", 2000)

	time.Sleep(time.Duration(SfxConfig.Resolution) * time.Second)

	v1m := mc.GetMetrics()

	if len(v1m) == 0 {
		fmt.Println(" V1m length is 0")
		t.Errorf("Test fail")
	} else {

		for i := 0; i < len(v1m); i++ {
			dp := v1m[i].GetHistogram().GetDataPoints()
			for j := 0; j < len(dp); j++ {
				sum := dp[j].GetSum()
				attr := dp[j].GetAttributes()
				for _, kv := range attr {
					switch kv.Value.GetStringValue() {
					case "Create":
						if sum != 2000 {
							t.Errorf("Create Sum does not match")
						}
					case "Get":
						if sum != 1000 {
							t.Errorf("Get Sum does not match")
						}
					case "Update":
						if sum != 3000 {
							t.Errorf("Update Sum does not match")
						}
					case "Destory":
						if sum != 500 {
							t.Errorf("Destory Sum does not match")
						}
					case "Set":
						if sum != 2500 {
							t.Errorf("Destory Sum does not match")
						}
					default:
						//do nothing
					}
				}
			}
		}

		if count := len(v1m[0].GetHistogram().GetDataPoints()); count != 6 {
			t.Errorf("Count is not correct: %d", count)
		}
	}
}

func TestOutBoundConnection(t *testing.T) {
	mc := runMockCollector(t, mockCollectorConfig{
		Port:    4318,
		WithTLS: false,
	})
	defer mc.MustStop(t)

	otel.InitMetricProvider(&SfxConfig)

	time.Sleep(time.Duration(1) * time.Second)

	otel.RecordOutboundConnection("127.0.0.1:1234", "SUCCESS", 2000)
	otel.RecordOutboundConnection("127.0.0.1:1234", "SUCCESS", 1000)
	otel.RecordOutboundConnection("127.0.1.0:1234", "SUCCESS", 3000)
	otel.RecordOutboundConnection("127.0.1.0:1234", "SUCCESS", 500)
	otel.RecordOutboundConnection("127.0.1.0:1234", "SUCCESS", 2500)

	time.Sleep(time.Duration(SfxConfig.Resolution) * time.Second)

	v1m := mc.GetMetrics()

	if len(v1m) == 0 {
		fmt.Println(" V1m length is 0")
		t.Errorf("Test fail")
	} else {

		for i := 0; i < len(v1m); i++ {
			dp := v1m[i].GetHistogram().GetDataPoints()
			for j := 0; j < len(dp); j++ {
				sum := dp[j].GetSum()
				attr := dp[j].GetAttributes()
				for _, kv := range attr {
					switch kv.Value.GetStringValue() {
					case "127.0.0.1:1234":
						if sum != 3000 {
							t.Errorf("Create Sum does not match")
						}
					case "127.0.1.0:1234":
						if sum != 6000 {
							t.Errorf("Destory Sum does not match")
						}
					default:
						//do nothing
					}
				}
			}
		}

		// Here it will be counted as only 2 distict dimentions
		if count := len(v1m[0].GetHistogram().GetDataPoints()); count != 2 {
			t.Errorf("Count is not correct: %d", count)
		}
	}
}

func TestSSConnection(t *testing.T) {
	mc := runMockCollector(t, mockCollectorConfig{
		Port:    4318,
		WithTLS: false,
	})
	defer mc.MustStop(t)

	otel.InitMetricProvider(&SfxConfig)

	time.Sleep(time.Duration(1) * time.Second)

	otel.RecordSSConnection("127.0.0.1:1234", "SUCCESS", 2000)
	otel.RecordSSConnection("127.0.0.1:1234", "SUCCESS", 1000)
	otel.RecordSSConnection("127.0.1.0:1234", "SUCCESS", 3000)
	otel.RecordSSConnection("127.0.1.0:1234", "SUCCESS", 500)
	otel.RecordSSConnection("127.0.1.0:1234", "SUCCESS", 2500)

	time.Sleep(time.Duration(SfxConfig.Resolution) * time.Second)

	v1m := mc.GetMetrics()

	if len(v1m) == 0 {
		fmt.Println(" V1m length is 0")
		t.Errorf("Test fail")
	} else {

		for i := 0; i < len(v1m); i++ {
			dp := v1m[i].GetHistogram().GetDataPoints()
			for j := 0; j < len(dp); j++ {
				sum := dp[j].GetSum()
				attr := dp[j].GetAttributes()
				for _, kv := range attr {
					switch kv.Value.GetStringValue() {
					case "127.0.0.1:1234":
						if sum != 3000 {
							t.Errorf("Create Sum does not match")
						}
					case "127.0.1.0:1234":
						if sum != 6000 {
							t.Errorf("Destory Sum does not match")
						}
					default:
						//do nothing
					}
				}
			}
		}

		// Here it will be counted as only 2 distict dimentions
		if count := len(v1m[0].GetHistogram().GetDataPoints()); count != 2 {
			t.Errorf("Count is not correct: %d", count)
		}
	}
}

func TestRecordCount(t *testing.T) {

	mc := runMockCollector(t, mockCollectorConfig{
		Port:    4318,
		WithTLS: false,
	})
	defer mc.MustStop(t)

	otel.InitMetricProvider(&SfxConfig)

	time.Sleep(time.Duration(1) * time.Second)

	otel.RecordCount(otel.ProcErr, []otel.Tags{{otel.Status, "Error"}})
	otel.RecordCount(otel.ProcErr, []otel.Tags{{otel.Status, "Error"}})
	otel.RecordCount(otel.ProcErr, []otel.Tags{{otel.Status, "Error"}})
	otel.RecordCount(otel.ProcErr, []otel.Tags{{otel.Status, "Error"}})
	otel.RecordCount(otel.ProcErr, []otel.Tags{{otel.Status, "Error"}})
	otel.RecordCount(otel.ProcErr, []otel.Tags{{otel.Status, "Error"}})

	otel.RecordCount(otel.ProcErr, []otel.Tags{{otel.Status, "Success"}})
	otel.RecordCount(otel.ProcErr, []otel.Tags{{otel.Status, "Success"}})
	otel.RecordCount(otel.ProcErr, []otel.Tags{{otel.Status, "Success"}})
	otel.RecordCount(otel.ProcErr, []otel.Tags{{otel.Status, "Success"}})
	otel.RecordCount(otel.ProcErr, []otel.Tags{{otel.Status, "Success"}})

	time.Sleep(time.Duration(SfxConfig.Resolution) * time.Second)

	v1m := mc.GetMetrics()

	if len(v1m) == 0 {
		fmt.Println(" V1m length is 0")
		t.Errorf("Test fail")
	} else {
		if v1m[0].GetName() == "ProcErr" {
			t.Errorf("Metric name is incorrect %s", v1m[0].GetName())
		}
		for i := 0; i < len(v1m[0].GetSum().GetDataPoints()); i++ {
			if v1m[0].GetSum().GetDataPoints()[i].GetAttributes()[0].GetKey() == "Success" {
				fmt.Println("Value :", v1m[0].GetSum().GetDataPoints()[0].GetAsInt())
				if v1m[0].GetSum().GetDataPoints()[i].GetAsInt() != 5 {
					t.Errorf("count is incorrect %d", v1m[0].GetSum().GetDataPoints()[i].GetAsInt())
				}
			} else if v1m[0].GetSum().GetDataPoints()[i].GetAttributes()[0].GetKey() == "Error" {
				fmt.Println("Value :", v1m[0].GetSum().GetDataPoints()[1].GetAsInt())
				if v1m[0].GetSum().GetDataPoints()[i].GetAsInt() != 6 {
					t.Errorf("count is incorrect %d", v1m[0].GetSum().GetDataPoints()[i].GetAsInt())
				}
			}
		}
	}
}

func TestJunoStats(t *testing.T) {

	mc := runMockCollector(t, mockCollectorConfig{
		Port:    4318,
		WithTLS: false,
	})
	defer mc.MustStop(t)

	otel.InitMetricProvider(&SfxConfig)
	time.Sleep(time.Duration(1) * time.Second)

	var workerStats [][]stats.IState = make([][]stats.IState, 2)

	var mcpu uint32 = 30
	var bshd uint16 = 5

	for i := 0; i < 2; i++ {
		workerStats[i] = append(workerStats[i],
			[]stats.IState{
				stats.NewUint32State(&mcpu, "pCPU", "Process CPU usage"),
				stats.NewUint16State(&bshd, "nBShd", "number of Bad Shards"),
			}...)
	}

	otel.InitSystemMetrics(otel.SvrTypeProxy, workerStats)

	time.Sleep(time.Duration(SfxConfig.Resolution+10) * time.Second)

	v1m := mc.GetMetrics()

	if len(v1m) == 0 {
		t.Errorf("Test fail")
	} else {
		for i := 0; i < 2; i++ {
			if v1m[i].GetName() == "pp.juno.server.proc_cpu_used" {
				if v1m[i].GetGauge().DataPoints[0].GetAsDouble() != 30 {
					t.Errorf("CPU utilization is incorrect %f", v1m[i].GetGauge().DataPoints[0].GetAsDouble())
				}
			} else if v1m[i].GetName() == "pp.juno.server.bad_shard" {
				if v1m[i].GetGauge().DataPoints[0].GetAsDouble() != 5 {
					t.Errorf("Bad Shard Count is incorrect %f", v1m[i].GetGauge().DataPoints[0].GetAsDouble())
				}
			} else {
				t.Errorf("Invlaid metric name")
			}
		}
	}

}
