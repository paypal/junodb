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
package com.paypal.juno.util;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JunoMetrics {
    private static final Logger log = LoggerFactory.getLogger("JunoMetrics");
    public static final String JUNO_LATENCY_METRIC = "juno.client.operation";
    public static final String JUNO_OPERATION_METRIC = "juno.client.operation.status";
    public static final String SUCCESS="SUCCESS";
    public static final String ERROR="ERROR";
    public static final String WARNING="WARNING";
    public static final String EXCEPTION="EXCEPTION";
    public static final String TYPE = "type";
    public static final String NAME = "name";
    public static final String STATUS = "status";
    public static final String ERROR_CAUSE = "cause";
    public static final String METRIC_PREFIX = "juno.client.";
    public static final String CONNECT_METRIC = METRIC_PREFIX + "connect.count";
    public static final String SPAN_METRIC = METRIC_PREFIX + "span";
    public static final String EVENT_METRIC = METRIC_PREFIX + "event.count";
    public static final String ERROR_METRIC = METRIC_PREFIX + "error.count";
    private static final ConcurrentMap<String, ConcurrentMap<String,Timer>> successOpTimerMetricList = new ConcurrentHashMap(25);
    private static final ConcurrentMap<String, ConcurrentMap<String,Timer>> failureOpTimerMetricList = new ConcurrentHashMap(25);
    private Timer timer;
    private String timerName;
    private Timer.Builder builder;

    private  JunoMetrics(){

    }

    /**
     * To record timer based metric for Juno operations. It records the time for JUNO_SSL_CLIENT txns and its status
     * @param metricName - Name of the metric. "juno.client.txn_latency_ms
     * @param operation
     * @param status
     * @param timeInMs
     */
    public static void recordOpTimer(String metricName, String operation,String pool, String status,long timeInMs){
        try {
                ConcurrentMap<String,Timer> OpTimer = status==SUCCESS?successOpTimerMetricList.get(operation):failureOpTimerMetricList.get(operation);
                if (OpTimer == null) {
                    OpTimer = new ConcurrentHashMap(5);
                    Timer timer = createNewTimer(metricName,operation,pool,status);
                    OpTimer.putIfAbsent(pool,timer);
                    OpTimer = status==SUCCESS?successOpTimerMetricList.putIfAbsent(operation, OpTimer):failureOpTimerMetricList.putIfAbsent(operation, OpTimer);
                }else{
                    Timer timer = OpTimer.get(pool);
                    if(timer == null){
                        Timer newTimer = createNewTimer(metricName,operation,pool,status);
                        OpTimer.putIfAbsent(pool,newTimer);
                    }
                }
                Timer timer = OpTimer.get(pool);
                timer.record(timeInMs, TimeUnit.MILLISECONDS);
        }catch(Exception e){
            //Do not do anything Just log
            log.debug("Exception while recording timer metric: "+e.getMessage());
        }
    }

    private static Timer createNewTimer(String metricName, String operation,String pool, String status){
        Timer timer = Timer.builder(metricName)
                .tags("operation", operation, "pool",pool, "status", status)
                .serviceLevelObjectives(Duration.ofMillis(1), Duration.ofMillis(3), Duration.ofMillis(5),
                        Duration.ofMillis(10), Duration.ofMillis(100), Duration.ofMillis(500), Duration.ofMillis(1000),
                        Duration.ofMillis(5000))
                .distributionStatisticExpiry(Duration.ofSeconds(10))
                .distributionStatisticBufferLength(1)
                .register(Metrics.globalRegistry);
        return timer;
    }

    public static void recordTimer(String type, String name, String status, long timeInMs){
        try {
            Timer.builder(SPAN_METRIC)
                    .tags(TYPE, type,
                          NAME, name,
                          STATUS, status)
                    .register(Metrics.globalRegistry)
                    .record(timeInMs, TimeUnit.MILLISECONDS);
        }catch(Exception e){
            //Do not do anything Just log
            log.debug("Exception while recording timer metric: "+e.getMessage());
        }
    }

    public static void recordOpCount(String pool, String op_type, String errorType){
        try {
            Counter counter = Counter
                    .builder(JUNO_OPERATION_METRIC)
                    .description("indicates instance count of the object")
                    .tag("pool",pool)
                    .tag("type",op_type)
                    .tag("status", errorType)
                    .tag("cause","none")
                    .register(Metrics.globalRegistry);
            counter.increment();
        }catch(Exception e){
            //Do not do anything Just log
            log.debug("Exception while recording counter metric: "+e.getMessage());
        }
    }

    public static void recordOpCount(String pool, String op_type, String errorType, String errorCause){
        try {
            Counter counter = Counter
                    .builder(JUNO_OPERATION_METRIC)
                    .description("indicates instance count of the object")
                    .tag("pool",pool)
                    .tag("type",op_type)
                    .tag("status",errorType)
                    .tag("cause",errorCause)
                    .register(Metrics.globalRegistry);
            counter.increment();
        }catch(Exception e){
            //Do not do anything Just log
            log.debug("Exception while recording counter metric: "+e.getMessage());
        }
    }

    public static void recordConnectCount(String endpoint, String status, String cause) {
        try {
            Counter.builder(CONNECT_METRIC)
                    .tags("endpoint", endpoint,
                            STATUS, status,
                            ERROR_CAUSE,cause)
                    .register(Metrics.globalRegistry)
                    .increment();
        } catch (Exception e) {
            //Do not do anything Just log
            log.debug("Exception while recording timer metric: " + e.getMessage());
        }
    }

    public static void recordEventCount(String type, String name, String status) {
        try {
            Counter.builder(EVENT_METRIC)
                    .tags(TYPE, type,
                            NAME, name,
                            STATUS, status)
                    .register(Metrics.globalRegistry)
                    .increment();
        } catch (Exception e) {
            //Do not do anything Just log
            log.debug("Exception while recording timer metric: " + e.getMessage());
        }
    }

    public static void recordErrorCount(String type, String name, String cause){
        try {
            Counter.builder(ERROR_METRIC)
                    .tags(TYPE, type,
                            NAME, name,
                            ERROR_CAUSE, cause)
                    .register(Metrics.globalRegistry)
                    .increment();
        }catch(Exception e){
            //Do not do anything Just log
            log.debug("Exception while recording timer metric: "+e.getMessage());
        }
    }

}
