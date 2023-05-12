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
package com.paypal.qa.juno;

import com.paypal.juno.client.JunoAsyncClient;
import com.paypal.juno.client.JunoClient;
import com.paypal.juno.client.JunoClientFactory;
import com.paypal.juno.client.JunoReactClient;
import com.paypal.juno.client.io.JunoRequest;
import com.paypal.juno.client.io.JunoResponse;
import com.paypal.juno.client.io.OperationStatus;
import com.paypal.juno.conf.JunoProperties;
import com.paypal.juno.conf.JunoPropertiesProvider;
import com.paypal.juno.exception.JunoException;
import com.paypal.juno.util.JunoMetrics;
import com.paypal.juno.util.SSLUtil;
import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.signalfx.SignalFxMeterRegistry;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.time.Duration;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.actuate.autoconfigure.metrics.export.signalfx.SignalFxProperties;
import org.springframework.boot.actuate.autoconfigure.metrics.export.signalfx.SignalFxPropertiesConfigAdapter;
import org.testng.AssertJUnit;
import org.testng.annotations.*;

public class MetricTest {
    private JunoClient junoClient;
    private JunoAsyncClient junoAsyncClient;
    private JunoReactClient junoReactClient;
    private Properties pConfig;
    private Logger LOGGER;
    private String ip;
    private String port;
    private String ipAddr;
    private int loop = 5; //set as 100 if we want to observe UI data
    SignalFxMeterRegistry sfxRegistry;
    SimpleMeterRegistry simpleMeterRegistry;

    @BeforeClass
    public void setup() throws JunoException, IOException, InterruptedException {
        LOGGER = LoggerFactory.getLogger(MetricTest.class);
        String uriLink = ""; // Set your Observability Collector Here
        AssertJUnit.assertTrue(!uriLink.isEmpty());
        SignalFxProperties properties = new SignalFxProperties();
        properties.setStep(Duration.ofSeconds(60));
        properties.setUri(uriLink);
        properties.setEnabled(true);
        properties.setConnectTimeout(Duration.ofSeconds(15));
        properties.setReadTimeout(Duration.ofSeconds(10));
        properties.setBatchSize(10000);
        properties.setAccessToken("");

        SignalFxPropertiesConfigAdapter config = new SignalFxPropertiesConfigAdapter(properties);
        sfxRegistry = new SignalFxMeterRegistry(config, Clock.SYSTEM);
        Metrics.globalRegistry.add(sfxRegistry);

        URL url = MetricTest.class.getResource("/com/paypal/juno/Juno.properties");
        pConfig = new Properties();
        pConfig.load(url.openStream());
        ip = pConfig.getProperty("juno.server.host");
        port = pConfig.getProperty("juno.server.port");
        InetAddress address = InetAddress.getByName(ip);
        ipAddr = address.getHostAddress();
        LOGGER.error( "Vera: juno ip is " + ip + " juno port is " + port + ", ipaddr is " + ipAddr);

        try{
            junoClient = JunoClientFactory.newJunoClient(new JunoPropertiesProvider(pConfig), SSLUtil.getSSLContext());
            junoAsyncClient = JunoClientFactory.newJunoAsyncClient(new JunoPropertiesProvider(pConfig), SSLUtil.getSSLContext());
            junoReactClient = JunoClientFactory.newJunoReactClient(new JunoPropertiesProvider(pConfig), SSLUtil.getSSLContext());
        }catch (Exception e) {
            throw new RuntimeException(e);
        }

        Thread.sleep(100);

    }

    @AfterClass
    public void teardown() {
        //System.out.println("Calling teardown");
        
        sfxRegistry.close();
        simpleMeterRegistry.close();
    }

    @BeforeMethod
    public void beforeEach() {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //System.out.println("Calling BeforeEach");
        simpleMeterRegistry = new SimpleMeterRegistry();
        Metrics.globalRegistry.add(simpleMeterRegistry);
    }

    @AfterMethod
    public void afterEach() {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //System.out.println("Calling AfterEach");
        simpleMeterRegistry.clear();
        simpleMeterRegistry.close();
        Metrics.globalRegistry.remove(simpleMeterRegistry);
    }

    @Test
    public void testSingleOperations() throws IOException, InterruptedException, CertificateException, KeyStoreException, NoSuchAlgorithmException, KeyManagementException {
        LOGGER.info( "\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());

        URL url1 = MetricTest.class.getResource("/com/paypal/juno/Juno.properties");
        Properties pConfig1 = new Properties();
        pConfig1.load(url1.openStream());
        pConfig1.setProperty(JunoProperties.MAX_CONNECTION_POOL_SIZE, "100");
        pConfig1.setProperty(JunoProperties.CONNECTION_POOL_SIZE, "100");
        JunoClient junoClient1 = JunoClientFactory.newJunoClient(new JunoPropertiesProvider(pConfig1), SSLUtil.getSSLContext());

        junoClient1.delete("test_key".getBytes());
        for(int i=0; i< loop; i++){
            byte[] key = DataGenUtils.genBytes(64);
            byte[] data = DataGenUtils.genBytes(10);

            //Create operation
            try{
                junoClient1.create(key,data);
                junoClient1.create(key,data);
            }catch (Exception e){ }

            //Create operation
            try{
                junoClient1.get(key);
                junoClient1.get("test_key".getBytes());
            }catch (Exception e){ }

            //Create operation
            try{
                junoClient1.set(key,data);
            }catch (Exception e){ }

            //Create operation
            try{
                junoClient1.update(key,data);
                junoClient1.update("test_key".getBytes(),data);
            }catch (Exception e){ }

            //Create operation
            try{
                junoClient1.delete(key);
            }catch (Exception e){ }

            Thread.sleep(100);
        }
        Thread.sleep(25000);

        Iterator<MeterRegistry> registryIterator = Metrics.globalRegistry.getRegistries().iterator();
        while(registryIterator.hasNext()){
            MeterRegistry registry = registryIterator.next();
            if(registry instanceof  SimpleMeterRegistry){
                Timer createTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","CREATE").tag("status","SUCCESS").timer();
                Timer getTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","GET").tag("status","SUCCESS").timer();
                Timer setTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","SET").tag("status","SUCCESS").timer();
                Timer updateTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","UPDATE").tag("status","SUCCESS").timer();
                Timer deleteTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","DESTROY").tag("status","SUCCESS").timer();
                Counter successEvtCnt = registry.find(JunoMetrics.CONNECT_METRIC).tag("endpoint", ipAddr+":"+port).tag(JunoMetrics.STATUS, "SUCCESS").counter();
                Counter uniqueErrCnt = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag(JunoMetrics.STATUS, "Duplicate key").tag(JunoMetrics.STATUS, OperationStatus.UniqueKeyViolation.getErrorText()).counter();
                Counter keyNotFoundCnt = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag(JunoMetrics.STATUS, "Key not found").tag(JunoMetrics.STATUS, OperationStatus.NoKey.getErrorText()).counter();

                AssertJUnit.assertEquals(loop*2, (int)createTimer.count(), 1);
                AssertJUnit.assertEquals(loop*2, (int)getTimer.count(),1);
                AssertJUnit.assertEquals(loop, (int)setTimer.count(),1);
                AssertJUnit.assertEquals(loop*2, (int)updateTimer.count(),1);
                AssertJUnit.assertEquals(loop, (int)deleteTimer.count(),1);
                AssertJUnit.assertEquals(100, (int)successEvtCnt.count(), 98); //this value relates to connection pool and take time to get value
                AssertJUnit.assertEquals(loop, (int)uniqueErrCnt.count());
                AssertJUnit.assertEquals(loop, (int)keyNotFoundCnt.count());
            }
        }
    }

    //    @Test
    public void testNullEmptyKeyNullPayload() throws IOException, InterruptedException {
        LOGGER.info( "\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());

        byte[] key = null;
        byte[] data = DataGenUtils.genBytes(10);
        byte[] data1 = null;
        byte[] key2 = "".getBytes();

        for(int i=0; i<loop; i++){

            byte[] key1 = DataGenUtils.genBytes(10);

            //Create operation
            try{
                junoClient.create(key,data);	//null key
            }catch (Exception e){ }

            try{
                junoClient.create(key1, data1);	//null payload
            }catch (Exception e){ }

            try{
                junoClient.create(key2, data); //empty key
            }catch (Exception e){ }

            //Get operation
            try{
                junoClient.get(key);
            }catch (Exception e){ }

            try{
                junoClient.get(key2);
            }catch (Exception e){ }

            //Update operation
            try{
                junoClient.update(key, data);
            }catch (Exception e){ }

            try{
                junoClient.update(key1, data1);
            }catch (Exception e){ }

            try{
                junoClient.update(key2, data);
            }catch (Exception e){ }

            //Set operation
            try{
                junoClient.set(key, data);
            }catch (Exception e){ }

            try{
                junoClient.set(key1, data1);
            }catch (Exception e){ }

            try{
                junoClient.set(key2, data);
            }catch (Exception e){ }

            //Destroy operation
            try{
                junoClient.delete(key2);
            }catch (Exception e){ }

            Thread.sleep(100);
        }
        Thread.sleep(25000);

        Iterator<MeterRegistry> registryIterator = Metrics.globalRegistry.getRegistries().iterator();
        while(registryIterator.hasNext()){
            MeterRegistry registry = registryIterator.next();
            if(registry instanceof  SimpleMeterRegistry){

                Timer createTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","CREATE").tag("status","SUCCESS").timer();
                Timer createErrTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","CREATE").tag("status","ERROR").timer();
                Timer getTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","GET").tag("status","ERROR").timer();
                Timer setTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","SET").tag("status","SUCCESS").timer();
                Timer setErrTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","SET").tag("status","ERROR").timer();
                Timer updateTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","UPDATE").tag("status","SUCCESS").timer();
                Timer updateErrTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","UPDATE").tag("status","ERROR").timer();
                Timer deleteTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","DESTROY").tag("status","ERROR").timer();

                Counter createErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "CREATE").tag("status", "Illegal argument").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();
                Counter setErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "SET").tag("status", "Illegal argument").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();
                Counter updateErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "UPDATE").tag("status", "Illegal argument").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();
                Counter getErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "GET").tag("status", "Illegal argument").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();
                Counter destroyErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "DESTROY").tag("status", "Illegal argument").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();

                Thread.sleep(1000);
                AssertJUnit.assertEquals(loop, (int)createTimer.count(), 1);
                AssertJUnit.assertEquals(loop*2, (int)createErrTimer.count(), 1);
                AssertJUnit.assertEquals(loop*2, (int)getTimer.count(), 1);
                AssertJUnit.assertEquals(loop, (int)setTimer.count(), 1);
                AssertJUnit.assertEquals(loop*2, (int)setErrTimer.count(), 1);
                AssertJUnit.assertEquals(loop, (int)updateTimer.count(), 1);
                AssertJUnit.assertEquals(loop*2, (int)updateErrTimer.count(), 1);
                AssertJUnit.assertEquals(loop, (int)deleteTimer.count(), 1);

                AssertJUnit.assertEquals(loop*2, (int)createErr.count());
                AssertJUnit.assertEquals(loop*2, (int)getErr.count());
                AssertJUnit.assertEquals(loop*2, (int)setErr.count());
                AssertJUnit.assertEquals(loop*2, (int)updateErr.count());
                AssertJUnit.assertEquals(loop, (int)destroyErr.count());
            }
        }
    }

    @Test
    public void testExceedsSize() throws IOException, InterruptedException {
        LOGGER.info( "\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());

        byte[] key = DataGenUtils.genBytes(257);
        byte[] data = DataGenUtils.genBytes(10);
        byte[] key1 = DataGenUtils.genBytes(25);
        byte[] data1 = DataGenUtils.genBytes(2048011);

        for(int i=0; i<loop; i++){

            //Create operation
            try{
                junoClient.create(key,data);
            }catch (Exception e){ }

            try{
                junoClient.create(key1,data1);
            }catch (Exception e){ }

            //Get operation
            try{
                junoClient.get(key);
            }catch (Exception e){ }

            //Update operation
            try{
                junoClient.update(key, data);
            }catch (Exception e){ }

            try{
                junoClient.update(key1, data1);
            }catch (Exception e){ }

            //Set operation
            try{
                junoClient.set(key,data);
            }catch (Exception e){ }

            try{
                junoClient.set(key1,data1);
            }catch (Exception e){ }

            //Destroy operation
            try{
                junoClient.delete(key);
            }catch (Exception e){ }

            Thread.sleep(200);
        }

        Iterator<MeterRegistry> registryIterator = Metrics.globalRegistry.getRegistries().iterator();
        while(registryIterator.hasNext()){
            MeterRegistry registry = registryIterator.next();
            if(registry instanceof  SimpleMeterRegistry){
                Timer createTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","CREATE").tag("status","ERROR").timer();
                Timer getTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","GET").tag("status","ERROR").timer();
                Timer setTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","SET").tag("status","ERROR").timer();
                Timer updateTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","UPDATE").tag("status","ERROR").timer();
                Timer deleteTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","DESTROY").tag("status","ERROR").timer();                Counter createKeyErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "CREATE").tag("status", "Illegal argument").tag("cause", "key_size_exceeded").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();
                Counter createPayloadErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "CREATE").tag("status", "Illegal argument").tag("cause", "payload_size_exceeded").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();

                Counter setKeyErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "SET").tag("status", "Illegal argument").tag("cause", "key_size_exceeded").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();
                Counter setPayloadErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "SET").tag("status", "Illegal argument").tag("cause", "payload_size_exceeded").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();

                Counter updateKeyErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "UPDATE").tag("status", "Illegal argument").tag("cause", "key_size_exceeded").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();
                Counter updatePayloadErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "UPDATE").tag("status", "Illegal argument").tag("cause", "payload_size_exceeded").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();

                Counter getKeyErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "GET").tag("status", "Illegal argument").tag("cause", "key_size_exceeded").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();
                Counter destroyKeyErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "DESTROY").tag("status", "Illegal argument").tag("cause", "key_size_exceeded").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();                AssertJUnit.assertEquals(loop*2, (int)createTimer.count(), 2);
                AssertJUnit.assertEquals(loop, (int)getTimer.count(), 2);
                AssertJUnit.assertEquals(loop*2, (int)setTimer.count(), 2);
                AssertJUnit.assertEquals(loop*2, (int)updateTimer.count(), 2);
                AssertJUnit.assertEquals(loop, (int)deleteTimer.count(), 2);

                AssertJUnit.assertEquals(loop, (int)createKeyErr.count());
                AssertJUnit.assertEquals(loop, (int)createPayloadErr.count());
                AssertJUnit.assertEquals(loop, (int)getKeyErr.count());
                AssertJUnit.assertEquals(loop, (int)setKeyErr.count());
                AssertJUnit.assertEquals(loop, (int)setPayloadErr.count());
                AssertJUnit.assertEquals(loop, (int)updateKeyErr.count());
                AssertJUnit.assertEquals(loop, (int)updatePayloadErr.count());
                AssertJUnit.assertEquals(loop, (int)destroyKeyErr.count());
            }
        }
    }

    @Test
    public void testInvalidLifetime() throws IOException, InterruptedException {
        LOGGER.info( "\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());

        byte[] key = DataGenUtils.genBytes(25);
        byte[] data = DataGenUtils.genBytes(10);

        for(int i=0; i<loop; i++){

            //Create operation
            try{
                junoClient.create(key, data, (long)0);
            }catch (Exception e){ }

            try{
                junoClient.create(key, data, (long)-1);
            }catch (Exception e){ }

            try{
                junoClient.create(key, data, (long)7770000); //exceeds max lifetime
            }catch (Exception e){ }

            //Get operation
            try{
                junoClient.create(key, data, (long)5);
                junoClient.get(key, (long)0);	////this is allowed for update, so successful
            }catch (Exception e){ }

            try{
                junoClient.get(key, (long)-1);
            }catch (Exception e){ }

            try{
                junoClient.get(key, (long)7770000);
            }catch (Exception e){ }

            //Update operation
            try{
                junoClient.update(key, data, (long)0);	//this is allowed for update, so successful
            }catch (Exception e){ }

            try{
                junoClient.update(key, data, (long)-1);
            }catch (Exception e){ }

            try{
                junoClient.update(key, data, (long)7770000);
            }catch (Exception e){ }

            //Set operation
            try{
                junoClient.set(key, data, (long)0); 	//this is allowed for set, so successful
            }catch (Exception e){ }

            try{
                junoClient.set(key, data, (long)-1);
            }catch (Exception e){ }

            try{
                junoClient.set(key, data, (long)7770000);
            }catch (Exception e){ }

            //Destroy operation
            try{
                junoClient.delete(key);
            }catch (Exception e){ }

            Thread.sleep(100);
        }

        Iterator<MeterRegistry> registryIterator = Metrics.globalRegistry.getRegistries().iterator();
        while(registryIterator.hasNext()){
            MeterRegistry registry = registryIterator.next();
            if(registry instanceof  SimpleMeterRegistry){
                Timer createTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","CREATE").tag("status","SUCCESS").timer();
                Timer getTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","GET").tag("status","SUCCESS").timer();
                Timer setTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","SET").tag("status","SUCCESS").timer();
                Timer updateTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","UPDATE").tag("status","SUCCESS").timer();
                Timer deleteTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","DESTROY").tag("status","SUCCESS").timer();

                Counter createInvalidTTLErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "CREATE").tag("status", "Illegal argument").tag("cause", "invalid_ttl").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();
                Counter createExceedsTTLErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "CREATE").tag("status", "Illegal argument").tag("cause", "ttl_exceeded_max").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();

                Counter getNegTTLErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "GET").tag("status", "Illegal argument").tag("cause", "invalid_ttl").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();
                Counter getExceedsTTLErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "GET").tag("status", "Illegal argument").tag("cause", "ttl_exceeded_max").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();

                Counter setNegTTLErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "SET").tag("status", "Illegal argument").tag("cause", "invalid_ttl").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();
                Counter setExceedsTTLErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "SET").tag("status", "Illegal argument").tag("cause", "ttl_exceeded_max").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();

                Counter updateNegTTLErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "UPDATE").tag("status", "Illegal argument").tag("cause", "invalid_ttl").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();
                Counter updateExceedsTTLErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "UPDATE").tag("status", "Illegal argument").tag("cause", "ttl_exceeded_max").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();

                AssertJUnit.assertEquals(loop, (int)createTimer.count(), 1);
                AssertJUnit.assertEquals(loop, (int)getTimer.count(), 1);
                AssertJUnit.assertEquals(loop, (int)setTimer.count(), 1);
                AssertJUnit.assertEquals(loop, (int)updateTimer.count(), 1);
                AssertJUnit.assertEquals(loop, (int)deleteTimer.count(), 1);

                AssertJUnit.assertEquals(2 * loop, (int)createInvalidTTLErr.count());
                AssertJUnit.assertEquals(loop, (int)createExceedsTTLErr.count());
                AssertJUnit.assertEquals(loop, (int)getNegTTLErr.count());
                AssertJUnit.assertEquals(loop, (int)getExceedsTTLErr.count());
                AssertJUnit.assertEquals(loop, (int)setNegTTLErr.count());
                AssertJUnit.assertEquals(loop, (int)setExceedsTTLErr.count());
                AssertJUnit.assertEquals(loop, (int)updateNegTTLErr.count());
                AssertJUnit.assertEquals(loop, (int)updateExceedsTTLErr.count());
            }
        }
    }

    //    @Test
    public void testTimeoutMetric() throws IOException, InterruptedException, CertificateException, KeyStoreException, NoSuchAlgorithmException, KeyManagementException {
        LOGGER.info( "\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());

        //response timeout config
        URL url2 = MetricTest.class.getResource("/com/paypal/juno/Juno.properties");
        Properties pConfig2 = new Properties();
        pConfig2.load(url2.openStream());
        pConfig2.setProperty(JunoProperties.APP_NAME, "QATestApp2");
        pConfig2.setProperty(JunoProperties.RECORD_NAMESPACE, "MetricNS2");
        pConfig2.setProperty(JunoProperties.RESPONSE_TIMEOUT, "1");
        JunoClient junoClient2 = JunoClientFactory.newJunoClient(new JunoPropertiesProvider(pConfig2), SSLUtil.getSSLContext());

        for(int i=0; i<loop; i++){

            byte[] key = DataGenUtils.genBytes(25);
            byte[] data = DataGenUtils.genBytes(10);

            //Create operation, response timeout
            try{
                junoClient2.create(key, data);
            }catch (Exception e){ }

            Thread.sleep(100);
        }
        Thread.sleep(25000);

        Iterator<MeterRegistry> registryIterator = Metrics.globalRegistry.getRegistries().iterator();
        while(registryIterator.hasNext()){
            MeterRegistry registry = registryIterator.next();
            if(registry instanceof  SimpleMeterRegistry){

                Timer createTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","CREATE").tag("status","ERROR").timer();

                Counter createRespTimeoutErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "CREATE").tag("status", "Response Timed out").tag("status", OperationStatus.ResponseTimeout.getErrorText()).counter();

                AssertJUnit.assertEquals(loop, (int)createTimer.count(), 1);
                AssertJUnit.assertEquals(loop, (int)createRespTimeoutErr.count());
            }
        }
    }

    @Test
    public void testAsyncSingleOperations() throws IOException, InterruptedException {
        LOGGER.info( "\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());

        junoAsyncClient.delete("test_key".getBytes()).subscribe();

        for(int i=0; i<loop; i++){
            byte[] key = DataGenUtils.genBytes(64);
            byte[] data = DataGenUtils.genBytes(10);

            //Create operation
            try{
                junoAsyncClient.create(key,data).subscribe();
                Thread.sleep(300);
                junoAsyncClient.create(key,data).subscribe();
            }catch (Exception e){ }

            //Create operation
            try{
                junoAsyncClient.get(key).subscribe();
                junoAsyncClient.get("test_key".getBytes()).subscribe();
            }catch (Exception e){ }

            //Create operation
            try{
                Thread.sleep(300);
                junoAsyncClient.set(key,data).subscribe();
            }catch (Exception e){ }

            //Create operation
            try{
                Thread.sleep(300);
                junoAsyncClient.update(key,data).subscribe();
                junoAsyncClient.update("test_key".getBytes(),data).subscribe();
            }catch (Exception e){ }

            //Create operation
            try{
                Thread.sleep(300);
                junoAsyncClient.delete(key).subscribe();
            }catch (Exception e){ }

            Thread.sleep(300);
        }

        //Thread.sleep(10000);
        Iterator<MeterRegistry> registryIterator = Metrics.globalRegistry.getRegistries().iterator();
        while(registryIterator.hasNext()){
            MeterRegistry registry = registryIterator.next();
            if(registry instanceof  SimpleMeterRegistry){

                Timer createTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","CREATE").tag("status","SUCCESS").timer();
                Timer getTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","GET").tag("status","SUCCESS").timer();
                Timer setTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","SET").tag("status","SUCCESS").timer();
                Timer updateTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","UPDATE").tag("status","SUCCESS").timer();
                Timer deleteTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","DESTROY").tag("status","SUCCESS").timer();

                Counter uniqueErrCnt = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("status", OperationStatus.UniqueKeyViolation.getErrorText()).counter();
                Counter keyNotFoundCnt = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("status", "Key not found").tag("status", OperationStatus.NoKey.getErrorText()).counter();

                AssertJUnit.assertEquals(loop*2, (int)createTimer.count(), 1);
                AssertJUnit.assertEquals(loop*2, (int)getTimer.count(),1);
                AssertJUnit.assertEquals(loop, (int)setTimer.count(),1);
                AssertJUnit.assertEquals(loop*2, (int)updateTimer.count(),1);
                AssertJUnit.assertEquals(loop, (int)deleteTimer.count(),1);
                AssertJUnit.assertEquals(loop, (int)keyNotFoundCnt.count(),1);
                AssertJUnit.assertEquals(loop, (int)uniqueErrCnt.count());
            }
        }
    }

    //    @Test
    public void testAsyncNullEmptyKeyNullPayload() throws IOException, InterruptedException {
        LOGGER.info( "\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());

        byte[] key = null;
        byte[] data = DataGenUtils.genBytes(10);
        byte[] data1 = null;
        byte[] key2 = "".getBytes();

        for(int i=0; i<loop; i++){

            byte[] key1 = DataGenUtils.genBytes(10);

            //Create operation
            try{
                junoAsyncClient.create(key,data).subscribe();	//null key
            }catch (Exception e){ }

            try{
                junoAsyncClient.create(key1, data1).subscribe();	//null payload
            }catch (Exception e){ }

            try{
                junoAsyncClient.create(key2, data).subscribe(); //empty key
            }catch (Exception e){ }

            //Get operation
            try{
                junoAsyncClient.get(key).subscribe();
            }catch (Exception e){ }

            try{
                junoAsyncClient.get(key2).subscribe();
            }catch (Exception e){ }

            //Update operation
            try{
                junoAsyncClient.update(key, data).subscribe();
            }catch (Exception e){ }

            try{
                Thread.sleep(100);
                junoAsyncClient.update(key1, data1).subscribe();
            }catch (Exception e){ }

            try{
                junoAsyncClient.update(key2, data).subscribe();
            }catch (Exception e){ }

            //Set operation
            try{
                junoAsyncClient.set(key, data).subscribe();
            }catch (Exception e){ }

            try{
                junoAsyncClient.set(key1, data1).subscribe();
            }catch (Exception e){ }

            try{
                junoAsyncClient.set(key2, data).subscribe();
            }catch (Exception e){ }

            //Destroy operation
            try{
                junoAsyncClient.delete(key).subscribe();
            }catch (Exception e){ }

            Thread.sleep(100);
        }
        Thread.sleep(25000);

        Iterator<MeterRegistry> registryIterator = Metrics.globalRegistry.getRegistries().iterator();
        while(registryIterator.hasNext()){
            MeterRegistry registry = registryIterator.next();
            if(registry instanceof  SimpleMeterRegistry){
                Timer createTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","CREATE").tag("status","SUCCESS").timer();
                Timer createErrTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","CREATE").tag("status","ERROR").timer();
                Timer getTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","GET").tag("status","ERROR").timer();
                Timer setTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","SET").tag("status","SUCCESS").timer();
                Timer setErrTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","SET").tag("status","ERROR").timer();
                Timer updateTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","UPDATE").tag("status","SUCCESS").timer();
                Timer updateErrTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","UPDATE").tag("status","ERROR").timer();
                Timer deleteTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","DESTROY").tag("status","ERROR").timer();

                Counter createErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "CREATE").tag("status", "Illegal argument").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();
                Counter setErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "SET").tag("status", "Illegal argument").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();
                Counter updateErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "UPDATE").tag("status", "Illegal argument").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();
                Counter getErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "GET").tag("status", "Illegal argument").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();
                Counter destroyErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "DESTROY").tag("status", "Illegal argument").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();

                Thread.sleep(2000);
                AssertJUnit.assertEquals(loop, (int)createTimer.count(), 1);
                AssertJUnit.assertEquals(loop*2, (int)createErrTimer.count(), 1);
                AssertJUnit.assertEquals(loop*2, (int)getTimer.count(), 1);
                AssertJUnit.assertEquals(loop, (int)setTimer.count(), 1);
                AssertJUnit.assertEquals(loop*2, (int)setErrTimer.count(), 1);
                AssertJUnit.assertEquals(loop, (int)updateTimer.count(), 1);
                AssertJUnit.assertEquals(loop*2, (int)updateErrTimer.count(), 1);
                AssertJUnit.assertEquals(loop, (int)deleteTimer.count(), 1);

                AssertJUnit.assertEquals(loop*2, (int)createErr.count());
                AssertJUnit.assertEquals(loop*2, (int)getErr.count());
                AssertJUnit.assertEquals(loop*2, (int)setErr.count());
                AssertJUnit.assertEquals(loop*2, (int)updateErr.count());
            }
        }
    }

    @Test
    public void testAsyncExceedsSize() throws IOException, InterruptedException {
        LOGGER.info( "\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());

        byte[] key = DataGenUtils.genBytes(257);
        byte[] data = DataGenUtils.genBytes(10);
        byte[] key1 = DataGenUtils.genBytes(25);
        byte[] data1 = DataGenUtils.genBytes(2048011);

        for(int i=0; i<loop; i++){

            //Create operation
            try{
                junoAsyncClient.create(key,data).subscribe();
                Thread.sleep(300);
            }catch (Exception e){ }

            try{
                junoAsyncClient.create(key1,data1).subscribe();
                Thread.sleep(300);
            }catch (Exception e){ }

            //Get operation
            try{
                junoAsyncClient.get(key).subscribe();
            }catch (Exception e){ }

            //Update operation
            try{
                Thread.sleep(300);
                junoAsyncClient.update(key, data).subscribe();
            }catch (Exception e){ }

            try{
                Thread.sleep(300);
                junoAsyncClient.update(key1, data1).subscribe();
            }catch (Exception e){ }

            //Set operation
            try{
                Thread.sleep(300);
                junoAsyncClient.set(key,data).subscribe();
            }catch (Exception e){ }

            try{
                Thread.sleep(300);
                junoAsyncClient.set(key1,data1).subscribe();
            }catch (Exception e){ }

            //Destroy operation
            try{
                junoAsyncClient.delete(key).subscribe();
            }catch (Exception e){ }

            Thread.sleep(300);
        }

        Iterator<MeterRegistry> registryIterator = Metrics.globalRegistry.getRegistries().iterator();
        while(registryIterator.hasNext()){
            MeterRegistry registry = registryIterator.next();
            if(registry instanceof  SimpleMeterRegistry){
                Timer createTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","CREATE").tag("status","ERROR").timer();
                Timer getTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","GET").tag("status","ERROR").timer();
                Timer setTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","SET").tag("status","ERROR").timer();
                Timer updateTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","UPDATE").tag("status","ERROR").timer();
                Timer deleteTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","DESTROY").tag("status","ERROR").timer();

                Counter createKeyErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "CREATE").tag("status", "Illegal argument").tag("cause", "key_size_exceeded").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();
                Counter createPayloadErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "CREATE").tag("status", "Illegal argument").tag("cause", "payload_size_exceeded").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();

                Counter setKeyErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "SET").tag("status", "Illegal argument").tag("cause", "key_size_exceeded").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();
                Counter setPayloadErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "SET").tag("status", "Illegal argument").tag("cause", "payload_size_exceeded").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();

                Counter updateKeyErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "UPDATE").tag("status", "Illegal argument").tag("cause", "key_size_exceeded").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();
                Counter updatePayloadErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "UPDATE").tag("status", "Illegal argument").tag("cause", "payload_size_exceeded").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();

                Counter getKeyErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "GET").tag("status", "Illegal argument").tag("cause", "key_size_exceeded").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();
                Counter destroyKeyErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "DESTROY").tag("status", "Illegal argument").tag("cause", "key_size_exceeded").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();                AssertJUnit.assertEquals(loop*2, (int)createTimer.count(), 2);
                AssertJUnit.assertEquals(loop, (int)getTimer.count(), 2);
                AssertJUnit.assertEquals(loop*2, (int)setTimer.count(), 2);
                AssertJUnit.assertEquals(loop*2, (int)updateTimer.count(), 2);
                AssertJUnit.assertEquals(loop, (int)deleteTimer.count(), 2);

                AssertJUnit.assertEquals(loop, (int)createKeyErr.count());
                AssertJUnit.assertEquals(loop, (int)createPayloadErr.count());
                AssertJUnit.assertEquals(loop, (int)getKeyErr.count());
                AssertJUnit.assertEquals(loop, (int)setKeyErr.count());
                AssertJUnit.assertEquals(loop, (int)setPayloadErr.count());
                AssertJUnit.assertEquals(loop, (int)updateKeyErr.count());
                AssertJUnit.assertEquals(loop, (int)updatePayloadErr.count());
                AssertJUnit.assertEquals(loop, (int)destroyKeyErr.count());
            }
        }
    }

    @Test
    public void testAsyncInvalidLifetime() throws IOException, InterruptedException {
        LOGGER.info( "\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());

        byte[] key = DataGenUtils.genBytes(25);
        byte[] data = DataGenUtils.genBytes(10);

        for(int i=0; i<loop; i++){

            //Create operation
            try{
                junoAsyncClient.create(key, data, (long)0).subscribe();
                Thread.sleep(300);
            }catch (Exception e){ }

            try{
                junoAsyncClient.create(key, data, (long)-1).subscribe();
                Thread.sleep(300);
            }catch (Exception e){ }

            try{
                junoAsyncClient.create(key, data, (long)7770000).subscribe(); //exceeds max lifetime
                Thread.sleep(300);
            }catch (Exception e){ }

            //Get operation
            try{
                junoAsyncClient.create(key, data, (long)5).subscribe();
                junoAsyncClient.get(key, (long)0).subscribe();
                Thread.sleep(300);
            }catch (Exception e){ }

            try{
                junoAsyncClient.get(key, (long)-1).subscribe();
            }catch (Exception e){ }

            try{
                junoAsyncClient.get(key, (long)7770000).subscribe();
                Thread.sleep(300);
            }catch (Exception e){ }

            //Update operation
            try{
                junoAsyncClient.update(key, data, (long)0).subscribe();
                Thread.sleep(300);
            }catch (Exception e){ }

            try{
                junoAsyncClient.update(key, data, (long)-1).subscribe();
                Thread.sleep(300);
            }catch (Exception e){ }

            try{
                junoAsyncClient.update(key, data, (long)7770000).subscribe();
                Thread.sleep(300);
            }catch (Exception e){ }

            //Set operation
            try{
                junoAsyncClient.set(key, data, (long)0).subscribe();
                Thread.sleep(300);
            }catch (Exception e){ }

            try{
                junoAsyncClient.set(key, data, (long)-1).subscribe();
                Thread.sleep(300);
            }catch (Exception e){ }

            try{
                junoAsyncClient.set(key, data, (long)7770000).subscribe();
                Thread.sleep(300);
            }catch (Exception e){ }

            //Destroy operation
            try{
                junoAsyncClient.delete(key).subscribe();
                Thread.sleep(300);
            }catch (Exception e){ }

            Thread.sleep(100);
        }

//        Thread.sleep (10000);

        Iterator<MeterRegistry> registryIterator = Metrics.globalRegistry.getRegistries().iterator();
        while(registryIterator.hasNext()){
            MeterRegistry registry = registryIterator.next();
            if(registry instanceof  SimpleMeterRegistry){
                Timer createTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","CREATE").tag("status","SUCCESS").timer();
                Timer getTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","GET").tag("status","SUCCESS").timer();
                Timer setTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","SET").tag("status","SUCCESS").timer();
                Timer updateTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","UPDATE").tag("status","SUCCESS").timer();
                Timer deleteTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","DESTROY").tag("status","SUCCESS").timer();

                Counter createInvalidTTLErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "CREATE").tag("cause", "invalid_ttl").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();
                Counter createExceedsTTLErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "CREATE").tag("cause", "ttl_exceeded_max").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();

                Counter getNegTTLErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "GET").tag("cause", "invalid_ttl").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();
                Counter getExceedsTTLErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "GET").tag("cause", "ttl_exceeded_max").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();

                Counter setNegTTLErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "SET").tag("cause", "invalid_ttl").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();
                Counter setExceedsTTLErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "SET").tag("cause", "ttl_exceeded_max").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();

                Counter updateNegTTLErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "UPDATE").tag("cause", "invalid_ttl").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();
                Counter updateExceedsTTLErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "UPDATE").tag("cause", "ttl_exceeded_max").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();

                AssertJUnit.assertEquals(loop, (int)createTimer.count(), 1);
                AssertJUnit.assertEquals(loop, (int)getTimer.count(), 1);
                AssertJUnit.assertEquals(loop, (int)setTimer.count(), 1);
                AssertJUnit.assertEquals(loop, (int)updateTimer.count(), 1);
                AssertJUnit.assertEquals(loop, (int)deleteTimer.count(), 1);

                AssertJUnit.assertEquals(2 * loop, (int)createInvalidTTLErr.count());
                AssertJUnit.assertEquals(loop, (int)createExceedsTTLErr.count());
                AssertJUnit.assertEquals(loop, (int)getNegTTLErr.count());
                AssertJUnit.assertEquals(loop, (int)getExceedsTTLErr.count());
                AssertJUnit.assertEquals(loop, (int)setNegTTLErr.count());
                AssertJUnit.assertEquals(loop, (int)setExceedsTTLErr.count());
                AssertJUnit.assertEquals(loop, (int)updateNegTTLErr.count());
                AssertJUnit.assertEquals(loop, (int)updateExceedsTTLErr.count());
            }
        }
    }

    @Test
    public void testAsyncTimeoutMetric() throws IOException, InterruptedException, CertificateException, KeyStoreException, NoSuchAlgorithmException, KeyManagementException {
        LOGGER.info( "\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());

        //response timeout config
        URL url2 = MetricTest.class.getResource("/com/paypal/juno/Juno.properties");
        Properties pConfig2 = new Properties();
        pConfig2.load(url2.openStream());
        pConfig2.setProperty(JunoProperties.APP_NAME, "QATestApp2");
        pConfig2.setProperty(JunoProperties.RECORD_NAMESPACE, "MetricNS2");
        pConfig2.setProperty(JunoProperties.RESPONSE_TIMEOUT, "1");
        JunoAsyncClient junoAsyncClient2 = JunoClientFactory.newJunoAsyncClient(new JunoPropertiesProvider(pConfig2), SSLUtil.getSSLContext());

        for(int i=0; i<loop; i++){

            byte[] key = DataGenUtils.genBytes(25);
            byte[] data = DataGenUtils.genBytes(10);

            //Create operation, response timeout
            try{
                junoAsyncClient2.create(key, data).subscribe();
            }catch (Exception e){ }

            Thread.sleep(300);
        }

        Iterator<MeterRegistry> registryIterator = Metrics.globalRegistry.getRegistries().iterator();
        while(registryIterator.hasNext()){
            MeterRegistry registry = registryIterator.next();
            if(registry instanceof  SimpleMeterRegistry){

                Timer createTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","CREATE").tag("status","ERROR").timer();

                Counter createRespTimeoutErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "CREATE").tag("status", "Response Timed out").tag("status", OperationStatus.ResponseTimeout.getErrorText()).counter();

                AssertJUnit.assertEquals(loop, (int)createTimer.count(), 1);
                AssertJUnit.assertEquals(loop, (int)createRespTimeoutErr.count());
            }
        }
    }

    @Test
    public void testAsyncBlockBatchOp() throws IOException, InterruptedException {
        LOGGER.info( "\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());

        int numKeys = 20;
        byte[][] key = new byte[numKeys][];
        byte[][] payload = new byte[numKeys][];
        Iterable<JunoResponse> batchResp = null;
        LOGGER.debug("Create " + numKeys + " keys using batch Create");
        List<JunoRequest> createList = new ArrayList<>();
        List<JunoRequest> getList = new ArrayList<>();
        List<JunoRequest> setList = new ArrayList<>();
        List<JunoRequest> updateList = new ArrayList<>();
        List<JunoRequest> deleteList = new ArrayList<>();
        for (int i = 0; i < numKeys; i ++) {
            key[i] = DataGenUtils.createKey(10).getBytes();
            Random r = new Random();
            int payloadlen = DataGenUtils.rand(r, 200, 2047);
            payload[i] = DataGenUtils.createKey(payloadlen).getBytes();
            createList.add(new JunoRequest(key[i], payload[i], (long)0, 100, System.currentTimeMillis(), JunoRequest.OperationType.Create));
            getList.add(new JunoRequest(key[i], 0, 100, JunoRequest.OperationType.Get));
            setList.add(new JunoRequest(key[i], payload[i], (long)0, 100, System.currentTimeMillis(), JunoRequest.OperationType.Set));
            updateList.add(new JunoRequest(key[i], payload[i], (long)0, 100, System.currentTimeMillis(), JunoRequest.OperationType.Update));
            deleteList.add(new JunoRequest(key[i], null, 0, 0, JunoRequest.OperationType.Destroy));
        }        for(int i=0; i<loop; i++){

            //Create operation
            try{
                batchResp = junoAsyncClient.doBatch(createList).toBlocking().toIterable();
                for (JunoResponse mResponse: batchResp) {
                    AssertJUnit.assertEquals (OperationStatus.Success,mResponse.getStatus());
                }
            }catch (Exception e){ }

            //Get operation
            try{
                batchResp = junoAsyncClient.doBatch(getList).toBlocking().toIterable();
                for (JunoResponse mResponse: batchResp) {
                    AssertJUnit.assertEquals (OperationStatus.Success,mResponse.getStatus());
                }
            }catch (Exception e){ }

            //Set operation
            try{
                batchResp = junoAsyncClient.doBatch(setList).toBlocking().toIterable();
                for (JunoResponse mResponse: batchResp) {
                    AssertJUnit.assertEquals (OperationStatus.Success,mResponse.getStatus());
                }
            }catch (Exception e){ }

            //Update operation
            try{
                batchResp = junoAsyncClient.doBatch(updateList).toBlocking().toIterable();
                for (JunoResponse mResponse: batchResp) {
                    AssertJUnit.assertEquals (OperationStatus.Success,mResponse.getStatus());
                }
            }catch (Exception e){ }

            //Destroy operation
            try{
                Iterable<JunoResponse> delResp = junoAsyncClient.doBatch(deleteList).toBlocking().toIterable();
                for (JunoResponse mResponse: delResp) {
                    AssertJUnit.assertEquals (OperationStatus.Success,mResponse.getStatus());
                }
            }catch (Exception e){ }

            Thread.sleep(100);
        }
        Thread.sleep(1500);

        Iterator<MeterRegistry> registryIterator = Metrics.globalRegistry.getRegistries().iterator();
        while(registryIterator.hasNext()){
            MeterRegistry registry = registryIterator.next();
            if(registry instanceof  SimpleMeterRegistry){

                Timer createTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","B_CREATE").tag("status","SUCCESS").timer();
                Timer getTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","B_GET").tag("status","SUCCESS").timer();
                Timer setTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","B_SET").tag("status","SUCCESS").timer();
                Timer updateTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","B_UPDATE").tag("status","SUCCESS").timer();
                Timer deleteTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","B_DESTROY").tag("status","SUCCESS").timer();

                AssertJUnit.assertEquals(numKeys*loop, (int)createTimer.count(),1);
                AssertJUnit.assertEquals(numKeys*loop, (int)getTimer.count(),1);
                AssertJUnit.assertEquals(numKeys*loop, (int)setTimer.count(),1);
                AssertJUnit.assertEquals(numKeys*loop, (int)updateTimer.count(),1);
                AssertJUnit.assertEquals(numKeys*loop, (int)deleteTimer.count(),1);
            }
        }
    }

    @Test
    public void testAsyncNonBlockBatchOp() throws IOException, InterruptedException {
        LOGGER.info( "\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());

        int numKeys = 20;
        byte[][] key = new byte[numKeys][];
        byte[][] payload = new byte[numKeys][];
        LOGGER.debug("Create " + numKeys + " keys using batch Create");
        List<JunoRequest> createList = new ArrayList<>();
        List<JunoRequest> getList = new ArrayList<>();
        List<JunoRequest> setList = new ArrayList<>();
        List<JunoRequest> updateList = new ArrayList<>();
        List<JunoRequest> deleteList = new ArrayList<>();
        for (int i = 0; i < numKeys; i ++) {
            key[i] = DataGenUtils.createKey(10).getBytes();
            Random r = new Random();
            int payloadlen = DataGenUtils.rand(r, 200, 2047);
            payload[i] = DataGenUtils.createKey(payloadlen).getBytes();
            createList.add(new JunoRequest(key[i], payload[i], (long)0, 100, System.currentTimeMillis(), JunoRequest.OperationType.Create));
            getList.add(new JunoRequest(key[i], 0, 100, JunoRequest.OperationType.Get));
            setList.add(new JunoRequest(key[i], payload[i], (long)0, 100, System.currentTimeMillis(), JunoRequest.OperationType.Set));
            updateList.add(new JunoRequest(key[i], payload[i], (long)0, 100, System.currentTimeMillis(), JunoRequest.OperationType.Update));
            deleteList.add(new JunoRequest(key[i], null, 0, 0, JunoRequest.OperationType.Destroy));
        }

        for(int i=0; i<loop; i++){

            //Create operation
            try{
                BatchTestSubscriber.async_dobatch(junoAsyncClient, createList);
            }catch (Exception e){ }

            //Get operation
            try{
                Thread.sleep(300);
                BatchTestSubscriber.async_dobatch(junoAsyncClient, getList);
            }catch (Exception e){ }

            //Set operation
            try{
                Thread.sleep(300);
                BatchTestSubscriber.async_dobatch(junoAsyncClient, setList);
            }catch (Exception e){ }

            //Update operation
            try{
                Thread.sleep(300);
                BatchTestSubscriber.async_dobatch(junoAsyncClient, updateList);
            }catch (Exception e){ }

            //Destroy operation
            try{
                Thread.sleep(300);
                BatchTestSubscriber.async_dobatch(junoAsyncClient, deleteList);
            }catch (Exception e){ }

            Thread.sleep(300);
        }
        Thread.sleep(1500);

        Iterator<MeterRegistry> registryIterator = Metrics.globalRegistry.getRegistries().iterator();
        while(registryIterator.hasNext()){
            MeterRegistry registry = registryIterator.next();
            if(registry instanceof  SimpleMeterRegistry){

                Timer createTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","B_CREATE").tag("status","SUCCESS").timer();
                Timer getTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","B_GET").tag("status","SUCCESS").timer();
                Timer setTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","B_SET").tag("status","SUCCESS").timer();
                Timer updateTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","B_UPDATE").tag("status","SUCCESS").timer();
                Timer deleteTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","B_DESTROY").tag("status","SUCCESS").timer();

                AssertJUnit.assertEquals(numKeys*loop, (int)createTimer.count(),1);
                AssertJUnit.assertEquals(numKeys*loop, (int)getTimer.count(),1);
                AssertJUnit.assertEquals(numKeys*loop, (int)setTimer.count(),1);
                AssertJUnit.assertEquals(numKeys*loop, (int)updateTimer.count(),1);
                AssertJUnit.assertEquals(numKeys*loop, (int)deleteTimer.count(),1);
            }
        }
    }

    @Test
    public void testAsyncZeroNullPayloadBatch() throws IOException, InterruptedException {
        LOGGER.info( "\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());

        int numKeys = 20;
        byte[][] key = new byte[numKeys][];
        byte[][] payload = new byte[numKeys][];
        List<JunoRequest> createList = new ArrayList<>();
        List<JunoRequest> getList = new ArrayList<>();
        List<JunoRequest> setList = new ArrayList<>();
        List<JunoRequest> updateList = new ArrayList<>();
        List<JunoRequest> deleteList = new ArrayList<>();
        for (int i = 0; i < numKeys; i ++) {
            payload[i] = DataGenUtils.createKey(10).getBytes();
            if ( i <= 1 ) {
                key[i] = DataGenUtils.createKey(25).getBytes();
            } else {
                if ( i % 2 == 0 ) {
                    key[i] = "".getBytes();
                } else {
                    key[i] = null;
                }
            }
            try {
                createList.add(new JunoRequest(key[i], payload[i], (long)0, 100, System.currentTimeMillis(), JunoRequest.OperationType.Create));
            } catch (Exception e) {}
            try {
                getList.add(new JunoRequest(key[i], 0, 100, JunoRequest.OperationType.Get));
            } catch (Exception e) {}
            try {
                setList.add(new JunoRequest(key[i], payload[i], (long)0, 100, System.currentTimeMillis(), JunoRequest.OperationType.Set));
            } catch (Exception e) {}
            try {
                updateList.add(new JunoRequest(key[i], payload[i], (long)0, 100, System.currentTimeMillis(), JunoRequest.OperationType.Update));
            } catch (Exception e) {}
            try {
                deleteList.add(new JunoRequest(key[i], null, 0, 0, JunoRequest.OperationType.Destroy));
            } catch (Exception e) {}
        }

        for(int i=0; i<loop; i++){

            //Create operation
            try{
                BatchTestSubscriber.async_dobatch(junoAsyncClient, createList);
            }catch (Exception e){ }

            //Get operation
            try{
                Thread.sleep(300);
                BatchTestSubscriber.async_dobatch(junoAsyncClient, getList);
            }catch (Exception e){ }

            //Set operation
            try{
                Thread.sleep(300);
                BatchTestSubscriber.async_dobatch(junoAsyncClient, setList);
            }catch (Exception e){ }

            //Update operation
            try{
                Thread.sleep(300);
                BatchTestSubscriber.async_dobatch(junoAsyncClient, updateList);
            }catch (Exception e){ }

            //Destroy operation
            try{
                Thread.sleep(100);
                BatchTestSubscriber.async_dobatch(junoAsyncClient, deleteList);
            }catch (Exception e){ }

            Thread.sleep(300);
        }
        Thread.sleep(1500);

        Iterator<MeterRegistry> registryIterator = Metrics.globalRegistry.getRegistries().iterator();
        while(registryIterator.hasNext()){
            MeterRegistry registry = registryIterator.next();
            if(registry instanceof  SimpleMeterRegistry){

                Timer createTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","B_CREATE").tag("status","SUCCESS").timer();
                Timer getTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","B_GET").tag("status","SUCCESS").timer();
                Timer setTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","B_SET").tag("status","SUCCESS").timer();
                Timer updateTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","B_UPDATE").tag("status","SUCCESS").timer();
                Timer deleteTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","B_DESTROY").tag("status","SUCCESS").timer();

                Counter createErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "B_CREATE").tag("status", "Illegal argument").tag("cause", "null_or_empty_key").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();
                Counter setErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "B_SET").tag("status", "Illegal argument").tag("cause", "null_or_empty_key").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();
                Counter updateErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "B_UPDATE").tag("status", "Illegal argument").tag("cause", "null_or_empty_key").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();
                Counter getErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "B_GET").tag("status", "Illegal argument").tag("cause", "null_or_empty_key").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();
                Counter destroyErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "B_DESTROY").tag("status", "Illegal argument").tag("cause", "null_or_empty_key").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();

                AssertJUnit.assertEquals(loop*2, (int)createTimer.count(),1);
                AssertJUnit.assertEquals(loop*2, (int)getTimer.count(),1);
                AssertJUnit.assertEquals(loop*2, (int)setTimer.count(),1);
                AssertJUnit.assertEquals(loop*2, (int)updateTimer.count(),1);
                AssertJUnit.assertEquals(loop*2, (int)deleteTimer.count(),1);

                AssertJUnit.assertEquals(loop*(numKeys-2), (int)createErr.count());
                AssertJUnit.assertEquals(loop*(numKeys-2), (int)getErr.count());
                AssertJUnit.assertEquals(loop*(numKeys-2), (int)setErr.count());
                AssertJUnit.assertEquals(loop*(numKeys-2), (int)updateErr.count());
                AssertJUnit.assertEquals(loop*(numKeys-2), (int)destroyErr.count());
            }
        }
    }

    @Test
    public void testAsyncMoreThan200PayloadBatch() throws IOException, InterruptedException {
        LOGGER.info( "\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());

        int numKeys = 20;
        byte[][] key = new byte[numKeys][];
        byte[][] payload = new byte[numKeys][];
        List<JunoRequest> createList = new ArrayList<>();
        List<JunoRequest> getList = new ArrayList<>();
        List<JunoRequest> setList = new ArrayList<>();
        List<JunoRequest> updateList = new ArrayList<>();
        List<JunoRequest> deleteList = new ArrayList<>();
        for (int i = 0; i < numKeys; i ++) {
            key[i] = DataGenUtils.createKey(10).getBytes();
            String str = DataGenUtils.createKey(25);
            payload[i] = DataGenUtils.genBytes(204801);
            payload[numKeys-1] = DataGenUtils.genBytes(20);
            createList.add(new JunoRequest(key[i], payload[i], (long)0, 100, System.currentTimeMillis(), JunoRequest.OperationType.Create));
            getList.add(new JunoRequest(key[i], 0, 100, JunoRequest.OperationType.Get));
            setList.add(new JunoRequest(key[i], payload[i], (long)0, 100, System.currentTimeMillis(), JunoRequest.OperationType.Set));
            updateList.add(new JunoRequest(key[i], payload[i], (long)0, 100, System.currentTimeMillis(), JunoRequest.OperationType.Update));
            deleteList.add(new JunoRequest(key[i], null, 0, 0, JunoRequest.OperationType.Destroy));
        }        for(int i=0; i<loop; i++){

            //Create operation
            try{
                BatchTestSubscriber.async_dobatch(junoAsyncClient, createList);
            }catch (Exception e){ }

            //Get operation
            try{
                Thread.sleep(300);
                BatchTestSubscriber.async_dobatch(junoAsyncClient, getList);
            }catch (Exception e){ }

            //Set operation
            try{
                Thread.sleep(300);
                BatchTestSubscriber.async_dobatch(junoAsyncClient, setList);
            }catch (Exception e){ }

            //Update operation
            try{
                Thread.sleep(300);
                BatchTestSubscriber.async_dobatch(junoAsyncClient, updateList);
            }catch (Exception e){ }

            //Destroy operation
            try{
                Thread.sleep(300);
                BatchTestSubscriber.async_dobatch(junoAsyncClient, deleteList);
            }catch (Exception e){ }

            Thread.sleep(300);
        }
        Thread.sleep(1500);

        Iterator<MeterRegistry> registryIterator = Metrics.globalRegistry.getRegistries().iterator();
        while(registryIterator.hasNext()){
            MeterRegistry registry = registryIterator.next();
            if(registry instanceof  SimpleMeterRegistry){

                Timer createTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","B_CREATE").tag("status","SUCCESS").timer();
                Timer getTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","B_GET").tag("status","SUCCESS").timer();
                Timer setTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","B_SET").tag("status","SUCCESS").timer();
                Timer updateTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","B_UPDATE").tag("status","SUCCESS").timer();
                Timer deleteTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","B_DESTROY").tag("status","SUCCESS").timer();

                Counter createExceedsPayloadErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "B_CREATE").tag("status", "Illegal argument").tag("cause", "payload_size_exceeded").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();
                Counter setExceedsPayloadErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "B_SET").tag("status", "Illegal argument").tag("cause", "payload_size_exceeded").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();
                Counter updateExceedsPayloadErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "B_UPDATE").tag("status", "Illegal argument").tag("cause", "payload_size_exceeded").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();

                AssertJUnit.assertEquals(loop, (int)createTimer.count(),1);
                AssertJUnit.assertEquals(loop*numKeys, (int)getTimer.count(),1);
                AssertJUnit.assertEquals(loop, (int)setTimer.count(),1);
                AssertJUnit.assertEquals(loop, (int)updateTimer.count(),1);
                AssertJUnit.assertEquals(loop*numKeys, (int)deleteTimer.count(),1);

                AssertJUnit.assertEquals(loop*(numKeys-1), (int)createExceedsPayloadErr.count());
                AssertJUnit.assertEquals(loop*(numKeys-1), (int)updateExceedsPayloadErr.count());
                AssertJUnit.assertEquals(loop*(numKeys-1), (int)setExceedsPayloadErr.count());
            }
        }
    }

    @Test
    public void testAsyncTTLMoreThan3DaysBatch() throws IOException, InterruptedException {//BUG, same as previous one
        LOGGER.info( "\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());

        int numKeys = 20;
        long[] ttl = new long[numKeys];
        byte[][] key = new byte[numKeys][];
        byte[][] payload = new byte[numKeys][];
        List<JunoRequest> createList = new ArrayList<>();
        List<JunoRequest> getList = new ArrayList<>();
        List<JunoRequest> setList = new ArrayList<>();
        List<JunoRequest> updateList = new ArrayList<>();
        List<JunoRequest> deleteList = new ArrayList<>();
        for (int i = 0; i < numKeys; i ++) {
            key[i] = DataGenUtils.createKey(10).getBytes();
            payload[i] = DataGenUtils.genBytes(20);
            if ( i < 5 ) {
                ttl[i] = 100;
            } else {
                if ( i % 3 == 0 ) {
                    ttl[i] = 0;
                } else if ( i % 3 == 1 ) {
                    ttl[i] = -1;
                } else {
                    ttl [i] = 259201;
                }
            }
            createList.add(new JunoRequest(key[i], payload[i], (long)0, ttl[i], System.currentTimeMillis(), JunoRequest.OperationType.Create));
            getList.add(new JunoRequest(key[i], 0, ttl[i], JunoRequest.OperationType.Get));
            setList.add(new JunoRequest(key[i], payload[i], (long)0, ttl[i], System.currentTimeMillis(), JunoRequest.OperationType.Set));
            updateList.add(new JunoRequest(key[i], payload[i], (long)0, ttl[i], System.currentTimeMillis(), JunoRequest.OperationType.Update));
            deleteList.add(new JunoRequest(key[i], null, 0, 0, JunoRequest.OperationType.Destroy));
        }

        for(int i=0; i<loop; i++){

            //Create operation
            try{
                BatchTestSubscriber.async_dobatch(junoAsyncClient, createList);
            }catch (Exception e){ }

            //Get operation
            try{
                Thread.sleep(300);
                BatchTestSubscriber.async_dobatch(junoAsyncClient, getList);
            }catch (Exception e){ }

            //Set operation
            try{
                Thread.sleep(300);
                BatchTestSubscriber.async_dobatch(junoAsyncClient, setList);
            }catch (Exception e){ }

            //Update operation
            try{
                Thread.sleep(300);
                BatchTestSubscriber.async_dobatch(junoAsyncClient, updateList);
            }catch (Exception e){ }

            //Destroy operation
            try{
                Thread.sleep(300);
                BatchTestSubscriber.async_dobatch(junoAsyncClient, deleteList);
            }catch (Exception e){ }

            Thread.sleep(300);
        }
        Thread.sleep(500);

        Iterator<MeterRegistry> registryIterator = Metrics.globalRegistry.getRegistries().iterator();
        while(registryIterator.hasNext()){
            MeterRegistry registry = registryIterator.next();
            if(registry instanceof  SimpleMeterRegistry){

                Timer createTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","B_CREATE").tag("status","SUCCESS").timer();
                Timer getTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","B_GET").tag("status","SUCCESS").timer();
                Timer setTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","B_SET").tag("status","SUCCESS").timer();
                Timer updateTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","B_UPDATE").tag("status","SUCCESS").timer();
                Timer deleteTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","B_DESTROY").tag("status","SUCCESS").timer();

                Counter createExceedsTTLErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "B_CREATE").tag("status", "Illegal argument").tag("cause", "ttl_exceeded_max").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();
                Counter setExceedsTTLErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "B_SET").tag("status", "Illegal argument").tag("cause", "ttl_exceeded_max").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();
                Counter updateExceedsTTLErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "B_UPDATE").tag("status", "Illegal argument").tag("cause", "ttl_exceeded_max").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();
                Counter getExceedsTTLErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "B_GET").tag("status", "Illegal argument").tag("cause", "ttl_exceeded_max").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();

                Counter createInvalidTTLErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "B_CREATE").tag("status", "Illegal argument").tag("cause", "invalid_ttl").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();
                Counter setNegTTLErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "B_SET").tag("status", "Illegal argument").tag("cause", "invalid_ttl").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();
                Counter updateNegTTLErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "B_UPDATE").tag("status", "Illegal argument").tag("cause", "invalid_ttl").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();
                Counter getNegTTLErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "B_GET").tag("status", "Illegal argument").tag("cause", "invalid_ttl").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();                AssertJUnit.assertEquals(loop*5, (int)createTimer.count(),1);
                AssertJUnit.assertEquals(loop*(numKeys/2), (int)getTimer.count(),1);
                AssertJUnit.assertEquals(loop*(numKeys/2), (int)setTimer.count(),1);
                AssertJUnit.assertEquals(loop*(numKeys/2), (int)updateTimer.count(),1);
                AssertJUnit.assertEquals(loop*numKeys, (int)deleteTimer.count(),1);

                AssertJUnit.assertEquals(loop*5, (int)createExceedsTTLErr.count());
                AssertJUnit.assertEquals(loop*5, (int)updateExceedsTTLErr.count());
                AssertJUnit.assertEquals(loop*5, (int)setExceedsTTLErr.count());
                AssertJUnit.assertEquals(loop*5, (int)getExceedsTTLErr.count());
                AssertJUnit.assertEquals(loop*5*2, (int)createInvalidTTLErr.count());
                AssertJUnit.assertEquals(loop*5, (int)updateNegTTLErr.count());
                AssertJUnit.assertEquals(loop*5, (int)setNegTTLErr.count());
                AssertJUnit.assertEquals(loop*5, (int)getNegTTLErr.count());
            }
        }
    }

    //   @Test
    public void testAsyncZeroNullKeyBatch() throws IOException, InterruptedException, CertificateException, KeyStoreException, NoSuchAlgorithmException, KeyManagementException {
        LOGGER.info( "\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());

        URL url1 = MetricTest.class.getResource("/com/paypal/juno/Juno.properties");
        Properties pConfig1 = new Properties();
        pConfig1.load(url1.openStream());
        pConfig1.setProperty(JunoProperties.MAX_CONNECTION_POOL_SIZE, "100");
        pConfig1.setProperty(JunoProperties.CONNECTION_POOL_SIZE, "100");
        JunoAsyncClient junoAsyncClient1 = JunoClientFactory.newJunoAsyncClient(new JunoPropertiesProvider(pConfig1), SSLUtil.getSSLContext());

        int numKeys = 20;
        byte[][] key = new byte[numKeys][];
        byte[][] payload = new byte[numKeys][];
        List<JunoRequest> createList = new ArrayList<>();
        List<JunoRequest> getList = new ArrayList<>();
        List<JunoRequest> setList = new ArrayList<>();
        List<JunoRequest> updateList = new ArrayList<>();
        List<JunoRequest> deleteList = new ArrayList<>();

        for (int i = 0; i < numKeys; i ++) {
            payload[i] = DataGenUtils.createKey(10).getBytes();
            String str = DataGenUtils.createKey(25);
            if ( i <= 1 ) {
                key[i] = DataGenUtils.createKey(25).getBytes();
            } else {
                if ( i % 2 == 0 ) {
                    key[i] = "".getBytes();
                } else {
                    key[i] = null;
                }
            }
            try{
                createList.add(new JunoRequest(key[i], payload[i], (long)0, 100, System.currentTimeMillis(), JunoRequest.OperationType.Create));
            }catch (Exception e) { }
            try {
                getList.add(new JunoRequest(key[i], 0, 100, JunoRequest.OperationType.Get));
            }catch (Exception e) { }
            try {
                setList.add(new JunoRequest(key[i], payload[i], (long)0, 100, System.currentTimeMillis(), JunoRequest.OperationType.Set));
            }catch (Exception e) { }
            try {
                updateList.add(new JunoRequest(key[i], payload[i], (long)0, 100, System.currentTimeMillis(), JunoRequest.OperationType.Update));
            }catch (Exception e) { }
            try {
                deleteList.add(new JunoRequest(key[i], null, 0, 0, JunoRequest.OperationType.Destroy));
            }catch (Exception e) { }
        }

        for(int i=0; i<loop; i++){

            //Create operation
            try{
                BatchTestSubscriber.async_dobatch(junoAsyncClient1, createList);
            }catch (Exception e){ }

            //Get operation
            try{
                Thread.sleep(300);
                BatchTestSubscriber.async_dobatch(junoAsyncClient1, getList);
            }catch (Exception e){ }

            //Set operation
            try{
                Thread.sleep(300);
                BatchTestSubscriber.async_dobatch(junoAsyncClient1, setList);
            }catch (Exception e){ }

            //Update operation
            try{
                Thread.sleep(300);
                BatchTestSubscriber.async_dobatch(junoAsyncClient1, updateList);
            }catch (Exception e){ }

            //Destroy operation
            try{
                Thread.sleep(300);
                BatchTestSubscriber.async_dobatch(junoAsyncClient1, deleteList);
            }catch (Exception e){ }

            Thread.sleep(300);
        }
        Thread.sleep(25000);

        Iterator<MeterRegistry> registryIterator = Metrics.globalRegistry.getRegistries().iterator();
        while(registryIterator.hasNext()){
            MeterRegistry registry = registryIterator.next();
            if(registry instanceof  SimpleMeterRegistry){

                Timer createTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","B_CREATE").tag("status","SUCCESS").timer();
                Timer getTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","B_GET").tag("status","SUCCESS").timer();
                Timer setTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","B_SET").tag("status","SUCCESS").timer();
                Timer updateTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","B_UPDATE").tag("status","SUCCESS").timer();
                Timer deleteTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","B_DESTROY").tag("status","SUCCESS").timer();

                Counter successEvtCnt = registry.find(JunoMetrics.CONNECT_METRIC).tag("endpoint", ipAddr+":"+port).tag("status", "SUCCESS").counter();

                Counter createErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "B_CREATE").tag("status", "Illegal argument").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();
                Counter setErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "B_SET").tag("status", "Illegal argument").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();
                Counter updateErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "B_UPDATE").tag("status", "Illegal argument").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();
                Counter getErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "B_GET").tag("status", "Illegal argument").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();
                Counter destroyErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "B_DESTROY").tag("status", "Illegal argument").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();

                AssertJUnit.assertEquals(loop*2, (int)createTimer.count(),1);
                AssertJUnit.assertEquals(loop*2, (int)getTimer.count(),1);
                AssertJUnit.assertEquals(loop*2, (int)setTimer.count(),1);
                AssertJUnit.assertEquals(loop*2, (int)updateTimer.count(),1);
                AssertJUnit.assertEquals(loop*2, (int)deleteTimer.count(),1);
                AssertJUnit.assertEquals(100, (int)successEvtCnt.count(), 98); //this value relates to connection pool and take time to get value
                AssertJUnit.assertEquals(loop*(numKeys-2), (int)createErr.count());
                AssertJUnit.assertEquals(loop*(numKeys-2), (int)getErr.count());
                AssertJUnit.assertEquals(loop*(numKeys-2), (int)setErr.count());
                AssertJUnit.assertEquals(loop*(numKeys-2), (int)updateErr.count());
                AssertJUnit.assertEquals(loop*(numKeys-2), (int)destroyErr.count());
            }
        }
    }

    @Test
    public void testReactSingleOperation() throws IOException, InterruptedException {
        LOGGER.info( "\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());

        junoReactClient.delete("test_key".getBytes());

        for(int i=0; i<loop; i++){
            byte[] key = DataGenUtils.genBytes(64);
            byte[] data = DataGenUtils.genBytes(10);

            //Create operation
            try{
                junoReactClient.create(key,data).block();
                junoReactClient.create(key,data).block();
            }catch (Exception e){ }

            //Create operation
            try{
                junoReactClient.get(key).block();
                junoReactClient.get("test_key".getBytes()).block();
            }catch (Exception e){ }

            //Create operation
            try{
                junoReactClient.set(key,data).block();
            }catch (Exception e){ }

            //Create operation
            try{
                junoReactClient.update(key,data).block();
                junoReactClient.update("test_key".getBytes(),data).block();
            }catch (Exception e){ }

            //Create operation
            try{
                junoReactClient.delete(key).block();
            }catch (Exception e){ }

            Thread.sleep(300);
        }

        Iterator<MeterRegistry> registryIterator = Metrics.globalRegistry.getRegistries().iterator();
        while(registryIterator.hasNext()){
            MeterRegistry registry = registryIterator.next();
            if(registry instanceof  SimpleMeterRegistry){
                Timer createTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","CREATE").tag("status","SUCCESS").timer();
                Timer getTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","GET").tag("status","SUCCESS").timer();
                Timer setTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","SET").tag("status","SUCCESS").timer();
                Timer updateTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","UPDATE").tag("status","SUCCESS").timer();
                Timer deleteTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","DESTROY").tag("status","SUCCESS").timer();
                Counter uniqueErrCnt = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("status", "Duplicate key").tag("status", OperationStatus.UniqueKeyViolation.getErrorText()).counter();
                Counter keyNotFoundCnt = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("status", "Key not found").tag("status", OperationStatus.NoKey.getErrorText()).counter();

                AssertJUnit.assertEquals(loop*2, (int)createTimer.count(), 1);
                AssertJUnit.assertEquals(loop*2, (int)getTimer.count(),1);
                AssertJUnit.assertEquals(loop, (int)setTimer.count(),1);
                AssertJUnit.assertEquals(loop*2, (int)updateTimer.count(),1);
                AssertJUnit.assertEquals(loop, (int)deleteTimer.count(),1);
                AssertJUnit.assertEquals(loop, (int)uniqueErrCnt.count(), 1);
                AssertJUnit.assertEquals(loop, (int)keyNotFoundCnt.count(), 1);
            }
        }
    }

    //    @Test
    public void testReactNullEmptyKeyNullPayload() throws IOException, InterruptedException {
        LOGGER.info( "\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());

        byte[] key = null;
        byte[] data = DataGenUtils.genBytes(10);
        byte[] data1 = null;
        byte[] key2 = "".getBytes();

        for(int i=0; i<loop; i++){

            byte[] key1 = DataGenUtils.genBytes(10);

            //Create operation
            try{
                junoReactClient.create(key,data).block();	//null key
            }catch (Exception e){ }

            try{
                junoReactClient.create(key1, data1).block();	//null payload
            }catch (Exception e){ }

            try{
                junoReactClient.create(key2, data).block(); //empty key
            }catch (Exception e){ }

            Thread.sleep(300);
            //Get operation
            try{
                junoReactClient.get(key).block();
            }catch (Exception e){ }

            try{
                junoReactClient.get(key2).block();
            }catch (Exception e){ }

            //Update operation
            try{
                junoReactClient.update(key, data).block();
            }catch (Exception e){ }

            try{
                junoReactClient.update(key1, data1).block();
            }catch (Exception e){ }

            try{
                junoReactClient.update(key2, data).block();
            }catch (Exception e){ }

            Thread.sleep(300);
            //Set operation
            try{
                junoReactClient.set(key, data).block();
            }catch (Exception e){ }

            try{
                junoReactClient.set(key1, data1).block();
            }catch (Exception e){ }

            try{
                junoReactClient.set(key2, data).block();
            }catch (Exception e){ }

            Thread.sleep(300);
            //Destroy operation
            try{
                junoReactClient.delete(key2).block();
            }catch (Exception e){ }

            Thread.sleep(100);
        }
        Thread.sleep(25000);

        Iterator<MeterRegistry> registryIterator = Metrics.globalRegistry.getRegistries().iterator();
        while(registryIterator.hasNext()){
            MeterRegistry registry = registryIterator.next();
            if(registry instanceof  SimpleMeterRegistry){

                Timer createTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","CREATE").tag("status","SUCCESS").timer();
                Timer createErrTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","CREATE").tag("status","ERROR").timer();
                Timer getTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","GET").tag("status","ERROR").timer();
                Timer setTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","SET").tag("status","SUCCESS").timer();
                Timer setErrTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","SET").tag("status","ERROR").timer();
                Timer updateTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","UPDATE").tag("status","SUCCESS").timer();
                Timer updateErrTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","UPDATE").tag("status","ERROR").timer();
                Timer deleteTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","DESTROY").tag("status","ERROR").timer();

                Counter createErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "CREATE").tag("status", "Illegal argument").tag("cause", "null_or_empty_key").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();
                Counter setErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "SET").tag("status", "Illegal argument").tag("cause", "null_or_empty_key").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();
                Counter updateErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "UPDATE").tag("status", "Illegal argument").tag("cause", "null_or_empty_key").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();
                Counter getErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "GET").tag("status", "Illegal argument").tag("cause", "null_or_empty_key").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();
                Counter destroyErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "DESTROY").tag("status", "Illegal argument").tag("cause", "null_or_empty_key").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();

                AssertJUnit.assertEquals(loop, (int)createTimer.count(), 1);
                AssertJUnit.assertEquals(loop*2, (int)createErrTimer.count(), 1);
                AssertJUnit.assertEquals(loop*2, (int)getTimer.count(), 1);
                AssertJUnit.assertEquals(loop, (int)setTimer.count(), 1);
                AssertJUnit.assertEquals(loop*2, (int)setErrTimer.count(), 1);
                AssertJUnit.assertEquals(loop, (int)updateTimer.count(), 1);
                AssertJUnit.assertEquals(loop*2, (int)updateErrTimer.count(), 1);
                AssertJUnit.assertEquals(loop, (int)deleteTimer.count(), 1);

                AssertJUnit.assertEquals(loop*2, (int)createErr.count());
                AssertJUnit.assertEquals(loop*2, (int)getErr.count());
                AssertJUnit.assertEquals(loop*2, (int)setErr.count());
                AssertJUnit.assertEquals(loop*2, (int)updateErr.count());
                AssertJUnit.assertEquals(loop, (int)destroyErr.count());
            }
        }
    }

    //    @Test
    public void testReactExceedsSize() throws IOException, InterruptedException, CertificateException, KeyStoreException, NoSuchAlgorithmException, KeyManagementException {
        LOGGER.info( "\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());

        URL url1 = MetricTest.class.getResource("/com/paypal/juno/Juno.properties");
        Properties pConfig1 = new Properties();
        pConfig1.load(url1.openStream());
        pConfig1.setProperty(JunoProperties.MAX_CONNECTION_POOL_SIZE, "100");
        pConfig1.setProperty(JunoProperties.CONNECTION_POOL_SIZE, "100");
        JunoReactClient junoReactClient1 = JunoClientFactory.newJunoReactClient(new JunoPropertiesProvider(pConfig1), SSLUtil.getSSLContext());

        byte[] key = DataGenUtils.genBytes(257);
        byte[] data = DataGenUtils.genBytes(10);
        byte[] key1 = DataGenUtils.genBytes(25);
        byte[] data1 = DataGenUtils.genBytes(2048011);

        for(int i=0; i<loop; i++){

            //Create operation
            try{
                junoReactClient1.create(key,data).block(); //key exceeds size
            }catch (Exception e){ }

            try{
                junoReactClient1.create(key1,data1).block();  //payload exceeds size
            }catch (Exception e){ }

            //Get operation
            try{
                junoReactClient1.get(key).block();
            }catch (Exception e){ }

            //Update operation
            try{
                junoReactClient1.update(key, data).block();
            }catch (Exception e){ }

            try{
                junoReactClient1.update(key1, data1).block();
            }catch (Exception e){ }

            //Set operation
            try{
                junoReactClient1.set(key,data).block();
            }catch (Exception e){ }

            try{
                junoReactClient1.set(key1,data1).block();
            }catch (Exception e){ }

            //Destroy operation
            try{
                junoReactClient1.delete(key).block();
            }catch (Exception e){ }

            Thread.sleep(100);
        }
        Thread.sleep(25000);

        Iterator<MeterRegistry> registryIterator = Metrics.globalRegistry.getRegistries().iterator();
        while(registryIterator.hasNext()){
            MeterRegistry registry = registryIterator.next();
            if(registry instanceof  SimpleMeterRegistry){
                Timer createTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","CREATE").tag("status","ERROR").timer();
                Timer getTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","GET").tag("status","ERROR").timer();
                Timer setTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","SET").tag("status","ERROR").timer();
                Timer updateTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","UPDATE").tag("status","ERROR").timer();
                Timer deleteTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","DESTROY").tag("status","ERROR").timer();

                Counter successEvtCnt = registry.find(JunoMetrics.CONNECT_METRIC).tag("endpoint", ipAddr+":"+port).tag("status", "SUCCESS").counter();

                Counter createKeyErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "CREATE").tag("status", "Illegal argument").tag("cause", "key_size_exceeded").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();
                Counter createPayloadErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "CREATE").tag("status", "Illegal argument").tag("cause", "payload_size_exceeded").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();

                Counter setKeyErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "SET").tag("status", "Illegal argument").tag("cause", "key_size_exceeded").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();
                Counter setPayloadErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "SET").tag("status", "Illegal argument").tag("cause", "payload_size_exceeded").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();

                Counter updateKeyErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "UPDATE").tag("status", "Illegal argument").tag("cause", "key_size_exceeded").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();
                Counter updatePayloadErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "UPDATE").tag("status", "Illegal argument").tag("cause", "payload_size_exceeded").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();

                Counter getKeyErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "GET").tag("status", "Illegal argument").tag("cause", "key_size_exceeded").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();
                Counter destroyKeyErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "DESTROY").tag("status", "Illegal argument").tag("cause", "payload_size_exceeded").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();                Thread.sleep(2000);
                AssertJUnit.assertEquals(loop*2, (int)createTimer.count(), 2);
                AssertJUnit.assertEquals(loop, (int)getTimer.count(), 2);
                AssertJUnit.assertEquals(loop*2, (int)setTimer.count(), 2);
                AssertJUnit.assertEquals(loop*2, (int)updateTimer.count(), 2);
                AssertJUnit.assertEquals(loop, (int)deleteTimer.count(), 2);

                AssertJUnit.assertEquals(100, (int)successEvtCnt.count(), 98); //this value relates to connection pool and take time to get value

                AssertJUnit.assertEquals(loop, (int)createKeyErr.count());
                AssertJUnit.assertEquals(loop, (int)createPayloadErr.count());
                AssertJUnit.assertEquals(loop, (int)getKeyErr.count());
                AssertJUnit.assertEquals(loop, (int)setKeyErr.count());
                AssertJUnit.assertEquals(loop, (int)setPayloadErr.count());
                AssertJUnit.assertEquals(loop, (int)updateKeyErr.count());
                AssertJUnit.assertEquals(loop, (int)updatePayloadErr.count());
                AssertJUnit.assertEquals(loop, (int)destroyKeyErr.count());
            }
        }
    }

    @Test
    public void testReactInvalidLifetime() throws IOException, InterruptedException {
        LOGGER.info( "\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());

        byte[] key = DataGenUtils.genBytes(25);
        byte[] data = DataGenUtils.genBytes(10);

        for(int i=0; i<loop; i++){

            //Create operation
            try{
                junoReactClient.create(key, data, (long)0).block();
            }catch (Exception e){ }

            try{
                junoReactClient.create(key, data, (long)-1).block();
            }catch (Exception e){ }

            try{
                junoReactClient.create(key, data, (long)7770000).block(); //exceeds max lifetime
            }catch (Exception e){ }

            //Get operation
            try{
                junoReactClient.create(key, data, (long)5).block();
                junoReactClient.get(key, (long)0).block();
            }catch (Exception e){ }

            try{
                junoReactClient.get(key, (long)-1).block();
            }catch (Exception e){ }

            try{
                junoReactClient.get(key, (long)7770000).block();
            }catch (Exception e){ }

            //Update operation
            try{
                junoReactClient.update(key, data, (long)0).block();
            }catch (Exception e){ }

            try{
                junoReactClient.update(key, data, (long)-1).block();
            }catch (Exception e){ }

            try{
                junoReactClient.update(key, data, (long)7770000).block();
            }catch (Exception e){ }

            //Set operation
            try{
                junoReactClient.set(key, data, (long)0).block();
            }catch (Exception e){ }

            try{
                junoReactClient.set(key, data, (long)-1).block();
            }catch (Exception e){ }

            try{
                junoReactClient.set(key, data, (long)7770000).block();
            }catch (Exception e){ }

            //Destroy operation
            try{
                junoReactClient.delete(key).block();
            }catch (Exception e){ }

            Thread.sleep(100);
        }

        Iterator<MeterRegistry> registryIterator = Metrics.globalRegistry.getRegistries().iterator();
        while(registryIterator.hasNext()){
            MeterRegistry registry = registryIterator.next();
            if(registry instanceof  SimpleMeterRegistry){
                Timer createTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","CREATE").tag("status","SUCCESS").timer();
                Timer getTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","GET").tag("status","SUCCESS").timer();
                Timer setTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","SET").tag("status","SUCCESS").timer();
                Timer updateTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","UPDATE").tag("status","SUCCESS").timer();
                Timer deleteTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","DESTROY").tag("status","SUCCESS").timer();

                Counter createInvlidTTLErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "CREATE").tag("status", "Illegal argument").tag("cause", "invalid_ttl").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();
                Counter createExceedsTTLErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "CREATE").tag("status", "Illegal argument").tag("cause", "ttl_exceeded_max").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();

                Counter getNegTTLErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "GET").tag("status", "Illegal argument").tag("cause", "invalid_ttl").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();
                Counter getExceedsTTLErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "GET").tag("status", "Illegal argument").tag("cause", "ttl_exceeded_max").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();

                Counter setNegTTLErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "SET").tag("status", "Illegal argument").tag("cause", "invalid_ttl").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();
                Counter setExceedsTTLErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "SET").tag("status", "Illegal argument").tag("cause", "ttl_exceeded_max").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();

                Counter updateNegTTLErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "UPDATE").tag("status", "Illegal argument").tag("cause", "invalid_ttl").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();
                Counter updateExceedsTTLErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "UPDATE").tag("status", "Illegal argument").tag("cause", "ttl_exceeded_max").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();

                AssertJUnit.assertEquals(loop, (int)createTimer.count(), 1);
                AssertJUnit.assertEquals(loop, (int)getTimer.count(), 1);
                AssertJUnit.assertEquals(loop, (int)setTimer.count(), 1);
                AssertJUnit.assertEquals(loop, (int)updateTimer.count(), 1);
                AssertJUnit.assertEquals(loop, (int)deleteTimer.count(), 1);

                AssertJUnit.assertEquals(2 * loop, (int)createInvlidTTLErr.count());
                AssertJUnit.assertEquals(loop, (int)createExceedsTTLErr.count());
                AssertJUnit.assertEquals(loop, (int)getNegTTLErr.count());
                AssertJUnit.assertEquals(loop, (int)getExceedsTTLErr.count());
                AssertJUnit.assertEquals(loop, (int)setNegTTLErr.count());
                AssertJUnit.assertEquals(loop, (int)setExceedsTTLErr.count());
                AssertJUnit.assertEquals(loop, (int)updateNegTTLErr.count());
                AssertJUnit.assertEquals(loop, (int)updateExceedsTTLErr.count());
            }
        }
    }

    //    @Test
    public void testReactTimeoutMetric() throws IOException, InterruptedException, CertificateException, KeyStoreException, NoSuchAlgorithmException, KeyManagementException {
        LOGGER.info( "\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());

        //response timeout config
        URL url2 = MetricTest.class.getResource("/com/paypal/juno/Juno.properties");
        Properties pConfig2 = new Properties();
        pConfig2.load(url2.openStream());
        pConfig2.setProperty(JunoProperties.APP_NAME, "QATestApp2");
        pConfig2.setProperty(JunoProperties.RECORD_NAMESPACE, "MetricNS2");
        pConfig2.setProperty(JunoProperties.RESPONSE_TIMEOUT, "1");
        JunoReactClient junoReactClient2 = JunoClientFactory.newJunoReactClient(new JunoPropertiesProvider(pConfig2), SSLUtil.getSSLContext());

        for(int i=0; i<loop; i++){

            byte[] key = DataGenUtils.genBytes(25);
            byte[] data = DataGenUtils.genBytes(10);

            //Create operation, response timeout
            try{
                junoReactClient2.create(key, data).block();
            }catch (Exception e){ }

            Thread.sleep(100);
        }
        Thread.sleep(25000);

        Iterator<MeterRegistry> registryIterator = Metrics.globalRegistry.getRegistries().iterator();
        while(registryIterator.hasNext()){
            MeterRegistry registry = registryIterator.next();
            if(registry instanceof  SimpleMeterRegistry){

                Timer createTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","CREATE").tag("status","ERROR").timer();

                Thread.sleep(1500);
                Counter createRespTimeoutErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "CREATE").tag("status", "Response Timed out").tag("status", OperationStatus.ResponseTimeout.getErrorText()).counter();

                Counter createEventErr = registry.find(JunoMetrics.ERROR_METRIC).tag("type", "JUNO_LATE_RESPONSE").tag("cause", "ERROR").counter();

                AssertJUnit.assertEquals(loop, (int)createTimer.count(), 1);
                AssertJUnit.assertEquals(loop, (int)createEventErr.count());
                AssertJUnit.assertEquals(loop, (int)createRespTimeoutErr.count());
            }
        }
    }

    @Test
    public void testReactWrongIpPortMetric() throws IOException, InterruptedException, CertificateException, KeyStoreException, NoSuchAlgorithmException, KeyManagementException {
        LOGGER.info( "\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());

        //Wrong ip port test config
        URL url3 = MetricTest.class.getResource("/com/paypal/juno/Juno.properties");
        Properties pConfig3 = new Properties();
        pConfig3.load(url3.openStream());
        pConfig3.setProperty(JunoProperties.APP_NAME, "QATestApp3");
        pConfig3.setProperty(JunoProperties.RECORD_NAMESPACE, "MetricWrongIpPort");
        pConfig3.setProperty(JunoProperties.HOST, "123.456.789.000");
        pConfig3.setProperty(JunoProperties.PORT, "6080");
        pConfig3.setProperty(JunoProperties.MAX_CONNECTION_POOL_SIZE, "200");
        pConfig3.setProperty(JunoProperties.CONNECTION_POOL_SIZE, "200");
        JunoReactClient junoReactClient3 = JunoClientFactory.newJunoReactClient(new JunoPropertiesProvider(pConfig3), SSLUtil.getSSLContext());

        for(int i=0; i<loop; i++){

            byte[] key = DataGenUtils.genBytes(25);
            byte[] data = DataGenUtils.genBytes(10);

            //Create operation, response timeout
            try{
                junoReactClient3.create(key, data).block();
            }catch (Exception e){ }

            Thread.sleep(300);
        }
        Thread.sleep(300);
        Iterator<MeterRegistry> registryIterator = Metrics.globalRegistry.getRegistries().iterator();
        while(registryIterator.hasNext()){
            MeterRegistry registry = registryIterator.next();
            if(registry instanceof  SimpleMeterRegistry){

                Timer createTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","CREATE").tag("pool","123.456.789.000:6080").tag("status","ERROR").timer();
                Counter createConnTimeoutOpStatusErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool", "123.456.789.000:6080").tag("type","CREATE").tag("status", "Connection Error").tag("status", OperationStatus.ConnectionError.getErrorText()).counter();
                Counter createConnTimeoutEventErr = registry.find(JunoMetrics.CONNECT_METRIC).tag("endpoint","123.456.789.000:6080").tag("status", JunoMetrics.ERROR).tag("cause", "java.net.UnknownHostException").counter();

                AssertJUnit.assertEquals(loop, (int)createTimer.count(),1);
                AssertJUnit.assertEquals(loop, (int)createConnTimeoutOpStatusErr.count(),1);
                //1200 is related to the time, it does not related to numKeys or loop, it's ok as long as we see error
                AssertJUnit.assertEquals(1200, (int)createConnTimeoutEventErr.count(), 200);
            }
        }
    }

    @Test
    public void testReactDownIpPortMetrics() throws IOException, InterruptedException, CertificateException, KeyStoreException, NoSuchAlgorithmException, KeyManagementException {
        LOGGER.info( "\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());

        //Wrong ip port test config
        URL url3 = MetricTest.class.getResource("/com/paypal/juno/Juno.properties");
        Properties pConfig3 = new Properties();
        pConfig3.load(url3.openStream());
        pConfig3.setProperty(JunoProperties.APP_NAME, "QATestApp3");
        pConfig3.setProperty(JunoProperties.RECORD_NAMESPACE, "MetricWrongIpPort");
        pConfig3.setProperty(JunoProperties.HOST, "10.176.18.190");
        pConfig3.setProperty(JunoProperties.PORT, "5080");
        pConfig3.setProperty(JunoProperties.MAX_CONNECTION_POOL_SIZE, "200");
        pConfig3.setProperty(JunoProperties.CONNECTION_POOL_SIZE, "200");
        String ip3 = pConfig3.getProperty("juno.server.host");
        String port3 = pConfig3.getProperty("juno.server.port");
        JunoReactClient junoReactClient3 = JunoClientFactory.newJunoReactClient(new JunoPropertiesProvider(pConfig3), SSLUtil.getSSLContext());

        Thread.sleep(300);
        for(int i=0; i< 100; i++){

            byte[] key = DataGenUtils.genBytes(25);
            byte[] data = DataGenUtils.genBytes(10);

            //Create operation, response timeout
            try{
                junoReactClient3.create(key, data).block();
            }catch (Exception e){ }

            Thread.sleep(100);
        }

        Iterator<MeterRegistry> registryIterator = Metrics.globalRegistry.getRegistries().iterator();
        while(registryIterator.hasNext()){
            MeterRegistry registry = registryIterator.next();
            if(registry instanceof  SimpleMeterRegistry){

                Timer createTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","CREATE").tag("pool","10.176.18.190:5080").tag("status","ERROR").timer();
                //Timer createSuccessTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","CREATE").tag("status","SUCCESS").timer();
                Counter createTimeoutOpStatusErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool", ip3+":"+port3).tag("type","CREATE").tag("status", "Connection Error").tag("status", OperationStatus.ConnectionError.getErrorText()).counter();

                //System.out.println(" Create Success Count:"+createSuccessTimer.count());
                AssertJUnit.assertEquals(100, (int)createTimer.count(),1);
                AssertJUnit.assertEquals(100, (int)createTimeoutOpStatusErr.count(),1);
            }
        }
    }

    @Test
    public void testReactBatchOperations() throws IOException, InterruptedException {
        LOGGER.info( "\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());

        int numKeys = 20;
        byte[][] key = new byte[numKeys][];
        byte[][] payload = new byte[numKeys][];
        LOGGER.debug("Create " + numKeys + " keys using batch Create");
        List<JunoRequest> createList = new ArrayList<>();
        List<JunoRequest> getList = new ArrayList<>();
        List<JunoRequest> setList = new ArrayList<>();
        List<JunoRequest> updateList = new ArrayList<>();
        List<JunoRequest> deleteList = new ArrayList<>();
        for (int i = 0; i < numKeys; i ++) {
            key[i] = DataGenUtils.createKey(10).getBytes();
            Random r = new Random();
            int payloadlen = DataGenUtils.rand(r, 200, 2047);
            payload[i] = DataGenUtils.createKey(payloadlen).getBytes();
            createList.add(new JunoRequest(key[i], payload[i], (long)0, 100, System.currentTimeMillis(), JunoRequest.OperationType.Create));
            getList.add(new JunoRequest(key[i], 0, 100, JunoRequest.OperationType.Get));
            setList.add(new JunoRequest(key[i], payload[i], (long)0, 100, System.currentTimeMillis(), JunoRequest.OperationType.Set));
            updateList.add(new JunoRequest(key[i], payload[i], (long)0, 100, System.currentTimeMillis(), JunoRequest.OperationType.Update));
            deleteList.add(new JunoRequest(key[i], null, 0, 0, JunoRequest.OperationType.Destroy));
        }        for(int i=0; i<loop; i++){

            //Create operation
            try{
                junoReactClient.doBatch(createList).blockLast();
            }catch (Exception e){ }

            //Get operation
            try{
                junoReactClient.doBatch(getList).blockLast();
            }catch (Exception e){ }

            //Set operation
            try{
                junoReactClient.doBatch(setList).blockLast();
            }catch (Exception e){ }

            //Update operation
            try{
                junoReactClient.doBatch(updateList).blockLast();
            }catch (Exception e){ }

            //Destroy operation
            try{
                junoReactClient.doBatch(deleteList).blockLast();
            }catch (Exception e){ }

            Thread.sleep(100);
        }
        Thread.sleep(1000);

        Iterator<MeterRegistry> registryIterator = Metrics.globalRegistry.getRegistries().iterator();
        while(registryIterator.hasNext()){
            MeterRegistry registry = registryIterator.next();
            if(registry instanceof  SimpleMeterRegistry){

                Timer createTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","B_CREATE").tag("status","SUCCESS").timer();
                Timer getTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","B_GET").tag("status","SUCCESS").timer();
                Timer setTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","B_SET").tag("status","SUCCESS").timer();
                Timer updateTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","B_UPDATE").tag("status","SUCCESS").timer();
                Timer deleteTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","B_DESTROY").tag("status","SUCCESS").timer();

                AssertJUnit.assertEquals(loop*numKeys, createTimer.count(),10);
                AssertJUnit.assertEquals(loop*numKeys, getTimer.count(),10);
                AssertJUnit.assertEquals(loop*numKeys, setTimer.count(),10);
                AssertJUnit.assertEquals(loop*numKeys, updateTimer.count(),10);
                AssertJUnit.assertEquals(loop*numKeys, deleteTimer.count(),10);
            }
        }
    }

    @Test
    public void testReactZeroNullPayloadBatch() throws IOException, InterruptedException {
        LOGGER.info( "\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());

        int numKeys = 20;
        byte[][] key = new byte[numKeys][];
        byte[][] payload = new byte[numKeys][];
        List<JunoRequest> createList = new ArrayList<>();
        List<JunoRequest> getList = new ArrayList<>();
        List<JunoRequest> setList = new ArrayList<>();
        List<JunoRequest> updateList = new ArrayList<>();
        List<JunoRequest> deleteList = new ArrayList<>();
        for (int i = 0; i < numKeys; i ++) {
            key[i] = DataGenUtils.createKey(10).getBytes();
            String str = DataGenUtils.createKey(25);
            payload[i] = str.getBytes();
            payload[5] = "".getBytes();
            payload[19]=null;
            createList.add(new JunoRequest(key[i], payload[i], (long)0, 100, System.currentTimeMillis(), JunoRequest.OperationType.Create));
            getList.add(new JunoRequest(key[i], 0, 100, JunoRequest.OperationType.Get));
            setList.add(new JunoRequest(key[i], payload[i], (long)0, 100, System.currentTimeMillis(), JunoRequest.OperationType.Set));
            updateList.add(new JunoRequest(key[i], payload[i], (long)0, 100, System.currentTimeMillis(), JunoRequest.OperationType.Update));
            deleteList.add(new JunoRequest(key[i], null, 0, 0, JunoRequest.OperationType.Destroy));
        }

        for(int i=0; i<loop; i++){

            //Create operation
            try{
                junoReactClient.doBatch(createList).blockLast();
            }catch (Exception e){ }

            //Get operation
            try{
                junoReactClient.doBatch(getList).blockLast();
            }catch (Exception e){ }

            //Set operation
            try{
                junoReactClient.doBatch(setList).blockLast();
            }catch (Exception e){ }

            //Update operation
            try{
                junoReactClient.doBatch(updateList).blockLast();
            }catch (Exception e){ }

            //Destroy operation
            try{
                junoReactClient.doBatch(deleteList).blockLast();
            }catch (Exception e){ }

            Thread.sleep(100);
        }
        Thread.sleep(1500);

        Iterator<MeterRegistry> registryIterator = Metrics.globalRegistry.getRegistries().iterator();
        while(registryIterator.hasNext()){
            MeterRegistry registry = registryIterator.next();
            if(registry instanceof  SimpleMeterRegistry){

                Timer createTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","B_CREATE").tag("status","SUCCESS").timer();
                Timer getTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","B_GET").tag("status","SUCCESS").timer();
                Timer setTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","B_SET").tag("status","SUCCESS").timer();
                Timer updateTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","B_UPDATE").tag("status","SUCCESS").timer();
                Timer deleteTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","B_DESTROY").tag("status","SUCCESS").timer();

                AssertJUnit.assertEquals(loop*numKeys, (int)createTimer.count(),1);
                AssertJUnit.assertEquals(loop*numKeys, (int)getTimer.count(),1);
                AssertJUnit.assertEquals(loop*numKeys, (int)setTimer.count(),1);
                AssertJUnit.assertEquals(loop*numKeys, (int)updateTimer.count(),1);
                AssertJUnit.assertEquals(loop*numKeys, (int)deleteTimer.count(),1);
            }
        }
    }

    @Test
    public void testReactMoreThan200PayloadBatch() throws IOException, InterruptedException {
        LOGGER.info( "\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());

        int numKeys = 20;
        byte[][] key = new byte[numKeys][];
        byte[][] payload = new byte[numKeys][];
        List<JunoRequest> createList = new ArrayList<>();
        List<JunoRequest> getList = new ArrayList<>();
        List<JunoRequest> setList = new ArrayList<>();
        List<JunoRequest> updateList = new ArrayList<>();
        List<JunoRequest> deleteList = new ArrayList<>();
        for (int i = 0; i < numKeys; i ++) {
            key[i] = DataGenUtils.createKey(10).getBytes();
            String str = DataGenUtils.createKey(25);
            payload[i] = DataGenUtils.genBytes(20);
            payload[numKeys-1] = DataGenUtils.genBytes(204801);
            createList.add(new JunoRequest(key[i], payload[i], (long)0, 100, System.currentTimeMillis(), JunoRequest.OperationType.Create));
            getList.add(new JunoRequest(key[i], 0, 100, JunoRequest.OperationType.Get));
            setList.add(new JunoRequest(key[i], payload[i], (long)0, 100, System.currentTimeMillis(), JunoRequest.OperationType.Set));
            updateList.add(new JunoRequest(key[i], payload[i], (long)0, 100, System.currentTimeMillis(), JunoRequest.OperationType.Update));
            deleteList.add(new JunoRequest(key[i], null, 0, 0, JunoRequest.OperationType.Destroy));
        }

        for(int i=0; i<loop; i++){

            //Create operation
            try{
                junoReactClient.doBatch(createList).blockLast();
            }catch (Exception e){ }

            //Get operation
            try{
                junoReactClient.doBatch(getList).blockLast();
            }catch (Exception e){ }

            //Set operation
            try{
                junoReactClient.doBatch(setList).blockLast();
            }catch (Exception e){ }

            //Update operation
            try{
                junoReactClient.doBatch(updateList).blockLast();
            }catch (Exception e){ }

            //Destroy operation
            try{
                junoReactClient.doBatch(deleteList).blockLast();
            }catch (Exception e){ }

            Thread.sleep(100);
        }
        Thread.sleep(1500);

        Iterator<MeterRegistry> registryIterator = Metrics.globalRegistry.getRegistries().iterator();
        while(registryIterator.hasNext()){
            MeterRegistry registry = registryIterator.next();
            if(registry instanceof  SimpleMeterRegistry){

                Timer createTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","B_CREATE").tag("status","SUCCESS").timer();
                Timer getTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","B_GET").tag("status","SUCCESS").timer();
                Timer setTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","B_SET").tag("status","SUCCESS").timer();
                Timer updateTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","B_UPDATE").tag("status","SUCCESS").timer();
                Timer deleteTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","B_DESTROY").tag("status","SUCCESS").timer();

                Counter createExceedsPayloadErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "B_CREATE").tag("status", "Illegal argument").tag("cause", "payload_size_exceeded").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();

                Counter setExceedsPayloadErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "B_SET").tag("status", "Illegal argument").tag("cause", "payload_size_exceeded").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();

                Counter updateExceedsPayloadErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "B_UPDATE").tag("status", "Illegal argument").tag("cause", "payload_size_exceeded").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();

                AssertJUnit.assertEquals(loop*(numKeys-1), (int)createTimer.count(),1);
                AssertJUnit.assertEquals(loop*numKeys, (int)getTimer.count(),1);
                AssertJUnit.assertEquals(loop*(numKeys-1), (int)setTimer.count(),1);
                AssertJUnit.assertEquals(loop*(numKeys-1), (int)updateTimer.count(),1);
                AssertJUnit.assertEquals(loop*numKeys, (int)deleteTimer.count(),1);

                AssertJUnit.assertEquals(loop, (int)createExceedsPayloadErr.count());
                AssertJUnit.assertEquals(loop, (int)updateExceedsPayloadErr.count());
                AssertJUnit.assertEquals(loop, (int)setExceedsPayloadErr.count());
            }
        }
    }

    @Test
    public void testReactTTLMoreThan3DaysBatch() throws IOException, InterruptedException {
        LOGGER.info( "\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());

        int numKeys = 20;
        long[] ttl = new long[numKeys];
        byte[][] key = new byte[numKeys][];
        byte[][] payload = new byte[numKeys][];
        List<JunoRequest> createList = new ArrayList<>();
        List<JunoRequest> getList = new ArrayList<>();
        List<JunoRequest> setList = new ArrayList<>();
        List<JunoRequest> updateList = new ArrayList<>();
        List<JunoRequest> deleteList = new ArrayList<>();
        for (int i = 0; i < numKeys; i ++) {
            key[i] = DataGenUtils.createKey(10).getBytes();
            payload[i] = DataGenUtils.genBytes(20);
            if ( i < 5 ) {
                ttl[i] = 100;
            } else {
                if ( i % 3 == 0 ) {
                    ttl[i] = 0;
                } else if ( i % 3 == 1 ) {
                    ttl[i] = -1;
                } else {
                    ttl [i] = 259201;
                }
            }
            createList.add(new JunoRequest(key[i], payload[i], (long)0, ttl[i], System.currentTimeMillis(), JunoRequest.OperationType.Create));
            getList.add(new JunoRequest(key[i], 0, ttl[i], JunoRequest.OperationType.Get));
            setList.add(new JunoRequest(key[i], payload[i], (long)0, ttl[i], System.currentTimeMillis(), JunoRequest.OperationType.Set));
            updateList.add(new JunoRequest(key[i], payload[i], (long)0, ttl[i], System.currentTimeMillis(), JunoRequest.OperationType.Update));
            deleteList.add(new JunoRequest(key[i], null, 0, 0, JunoRequest.OperationType.Destroy));
        }        for(int i=0; i<loop; i++){

            //Create operation
            try{
                junoReactClient.doBatch(createList).blockLast();
            }catch (Exception e){ }

            //Get operation
            try{
                junoReactClient.doBatch(getList).blockLast();
            }catch (Exception e){ }

            //Set operation
            try{
                junoReactClient.doBatch(setList).blockLast();
            }catch (Exception e){ }

            //Update operation
            try{
                junoReactClient.doBatch(updateList).blockLast();
            }catch (Exception e){ }

            //Destroy operation
            try{
                junoReactClient.doBatch(deleteList).blockLast();
            }catch (Exception e){ }

            Thread.sleep(100);
        }
        Thread.sleep(1500);

        Iterator<MeterRegistry> registryIterator = Metrics.globalRegistry.getRegistries().iterator();
        while(registryIterator.hasNext()){
            MeterRegistry registry = registryIterator.next();
            if(registry instanceof  SimpleMeterRegistry){

                Timer createTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","B_CREATE").tag("status","SUCCESS").timer();
                Timer getTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","B_GET").tag("status","SUCCESS").timer();
                Timer setTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","B_SET").tag("status","SUCCESS").timer();
                Timer updateTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","B_UPDATE").tag("status","SUCCESS").timer();
                Timer deleteTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","B_DESTROY").tag("status","SUCCESS").timer();

                Counter createExceedsTTLErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "B_CREATE").tag("status", "Illegal argument").tag("cause", "ttl_exceeded_max").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();
                Counter setExceedsTTLErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "B_SET").tag("status", "Illegal argument").tag("cause", "ttl_exceeded_max").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();
                Counter updateExceedsTTLErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "B_UPDATE").tag("status", "Illegal argument").tag("cause", "ttl_exceeded_max").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();
                Counter getExceedsTTLErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "B_GET").tag("status", "Illegal argument").tag("cause", "ttl_exceeded_max").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();

                Counter createInvalidTTLErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "B_CREATE").tag("status", "Illegal argument").tag("cause", "invalid_ttl").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();
                Counter setNegTTLErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "B_SET").tag("status", "Illegal argument").tag("cause", "invalid_ttl").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();
                Counter updateNegTTLErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "B_UPDATE").tag("status", "Illegal argument").tag("cause", "invalid_ttl").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();
                Counter getNegTTLErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "B_GET").tag("status", "Illegal argument").tag("cause", "invalid_ttl").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();

                Counter createZeroTTLErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "B_CREATE").tag("status", "Illegal argument").tag("cause", "invalid_ttl").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();

                AssertJUnit.assertEquals(loop*5, (int)createTimer.count(),1);
                AssertJUnit.assertEquals(loop*10, (int)getTimer.count(),1);
                AssertJUnit.assertEquals(loop*10, (int)setTimer.count(),1);
                AssertJUnit.assertEquals(loop*10, (int)updateTimer.count(),1);
                AssertJUnit.assertEquals(loop*numKeys, (int)deleteTimer.count(),1);

                AssertJUnit.assertEquals(loop*5, (int)createExceedsTTLErr.count());
                AssertJUnit.assertEquals(loop*5, (int)updateExceedsTTLErr.count());
                AssertJUnit.assertEquals(loop*5, (int)setExceedsTTLErr.count());
                AssertJUnit.assertEquals(loop*5, (int)getExceedsTTLErr.count());
                AssertJUnit.assertEquals(loop*5*2, (int)createInvalidTTLErr.count());
                AssertJUnit.assertEquals(loop*5, (int)updateNegTTLErr.count());
                AssertJUnit.assertEquals(loop*5, (int)setNegTTLErr.count());
                AssertJUnit.assertEquals(loop*5, (int)getNegTTLErr.count());
            }
        }
    }

    @Test
    public void testReactZeroNullKeyBatch() throws IOException, InterruptedException {

        int numKeys = 20;
        byte[][] key = new byte[numKeys][];
        byte[][] payload = new byte[numKeys][];
        List<JunoRequest> createList = new ArrayList<>();
        List<JunoRequest> getList = new ArrayList<>();
        List<JunoRequest> setList = new ArrayList<>();
        List<JunoRequest> updateList = new ArrayList<>();
        List<JunoRequest> deleteList = new ArrayList<>();
        for (int i = 0; i < numKeys; i ++) {
            payload[i] = DataGenUtils.createKey(10).getBytes();
            if ( i <= 1 ) {
                key[i] = DataGenUtils.createKey(25).getBytes();
            } else {
                if ( i % 2 == 0 ) {
                    key[i] = "".getBytes();
                } else {
                    key[i] = null;
                }
            }
            try{
                createList.add(new JunoRequest(key[i], payload[i], (long)0, 100, System.currentTimeMillis(), JunoRequest.OperationType.Create));
            }catch (Exception e) { }
            try {
                getList.add(new JunoRequest(key[i], 0, 100, JunoRequest.OperationType.Get));
            }catch (Exception e) { }
            try {
                setList.add(new JunoRequest(key[i], payload[i], (long)0, 100, System.currentTimeMillis(), JunoRequest.OperationType.Set));
            }catch (Exception e) { }
            try {
                updateList.add(new JunoRequest(key[i], payload[i], (long)0, 100, System.currentTimeMillis(), JunoRequest.OperationType.Update));
            }catch (Exception e) { }
            try {
                deleteList.add(new JunoRequest(key[i], null, 0, 0, JunoRequest.OperationType.Destroy));
            }catch (Exception e) { }
        }

        for(int i=0; i<loop; i++){

            //Create operation
            try{
                junoReactClient.doBatch(createList).blockLast();
            }catch (Exception e){ }

            //Get operation
            try{
                junoReactClient.doBatch(getList).blockLast();
            }catch (Exception e){ }

            //Set operation
            try{
                junoReactClient.doBatch(setList).blockLast();
            }catch (Exception e){ }

            //Update operation
            try{
                junoReactClient.doBatch(updateList).blockLast();
            }catch (Exception e){ }

            //Destroy operation
            try{
                junoReactClient.doBatch(deleteList).blockLast();
            }catch (Exception e){ }

            Thread.sleep(100);
        }
        Thread.sleep(1500);

        Iterator<MeterRegistry> registryIterator = Metrics.globalRegistry.getRegistries().iterator();
        while(registryIterator.hasNext()){
            MeterRegistry registry = registryIterator.next();
            if(registry instanceof  SimpleMeterRegistry){

                Timer createTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","B_CREATE").tag("status","SUCCESS").timer();
                Timer getTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","B_GET").tag("status","SUCCESS").timer();
                Timer setTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","B_SET").tag("status","SUCCESS").timer();
                Timer updateTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","B_UPDATE").tag("status","SUCCESS").timer();
                Timer deleteTimer = registry.find(JunoMetrics.JUNO_LATENCY_METRIC).tag("operation","B_DESTROY").tag("status","SUCCESS").timer();

                Counter createErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "B_CREATE").tag("status", "Illegal argument").tag("cause", "null_or_empty_key").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();
                Counter setErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "B_SET").tag("status", "Illegal argument").tag("cause", "null_or_empty_key").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();
                Counter updateErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "B_UPDATE").tag("status", "Illegal argument").tag("cause", "null_or_empty_key").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();
                Counter getErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "B_GET").tag("status", "Illegal argument").tag("cause", "null_or_empty_key").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();
                Counter destroyErr = registry.find(JunoMetrics.JUNO_OPERATION_METRIC).tag("pool",ip+":"+port).tag("type", "B_DESTROY").tag("status", "Illegal argument").tag("cause", "null_or_empty_key").tag("status", OperationStatus.IllegalArgument.getErrorText()).counter();

                AssertJUnit.assertEquals(loop*2, (int)createTimer.count(),1);
                AssertJUnit.assertEquals(loop*2, (int)getTimer.count(),1);
                AssertJUnit.assertEquals(loop*2, (int)setTimer.count(),1);
                AssertJUnit.assertEquals(loop*2, (int)updateTimer.count(),1);
                AssertJUnit.assertEquals(loop*2, (int)deleteTimer.count(),1);

                AssertJUnit.assertEquals(loop*(numKeys-2), (int)createErr.count());
                AssertJUnit.assertEquals(loop*(numKeys-2), (int)getErr.count());
                AssertJUnit.assertEquals(loop*(numKeys-2), (int)setErr.count());
                AssertJUnit.assertEquals(loop*(numKeys-2), (int)updateErr.count());
                AssertJUnit.assertEquals(loop*(numKeys-2), (int)destroyErr.count());
            }
        }
    }
}
