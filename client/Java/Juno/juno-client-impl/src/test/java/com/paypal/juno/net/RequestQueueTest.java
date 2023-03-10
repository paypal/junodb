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
package com.paypal.juno.net;

import com.paypal.juno.client.JunoAsyncClient;
import com.paypal.juno.client.JunoClientConfigHolder;
import com.paypal.juno.client.JunoClientFactory;
import com.paypal.juno.client.io.JunoResponse;
import com.paypal.juno.conf.JunoPropertiesProvider;
import com.paypal.juno.mock.MockJunoUnresponsiveServer;
import com.paypal.juno.transport.socket.SocketConfigHolder;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import org.junit.*;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import rx.Single;
import rx.SingleSubscriber;
import static org.junit.Assert.*;

@RunWith(MockitoJUnitRunner.Silent.class)
public class RequestQueueTest {
    private RequestQueue unresReqQueue;
    private boolean recycleEventFired;
    private boolean connectTimeoutFired;
    private JunoPropertiesProvider unresJpp;
    private SocketConfigHolder unresSocCfg;
    private JunoClientConfigHolder unresClientCfgHldr;
    private MockJunoUnresponsiveServer mjus;
    private List<String> responses = new ArrayList<>();
    private boolean unfreeze = false, timeoutTriggered = false;
    private JunoAsyncClient unresClient;
    private long startTime = System.currentTimeMillis(), endTime = System.currentTimeMillis();

    @Before
    public void initialize() {
        timeoutTriggered = false;
        connectTimeoutFired = false;
        unfreeze = false;
        responses.clear();
        recycleEventFired = false;
        RequestQueue.clear();

        //Unresponsive Server Setup
        mjus = new MockJunoUnresponsiveServer(15000, false);
        mjus.start();

        // Unresponsive Client Setup
        URL unresUrl = RequestQueue.class.getClassLoader().getResource("junoUnresponsive.properties");
        unresJpp = new JunoPropertiesProvider(unresUrl);
        unresClientCfgHldr = new JunoClientConfigHolder(unresJpp);
        unresSocCfg = new SocketConfigHolder(unresClientCfgHldr);
        unresClient = JunoClientFactory.newJunoAsyncClient(unresUrl);

        unresReqQueue = RequestQueue.getInstance(unresSocCfg);
        unresReqQueue.addPropertyChangeListener(evt -> {
            if(evt.getPropertyName().equals("recycleNow")) {
                if (timeoutTriggered) {
                    unfreeze = true;
                    endTime = System.currentTimeMillis();
                    timeoutTriggered = false;
                }
                if (unfreeze) mjus.setFreeze(false);
                recycleEventFired = true;
                responses.add(evt.getPropertyName());
            }

            if(evt.getPropertyName().equals("RECYCLE_CONNECT_TIMEOUT")) {
                connectTimeoutFired = true;
                startTime = System.currentTimeMillis();
                timeoutTriggered = true;
            }
        });
    }

    @After
    public void tearDown() {
        if(mjus != null) {
            mjus.stop();
            mjus.stopMockServer();
        }
    }


    @Test
    public void testCheckForUnresponsiveConnection_withOneServerFreeze()  {
        final int[] j = {0};
        int i = 0;
        SingleSubscriber<JunoResponse> getSubscriber = new SingleSubscriber<JunoResponse>() {
            @Override
            public void onSuccess(JunoResponse res) {
                j[0]++;;
            }

            @Override
            public void onError(Throwable e) {
            }
        };

        long triggerFreeze = 10000;

        Timer timer = new Timer();
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                System.out.println("Mock Server Freeze Triggered");
                mjus.setFreeze(true);
                unfreeze = true;
            }
        };
        timer.schedule(task, triggerFreeze);

        for(int k = 0; k < 20; k++) {
            for (i = 0; i < 100; i++) {
                try {
                    Single<JunoResponse> response = unresClient.get("insertTest".getBytes());
                    response.subscribe(getSubscriber);
                    try {
                        Thread.sleep(10); // 2 Seconds
                    } catch (Exception e) {}
                } catch (Exception e) {
                }
            }
        }

        assertTrue(recycleEventFired);
        assertTrue(responses.size() > 0);

        for(String response : responses){
            assertEquals("recycleNow", response);
        }
        mjus.setFreeze(false);
    }

    @Test
    public void testCheckForUnresponsiveConnection_withFrozenServer()  {
        final int[] j = {0};
        int i = 0;
        SingleSubscriber<JunoResponse> getSubscriber = new SingleSubscriber<JunoResponse>() {
            @Override
            public void onSuccess(JunoResponse res) {
                j[0]++;;
            }

            @Override
            public void onError(Throwable e) {
            }
        };

        long triggerFreeze = 10000;

        Timer timer = new Timer();
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                System.out.println("Mock Server Freeze Triggered");
                mjus.setFreeze(true);
            }
        };
        timer.schedule(task, triggerFreeze);

        for(int k = 0; k < 20; k++) {
            for (i = 0; i < 100; i++) {
                try {
                    Single<JunoResponse> response = unresClient.get("insertTest".getBytes());
                    response.subscribe(getSubscriber);
                    try {
                        Thread.sleep(10); // 2 Seconds
                    } catch (Exception e) {}
                } catch (Exception e) {
                }
            }
        }

        assertTrue(recycleEventFired);
        assertTrue(responses.size() > 0);

        for(String response : responses){
            assertEquals("recycleNow", response);
        }
        mjus.setFreeze(false);
    }


    @Test
    public void testCheckForUnresponsiveConnection_with2ServerFreeze()  {
        final int[] j = {0};
        int i = 0;
        SingleSubscriber<JunoResponse> getSubscriber = new SingleSubscriber<JunoResponse>() {
            @Override
            public void onSuccess(JunoResponse res) {
                j[0]++;;
            }

            @Override
            public void onError(Throwable e) {
            }
        };

        long triggerFreeze = 10000;

        Timer timer = new Timer();
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                System.out.println("Mock Server Freeze Triggered");
                mjus.setFreeze(true);
            }
        };
        timer.schedule(task, triggerFreeze);

        for(int k = 0; k < 30; k++) {
            if(responses.size() >= 1) unfreeze = true;
            for (i = 0; i < 100; i++) {
                try {
                    Single<JunoResponse> response = unresClient.get("insertTest".getBytes());
                    response.subscribe(getSubscriber);
                    try {
                        Thread.sleep(10); // 2 Seconds
                    } catch (Exception e) {}
                } catch (Exception e) {
                }
            }
        }

        assertTrue(recycleEventFired);
        assertTrue(responses.size() > 0);

        for(String response : responses){
            assertEquals("recycleNow", response);
        }
        mjus.setFreeze(false);
    }

    @Test
    public void testCheckForUnresponsiveConnection_withUnresponsiveServerAndConnectionTimeoutIntervalTriggered()  {
        final int[] j = {0};
        int i = 0;
        SingleSubscriber<JunoResponse> getSubscriber = new SingleSubscriber<JunoResponse>() {
            @Override
            public void onSuccess(JunoResponse res) {
                j[0]++;;
            }

            @Override
            public void onError(Throwable e) {
            }
        };

        long triggerFreeze = 10000;

        Timer timer = new Timer();
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                System.out.println("Mock Server Freeze Triggered");
                mjus.setFreeze(true);
            }
        };
        timer.schedule(task, triggerFreeze);

        for(int k = 0; k < 200; k++) {
            for (i = 0; i < 100; i++) {
                try {
                    Single<JunoResponse> response = unresClient.get("insertTest".getBytes());
                    response.subscribe(getSubscriber);
                    try {
                        Thread.sleep(10); // 2 Seconds
                    } catch (Exception e) {}
                } catch (Exception e) {
                }
            }
        }

        assertTrue(recycleEventFired);
        assertTrue(responses.size() > 0);
        assertTrue(connectTimeoutFired);

        mjus.setFreeze(false);
    }

    @Test
    public void testCheckForUnresponsiveConnection_withNoFailures() throws InterruptedException {
        final int[] j = {0};
        int i = 0;
        SingleSubscriber<JunoResponse> getSubscriber = new SingleSubscriber<JunoResponse>() {
            @Override
            public void onSuccess(JunoResponse res) {
                j[0]++;;
            }

            @Override
            public void onError(Throwable e) {
            }
        };

        for(int k = 0; k < 20; k++) {
            for (i = 0; i < 500; i++) {
                try {
                    Single<JunoResponse> response = unresClient.get("insertTest".getBytes());
                    response.subscribe(getSubscriber);
                } catch (Exception e) {
                }
            }

            try {
                Thread.sleep(2 * unresSocCfg.getResponseTimeout());
            } catch (Exception e) {
            }
        }

        assertEquals(responses.size(), 0);
        assertFalse(recycleEventFired);
    }

    @Test
    public void testCheckForUnresponsiveConnection_withZeroMessages() throws InterruptedException {
        for(int i = 0; i < 20; i++){
            Thread.sleep(1000);
        }

        assertEquals(responses.size(), 0);
        assertFalse(recycleEventFired);
    }

}