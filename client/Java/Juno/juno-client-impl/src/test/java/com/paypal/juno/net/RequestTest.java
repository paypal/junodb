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

import com.paypal.juno.client.io.JunoRequest;
import com.paypal.juno.io.protocol.OperationMessage;
import com.paypal.juno.transport.socket.SocketConfig;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import static org.junit.Assert.*;


@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class RequestTest {
	
	public RequestTest() {}
	
	@Before
	public void initialize() throws Exception {
		RequestQueue.clear();
	}
	
	@After
	public void tearDown() {
		RequestQueue.clear();	
	}
	
	@Test
	public void test1ResponseTimeout() {
		
		SocketConfig cfg = new SocketConfig(false);
		cfg.setResponseTimeout(-1);
		 
		RequestQueue reqQueue = RequestQueue.getInstance(cfg);
		ConcurrentHashMap<Integer, BlockingQueue<OperationMessage>> respQueueMap = reqQueue.getOpaqueResMap();
		BlockingQueue<OperationMessage> respQ = new ArrayBlockingQueue<OperationMessage>(10);
		respQueueMap.put(10, respQ);
		
		for (int i = 0; i < 3; i++) {
			PingMessage ping = new PingMessage("JunoNetTest_1", 10);
			reqQueue.enqueue(ping);
		}
		
		try {
			Thread.sleep(6000);
		} catch (Exception e) {
		}
		
		assertTrue(reqQueue.isConnected());
		assertEquals(respQ.size(), 0);
	}
	
	@Test
	public void test2Request() {
		
		SocketConfig cfg = new SocketConfig(true);
		RequestQueue reqQueue = RequestQueue.getInstance(cfg);
		ConcurrentHashMap<Integer, BlockingQueue<OperationMessage>> respQueueMap = reqQueue.getOpaqueResMap();

		try {
			 Thread.sleep(5000);
		} catch (Exception e) {
		}
		 
		for (int i = 0; i < 3; i++) {
		 
			 PingMessage ping = new PingMessage("JunoNetTest_2", 0);
			 BlockingQueue<OperationMessage> respQueue = new ArrayBlockingQueue<OperationMessage>(2);
			 respQueueMap.put(0, respQueue);
			 
			 reqQueue.enqueue(ping);
		}
		
		try {
			 Thread.sleep(5000);
		} catch (Exception e) {
			 
		}
		
		assertTrue(reqQueue.isConnected());
	}

	@Test
	public void testJunoRequest1() {
		//Test the API 1
		JunoRequest req1 = new JunoRequest("key1".getBytes(), "value1".getBytes(), 0, JunoRequest.OperationType.Create);
		assertEquals(new String(req1.key()), "key1");
		assertEquals(new String(req1.getValue()),"value1");
		assertEquals(req1.getVersion(),0);
		assertEquals(req1.getType(),JunoRequest.OperationType.Create);

		//Test the API 2
		JunoRequest req2 = new JunoRequest("key2".getBytes(), 10,10, JunoRequest.OperationType.Get);
		assertEquals(new String(req2.key()), "key2");
		assertEquals(req2.getVersion(),10);
		assertEquals(req2.getTimeToLiveSec(),new Long(10));
		assertEquals(req2.getType(),JunoRequest.OperationType.Get);
		assertFalse(req1.equals(req2));
		System.out.println("hash code :"+req2.hashCode());
		assertEquals(-247185975,req2.hashCode());
		System.out.println("String value of req1:"+req2.toString());
	}
}

