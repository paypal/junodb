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

import com.paypal.juno.transport.socket.SocketConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import static org.junit.Assert.assertTrue;


@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ShutdownTest {

	public ShutdownTest() {}

	@Before
	public void initialize() throws Exception {
		RequestQueue.clear();
	}

	@After
	public void tearDown() {
		RequestQueue.clear();
	}

	@Test
	public void testShutdown() {
		SocketConfig cfg = new SocketConfig(false);
		RequestQueue reqQueue = RequestQueue.getInstance(cfg);
		WorkerPool wp = new WorkerPool(cfg, reqQueue);

		try {
			Thread.sleep(2000);
		} catch (Exception e) {
		}
		boolean yes = wp.isConnected();
		assertTrue(yes);

		wp.shutdown();
		try {
			Thread.sleep(5000);
		} catch (Exception e) {
		}

		assertTrue(WorkerPool.isQuit());
	}
}

