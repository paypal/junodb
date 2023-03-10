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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * To trigger error handling code.
 * 
 */

enum TestEvent {
	INTERRUPTED(1),
	INTERRUPTED_2(2),
	EXCEPTION(3),
	EXCEPTION_2(4),
	EXCEPTION_3(5),

	SEND_FAIL(6),
	READ_FAIL(7),
	CONNECTION_LOST(8),
	MISSING_RESPONSE(9),
	DNS_DELAY(10);
	
	private static final Logger LOGGER = LoggerFactory.getLogger(TestEvent.class);
	
	private static int mask = 0xffffffff;
	private final int val;
	private final int code;
	
	// code is a power of 2.
	TestEvent(int val) {
		this.val = val;
		this.code = 2 << val;
	}
	
	int getValue() {
		return val;
	}
	
	synchronized int maskedValue() {
		if ((mask & code)  == 0) {
			return 0;
		}
		
		mask ^= code;
		
		return val;
	}
	
	void triggerException() throws Exception {
		
		switch (val) {
		case 1:
		case 2:
			throw new InterruptedException("Test mode: event "+val);
		case 3:
		case 4:
		case 5:
			throw new RuntimeException("Test Mode: event "+val);
		}
	}
}
