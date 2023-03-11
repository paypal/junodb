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

import java.net.InetAddress;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BaseProcessor {

	private static final Logger LOGGER = LoggerFactory.getLogger(BaseProcessor.class);
	
	private BlockingQueue<String> pingRespQueue = new ArrayBlockingQueue<String>(1);
	
	boolean useLTM() {
		return false;
	}
	
	void setPingIp(String ip) {
		pingRespQueue.offer(ip);
	}
	
	InetAddress getPingIp() throws InterruptedException {
		
		String str = pingRespQueue.poll(300, TimeUnit.MILLISECONDS); 
		if (str == null || str.length() == 0) {
			return null;
		}
		
		InetAddress ip = null;
		try {
			ip = InetAddress.getByName(str);
		} catch (Exception e) {
			LOGGER.warn("Failed to get InetAddress for "+str+" "+e.toString());
			return null;
		}
		
		return ip;
	}
	
	void clearPingRespQueue() throws InterruptedException {
		
		pingRespQueue.poll(2, TimeUnit.MILLISECONDS); 
	}
}
