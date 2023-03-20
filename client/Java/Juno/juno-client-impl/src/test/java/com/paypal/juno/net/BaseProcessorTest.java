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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertTrue;


public class BaseProcessorTest {
	
	private BaseProcessor p;
	
	public BaseProcessorTest() {}
	
	@Before
	public void initialize() throws Exception {
		p = new BaseProcessor();
	}
	
	@After
	public void tearDown() {}
	
	@Test
	public void testUseLTM() {
		
		p.useLTM();
		assertTrue(true);
	}
	
	@Test
	public void testPingIp() {

		String source = "";
		String target = "";
		
		for (int i = 0; i < 3; i++) {
			try {
				InetAddress ip = p.getPingIp();
				if (ip == null) {
					source = "100.5.0.200";
					p.setPingIp(source);
					continue;
				}
				target = ip.toString().substring(1);
			
				p.clearPingRespQueue();
				break;
			} catch (Exception e) {	
				continue;
			}
		}
		boolean same = source.equals(target); 
		assertTrue(same);
	}
}

