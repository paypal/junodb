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

import com.paypal.juno.io.protocol.MetaOperationMessage;
import com.paypal.juno.io.protocol.OperationMessage;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class PingMessageTest {
	
	public PingMessageTest() {}
	
	@Before
	public void initialize() throws Exception {}
	
	@After
	public void tearDown() {}
	
	@Test
	public void testPingResp() {
		BaseProcessor processor = new BaseProcessor();
		OperationMessage msg = new PingMessage(null, 0);
		boolean yes = PingMessage.isPingResp(msg, processor);
		assertEquals(yes, true);
		
		// No meta component
		msg.setMetaComponent(null);
		yes = PingMessage.isPingResp(msg, processor);
		assertFalse(yes);
		
		// No sourcefield
		MetaOperationMessage mo = new MetaOperationMessage(0L,(byte)OperationMessage.Type.Meta.getValue());
		msg.setMetaComponent(mo);		
		yes = PingMessage.isPingResp(msg, processor);
		assertFalse(yes);
		
		// app is not JunoInternal
		byte[] ip = new byte[4];
		ip[0] = 115;
		mo = new MetaOperationMessage(0L,(byte)OperationMessage.Type.Meta.getValue());
		mo.addSourceField(ip, 0, new String("SomeApp").getBytes());
				
		msg.setMetaComponent(mo);
		yes = PingMessage.isPingResp(msg, processor);
		assertFalse(yes);
		
		// ip is not 4 bytes
		ip = new byte[3];
		ip[0] = 115;
		mo = new MetaOperationMessage(0L,(byte)OperationMessage.Type.Meta.getValue());
		mo.addSourceField(ip, 0, new String("JunoInternal").getBytes());
	
		msg.setMetaComponent(mo);
		yes = PingMessage.isPingResp(msg, processor);
		assertTrue(yes);
		
		// ip is a loopback address
		ip = new byte[4];
		ip[0] = 127;
		mo = new MetaOperationMessage(0L,(byte)OperationMessage.Type.Meta.getValue());
		mo.addSourceField(ip, 0, new String("JunoInternal").getBytes());
	
		msg.setMetaComponent(mo);
		yes = PingMessage.isPingResp(msg, processor);
		assertTrue(yes);
		
	}
	
}

