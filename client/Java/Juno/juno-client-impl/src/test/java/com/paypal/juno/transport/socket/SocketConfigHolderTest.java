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
package com.paypal.juno.transport.socket;

import com.paypal.juno.client.JunoClientConfigHolder;
import com.paypal.juno.conf.JunoPropertiesProvider;
import com.paypal.juno.conf.JunoPropertiesProviderTest;
import com.paypal.juno.exception.JunoException;
import java.net.InetSocketAddress;
import java.net.URL;
import org.junit.Test;
import org.testng.AssertJUnit;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class SocketConfigHolderTest{
	URL url = JunoPropertiesProviderTest.class.getClassLoader().getResource("juno.properties");
	JunoPropertiesProvider jpp = new JunoPropertiesProvider(url);

	
	@Test
	public void TestSocketConfigHolder(){
		try{
		JunoClientConfigHolder jch = new JunoClientConfigHolder(jpp);
		SocketConfigHolder sch = new SocketConfigHolder(jch);
		assertEquals(sch.getConnectionLifeTime(),5000);
		InetSocketAddress addr = sch.getInetAddress();
		assertNotNull(addr);
		assertEquals(sch.getConnectTimeout(),1000);
		}catch(JunoException e){
			AssertJUnit.assertTrue ("Exception :"+e.getMessage(), false);
		}
	}
}