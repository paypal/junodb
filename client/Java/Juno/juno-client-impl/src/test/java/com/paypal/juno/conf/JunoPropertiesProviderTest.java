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
package com.paypal.juno.conf;

import java.net.URL;
import java.util.Properties;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class JunoPropertiesProviderTest{
	URL url = JunoPropertiesProviderTest.class.getClassLoader().getResource("juno.properties");
	JunoPropertiesProvider jpp = new JunoPropertiesProvider(url);
	JunoPropertiesProvider jppDefault = new JunoPropertiesProvider(new Properties());
	
	@Test
	public void TestDefaultValues(){
		assertEquals(new Integer(200) ,jppDefault.getConnectionTimeout());
		assertEquals(new Long(259200),jppDefault.getDefaultLifetime());
		assertEquals(new Integer(1),jppDefault.getConnectionPoolSize());
		assertEquals(new Integer(200), jppDefault.getResponseTimeout());
	}
	
	@Test
	public void TestValueReadFromPropertiesFile(){
		assertEquals(new Integer(1000),jpp.getConnectionTimeout());
		assertEquals(new Long(259200),jpp.getDefaultLifetime());
		assertEquals("JunoTest",jpp.getAppName());
		assertEquals("JunoTest",jpp.getRecordNamespace());
		assertEquals("127.0.0.1",jpp.getHost());
		assertEquals(new Integer(8090),jpp.getPort());
		assertEquals(false,jpp.useSSL());
		assertEquals(true,jpp.getOperationRetry());
		assertEquals(true,jpp.getByPassLTM());
	}
	
	@Test
	public void TestToString(){
		//System.out.println("msg1"+jpp.toString());
		assertEquals("JunoPropertiesProvider{ connectionTimeoutMS=1000, connectionPoolSize=1, defaultLifetime=259200, maxLifetime=259200, host='127.0.0.1', port='8090', appName='JunoTest, recordNamespace='JunoTest, useSSL = false, usePayloadCompression =true, responseTimeout = 1000, maxConnectionPoolSize=1, maxConnectionLifetime=30000, maxKeySize=128, maxValueSize=204800, maxLifetime=259200, maxNameSpaceLength=64, operationRetry=true, byPassLTM=true, reconnectOnFail=true}",jpp.toString());
	}
	
	@Test
	public void TestDefaultToString(){
		//System.out.println("msg2"+jppDefault.toString());
		assertEquals("JunoPropertiesProvider{ connectionTimeoutMS=200, connectionPoolSize=1, defaultLifetime=259200, maxLifetime=259200, host='', port='0', appName=', recordNamespace=', useSSL = true, usePayloadCompression =false, responseTimeout = 200, maxConnectionPoolSize=1, maxConnectionLifetime=30000, maxKeySize=128, maxValueSize=204800, maxLifetime=259200, maxNameSpaceLength=64, operationRetry=false, byPassLTM=true, reconnectOnFail=false}",jppDefault.toString());
	}
}