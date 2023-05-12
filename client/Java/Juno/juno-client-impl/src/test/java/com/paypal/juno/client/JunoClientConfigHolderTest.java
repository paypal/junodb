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
package com.paypal.juno.client;

import com.paypal.juno.conf.JunoProperties;
import com.paypal.juno.conf.JunoPropertiesProvider;
import com.paypal.juno.exception.JunoClientConfigException;
import com.paypal.juno.exception.JunoException;
import java.util.Properties;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class JunoClientConfigHolderTest {

	@Before
	public void initialize() throws Exception {
		
	}
	
	@After
	public void tearDown() {
		
	}
	
	@Test
	public void NameSpaceSizeTest(){
		Properties prop = new Properties();
		
		prop.setProperty(JunoProperties.APP_NAME, "NameSpaceMaxSizeTest");
		JunoPropertiesProvider jpp = new JunoPropertiesProvider(prop);
		try{
			JunoClientConfigHolder clientCfgHldr = new JunoClientConfigHolder(jpp);
		}catch(JunoClientConfigException jce){
			assertEquals(jce.getMessage(),"Juno configuration value for property, juno.record_namespace cannot be null or empty");
			//System.out.println(jce.getMessage());
		}
		
		prop.setProperty(JunoProperties.RECORD_NAMESPACE, "");
		jpp = new JunoPropertiesProvider(prop);
		try{
			JunoClientConfigHolder clientCfgHldr = new JunoClientConfigHolder(jpp);
		}catch(JunoClientConfigException jce){
			assertEquals(jce.getMessage(),"Juno configuration value for property, juno.record_namespace cannot be null or empty");
			//System.out.println(jce.getMessage());
		}
		
		prop.setProperty(JunoProperties.RECORD_NAMESPACE, "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz");
		jpp = new JunoPropertiesProvider(prop);
		try{
			JunoClientConfigHolder clientCfgHldr = new JunoClientConfigHolder(jpp);
		}catch(JunoClientConfigException jce){
			assertEquals(jce.getMessage(),"Namespace length exceeds MAX LENGTH of 64 bytes");
			//System.out.println(jce.getMessage());
		}
	}
	
	@Test
	public void AppNameSizeTest(){
		Properties prop = new Properties();
		prop.setProperty(JunoProperties.RECORD_NAMESPACE, "AppNameMaxSizeTest");
		prop.setProperty(JunoProperties.APP_NAME, "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz");
		JunoPropertiesProvider jpp = new JunoPropertiesProvider(prop);
		try{
			JunoClientConfigHolder clientCfgHldr = new JunoClientConfigHolder(jpp);
		}catch(JunoClientConfigException jce){
			assertEquals("Application Name length exceeds MAX LENGTH of 32 bytes",jce.getMessage());
			//System.out.println(jce.getMessage());
		}
		
		prop.setProperty(JunoProperties.APP_NAME, "");
		jpp = new JunoPropertiesProvider(prop);
		try{
			JunoClientConfigHolder clientCfgHldr = new JunoClientConfigHolder(jpp);
		}catch(JunoClientConfigException jce){
			assertEquals(jce.getMessage(),"Juno configuration value for property, juno.application_name cannot be null or empty");
			//System.out.println(jce.getMessage());
		}
	}
	
	@Test
	public void ValidateHost(){
		Properties prop = new Properties();
		prop.setProperty(JunoProperties.RECORD_NAMESPACE, "abcdefghijklmnopqrstuvwxyzabc");
		prop.setProperty(JunoProperties.APP_NAME, "ValidateAppNameTest");
		JunoPropertiesProvider jpp = new JunoPropertiesProvider(prop);
		try{
			JunoClientConfigHolder clientCfgHldr = new JunoClientConfigHolder(jpp);
		}catch(JunoClientConfigException jce){
			assertEquals(jce.getMessage(),"Juno server not configured...");
			//System.out.println(jce.getMessage());
		}
	}
	
	@Test
	public void ValidateServerPort(){
		Properties prop = new Properties();
		prop.setProperty(JunoProperties.RECORD_NAMESPACE, "abcdefghijklmnopqrstuvwxyzabc");
		prop.setProperty(JunoProperties.APP_NAME, "ValidateAppNameTest");
		prop.setProperty(JunoProperties.HOST, "127.0.0.1");
		JunoPropertiesProvider jpp = new JunoPropertiesProvider(prop);
		try{
			JunoClientConfigHolder clientCfgHldr = new JunoClientConfigHolder(jpp);
		}catch(JunoClientConfigException jce){
			assertEquals(jce.getMessage(),"Invalid Juno server port...");
			//System.out.println(jce.getMessage());
		}
		
		prop.setProperty(JunoProperties.PORT,"-1");
		jpp = new JunoPropertiesProvider(prop);
		try{
			JunoClientConfigHolder clientCfgHldr = new JunoClientConfigHolder(jpp);
		}catch(JunoClientConfigException jce){
			//System.out.println(jce.getMessage());
			assertEquals(jce.getMessage(),"Invalid Juno server port...");
		}
	}
	
	@Test
	public void ValidateSocketConnectionTimout(){
		Properties prop = new Properties();
		prop.setProperty(JunoProperties.RECORD_NAMESPACE, "abcdefghijklmnopqrstuvwxyzabc");
		prop.setProperty(JunoProperties.APP_NAME, "ValidateAppNameTest");
		prop.setProperty(JunoProperties.HOST, "127.0.0.1");
		prop.setProperty(JunoProperties.PORT, "14368");
		prop.setProperty(JunoProperties.CONNECTION_TIMEOUT,"");
		JunoPropertiesProvider jpp; 
		try{
			jpp = new JunoPropertiesProvider(prop);
			new JunoClientConfigHolder(jpp);
		}catch(JunoClientConfigException jce){
			assertEquals(jce.getMessage(),"Integer property not valid - Value = ");
		}
		
		prop.setProperty(JunoProperties.CONNECTION_TIMEOUT,"1000");
		jpp = new JunoPropertiesProvider(prop);
			//System.out.println("Message:"+jpp.toString());
			assertEquals("JunoPropertiesProvider{ connectionTimeoutMS=1000, connectionPoolSize=1, defaultLifetime=259200, maxLifetime=259200, host='127.0.0.1', port='14368', appName='ValidateAppNameTest, recordNamespace='abcdefghijklmnopqrstuvwxyzabc, useSSL = true, usePayloadCompression =false, responseTimeout = 200, maxConnectionPoolSize=1, maxConnectionLifetime=30000, maxKeySize=128, maxValueSize=204800, maxLifetime=259200, maxNameSpaceLength=64, operationRetry=false, byPassLTM=true, reconnectOnFail=false}",jpp.toString());
		try{
			JunoClientConfigHolder clientCfgHldr = new JunoClientConfigHolder(jpp);
			clientCfgHldr.getConnectionTimeoutMsecs();
		}catch(JunoClientConfigException jce){
			// We should not be here. As it would use the default send buffer size i.e 0.
			fail("ValidateSocketConnectionTimout failed for value 1001. Execption:"+jce.getMessage());
			//System.out.println(jce.getMessage());
		}
		
		prop.setProperty(JunoProperties.CONNECTION_TIMEOUT,"-1");
		jpp = new JunoPropertiesProvider(prop);
		try{
			JunoClientConfigHolder clientCfgHldr = new JunoClientConfigHolder(jpp);
			clientCfgHldr.getConnectionTimeoutMsecs();
		}catch(JunoClientConfigException jce){
			//System.out.println("Exception :"+jce.getMessage());
			assertEquals(jce.getMessage(),"Juno configuration value for property juno.connection.timeout_msec cannot be less than 1");
		}
		
		prop.setProperty(JunoProperties.CONNECTION_TIMEOUT,"10000");
		jpp = new JunoPropertiesProvider(prop);
		try{
			JunoClientConfigHolder clientCfgHldr = new JunoClientConfigHolder(jpp);
			clientCfgHldr.getConnectionTimeoutMsecs();
		}catch(JunoClientConfigException jce){
			//System.out.println("Exception :"+jce.getMessage());
			assertEquals(jce.getMessage(),"Juno configuration value for property juno.connection.timeout_msec cannot be greater than 5000");
			
		}
	}
	
	@Test
	public void ValidateConnectionLifeTime(){
		Properties prop = new Properties();
		prop.setProperty(JunoProperties.RECORD_NAMESPACE, "abcdefghijklmnopqrstuvwxyzabc");
		prop.setProperty(JunoProperties.APP_NAME, "ValidateAppNameTest");
		prop.setProperty(JunoProperties.HOST, "127.0.0.1");
		prop.setProperty(JunoProperties.PORT, "14368");
		prop.setProperty(JunoProperties.CONNECTION_LIFETIME,"0");
		JunoPropertiesProvider jpp; 
		try{
			jpp = new JunoPropertiesProvider(prop);
			JunoClientConfigHolder clientCfgHldr = new JunoClientConfigHolder(jpp);
		}catch(JunoClientConfigException jce){
			//System.out.println("Exception :"+jce.getMessage());
			assertEquals(jce.getMessage(),"Juno configuration value for property juno.connection.recycle_duration_msec cannot be less than 5000");
		}
	}
	
	@Test
	public void ValidateMaxConnectionPoolSize(){
		Properties prop = new Properties();
		prop.setProperty(JunoProperties.RECORD_NAMESPACE, "abcdefghijklmnopqrstuvwxyzabc");
		prop.setProperty(JunoProperties.APP_NAME, "ValidateAppNameTest");
		prop.setProperty(JunoProperties.HOST, "127.0.0.1");
		prop.setProperty(JunoProperties.PORT, "14368");
		prop.setProperty(JunoProperties.CONNECTION_TIMEOUT,"1000");
		prop.setProperty(JunoProperties.CONNECTION_POOL_SIZE,"0");
		
		JunoPropertiesProvider jpp; 

		try{
			jpp = new JunoPropertiesProvider(prop);
			new JunoClientConfigHolder(jpp);
		}catch(JunoException e){
			assertTrue(e instanceof JunoClientConfigException);
			assertEquals(e.getMessage(),"Juno configuration value for property juno.connection.pool_size cannot be less than 1");
		}
		
		prop.setProperty(JunoProperties.CONNECTION_POOL_SIZE,"100");
		
		try{
			jpp = new JunoPropertiesProvider(prop);
			new JunoClientConfigHolder(jpp);
		}catch(JunoException e){
			assertTrue(e instanceof JunoClientConfigException);
			assertEquals("Juno configuration value for property juno.connection.pool_size cannot be greater than 3",e.getMessage());
		}
	}
	
	@Test
	public void ValidateResponseTimeout(){
		Properties prop = new Properties();
		prop.setProperty(JunoProperties.RECORD_NAMESPACE, "abcdefghijklmnopqrstuvwxyzabc");
		prop.setProperty(JunoProperties.APP_NAME, "ValidateAppNameTest");
		prop.setProperty(JunoProperties.HOST, "127.0.0.1");
		prop.setProperty(JunoProperties.PORT, "14368");
		prop.setProperty(JunoProperties.RESPONSE_TIMEOUT,"0");
		
		JunoPropertiesProvider jpp; 

		try{
			jpp = new JunoPropertiesProvider(prop);
			new JunoClientConfigHolder(jpp);
		}catch(JunoException e){
			assertTrue(e instanceof JunoClientConfigException);
			//System.out.println("msg1 :"+e.getMessage());
			assertEquals(e.getMessage(),"Juno configuration value for property juno.response.timeout_msec cannot be less than 1");
		}
		
		prop.setProperty(JunoProperties.RESPONSE_TIMEOUT,"10000");
		try{
			jpp = new JunoPropertiesProvider(prop);
			new JunoClientConfigHolder(jpp);
		}catch(JunoException e){
			assertTrue(e instanceof JunoClientConfigException);
			//System.out.println("msg2 :"+e.getMessage());
			assertEquals(e.getMessage(),"Juno configuration value for property juno.response.timeout_msec cannot be greater than 5000");
		}

	}
	
	@Test
	public void ValidateTTL(){
		Properties prop = new Properties();
		prop.setProperty(JunoProperties.RECORD_NAMESPACE, "abcdefghijklmnopqrstuvwxyzabc");
		prop.setProperty(JunoProperties.APP_NAME, "ValidateAppNameTest");
		prop.setProperty(JunoProperties.HOST, "127.0.0.1");
		prop.setProperty(JunoProperties.PORT, "14368");
		prop.setProperty(JunoProperties.RESPONSE_TIMEOUT,"0");
		prop.setProperty(JunoProperties.DEFAULT_LIFETIME,"-1");
		
		JunoPropertiesProvider jpp; 

		try{
			jpp = new JunoPropertiesProvider(prop);
			new JunoClientConfigHolder(jpp);
		}catch(JunoException e){
			assertTrue(e instanceof JunoClientConfigException);
			assertEquals(e.getMessage(),"Juno configuration value for property juno.default_record_lifetime_sec cannot be less than 1");
		}
		
		prop.setProperty(JunoProperties.DEFAULT_LIFETIME,"999999");
		
		try{
			jpp = new JunoPropertiesProvider(prop);
			new JunoClientConfigHolder(jpp);
		}catch(JunoClientConfigException jce){
			assertEquals(jce.getMessage(),"Juno configuration value for property juno.default_record_lifetime_sec cannot be greater than 259200");
		}
	}
}
