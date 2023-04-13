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
package com.paypal.qa.juno;

import com.paypal.juno.client.JunoClientFactory;
import com.paypal.juno.client.io.JunoResponse;
import com.paypal.juno.client.io.OperationStatus;
import com.paypal.juno.conf.JunoProperties;
import com.paypal.juno.conf.JunoPropertiesProvider;
import com.paypal.juno.exception.JunoException;
import com.paypal.juno.util.SSLUtil;
import java.io.IOException;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;
import org.slf4j.Logger;import com.paypal.juno.client.JunoReactClient;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.xerial.snappy.Snappy;

public class ReactorCreateTest {
	private JunoReactClient junoReactClient;
	private JunoReactClient junoReactClient1;
	private JunoReactClient junoReactClient2;
	private JunoReactClient junoReactClient3;	
	private JunoReactClient junoReactClient4;
	private Properties pConfig;
	private Properties pConfig1;
	private Properties pConfig2;
	private Properties pConfig3;
	private Logger LOGGER;

	@BeforeClass
	public void setup() throws  IOException, InterruptedException {

		LOGGER = LoggerFactory.getLogger(ReactorCreateTest.class);
		
		URL url = ReactorCreateTest.class.getResource("/com/paypal/juno/Juno.properties");
		pConfig = new Properties();
		pConfig.load(url.openStream());
		pConfig.setProperty(JunoProperties.APP_NAME, "JunoReactTestApp");
		pConfig.setProperty(JunoProperties.RECORD_NAMESPACE, "JunoReactNS1");
		
		URL url1 = ReactorCreateTest.class.getResource("/com/paypal/juno/Juno.properties");
		junoReactClient1 = JunoClientFactory.newJunoReactClient(url1);
		
		URL url2 = ReactorCreateTest.class.getResource("/com/paypal/juno/Juno.properties");
		pConfig2 = new Properties();
		pConfig2.load(url2.openStream());
		pConfig2.setProperty(JunoProperties.USE_PAYLOADCOMPRESSION, "false");
		pConfig2.setProperty(JunoProperties.APP_NAME, "JunoReactTestApp2");
		pConfig2.setProperty(JunoProperties.RECORD_NAMESPACE, "JunoReactNS1");
		
		pConfig3 = new Properties();
		pConfig3.load(url1.openStream());
		pConfig3.setProperty(JunoProperties.RECORD_NAMESPACE, "JunoReactNS3");
		pConfig3.setProperty(JunoProperties.DEFAULT_LIFETIME, "2900");
		pConfig3.setProperty(JunoProperties.USE_PAYLOADCOMPRESSION, "false");
		
		pConfig3.setProperty(JunoProperties.USE_PAYLOADCOMPRESSION, "true");

		try{
			junoReactClient = JunoClientFactory.newJunoReactClient(new JunoPropertiesProvider(pConfig), SSLUtil.getSSLContext());
			junoReactClient2 = JunoClientFactory.newJunoReactClient(new JunoPropertiesProvider(pConfig2), SSLUtil.getSSLContext());
			junoReactClient3 = JunoClientFactory.newJunoReactClient(new JunoPropertiesProvider(pConfig3), SSLUtil.getSSLContext());
			junoReactClient4 = JunoClientFactory.newJunoReactClient(new JunoPropertiesProvider(pConfig3), SSLUtil.getSSLContext());
		}catch (Exception e) {
			throw new RuntimeException(e);
		}

		Thread.sleep(1000);
		
	}

	@AfterClass
	public  void cleanSetup() throws Exception{
		
	}

	/**
	 * Send a insert request to the Juno 2.0 server with specified key.
	 * It should not generate any exception, if there is no problem on server side.
	 * @throws JunoException 
	 * @throws IOException
	 */
	@Test
	public void testReactCreateWithKey() throws JunoException, IOException{
		JunoPropertiesProvider prop = new JunoPropertiesProvider(pConfig);
		JunoPropertiesProvider prop3 = new JunoPropertiesProvider(pConfig3);		
		LOGGER.info( "\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());

		try {
		for(int i=0; i < 3; i++){			 //create(key, data)
			
			LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
			byte[] data = DataGenUtils.genBytes(10);
			byte[] data1 = DataGenUtils.genBytes(10);
			byte[] key = DataGenUtils.genBytes(64);
			JunoResponse mResponse = junoReactClient.create(key, data).block();
			AssertJUnit.assertEquals (OperationStatus.Success, mResponse.getStatus());
			AssertJUnit.assertTrue(mResponse.getTtl() <= prop.getDefaultLifetime() && mResponse.getTtl() >= prop.getDefaultLifetime()-3);
			JunoResponse mResponse3 = junoReactClient3.create(key, data1).block();	
			AssertJUnit.assertEquals (OperationStatus.Success, mResponse3.getStatus());
			AssertJUnit.assertTrue(mResponse3.getTtl() <= prop3.getDefaultLifetime() && mResponse3.getTtl() >= prop3.getDefaultLifetime()-3);
			
			mResponse = junoReactClient.get(key).block();
			AssertJUnit.assertEquals (OperationStatus.Success, mResponse.getStatus());
			AssertJUnit.assertEquals (new String(data), new String(mResponse.getValue()));
			AssertJUnit.assertTrue(mResponse.getTtl() <= prop.getDefaultLifetime() && mResponse.getTtl() > prop.getDefaultLifetime()-3);
			mResponse3 = junoReactClient3.get(key).block();
			AssertJUnit.assertEquals (OperationStatus.Success, mResponse3.getStatus());
			AssertJUnit.assertEquals (new String(data1), new String(mResponse3.getValue()));
			AssertJUnit.assertTrue(mResponse3.getTtl() <= 2900 && mResponse3.getTtl() >= 2900-3);
			LOGGER.info("0");
			LOGGER.info("Completed");
		}		
		for(int i=0; i < 3; i++){		 //create(key,data,ttl)		
			
			LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));
			byte[] data = DataGenUtils.genBytes(10);
			byte[] data1 = DataGenUtils.genBytes(10);
			byte[] key = DataGenUtils.genBytes(64);
			JunoResponse mResponse = junoReactClient.create(key, data, (long)20).block();	
			AssertJUnit.assertEquals (OperationStatus.Success, mResponse.getStatus());
			JunoResponse mResponse3 = junoReactClient3.create(key, data1, (long)100).block();	
			AssertJUnit.assertEquals (OperationStatus.Success, mResponse3.getStatus());
			mResponse = junoReactClient.get(key).block();
			AssertJUnit.assertEquals (OperationStatus.Success, mResponse.getStatus());
			AssertJUnit.assertEquals (mResponse.getVersion(), 1);
			AssertJUnit.assertEquals (new String(data), new String(mResponse.getValue()));
			AssertJUnit.assertTrue(mResponse.getTtl() <= 20 && mResponse.getTtl() > 20-3);
			mResponse3 = junoReactClient3.get(key).block();
			AssertJUnit.assertEquals (OperationStatus.Success, mResponse3.getStatus());
			AssertJUnit.assertEquals (mResponse3.getVersion(), 1);
			AssertJUnit.assertEquals (new String(data1), new String(mResponse3.getValue()));
			AssertJUnit.assertTrue(mResponse3.getTtl() <= 100 && mResponse3.getTtl() > 100-3);
			
			LOGGER.info("0");
			LOGGER.info("Completed");
		}
		}catch(Exception e) {
			LOGGER.debug("Exception..");
			AssertJUnit.assertFalse("Exception :"+e.getMessage(),true);
		}
	}
	
	/**
	 * Send a insert request twice with same key to Juno 2.0 server 
	 * It should generate UniqueKeyViolation or JunoException exception.
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testReactCreateWithSameKey() throws JunoException, IOException{
		LOGGER.info( "TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		
		byte[] key = DataGenUtils.genBytes(10);
		byte[] data = DataGenUtils.genBytes(10);
		JunoPropertiesProvider prop = new JunoPropertiesProvider(pConfig);

		JunoResponse junoResponse = junoReactClient.create(key, data).block();
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse.getStatus());
		junoResponse = junoReactClient.get(key).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		AssertJUnit.assertEquals(key, junoResponse.key());
		AssertJUnit.assertTrue(1 == junoResponse.getVersion());
		//It should have used the configured default TTL
		AssertJUnit.assertTrue( junoResponse.getTtl() >= (prop.getDefaultLifetime() -3) &&  junoResponse.getTtl() <= prop.getDefaultLifetime());
		AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
		AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));
		
		try {
			JunoResponse mResponse = junoReactClient.create(key, data).block();
			AssertJUnit.assertEquals (mResponse.getStatus(), OperationStatus.UniqueKeyViolation);
                        JunoResponse mResponse2 = junoReactClient2.create(key, data).block();
                        AssertJUnit.assertEquals (mResponse2.getStatus(), OperationStatus.UniqueKeyViolation);
			LOGGER.info("DupKey");
			LOGGER.info("0");
		} catch (JunoException ex) {
			AssertJUnit.assertTrue ("Exception seen for Duplicate key", false);
		} finally {
			LOGGER.info("Completed");
		}
	}
	/**
	 * Send a insert request with empty key and should generate IllegalArgumentException.
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testReactCreateWithEmptyKey() throws JunoException, IOException{//TODO: reevaluate
		LOGGER.info( "\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));
		
		byte[] key = "".getBytes();
		byte[] data = DataGenUtils.genBytes(10);
		JunoResponse mResponse = null;
		try{
			junoReactClient.create(key, data).block();
			AssertJUnit.assertTrue ("Exception is not seen for empty key", false);
		}catch(Exception mex){
			Assert.assertEquals("The Document key must not be null or empty", mex.getCause().getMessage());
			LOGGER.info("Exception", mex.getMessage());
			LOGGER.info("2");			
			LOGGER.info("Completed");
		} 
		key = "Test_key".getBytes();
		try{
			junoReactClient.create(key, data,(long)0).block();
			AssertJUnit.assertTrue ("Exception is not seen for empty key", false);
		}catch(Exception mex){
			Assert.assertEquals("The Document's TTL cannot be 0 or negative.", mex.getCause().getMessage());
			LOGGER.info("Exception", mex.getMessage());
			LOGGER.info("2");			
			LOGGER.info("Completed");
		} 
	}
	/**
	 * Send a insert request with Null key and should generate IllegalArgumentException.
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testReactCreateWithNullKey() throws JunoException, IOException{
		LOGGER.info( "\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));
		
		byte[] key = null;
		byte[] data = DataGenUtils.genBytes(10);
		try{
			junoReactClient.create(key, data).block();
			AssertJUnit.assertTrue ("Exception is not seen for null key", false);
		} catch (JunoException mex) {
			AssertJUnit.assertEquals("The Document key must not be null or empty", mex.getCause().getMessage());
			LOGGER.info("Exception", mex.getMessage());
			LOGGER.info("2");			
			LOGGER.info("Completed");
		} 
	}
	/**
	 * Send a insert request with 128bytes key and should not generate exceptions
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testReactCreateWith128BytesKey() throws JunoException, IOException{
		LOGGER.info( "\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		JunoPropertiesProvider prop = new JunoPropertiesProvider(pConfig);
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));
		
		byte[] key = DataGenUtils.genBytes(128);
		byte[] data = DataGenUtils.genBytes(10);
		JunoResponse junoResponse = junoReactClient.create(key, data).block();
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse.getStatus());
		junoResponse = junoReactClient.get(key).block();
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse.getStatus());
		AssertJUnit.assertEquals(key, junoResponse.key());
		AssertJUnit.assertTrue(1 == junoResponse.getVersion());
		AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
		AssertJUnit.assertTrue(junoResponse.getTtl() <= prop.getDefaultLifetime() && junoResponse.getTtl() >= prop.getDefaultLifetime()-3);
		AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));
		LOGGER.info("0");
		LOGGER.info("Completed");
	}
	/**
	 * Send a insert request with 256bytes key and should generate IllegalArgumentException
	 * with "Invalid key length. current length=257, max length=256"
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testReactCreateWith129BytesKey() throws JunoException, IOException{
		LOGGER.info( "\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));
		
		byte[] key = DataGenUtils.genBytes(129);
		byte[] data = DataGenUtils.genBytes(10);
		try{
			junoReactClient.create(key, data).block();
			Assert.assertTrue(false);
		}catch(Exception mex){
			LOGGER.debug(mex.getCause().getMessage());
			Assert.assertEquals("The Document key must not be larger than 128 bytes", mex.getCause().getMessage());	
			LOGGER.info("Exception", mex.getCause().getMessage());
			LOGGER.info("2");			
			LOGGER.info("Completed");
		} 
	}
	/**
	 * Send a insert request special characters key and should not generate exceptions
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testReactCreateWithSpecialCharsKey() throws JunoException, IOException{
		LOGGER.info( "\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));
		
		byte[] key = "Q:������������������A:����@@#$%^&*()_+?>,<|}{[]~abc780=.__?|".getBytes();
		byte[] data = "Q:������������������������������������A:���������������������������".getBytes();
		JunoResponse junoResponse = junoReactClient.delete(key).block();
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse.getStatus());
		junoResponse = junoReactClient.create(key, data, (long)100).block();
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse.getStatus());
		junoResponse = junoReactClient.get(key).block();
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse.getStatus());
		AssertJUnit.assertEquals(key, junoResponse.key());
		AssertJUnit.assertTrue(junoResponse.getTtl() <= 100 && junoResponse.getTtl() >= 100-3);
		AssertJUnit.assertTrue(1 == junoResponse.getVersion());
		AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
		AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));
		LOGGER.info("0");
		LOGGER.info("Completed");
	}
	/**
	 * Send a insert request with normal lifetime and should not generate exceptions
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testReactCreateWith10SecLifetime() throws JunoException, IOException, InterruptedException {
		LOGGER.info( "\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));
		
		byte[] key = DataGenUtils.genBytes(10);
		byte[] data = DataGenUtils.genBytes(10);
		long lifetime = 10;
		JunoResponse junoResponse = junoReactClient.create(key, data, lifetime).block();
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse.getStatus());
		junoResponse = junoReactClient.get(key).block();
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse.getStatus());
		AssertJUnit.assertEquals(key, junoResponse.key());
		AssertJUnit.assertTrue(1 == junoResponse.getVersion());	
		AssertJUnit.assertTrue(junoResponse.getTtl() <= lifetime && junoResponse.getTtl() > lifetime-3);	
		AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
		AssertJUnit.assertEquals(OperationStatus.Success,junoResponse.getStatus());
		AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));
		LOGGER.info("0");
		LOGGER.info("Completed");
	}
	/**
	 * Send a insert request with zero lifetime, default lifetime should be used
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testReactCreateWithZeroLifetime() throws JunoException, Exception{
		LOGGER.info( "\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));
		
		byte[] key = DataGenUtils.genBytes(10);
		byte[] data = DataGenUtils.genBytes(10);
		try{
			junoReactClient.create(key, data, (long)0).block();
			AssertJUnit.assertTrue(false);
		}catch(Exception mex){
			AssertJUnit.assertTrue(mex.getCause().getMessage().contains("The Document's TTL cannot be 0 or negative."));
			LOGGER.info("Exception", mex.getCause().getMessage());
			LOGGER.info("2");			
			LOGGER.info("Completed");
		} 
	}
	/**
	 * Send a insert request with Negative lifetime and should generate an
	 * IllegalArgumentException
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testReactCreateWithNegativeLifetime() throws JunoException, IOException{
		LOGGER.info( "\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));
		
		byte[] key = DataGenUtils.genBytes(10);
		byte[] data = DataGenUtils.genBytes(10);
		try{
			junoReactClient.create(key, data, (long)-1).block();
			AssertJUnit.assertTrue ("Exception is NOT seen for negative lifetime", false);
		}catch(JunoException mex) {
			AssertJUnit.assertTrue(mex.getCause().getMessage().contains("The Document's TTL cannot be negative. "));
			LOGGER.info("Exception", mex.getCause().getMessage());
			LOGGER.info("2");			
			LOGGER.info("Completed");
		} 
	}
	/**
	 * Send a insert request with null lifetime and should generate an
	 * IllegalArgumentException //TODO: should 0 lifetime for create throw exception??
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testReactCreateWithNullLifetime() throws JunoException, IOException{
		LOGGER.info( "\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));
		
		byte[] key = DataGenUtils.genBytes(10);
		byte[] data = DataGenUtils.genBytes(10);
		try{
			junoReactClient.create(key, data, (long)0).block();
			AssertJUnit.assertTrue(false);				
		}catch(JunoException mex) {
			AssertJUnit.assertEquals("The Document's TTL cannot be 0 or negative.",mex.getCause().getMessage());
			LOGGER.info("Exception", mex.getCause().getMessage());
			LOGGER.info("2");			
			LOGGER.info("Completed");
		} 
	}
	
	/**
	 * Send a insert request with max lifetime 3 days and should not generate exceptions
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testReactCreateWith3DaysLifetime() throws JunoException, IOException{
		LOGGER.info( "\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));

		byte[] key = DataGenUtils.genBytes(10);
		byte[] data = DataGenUtils.genBytes(10);
		long lifetime = 259200;
		
		JunoResponse junoResponse = junoReactClient.create(key, data, lifetime).block();
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse.getStatus());
		junoResponse = junoReactClient.get(key).block();
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse.getStatus());
		AssertJUnit.assertEquals(key, junoResponse.key());	
		AssertJUnit.assertTrue(1 == junoResponse.getVersion());
		AssertJUnit.assertTrue(lifetime-3 <= junoResponse.getTtl() && junoResponse.getTtl() <= lifetime);	
		AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
		AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));
		LOGGER.info("0");
		LOGGER.info("Completed");
	}
	/**
	 * Send a insert request with more than max lifetime 3 days and should 
	 * generate IllegalArgumentException
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testReactCreateWithMorethan3DaysLifetime() throws JunoException, IOException{
		LOGGER.info( "\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));
		
		byte[] key = DataGenUtils.genBytes(10);
		byte[] data = DataGenUtils.genBytes(10);
		long lifetime = 259201;
		try{
			junoReactClient.create(key, data, lifetime).block();
			AssertJUnit.assertTrue ("Exception is not seen for TTL > 3 days", false);
		}catch(JunoException mex) {
			AssertJUnit.assertTrue(mex.getCause().getMessage().contains("Invalid lifetime"));
			LOGGER.info("Exception", mex.getCause().getMessage());
			LOGGER.info("2");			
			LOGGER.info("Completed");
		} 
	}
	/**
	 * Send a insert request with null payload and should create successfully
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testReactCreateWithNullPayload() throws JunoException, IOException{
		LOGGER.info( "\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));

		byte[] key = DataGenUtils.genBytes(64);
		try{
			JunoResponse res = junoReactClient.create(key, null).block();
			AssertJUnit.assertEquals(OperationStatus.Success,res.getStatus());
			AssertJUnit.assertEquals(1,res.getVersion());
			//AssertJUnit.assertTrue ("Exception is NOT seen for Null payload", false);
		}catch(Exception mex){
			AssertJUnit.assertFalse(true);
			LOGGER.info("Exception", mex.getCause().getMessage());
			LOGGER.info("2");			
			LOGGER.info("Completed");
		} 
	}
	/**
	 * Send a insert request with zero size payload and should create successfully 
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testReactCreateWithZeroPayload() throws JunoException, IOException{
		LOGGER.info( "\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));
		
		byte[] data = DataGenUtils.genBytes(0);
		byte[] key = DataGenUtils.genBytes(64);
		
		JunoResponse mResponse = junoReactClient.create(key, data).block();
		AssertJUnit.assertEquals(OperationStatus.Success , mResponse.getStatus());
		AssertJUnit.assertEquals(1, mResponse.getVersion());
			
		mResponse = junoReactClient.get(key).block();
		AssertJUnit.assertEquals (OperationStatus.Success, mResponse.getStatus());
		AssertJUnit.assertEquals(1, mResponse.getVersion());
		AssertJUnit.assertEquals(key, mResponse.getKey());
		LOGGER.info("0");
		LOGGER.info("Completed");	
	}
	
	/**
	 * Send a insert request with 100KB payload and should not generate exception
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testReactCreateWith100KBPayload() throws JunoException, Exception{
		LOGGER.info( "\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));
				
		byte[] data = DataGenUtils.genBytes(100200);
		byte[] key = DataGenUtils.genBytes(64);
		long lifetime = 4;
		JunoResponse junoResponse = junoReactClient.create(key, data, lifetime).block();
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse.getStatus());
		junoResponse = junoReactClient.get(key).block();
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse.getStatus());
		AssertJUnit.assertEquals(key, junoResponse.key());
		AssertJUnit.assertTrue(1 == junoResponse.getVersion());
		AssertJUnit.assertTrue(junoResponse.getTtl() <= lifetime &&  junoResponse.getTtl() >= lifetime-3);
		AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
		AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));
		
		LOGGER.debug("Sleep 3 seconds to expire daata");
		Thread.sleep (5000);
		try {
			JunoResponse mResponse = junoReactClient.get(key).block();
			AssertJUnit.assertEquals (OperationStatus.NoKey, mResponse.getStatus());
		} catch (JunoException mex) {
			AssertJUnit.assertTrue ("Exception is not seen for empty key", false);
		} finally {			
			LOGGER.info("0");	
			LOGGER.info("Completed");
		}
	} 

	/**
	 * Send a insert request with 200KB payload and should not generate exception
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testReactCreateWith200KBPayload() throws IOException{
		LOGGER.info( "\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));
		
		byte[] data = DataGenUtils.genBytes(204800);
		byte[] key = DataGenUtils.genBytes(64);
		JunoResponse junoResponse = junoReactClient.create(key, data).block();
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse.getStatus());
		junoResponse = junoReactClient.get(key).block();
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse.getStatus());
		AssertJUnit.assertEquals(key, junoResponse.key());
		AssertJUnit.assertTrue(1 == junoResponse.getVersion());
		AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
		AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));
		LOGGER.info("0");
		LOGGER.info("Completed");
	}

	/**
	 * Send a insert request with more than 200KB payload and should generate 
	 * IllegalArgumentException
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testReactCreateWithMorethan200KBPayload() throws JunoException, IOException{
		LOGGER.info( "\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));

		byte[] data = DataGenUtils.genBytes(204801);
		byte[] key = DataGenUtils.genBytes(64);
		try{
			junoReactClient.create(key, data).block();
			AssertJUnit.assertTrue ("Exception is not seen for payload > 200KB", false);
		}catch(JunoException mex) {
			AssertJUnit.assertTrue(mex.getCause().getMessage().contains("The Document Value must not be larger than"));
			LOGGER.info("Exception", mex.getCause().getMessage());
			LOGGER.info("2");			
			LOGGER.info("Completed");
		} 
	}
	
	/**
	 * Send a insert request with more than 1KB payload and see if its compressed 
	 * IllegalArgumentException
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testReactCreateWithAndWithoutCompression() throws JunoException, IOException{
		LOGGER.info( "\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));

		byte[] data = new String("Couchbase stores data as key value pairs where the value is a JSON document and the key is an identifier for retrieving that document. By default cbexport will only export the value portion of the document. If you wish to include the key in the exported document then this option should be specified. The value passed to this option should be the field name that the key is stored under.Skips the SSL verification phase. Specifying this flag will allow a connection using SSL encryption, but will not verify the identity of the server you connect to. You are vulnerable to a man-in-the-middle attack if you use this flag. Either this flag or the --cacert flag must be specified when using an SSL encrypted connection Specifies a CA certificate that will be used to verify the identity of the server being connecting to. Either this flag or the --no-ssl-verify flag must be specified when using an SSL encrypted connection. Specifies the number of concurrent clients to use when exporting data. Fewer clients means exports will take longer, but there will be less cluster resources used to complete the export. More clients means faster exports, but at the cost of more cluster resource usage. This parameter defaults to 1 if it is not specified and it is recommended that this parameter is not set to be higher than the number of CPUs on the machine where the export is taking place. Exports JSON data from Couchbase. The cbexport-json command supports exporting JSON docments to a file with a document on each line or a file that contain a JSON list where each element is a document. The file format to export to can be specified with the --format flag. See the DATASET FORMATS section below for more details on the supported file formats.").getBytes();
		byte[] key = DataGenUtils.genBytes(64);
		byte[] key1 = DataGenUtils.genBytes(64);
		try{
			//Write with payload compression enabled client and read it via client without compression enabled.
			JunoResponse junoResponse = junoReactClient4.create(key, data).block();
			AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
			junoResponse = junoReactClient3.get(key).block();
			AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
			AssertJUnit.assertTrue(Arrays.equals(data, junoResponse.getValue()));
			
			//Write with out compression and read using compression enabled.
			junoResponse = junoReactClient3.create(key1, data).block();
			AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
			junoResponse = junoReactClient4.get(key1).block();
			AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
			AssertJUnit.assertTrue(Arrays.equals(data, junoResponse.getValue()));
			
			LOGGER.info("1");			
			LOGGER.info("Completed");
		}catch(JunoException mex) {
			AssertJUnit.assertTrue(mex.getCause().getMessage().contains("The Document Value must not be larger than"));
			LOGGER.info("Exception", mex.getCause().getMessage());
			LOGGER.info("2");			
			LOGGER.info("Completed");
		} 
	}
	
	/*************************************************
	 * Send a insert request with same key for two different name spaces and should not
	 * generate exceptions
	 * 
	 *************************************************/
	@Test
	public void testReactCreateWithSameKeyInDifferentNS() throws JunoException, IOException{
		LOGGER.info( "\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));
		
		byte[] data = DataGenUtils.genBytes(10);
		byte[] data1 = DataGenUtils.genBytes(10);
		byte[] key = "TestKey1".getBytes();
		JunoResponse junoResponse = junoReactClient.delete(key).block();
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse.getStatus());
		junoResponse = junoReactClient1.delete(key).block();
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse.getStatus());
		junoResponse = junoReactClient.create(key, data).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		AssertJUnit.assertEquals(key, junoResponse.key());
		AssertJUnit.assertTrue(1 == junoResponse.getVersion());
		junoResponse = junoReactClient.get(key).block();
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse.getStatus());
		AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
		AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));

		JunoResponse junoResponse1 = junoReactClient1.create(key, data1).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse1.getStatus());
		AssertJUnit.assertEquals(key, junoResponse1.key());
		AssertJUnit.assertTrue(1 == junoResponse1.getVersion());
		junoResponse1 = junoReactClient1.get(key).block();
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse1.getStatus());
		AssertJUnit.assertEquals(data1.length, junoResponse1.getValue().length);
		AssertJUnit.assertEquals(new String(data1), new String(junoResponse1.getValue()));

		junoResponse = junoReactClient1.delete(key).block();
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse.getStatus());
		junoResponse = junoReactClient.get(key).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		try {
			junoResponse1 = junoReactClient1.get(key).block();
			AssertJUnit.assertEquals(OperationStatus.NoKey, junoResponse1.getStatus());
		} catch (JunoException mex) {
			AssertJUnit.assertTrue ("Exception is seen for No key", false);
		}
				
		junoResponse1 = junoReactClient1.create(key, data, (long)30).block(); //insert same key/value with different ttl
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse1.getStatus());
		AssertJUnit.assertEquals(junoResponse.key(), junoResponse1.key());
		AssertJUnit.assertEquals(junoResponse.getVersion(), junoResponse1.getVersion());
		AssertJUnit.assertNotSame(junoResponse.getTtl(), junoResponse1.getTtl());
		junoResponse1 = junoReactClient1.get(key).block();
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse1.getStatus());
		AssertJUnit.assertEquals(junoResponse.getValue().length, junoResponse1.getValue().length);
		AssertJUnit.assertEquals(new String(junoResponse.getValue()), new String(junoResponse1.getValue()));
		LOGGER.info("0");
		LOGGER.info("Completed");
	}
	/*************************************************
	 * insert two records with same key and name space.
	 * The 2nd insert fails and should generate an exception
	 *************************************************/
	@Test
	public void testReactCreateWithSameKeysInSameNS() throws JunoException, IOException{
		LOGGER.info( "\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));
		
		byte[] data = DataGenUtils.genBytes(10);
		byte[] data2 = DataGenUtils.genBytes(10);
		byte[] key = DataGenUtils.genBytes(10);
			
		JunoResponse junoResponse =junoReactClient.create(key, data).block();
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse.getStatus());
		junoResponse = junoReactClient.get(key).block();
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse.getStatus());
		AssertJUnit.assertEquals(key, junoResponse.key());
		AssertJUnit.assertTrue(1 == junoResponse.getVersion());
		AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
		AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));

		//Create with same key, different data			
		try {
			JunoResponse mResponse = junoReactClient.create(key, data2).block();
			AssertJUnit.assertEquals (OperationStatus.UniqueKeyViolation,mResponse.getStatus());	
			LOGGER.info("DupKey");
			LOGGER.info("0");
		} catch (JunoException ex) {
			AssertJUnit.assertTrue ("Exception is seen for Duplicate key", false);
		} 
		
		JunoResponse gResponse = junoReactClient2.get(key).block();
		AssertJUnit.assertEquals (OperationStatus.Success, gResponse.getStatus());
		AssertJUnit.assertEquals(new String(data), new String(gResponse.getValue()));	
		LOGGER.info("0");
		LOGGER.info("Completed");
	}
	
	/*************************************************
	 * insert two clients with same ns but diff app name
	 * Create 1 record each using clients1 and client2.
	 * The 2nd insert will fail.
	 *************************************************/
	@Test
	public void testReactCreateWithSameNameSpaceDiffAppname() throws JunoException, IOException{
		LOGGER.info( "\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));
		byte[] data = DataGenUtils.genBytes(10);
		byte[] key = DataGenUtils.genBytes(64);
		
		JunoResponse junoResponse = junoReactClient.create(key, data).block();
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse.getStatus());
		junoResponse = junoReactClient.get(key).block();
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse.getStatus());
		AssertJUnit.assertEquals(key, junoResponse.key());
		AssertJUnit.assertTrue(1 == junoResponse.getVersion());
		AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
		AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));
		
		//Create same key
		try {
			JunoResponse mResponse = junoReactClient2.create(key, data).block();
			AssertJUnit.assertEquals (mResponse.getStatus(), OperationStatus.UniqueKeyViolation);	
			LOGGER.info("DupKey");
			LOGGER.info("0");
		} catch (JunoException ex) {
			AssertJUnit.assertTrue(false);
		} finally {
			LOGGER.info("Completed");
		}
	}
	
	//Need to rewrite the below test cases
	/**********************************************
	 * app_name length > 32 (MF_APP_NAME_MAX_LEN)
	 *********************************************/
	@Test
	public void testValidateAppnameLength() throws JunoException, IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException, KeyManagementException {
		LOGGER.info( "\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));
		
		byte[] data = DataGenUtils.genBytes(10);
		byte[] key = DataGenUtils.genBytes(64);		
		String appname = DataGenUtils.genString(32);
		String appname1 = DataGenUtils.genString(33);
				
		URL url = ReactorCreateTest.class.getResource("/com/paypal/juno/Juno.properties");
		Properties pConfig = new Properties();
		pConfig.load(url.openStream());
		pConfig.setProperty(JunoProperties.APP_NAME, appname);
		junoReactClient = JunoClientFactory.newJunoReactClient(new JunoPropertiesProvider(pConfig), SSLUtil.getSSLContext());
		JunoResponse junoResponse = junoReactClient.create(key, data).block();
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse.getStatus());
		JunoResponse result = junoReactClient.get(key).block();
		AssertJUnit.assertEquals (OperationStatus.Success, result.getStatus());
		AssertJUnit.assertEquals(data.length, result.getValue().length);
		AssertJUnit.assertEquals(new String(data), new String(result.getValue()));
		
		pConfig.setProperty(JunoProperties.APP_NAME, "JunoTestApp"); //set back to orig
		junoReactClient = JunoClientFactory.newJunoReactClient(new JunoPropertiesProvider(pConfig), SSLUtil.getSSLContext());

		try{
			pConfig.setProperty(JunoProperties.APP_NAME, appname1);
			junoReactClient = JunoClientFactory.newJunoReactClient(new JunoPropertiesProvider(pConfig), SSLUtil.getSSLContext());
			junoReactClient.create(key, data).block();	
			AssertJUnit.assertTrue(false);
		}catch(Exception mex){
			AssertJUnit.assertTrue(mex.getCause().getMessage().contains("Application Name length exceeds MAX LENGTH of 32 bytes"));
			LOGGER.info("Exception", mex.getCause().getMessage());
			LOGGER.info("2");			
			LOGGER.info("Completed");		
		} finally {
			pConfig.setProperty(JunoProperties.APP_NAME, "JunoTestApp");	// set back to orig
			junoReactClient = JunoClientFactory.newJunoReactClient(new JunoPropertiesProvider(pConfig), SSLUtil.getSSLContext());
		}
	}
	/*****************************************************
	 * namespace_name lenght > 64 (MF_NAMESPACE_MAX_LEN)
	 *****************************************************/
	@Test
	public void testValidateNamespaceLength() throws JunoException, IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException, KeyManagementException {
		LOGGER.info( "\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));
		
		byte[] data = DataGenUtils.genBytes(10);
		byte[] key = DataGenUtils.genBytes(64);
		byte[] key1 = DataGenUtils.genBytes(64);
		String namespace = DataGenUtils.genString(64);
		String namespace1 = DataGenUtils.genString(65);
		
		pConfig.setProperty(JunoProperties.RECORD_NAMESPACE, namespace);
		junoReactClient = JunoClientFactory.newJunoReactClient(new JunoPropertiesProvider(pConfig), SSLUtil.getSSLContext());		
		JunoResponse junoResponse = junoReactClient.create(key, data).block();
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse.getStatus());
		JunoResponse result = junoReactClient.get(key).block();
		AssertJUnit.assertEquals (OperationStatus.Success, result.getStatus());
		AssertJUnit.assertEquals(data.length, result.getValue().length);
		AssertJUnit.assertEquals(new String(data), new String(result.getValue()));	
		pConfig.setProperty(JunoProperties.RECORD_NAMESPACE, "JunoNS1"); //set back to orig junoReactClient
		junoReactClient = JunoClientFactory.newJunoReactClient(new JunoPropertiesProvider(pConfig), SSLUtil.getSSLContext());		
		try{
			pConfig.setProperty(JunoProperties.RECORD_NAMESPACE, namespace1);
			junoReactClient = JunoClientFactory.newJunoReactClient(new JunoPropertiesProvider(pConfig), SSLUtil.getSSLContext());
			AssertJUnit.assertTrue(false);			
		}catch(Exception mex){
			AssertJUnit.assertEquals("Namespace length exceeds MAX LENGTH of 64 bytes", mex.getMessage());
			LOGGER.info("Exception", mex.getCause().getMessage());
			LOGGER.info("2");			
			LOGGER.info("Completed");						
		} finally {
			pConfig.setProperty(JunoProperties.RECORD_NAMESPACE, "JunoNS1"); //set back to orig junoReactClient1
			junoReactClient = JunoClientFactory.newJunoReactClient(new JunoPropertiesProvider(pConfig), SSLUtil.getSSLContext());		
		}
	}

	/*****************************************************************
	 * m_max_lifetime > Juno_CONFIG_DEFAULT_MAX_LIFETIME (259200)
	 *****************************************************************/
	@Test
	public void testValidateByChangingMaxLifeTime() throws JunoException, IOException{
		LOGGER.info( "\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));
		
		byte[] data = DataGenUtils.genBytes(10);
		byte[] key = DataGenUtils.genBytes(64);
		try{
			junoReactClient2.create(key, data, (long)259201).block();
			AssertJUnit.assertTrue ("Exception is NOT seen for invalid lifetime", false);
		}catch(Exception mex){
			AssertJUnit.assertTrue(mex.getCause().getMessage().contains("Invalid lifetime. current lifetime=259201, max configured lifetime=259200"));
			LOGGER.info("Exception", mex.getCause().getMessage());
			LOGGER.info("2");			
			LOGGER.info("Completed");
		} 
	}
	
	/***********************************************************************
	 * m_max_payload_size > Juno_CONFIG_DEFAULT_MAX_PAYLOAD_SIZE (204800)
	 ***********************************************************************/
	@Test
	public void testValidateByChangingMaxPayloadSize() throws JunoException, IOException{
		LOGGER.info( "\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));
		
		byte[] data = DataGenUtils.genBytes(204801);
		byte[] key = DataGenUtils.genBytes(64);
		try{
			junoReactClient2.create(key, data).block();
			AssertJUnit.assertTrue ("Exception is NOT seen for invalid payload", false);
		}catch(JunoException mex) {
			AssertJUnit.assertTrue(mex.getCause().getMessage().contains("The Document Value must not be larger than "));
			LOGGER.info("Exception", mex.getCause().getMessage());
			LOGGER.info("2");			
			LOGGER.info("Completed");
		} 
	}

	@Test
	public void testReactCreateWithChineseKey() throws JunoException, Exception {
		LOGGER.info( "\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));
		
		JunoPropertiesProvider prop = new JunoPropertiesProvider(pConfig);
		String key = "Q:������������������������A:������. Q:����� A:����"; 
		String key1 = DataGenUtils.createKey(127);

		byte[] data =  "New data testing".getBytes();
		byte[] data1 = "New data111111111111".getBytes();
		
		byte [] keybyte = key.getBytes();
		LOGGER.debug(new String (keybyte) + "length is: " + keybyte.length);
		JunoResponse junoResponse = junoReactClient.delete(keybyte).block();
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse.getStatus());
		junoResponse  = junoReactClient.create(keybyte, data).block();
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse.getStatus());
		JunoResponse junoResponse1 = junoReactClient.create(key1.getBytes(), data1).block();
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse1.getStatus());
		junoResponse =  junoReactClient.get(keybyte).block();
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse.getStatus());
		junoResponse1 =  junoReactClient.get(key1.getBytes()).block();
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse1.getStatus());
		LOGGER.debug("Status: " + junoResponse1.getStatus());
		LOGGER.debug("Data: " + new String (junoResponse.getValue()));
		LOGGER.debug("Data1: " + new String (junoResponse1.getValue()));		
		LOGGER.info("0");
		LOGGER.info("Completed");
	}
	
	@Test
	public void testReactCreateWithChineseKeyBigThan128() throws JunoException, Exception {
		LOGGER.info( "\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));
		
		JunoPropertiesProvider prop = new JunoPropertiesProvider(pConfig);
		String key = "Q:������������������������A:������. Q:������������������������� A:������������B:�����B:�����B:������������������������� AQ:������������������������� "; 

		byte[] data =  "New data testing".getBytes();
		try{
			byte [] keybyte = key.getBytes();
			LOGGER.debug(new String (keybyte) + "length is: " + keybyte.length);
			JunoResponse junoResponse = junoReactClient.delete(keybyte).block();
			AssertJUnit.assertEquals (OperationStatus.Success, junoResponse.getStatus());
			junoResponse  = junoReactClient.create(keybyte, data).block();
			AssertJUnit.assertEquals (OperationStatus.Success, junoResponse.getStatus());
			junoResponse =  junoReactClient.get(keybyte).block();
			LOGGER.debug("Data: " + new String (junoResponse.getValue()));
			AssertJUnit.assertTrue(false);
		}catch(Exception mex){
			AssertJUnit.assertTrue(mex.getCause().getMessage().contains("The Document key must not be larger than " + prop.getMaxKeySize()));
			LOGGER.info("Exception", mex.getCause().getMessage());
			LOGGER.info("2");			
			LOGGER.info("Completed");
		} 
	}
	
	//@Test
	public void testReactCreateResponseTimeout() throws IOException{
		LOGGER.info( "\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));
		
		URL url3 = BatchCreateTest.class.getResource("/com/paypal/juno/Juno.properties");
		Properties pConfig3 = new Properties();
		pConfig3.load(url3.openStream());
		pConfig3.setProperty(JunoProperties.APP_NAME, "QATestApp3");
		pConfig3.setProperty(JunoProperties.RECORD_NAMESPACE, "NS3");
		pConfig3.setProperty(JunoProperties.RESPONSE_TIMEOUT, "1");
		JunoReactClient junoReactClient3 = null;
		try {
			junoReactClient3 = JunoClientFactory.newJunoReactClient(new JunoPropertiesProvider(pConfig3), SSLUtil.getSSLContext());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		byte[] key = DataGenUtils.genBytes(128);
		byte[] payload = DataGenUtils.genBytes(204700);
		long ttl = (long)10000;
		
		try {
			JunoResponse junoResponse  = junoReactClient3.create(key, payload).block();	
			AssertJUnit.assertTrue("should hit response timeout exception", false);
		} catch (JunoException mex) {
			LOGGER.debug("Exception occurs: " + mex.getMessage());
			AssertJUnit.assertTrue(mex.getMessage().contains("Response Timed out"));
			LOGGER.info("Exception", mex.getMessage());
			LOGGER.info("2");			
			LOGGER.info("Completed");			
		} 
	}
	
	//@Test 
	/**
	 * This test case will run successfully only after shutdown proxy, so either disable
	 * this test case or shutdown proxy and then run this test 
	 * @throws JunoException //TODO: it doesn't throw connection timeout, but only response timeout
	 */
	
	//TODO: BUG? exception is not connection timeout but response timeout
	//@Test
	public void testReactCreateConnectionTimeout() throws IOException{
		LOGGER.info( "\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));
		
		URL url1 = BatchCreateTest.class.getResource("/com/paypal/juno/Juno.properties");
		pConfig1 = new Properties();
		pConfig1.load(url1.openStream());
		pConfig1.setProperty(JunoProperties.APP_NAME, "QATestApp");
		pConfig1.setProperty(JunoProperties.RECORD_NAMESPACE, "NS2");
		pConfig1.setProperty(JunoProperties.CONNECTION_TIMEOUT, "1");
		JunoReactClient junoReactClient3 = null;
		try {
			junoReactClient3 = JunoClientFactory.newJunoReactClient(new JunoPropertiesProvider(pConfig1), SSLUtil.getSSLContext());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		byte[] key = DataGenUtils.genBytes(12);
		byte[] payload = DataGenUtils.genBytes(20);
		long ttl = (long)10000;
		
		try {
			JunoResponse junoResponse  = junoReactClient3.create(key, payload).block();	
			AssertJUnit.assertTrue("should hit connection timeout exception", false);
		} catch (JunoException mex) {
			LOGGER.debug("Exception occurs: " + mex.getMessage());
			AssertJUnit.assertTrue(mex.getMessage().contains("Connection Timedout"));
			LOGGER.info("Exception", mex.getCause().getMessage());
			LOGGER.info("2");			
			LOGGER.info("Completed");
		} 
	}	
	
	/**
	 * Test client call won't be disrupted after server timeout
	 */
	@Test
	public void testReactCreateServerIdletimeout() throws JunoException, IOException{		
		LOGGER.info( "\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		JunoPropertiesProvider prop = new JunoPropertiesProvider(pConfig);
		JunoPropertiesProvider prop3 = new JunoPropertiesProvider(pConfig3);
		try {
		for(int i=0; i < 10; i++){			 //create(key, data)
			
			LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
			byte[] data = DataGenUtils.genBytes(10);
			byte[] key = DataGenUtils.genBytes(64);
			JunoResponse mResponse = junoReactClient.create(key, data).block();	
			AssertJUnit.assertEquals (OperationStatus.Success, mResponse.getStatus());
			AssertJUnit.assertTrue(mResponse.getTtl() <= prop.getDefaultLifetime() && mResponse.getTtl() >= prop.getDefaultLifetime()-3);
			JunoResponse mResponse3 = junoReactClient3.create(key, data).block();	
			AssertJUnit.assertEquals (OperationStatus.Success, mResponse3.getStatus());
			AssertJUnit.assertEquals(mResponse3.getTtl(), prop3.getDefaultLifetime().longValue());
			AssertJUnit.assertTrue(mResponse3.getTtl() <= prop3.getDefaultLifetime() && mResponse3.getTtl() >= prop3.getDefaultLifetime()-3);
			
			mResponse = junoReactClient.get(key).block();
			AssertJUnit.assertEquals (OperationStatus.Success, mResponse.getStatus());
			AssertJUnit.assertEquals (new String(data), new String(mResponse.getValue()));
			AssertJUnit.assertTrue (mResponse.getStatus().getCode() == OperationStatus.Success.getCode());
			LOGGER.debug("default ttl is " + mResponse.getTtl());
			AssertJUnit.assertTrue(mResponse.getTtl() <= prop.getDefaultLifetime() && mResponse.getTtl() > prop.getDefaultLifetime()-3);
			mResponse3 = junoReactClient3.get(key).block();
			AssertJUnit.assertEquals (OperationStatus.Success, mResponse3.getStatus());
			AssertJUnit.assertEquals (new String(data), new String(mResponse3.getValue()));
			AssertJUnit.assertTrue (mResponse3.getStatus().getCode() == OperationStatus.Success.getCode());
			AssertJUnit.assertTrue(mResponse3.getTtl() <= prop3.getDefaultLifetime() && mResponse3.getTtl() >= prop3.getDefaultLifetime()-3);
			LOGGER.info("0");
			LOGGER.info("Completed");
		}	
		}catch(Exception e) {
			AssertJUnit.assertTrue("shouldn't throw exception here", false);
		}
	}

        @Test
        public void testOrigSizeExceedsNotAfterCompress() throws Exception {
                LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
                LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));
                byte[] data = DataGenUtils.createCompressablePayload(300000).getBytes();
                byte[] key = new String(DataGenUtils.createKey(50) + "testExceedsCompressLimit").getBytes();
                long lifetime = 25;

                JunoPropertiesProvider prop = new JunoPropertiesProvider(pConfig2);
                prop = new JunoPropertiesProvider(pConfig);
                int dataLength = Snappy.compress(data).length;
                LOGGER.info("data original length is:" + data.length + "compressed length is " + dataLength);

                JunoResponse junoResponse = junoReactClient.create(key, data, lifetime).block();
                AssertJUnit.assertEquals (OperationStatus.Success,junoResponse.getStatus());
                junoResponse = junoReactClient.get(key).block();
                AssertJUnit.assertEquals (OperationStatus.Success,junoResponse.getStatus());
                AssertJUnit.assertEquals(key, junoResponse.key());
                AssertJUnit.assertTrue(1 == junoResponse.getVersion());
                AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
                AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));
        }}
