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

import com.paypal.juno.client.JunoClient;
import com.paypal.juno.client.JunoClientFactory;
import com.paypal.juno.client.io.JunoResponse;
import com.paypal.juno.client.io.OperationStatus;
import com.paypal.juno.client.io.RecordContext;
import com.paypal.juno.conf.JunoProperties;
import com.paypal.juno.conf.JunoPropertiesProvider;
import com.paypal.juno.exception.JunoException;
import com.paypal.juno.util.SSLUtil;
import java.io.IOException;
import java.net.URL;
import java.util.Properties;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.xerial.snappy.Snappy;

public class SetTest{
	private JunoClient junoClient;
	private JunoClient junoClient1;
	private JunoClient junoClient2;
	private Properties pConfig;
	private Properties pConfig1;
	private Properties pConfig2;
	private Logger LOGGER;

	@BeforeClass
	public void setup() throws  IOException, InterruptedException {

		LOGGER = LoggerFactory.getLogger(SetTest.class);
        

		URL url = SetTest.class.getResource("/com/paypal/juno/Juno.properties");
		pConfig = new Properties();
		pConfig.load(url.openStream());
		pConfig.setProperty(JunoProperties.APP_NAME, "QATestApp");
		pConfig.setProperty(JunoProperties.RECORD_NAMESPACE, "NS1");
		pConfig.setProperty(JunoProperties.USE_PAYLOADCOMPRESSION, "true");
		URL urlPem = SetTest.class.getResource("/secrets/server.pem");
		URL urlCrt = SetTest.class.getResource("/secrets/server.crt");
		junoClient = new JunoTestClientImpl(new JunoPropertiesProvider(pConfig), SSLUtil.getSSLContext(urlCrt.getPath(), urlPem.getPath()),0);

		URL url1 = SetTest.class.getResource("/com/paypal/juno/Juno.properties");
		pConfig1 = new Properties();
		pConfig1.load(url1.openStream());
		pConfig1.setProperty(JunoProperties.APP_NAME, "QATestApp");
		pConfig1.setProperty(JunoProperties.RECORD_NAMESPACE, "NS2");

		URL url2 = JunoClientFactory.class.getResource("/com/paypal/juno/Juno.properties");
		pConfig2 = new Properties();
		pConfig2.load(url2.openStream());
		pConfig2.setProperty(JunoProperties.APP_NAME, "QATestApp");
		pConfig2.setProperty(JunoProperties.RECORD_NAMESPACE, "NS1");

		try{
			junoClient1 = JunoClientFactory.newJunoClient(new JunoPropertiesProvider(pConfig1), SSLUtil.getSSLContext("server.pem", "server.crt"));
			junoClient2 = JunoClientFactory.newJunoClient(new JunoPropertiesProvider(pConfig2), SSLUtil.getSSLContext());
		}catch (Exception e) {
			throw new RuntimeException(e);
		}

		Thread.sleep(1000);
	}

	/**
	 * Send a set request to the juno 2.0 server with specified key which does not exist.
	 * It should not generate any exception, if there is no problem on server side.
	 * @throws JunoException
	 */
	@Test
	public void testsetWithKey() throws IOException{
		JunoPropertiesProvider prop = new JunoPropertiesProvider(pConfig);
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	    
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] key = DataGenUtils.genBytes(64);
		byte[] data = DataGenUtils.genBytes(10);
		byte[] data1 = DataGenUtils.genBytes(11);
		byte[] data2 = DataGenUtils.genBytes(20);
		byte[] data3 = DataGenUtils.genBytes(40);
		byte[] data4 = DataGenUtils.genBytes(800);		
		long lifetime = 22000;
		
		JunoResponse mResponse = junoClient.set(key, data);
		AssertJUnit.assertEquals (OperationStatus.Success,mResponse.getStatus());
		AssertJUnit.assertTrue (mResponse.getStatus().getCode() == OperationStatus.Success.getCode());
		LOGGER.debug("set ttl is " + mResponse.getTtl());
		AssertJUnit.assertTrue(mResponse.getTtl() == prop.getDefaultLifetime());
						
		mResponse = junoClient.set(key, data1); //ttl won't be updated
		AssertJUnit.assertEquals (OperationStatus.Success,mResponse.getStatus());
		AssertJUnit.assertTrue(2 == mResponse.getVersion());
		AssertJUnit.assertTrue(mResponse.getTtl() <= prop.getDefaultLifetime() && mResponse.getTtl() >= prop.getDefaultLifetime()-3);
		
		JunoResponse junoResponse = junoClient.set(key,data2, lifetime);
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		AssertJUnit.assertTrue(3 == junoResponse.getVersion());
		AssertJUnit.assertTrue(lifetime == junoResponse.getTtl());		
		
		junoResponse = junoClient.set(key, data3, 10);
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		AssertJUnit.assertTrue(4 == junoResponse.getVersion());
		AssertJUnit.assertTrue(junoResponse.getTtl() >=9 );
		
		junoResponse = junoClient.set(key, data4, 0);
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		AssertJUnit.assertTrue(5 == junoResponse.getVersion());
		AssertJUnit.assertTrue(junoResponse.getTtl() <= lifetime && junoResponse.getTtl() >= lifetime-3);	

		junoResponse = junoClient.get(key);
		AssertJUnit.assertEquals (OperationStatus.Success,junoResponse.getStatus());
		AssertJUnit.assertEquals(key, junoResponse.key());
		AssertJUnit.assertTrue(5 == junoResponse.getVersion());
		AssertJUnit.assertEquals(data4.length, junoResponse.getValue().length);
		AssertJUnit.assertEquals(new String(data4), new String(junoResponse.getValue()));
		AssertJUnit.assertTrue(junoResponse.getTtl() <= lifetime && junoResponse.getTtl() >= lifetime-3);
				
		key = DataGenUtils.genBytes(64);
		data = DataGenUtils.genBytes(10);
		JunoResponse junoResponse1 = junoClient1.set(key, data, 0); //it gets default time from server side which is 3600
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse1.getStatus());
		AssertJUnit.assertTrue(1 == junoResponse1.getVersion());
		
		LOGGER.info("0");
		LOGGER.info("Completed");
	}
	
	/**
	 * Send a set request twice with same key to juno 2.0 server 
	 * It should be successfull.
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testsetWithSameKey() throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	    
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] key = DataGenUtils.genBytes(64);
		byte[] data = DataGenUtils.genBytes(10);
		byte[] data2 = DataGenUtils.genBytes(20);

		long lifetime = 100;
		JunoResponse junoResponse = junoClient.set(key, data);
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		junoResponse = junoClient.get(key);
		AssertJUnit.assertEquals (OperationStatus.Success,junoResponse.getStatus());
		AssertJUnit.assertEquals(key, junoResponse.key());
		AssertJUnit.assertTrue(1 == junoResponse.getVersion());
		AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
		AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));

		JunoResponse mResponse = junoClient1.set(key, data2, lifetime);
		AssertJUnit.assertEquals (OperationStatus.Success,mResponse.getStatus());
		AssertJUnit.assertTrue(1 == junoResponse.getVersion());
		AssertJUnit.assertTrue(key == junoResponse.getKey());	
		mResponse = junoClient1.get(key);
		AssertJUnit.assertEquals(OperationStatus.Success, mResponse.getStatus());
		AssertJUnit.assertEquals(new String(data2), new String(mResponse.getValue()));
		mResponse = junoClient.get(key);
		AssertJUnit.assertEquals(OperationStatus.Success, mResponse.getStatus());
		AssertJUnit.assertEquals(new String(data), new String(mResponse.getValue()));
			
		LOGGER.info("0");
		LOGGER.info("Completed");
	}

	/**
	 * Send a set request twice with same key to juno 2.0 server
	 * It should be successfull.
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testsetWithoutTTL() throws JunoException, IOException {
		LOGGER.info("\n***TEST CASE: " + new Object() {}.getClass().getEnclosingMethod().getName());
		LOGGER.info(new Object() {}.getClass().getEnclosingMethod().getName());
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));

		byte[] key = DataGenUtils.genBytes(64);
		byte[] data = DataGenUtils.genBytes(10);
		JunoPropertiesProvider prop = new JunoPropertiesProvider(pConfig);

		JunoResponse junoResponse = junoClient.set(key, data);
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		junoResponse = junoClient.get(key);
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		AssertJUnit.assertEquals(key, junoResponse.key());
		AssertJUnit.assertTrue(1 == junoResponse.getVersion());
		AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
		AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));

		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		junoResponse = junoClient.get(key); // It will show that 10 sec has elapsed from original TTL
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		AssertJUnit.assertTrue(junoResponse.getTtl() >= (prop.getDefaultLifetime()-14) && (junoResponse.getTtl() <= prop.getDefaultLifetime()-10));
		//System.out.println("TTL now is:"+junoResponse.getTtl());

		junoResponse = junoClient.set(key, data);
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		junoResponse = junoClient.get(key); // It will show that 1800 sec as TTL.
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		AssertJUnit.assertEquals(key, junoResponse.key());
		AssertJUnit.assertTrue(2 == junoResponse.getVersion());
		AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));
		AssertJUnit.assertTrue(junoResponse.getTtl() >= (prop.getDefaultLifetime() - 4) && junoResponse.getTtl() <= prop.getDefaultLifetime());
		//System.out.println("TTL now is:"+junoResponse.getTtl());

		LOGGER.info("0");
		LOGGER.info("Completed");
	}
	/**
	 * Send a set request with empty key and should generate IllegalArgumentException.
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testSetWithEmptyKey() throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	    
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] key = "".getBytes();
		byte[] data = DataGenUtils.genBytes(10);
		JunoResponse mResponse = null;

		try{			
			mResponse = junoClient.set(key, data);
			AssertJUnit.assertTrue(false);			
		}catch(Exception mex){
			AssertJUnit.assertTrue(mex.getCause().getMessage().contains("The Document key must not be null or empty"));
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
	public void testsetWithNullKey() throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	    
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] key = null;
		byte[] data = DataGenUtils.genBytes(10);
		try{
			long lifetime = 100;
			junoClient.set(key, data, lifetime);
			AssertJUnit.assertTrue ("Exception is not seen for null key", false);
		}catch(Exception mex){
			AssertJUnit.assertTrue(mex.getCause().getMessage().contains("key must not be null"));
			LOGGER.info("Exception", mex.getMessage());
			LOGGER.info("2");	
			LOGGER.info("Completed");
		}
	}
	/**
	 * Send a set request with 128bytes key and should not generate exceptions
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testsetWith128BytesKey() throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	    
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] key = new String(DataGenUtils.createKey(50) + "SETWith128BytesKey").getBytes();
		byte[] data = DataGenUtils.genBytes(10);
		long lifetime = 100;
		JunoResponse junoResponse = null;
		try {
			junoResponse = junoClient.set(key, data, lifetime);
		} catch (JunoException ex) {
			LOGGER.debug("Exception Occured: " + ex.getMessage());
			AssertJUnit.assertTrue(false);
		}
		AssertJUnit.assertEquals (OperationStatus.Success,junoResponse.getStatus());

		junoResponse = junoClient2.get(key);
		AssertJUnit.assertEquals (OperationStatus.Success,junoResponse.getStatus());
		AssertJUnit.assertEquals(key, junoResponse.key());
		AssertJUnit.assertTrue(1 == junoResponse.getVersion());
		AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
		AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));

		LOGGER.debug("Call insert() to create a record with same key, verify DupKey error");
		try {
			JunoResponse mResponse = junoClient2.create(key, data);
			AssertJUnit.assertEquals (OperationStatus.UniqueKeyViolation,mResponse.getStatus());
		} catch (JunoException mex) {
			AssertJUnit.assertTrue ("Exception is seen for duplicate key", false);		
			LOGGER.info("0");
		}		
		LOGGER.info("Completed");
	}
	/**
	 * Send a set request with 129bytes key and should generate IllegalArgumentException
	 * with "Invalid key length. current length=257, max length=256"
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testsetWith129BytesKey() throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	    
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] key = DataGenUtils.genBytes(129);
		byte[] data = DataGenUtils.genBytes(10);
		try{
			junoClient.set(key, data, 100L);
			AssertJUnit.assertTrue ("Exception is NOT seen for Key = 129 bytes length", false);
		}catch(Exception mex){
			LOGGER.debug(mex.getCause().getMessage());
			AssertJUnit.assertTrue(mex.getCause().getMessage().contains("The Document key must not be larger than 128 bytes"));
			LOGGER.info("Exception", mex.getMessage());
			LOGGER.info("2");			
			LOGGER.info("Completed");
		}
	}
	
	/**
	 * Send a set request special characters key and should not generate exceptions
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testsetWithSpecialCharsKey() throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	    
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] key = "@@#$%^&*()_+?>,<|}{[]~abc780=.__?|".getBytes();
		byte[] key1 = "Q:������������A:����. Q:������������������ A:���  Q:".getBytes();
		byte[] data = "Q:你好嗎？A:我很好".getBytes();
		byte[] data1 = "Q:������������A:���������. Q:������������������ A:������������������  Q:������������������ A:������������������  Ok  ���������".getBytes();

		LOGGER.debug("key length is " + key.length + " key1 length is " + key1.length);	
		try {
			junoClient.delete(key);
			junoClient.delete(key1);
			Thread.sleep(5000);
		} catch (Exception e) {
			LOGGER.debug(e.getMessage());
		}
		JunoResponse junoResponse = junoClient.set(key, data);
		AssertJUnit.assertEquals (OperationStatus.Success,junoResponse.getStatus());
		AssertJUnit.assertEquals(key, junoResponse.key());
		AssertJUnit.assertTrue(1 == junoResponse.getVersion());
		junoResponse = junoClient.get(key);
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
		AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));
		
		JunoResponse junoResponse1 = junoClient.set(key1, data1);
		AssertJUnit.assertEquals (OperationStatus.Success,junoResponse1.getStatus());
		AssertJUnit.assertEquals(key1, junoResponse1.key());
		AssertJUnit.assertTrue(1 == junoResponse1.getVersion());
		junoResponse1 = junoClient.get(key1);
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse1.getStatus());
		AssertJUnit.assertEquals(data1.length, junoResponse1.getValue().length);
		AssertJUnit.assertEquals(new String(data1), new String(junoResponse1.getValue()));

		JunoResponse junoResponse2 = junoClient2.get(key);
		AssertJUnit.assertEquals (OperationStatus.Success,junoResponse2.getStatus());
		AssertJUnit.assertEquals(key, junoResponse2.key());
		AssertJUnit.assertTrue(1 == junoResponse2.getVersion());
		AssertJUnit.assertEquals(data.length, junoResponse2.getValue().length);
		AssertJUnit.assertEquals(new String(data), new String(junoResponse2.getValue()));
		LOGGER.info("0");
		LOGGER.info("Completed");
	}
	
	/**
	 * Send a set request with normal lifetime and should not generate exceptions
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testsetWith10SecLifetime() throws JunoException, IOException, InterruptedException {
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	    
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] key = new String(DataGenUtils.createKey(40) + "SETWith10SecLifetime").getBytes();
		byte[] data = DataGenUtils.genBytes(10);
		long lifetime = 15;
		
		JunoResponse junoResponse = junoClient.set(key, data, lifetime);
		AssertJUnit.assertEquals (OperationStatus.Success,junoResponse.getStatus());
		Thread.sleep (7000);

		junoResponse = junoClient2.get(key);
		AssertJUnit.assertEquals (OperationStatus.Success,junoResponse.getStatus());
		AssertJUnit.assertEquals(key, junoResponse.key());
		AssertJUnit.assertTrue(1 == junoResponse.getVersion());		
		AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
		AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));
		LOGGER.debug("Sleep more for the key to expire");
		Thread.sleep (10000);
		try {
			JunoResponse mResponse = junoClient2.get(key);
			AssertJUnit.assertEquals(OperationStatus.NoKey,mResponse.getStatus());
		} catch (JunoException mex) {
			AssertJUnit.assertTrue ("Exception is seen for no key", false);
			LOGGER.info("0");
		} finally {
			LOGGER.info("Completed");		
		}
	}
	
	/**
	 * Send a set request with zero lifetime, default lifetime should be used
	 * Call insert() to insert same key, should get Duplicate error
	 * @throws JunoException //TODO: ask why get default 3600 not 1800?
	 * @throws IOException
	 */
	@Test
	public void testsetWithZeroLifetime() throws JunoException, Exception{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	    
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		JunoPropertiesProvider prop = new JunoPropertiesProvider(pConfig);
		byte[] key = new String(DataGenUtils.createKey(30) + "set").getBytes();
		byte[] data = DataGenUtils.genBytes(10);
		byte[] data2 = DataGenUtils.genBytes(10);
	
		JunoResponse mResponse = junoClient.set(key, data, 15);
		AssertJUnit.assertEquals(OperationStatus.Success, mResponse.getStatus());
		AssertJUnit.assertTrue (mResponse.getTtl() == 15);
		mResponse = junoClient.set(key, data);
		AssertJUnit.assertEquals(OperationStatus.Success, mResponse.getStatus());
		mResponse = junoClient.get(key);
		AssertJUnit.assertEquals(OperationStatus.Success, mResponse.getStatus());
		AssertJUnit.assertTrue (mResponse.getTtl() <= prop.getDefaultLifetime() && mResponse.getTtl() > prop.getDefaultLifetime()-3);			
		mResponse = junoClient.set(key, data, 0L);
		AssertJUnit.assertEquals(OperationStatus.Success, mResponse.getStatus());
		AssertJUnit.assertTrue (mResponse.getTtl() <= prop.getDefaultLifetime() && mResponse.getTtl() > prop.getDefaultLifetime()-3);
		AssertJUnit.assertEquals(3,mResponse.getVersion());
		mResponse = junoClient2.set(key, data2);
		AssertJUnit.assertEquals(OperationStatus.Success, mResponse.getStatus());
		AssertJUnit.assertEquals(4,mResponse.getVersion());
		mResponse = junoClient.get(key);
		AssertJUnit.assertEquals(OperationStatus.Success, mResponse.getStatus());
		AssertJUnit.assertEquals(4,mResponse.getVersion());
		AssertJUnit.assertEquals(data2.length,mResponse.getValue().length);
		AssertJUnit.assertEquals(new String(data2), new String(mResponse.getValue()));

		JunoResponse mResponse1 = junoClient1.set(key, data, 0);
		AssertJUnit.assertEquals(OperationStatus.Success, mResponse1.getStatus());
		mResponse1 = junoClient1.get(key);
		AssertJUnit.assertEquals(OperationStatus.Success, mResponse1.getStatus());
		//System.out.println("lifetime for junoClient1 is " + mResponse1.getTtl());
		LOGGER.debug("lifetime for junoClient1 is " + mResponse1.getTtl() + ", default is " +  prop.getDefaultLifetime());
		AssertJUnit.assertEquals(1,mResponse1.getVersion());
		AssertJUnit.assertEquals(data.length, mResponse1.getValue().length);
		AssertJUnit.assertEquals(new String(data), new String(mResponse1.getValue()));
		LOGGER.info("0");
		LOGGER.info("Completed");
	}
	
	/**
	 * Send a set request with Negative lifetime and should generate an
	 * IllegalArgumentException
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testsetWithNegativeLifetime() throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	    
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] key = DataGenUtils.genBytes(10);
		byte[] data = DataGenUtils.genBytes(10);
		try{
			junoClient.set(key, data, (long)-1);
			AssertJUnit.assertTrue ("Exception is NOT seen for negative lifetime", false);
		}catch(Exception mex){
			LOGGER.debug(mex.getMessage());			
			AssertJUnit.assertTrue(mex.getCause().getMessage().contains("The Document's TTL cannot be negative."));
			LOGGER.info("Exception", mex.getMessage());
			LOGGER.info("2");			
			LOGGER.info("Completed");
		}
	}

	/**
	 * Send a set request with max lifetime 3 days and should not generate exceptions
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testsetWith3DaysLifetime() throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	    
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] key = new String(DataGenUtils.createKey(50) + "SETWith3DaysLifetime").getBytes();
		byte[] data = DataGenUtils.genBytes(10);
		long lifetime = 259200;
		JunoResponse junoResponse = junoClient.set(key, data, lifetime);
		AssertJUnit.assertEquals (OperationStatus.Success,junoResponse.getStatus());

		junoResponse = junoClient2.get(key, (long)10);
		AssertJUnit.assertEquals (OperationStatus.Success,junoResponse.getStatus());
		AssertJUnit.assertEquals(key, junoResponse.key());
		AssertJUnit.assertTrue(1 == junoResponse.getVersion());
		AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
		AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));
		LOGGER.info("0");
		LOGGER.info("Completed");
	}
	
	/**
	 * Send a set request with more than max lifetime 3 days and should 
	 * generate IllegalArgumentException
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testsetWithMorethan3DaysLifetime() throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	    
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] key = DataGenUtils.genBytes(10);
		byte[] data = DataGenUtils.genBytes(10);
		long lifetime = 259201;
		try{
			junoClient.set(key, data, lifetime);
			AssertJUnit.assertTrue ("Exception is not seen for TTL > 3 days", false);
		}catch(Exception mex){
			AssertJUnit.assertTrue(mex.getCause().getMessage().contains("Invalid lifetime."));
			LOGGER.info("Exception", mex.getMessage());
			LOGGER.info("2");			
			LOGGER.info("Completed");
		}
	}
	/**
	 * Send a set request with null payload and should generate IllegalArgumentException
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testsetWithNullPayload() throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	    
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	

		byte[] key = DataGenUtils.genBytes(64);
		try{
			JunoResponse res = junoClient.set(key, null, 50L);
			AssertJUnit.assertEquals (OperationStatus.Success,res.getStatus());
			AssertJUnit.assertEquals(key, res.key());
			AssertJUnit.assertTrue(1 == res.getVersion());
			AssertJUnit.assertEquals(0 , res.getValue().length);
		}catch(Exception mex){
			LOGGER.debug(mex.getMessage());
			AssertJUnit.assertFalse(true);
			LOGGER.info("Exception", mex.getMessage());
			LOGGER.info("2");			
			LOGGER.info("Completed");
		}
	}
	
	/**
	 * Send a set request with zero size payload and should be successfull
	 * @throws JunoException
	 * @throws IOException //TODO: ask set can not be zero payload but create allows zero payload?
	 */
	@Test
	public void testsetWithZeroPayload() throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	    
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] data = DataGenUtils.genBytes(0);
		byte[] key = DataGenUtils.genBytes(64);
		try{
			JunoResponse junoResponse = junoClient.set(key, data, 20L);
			LOGGER.info("0");
		}catch(Exception mex){			
			AssertJUnit.assertTrue("should be able to set empty payload, why come here?", false);
			LOGGER.info("2");						
		} finally {
			LOGGER.info("Completed");
		}
	}
	/**
	 * Send a set request with 100KB payload and should not generate exception
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testsetWith100KBPayload() throws JunoException, Exception{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	    
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] data = DataGenUtils.genBytes(102400);
		byte[] key = new String(DataGenUtils.createKey(50) + "SETWith100KBPayload").getBytes();
		long lifetime = 25;
		
		JunoPropertiesProvider prop = new JunoPropertiesProvider(pConfig2);
		LOGGER.debug("property config in 100K is :" + prop.getRecordNamespace()  + " " + prop.getAppName());
		prop = new JunoPropertiesProvider(pConfig);
		LOGGER.debug("property config1 in 100K is :" + prop.getRecordNamespace()  + " " + prop.getAppName());
		
		JunoResponse junoResponse = junoClient.set(key, data, lifetime);
		AssertJUnit.assertEquals (OperationStatus.Success,junoResponse.getStatus());

		junoResponse = junoClient.get(key);
		AssertJUnit.assertEquals (OperationStatus.Success,junoResponse.getStatus());
		AssertJUnit.assertEquals(key, junoResponse.key());
		AssertJUnit.assertTrue(1 == junoResponse.getVersion());
		AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
		AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));
		LOGGER.debug("Sleep 3 seconds before geting daata");

		JunoResponse mResponse = junoClient2.get(key);
		AssertJUnit.assertEquals (OperationStatus.Success,mResponse.getStatus());
		AssertJUnit.assertEquals(new String(data), new String(mResponse.getValue()));
		LOGGER.info("0");
		LOGGER.info("Completed");
	}

	/**
	 * Send a insert request with 200KB payload and should not generate exception
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testsetWith200KBPayload() throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	    
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] data = DataGenUtils.genBytes(204800);
		byte[] key = new String(DataGenUtils.createKey(64) + "SETWith200KBPayload").getBytes();
		JunoResponse junoResponse = junoClient.set(key, data, 15L);
		AssertJUnit.assertEquals (OperationStatus.Success,junoResponse.getStatus());

		junoResponse = junoClient2.get(key);
		AssertJUnit.assertEquals (OperationStatus.Success,junoResponse.getStatus());
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
	public void testsetWithMorethan200KBPayload() throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	    
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] data = DataGenUtils.genBytes(204801);
		byte[] key = DataGenUtils.genBytes(64);
		try{
			junoClient.set(key, data, 10L);
			AssertJUnit.assertTrue ("Exception is not seen for payload > 200KB", false);
		}catch(Exception mex){
			LOGGER.debug(mex.getMessage());
			AssertJUnit.assertTrue(mex.getCause().getMessage().contains("The Document Value must not be larger than 204800"));
			LOGGER.info("Exception", mex.getMessage());
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
	public void testsetWithSameKeyInDifferentNameSpaces() throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	        
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] data = DataGenUtils.genBytes(10);
		byte[] data1 = DataGenUtils.genBytes(20);
		byte[] key = DataGenUtils.genBytes(100);
		
		JunoResponse junoResponse = junoClient.set(key, data, 100L);
		AssertJUnit.assertEquals (OperationStatus.Success,junoResponse.getStatus());

		junoResponse = junoClient.get(key);
		AssertJUnit.assertEquals (OperationStatus.Success,junoResponse.getStatus());
		AssertJUnit.assertEquals(key, junoResponse.key());
		AssertJUnit.assertTrue(1 == junoResponse.getVersion());
		AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
		AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));

		junoResponse =  junoClient1.set(key, data1);
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		JunoResponse junoResponse1 = junoClient1.get(key);
		AssertJUnit.assertEquals (OperationStatus.Success,junoResponse1.getStatus());
		AssertJUnit.assertEquals(key, junoResponse1.key());
		AssertJUnit.assertTrue(1 == junoResponse1.getVersion());
		AssertJUnit.assertEquals(new String(data1), new String(junoResponse1.getValue()));

		AssertJUnit.assertEquals(junoResponse.key(), junoResponse1.key());
		AssertJUnit.assertEquals(junoResponse.getVersion(), junoResponse1.getVersion());	
		LOGGER.info("0");
		LOGGER.info("Completed");
	}
	/*************************************************
	 * insert two records using set() with same key and name space.
	 * The 2nd set call should succeed
	 *************************************************/
	@Test
	public void testsetWithSameKeysInSameNameSpace() throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	        
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] data = DataGenUtils.genBytes(10);
		byte[] data2 = DataGenUtils.genBytes(10);
		byte[] key = new String(DataGenUtils.createKey(6) + "SETWithSameKeysInSameNameSpace").getBytes();

		JunoResponse junoResponse = junoClient.set(key, data, 10L);
		AssertJUnit.assertEquals (OperationStatus.Success,junoResponse.getStatus());

		junoResponse = junoClient.get(key);
		AssertJUnit.assertEquals (OperationStatus.Success,junoResponse.getStatus());
		AssertJUnit.assertEquals(key, junoResponse.key());
		AssertJUnit.assertTrue(1 == junoResponse.getVersion());
		AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
		AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));

		//Create with same key, different data
		JunoResponse mResponse = junoClient2.set(key, data2, 100L);
		AssertJUnit.assertEquals (OperationStatus.Success,mResponse.getStatus());
		mResponse = junoClient.get(key);
		AssertJUnit.assertEquals (OperationStatus.Success,mResponse.getStatus());
		AssertJUnit.assertTrue(2 == mResponse.getVersion());
		AssertJUnit.assertEquals(new String(data2), new String(mResponse.getValue()));	
		LOGGER.info("0");
		LOGGER.info("Completed");
	}
	
	/*************************************************
	 * insert two clients using insert() then set() with same ns but diff app name
	 * The 2nd set() should succeed
	 *************************************************/
	@Test
	public void testsetWithSameNameSpaceDiffAppname() throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	        
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] data = DataGenUtils.genBytes(10);
		byte[] data2 = DataGenUtils.genBytes(20);
		byte[] key = DataGenUtils.createKey(10).getBytes();
		LOGGER.debug("key is " + new String(key));
		
		JunoResponse junoResponse = junoClient.create(key, data, 100L);
		AssertJUnit.assertEquals (OperationStatus.Success,junoResponse.getStatus());

		junoResponse = junoClient.get(key);
		AssertJUnit.assertEquals (OperationStatus.Success,junoResponse.getStatus());
		AssertJUnit.assertEquals(key, junoResponse.key());
		AssertJUnit.assertTrue(1 == junoResponse.getVersion());
		AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
		AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));

		//Create same key using set() should be OK
		junoClient2.set(key, data2, 10L);
		JunoResponse mResponse = junoClient2.get(key);
		AssertJUnit.assertEquals (OperationStatus.Success,mResponse.getStatus());
		AssertJUnit.assertEquals(new String(data2), new String(mResponse.getValue()));
		AssertJUnit.assertTrue(2 == mResponse.getVersion());
		LOGGER.info("0");
		LOGGER.info("Completed");
	}

	/*************************************************
	 * Do a set(), update, conditionalUpdate()
	 * delete()
	 *************************************************/
	@Test
	public void testSetWithOtherOps() throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	        
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] data = DataGenUtils.genBytes(10);
		byte[] data2 = DataGenUtils.genBytes(20);
		byte[] data3 = DataGenUtils.genBytes(15);
		byte[] key = new String(DataGenUtils.createKey(60) + "SETWithOtherOps").getBytes();

		JunoResponse junoResponse = junoClient.set(key, data, 100L);
		AssertJUnit.assertEquals (OperationStatus.Success,junoResponse.getStatus());

		junoResponse = junoClient2.get(key);
		AssertJUnit.assertEquals (OperationStatus.Success,junoResponse.getStatus());
		AssertJUnit.assertEquals(key, junoResponse.key());
		AssertJUnit.assertTrue(1 == junoResponse.getVersion());
		AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
		AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));

		JunoResponse mResponse = junoClient2.update(key, data2, 0L);
		AssertJUnit.assertEquals (OperationStatus.Success,mResponse.getStatus());

		mResponse = junoClient.get(key);
		AssertJUnit.assertEquals (OperationStatus.Success,mResponse.getStatus());
		AssertJUnit.assertEquals(new String(data2), new String(mResponse.getValue()));
		AssertJUnit.assertTrue(2 == mResponse.getVersion());

		LOGGER.debug("\nUpdate the key using conditional update");
		long lifetime = 100L;
		JunoResponse junoResponse1 = junoClient.compareAndSet(mResponse.getRecordContext(), data3, lifetime);
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse1.getStatus());
		AssertJUnit.assertTrue(3 == junoResponse1.getVersion());
		junoResponse1 = junoClient.get(key);
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse1.getStatus());
		AssertJUnit.assertEquals(new String(data3), new String(junoResponse1.getValue()));

		junoResponse1 = junoClient2.delete(key);
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse1.getStatus());
		try {
			JunoResponse result1 = junoClient.get(key);
			AssertJUnit.assertEquals (OperationStatus.NoKey, result1.getStatus());
		} catch (JunoException mex) {
			AssertJUnit.assertTrue ("Exception is seen for key", false);
			LOGGER.info("0");
		} finally {
			LOGGER.info("Completed");
		}		
	}

	@Test
	public void testCompareAndTestWithWrongVersion()throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
        LOGGER.info(new Object(){}.getClass().getEnclosingMethod().getName()); 
        LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
        
		byte[] data1 = DataGenUtils.genBytes(10);
		byte[] data2 = DataGenUtils.genBytes(20);
		byte[] key = new String(DataGenUtils.createKey(60) + "SETWithOtherOps").getBytes();

		JunoResponse junoResponse = junoClient.set(key, data1, 100L);
		AssertJUnit.assertEquals (OperationStatus.Success,junoResponse.getStatus());

		junoResponse = junoClient2.get(key);
		AssertJUnit.assertEquals (OperationStatus.Success,junoResponse.getStatus());
		AssertJUnit.assertEquals(key, junoResponse.key());
		AssertJUnit.assertTrue(1 == junoResponse.getVersion());
		AssertJUnit.assertEquals(data1.length, junoResponse.getValue().length);
		AssertJUnit.assertEquals(new String(data1), new String(junoResponse.getValue()));
		
		RecordContext reqCtx = new RecordContext(key,0,0,120);
		try{
			 junoClient.compareAndSet(reqCtx, data2, 240);
			 AssertJUnit.assertTrue ("Exception should happen for version = 0", false);
		}catch(Exception mex){
			AssertJUnit.assertTrue(mex.getCause().getMessage().contains("The Document version cannot be less than 1"));
			LOGGER.info("Exception", mex.getMessage());
			LOGGER.info("2");			
			LOGGER.info("Completed");
		}
		
	}
	
	
	/***********************************************************************
	 * m_max_payload_size > JUNO_CONFIG_DEFAULT_MAX_PAYLOAD_SIZE (204800)
	 ***********************************************************************/
	@Test
	public void testValidateByChangingMaxPayloadSize() throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	        
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		pConfig.setProperty("MAX_PAYLOAD_SIZE", "204809");
		JunoClient junoClient2 = null;
		try {
			junoClient2 = JunoClientFactory.newJunoClient(new JunoPropertiesProvider(pConfig), SSLUtil.getSSLContext());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		byte[] data = DataGenUtils.genBytes(204801);
		byte[] key = DataGenUtils.genBytes(64);

		try{
			junoClient2.set(key, data, 10L);
			AssertJUnit.assertTrue ("Exception is NOT seen for invalid payload", false);
		}catch(Exception mex){
			LOGGER.debug("\nException occur: " + mex.getMessage());
			AssertJUnit.assertTrue(mex.getCause().getMessage().contains("The Document Value must not be larger than"));
			LOGGER.info("Exception", mex.getMessage());
			LOGGER.info("2");			
			LOGGER.info("Completed");
		}
	}

	@Test
	public void testsetWithChineseKey() throws JunoException, Exception {		
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	        
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] key = "Q:你好嗎？A:我很好. Q:你�?去哪裡？ A:我�?去迪斯尼  Q:�?�什麼想看？ A:米奇米尼�?鼠ok".getBytes(); 

		byte[] key1 = DataGenUtils.createKey(127).getBytes();				
		byte[] data =  "New data testing".getBytes();
		byte[] data1 = "New data111111111111".getBytes();
		LOGGER.debug("Key length: " +  key.length);
		LOGGER.debug("Key1 length: " +  key1.length);

		LOGGER.debug("\nKey length without UTF-8: " +  key.length);
		LOGGER.debug("Key1 length without UTF-8: " +  key1.length);

		byte [] tmp = key;
		LOGGER.debug(new String (tmp) + " " + tmp.length);
		try {
			junoClient.delete(key);
			Thread.sleep(5000);
		} catch (Exception e) {
			LOGGER.debug("Exception Occured: " + e.getMessage());
		}
		long lifetime = 100;
		JunoResponse junoResponse = junoClient.set(key, data, lifetime);
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		JunoResponse junoResponse1 = junoClient.set(key1, data1, lifetime);
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse1.getStatus());
		junoResponse =  junoClient.get(key);
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		junoResponse1 =  junoClient.get(key1);
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse1.getStatus());

		LOGGER.debug("Status: " + junoResponse1.getStatus());	
		AssertJUnit.assertEquals (OperationStatus.Success,junoResponse1.getStatus());
		AssertJUnit.assertEquals(new String(data1), new String(junoResponse1.getValue()));
		AssertJUnit.assertTrue(1 == junoResponse1.getVersion());
		AssertJUnit.assertEquals (OperationStatus.Success,junoResponse.getStatus());
		AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));
		AssertJUnit.assertTrue(1 == junoResponse.getVersion());		
		LOGGER.info("0");
		LOGGER.info("Completed");
	}

	@Test
	public void testSetWithCompression() throws Exception {
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));
		byte[] data = DataGenUtils.createCompressablePayload(100000).getBytes();
		byte[] key = new String(DataGenUtils.createKey(50) + "testSetWithCompression").getBytes();
		long lifetime = 25;
		
		JunoPropertiesProvider prop = new JunoPropertiesProvider(pConfig2);
		prop = new JunoPropertiesProvider(pConfig);
                int dataLength = Snappy.compress(data).length;
                LOGGER.info("data original length is " + data.length + "compressed length is " + dataLength);

		JunoResponse junoResponse = junoClient.set(key, data, lifetime);
		AssertJUnit.assertEquals (OperationStatus.Success,junoResponse.getStatus());
		junoResponse = junoClient.get(key);
		AssertJUnit.assertEquals (OperationStatus.Success,junoResponse.getStatus());
		AssertJUnit.assertEquals(key, junoResponse.key());
		AssertJUnit.assertTrue(1 == junoResponse.getVersion());
		AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
		AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));

		JunoResponse mResponse = junoClient2.get(key);
		AssertJUnit.assertEquals (OperationStatus.Success,mResponse.getStatus());
		AssertJUnit.assertEquals(new String(data), new String(mResponse.getValue()));
		LOGGER.info("0");
		LOGGER.info("Completed");
	}

        /**
         * Send a set request with 150K byte payload which will trigger compression, data can be read 
         * by mayfly java client testGetFromJunoInsert func or junocli, junocli command is:
         * ./junocli -s host:port -ssl -c config.toml get -ns "NS1" _testSetWithCompressionCrossRead
         * exception
         * @throws Exception
         */
	@Test
        public void testSetWithCompressionCrossMayfly() throws Exception {
                LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
                LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));
                byte[] data = DataGenUtils.createCompressablePayload(150000).getBytes();
                byte[] key = new String("_testSetWithCompressionCrossRead").getBytes();
                long lifetime = 2500;

                JunoPropertiesProvider prop = new JunoPropertiesProvider(pConfig2);
                prop = new JunoPropertiesProvider(pConfig);
                int dataLength = Snappy.compress(data).length;
                LOGGER.info("data original length is " + data.length + ", compressed length is " + dataLength);

				JunoResponse junoResponse = junoClient.delete(key);
				AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
				Thread.sleep (2000);
                junoResponse = junoClient.set(key, data, lifetime);
                AssertJUnit.assertEquals (OperationStatus.Success,junoResponse.getStatus());
                junoResponse = junoClient.get(key);
                AssertJUnit.assertEquals (OperationStatus.Success,junoResponse.getStatus());
                AssertJUnit.assertEquals(key, junoResponse.key());
                AssertJUnit.assertTrue(1 == junoResponse.getVersion());
                AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
                AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));

                JunoResponse mResponse = junoClient2.get(key);
                AssertJUnit.assertEquals (OperationStatus.Success,mResponse.getStatus());
                AssertJUnit.assertEquals(new String(data), new String(mResponse.getValue()));
                LOGGER.info("0");
                LOGGER.info("Completed");
        }

        @Test
        public void testExceedsCompressLimit() throws Exception {
                LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
                LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));
                byte[] data = DataGenUtils.createCompressablePayload(800000).getBytes();
                byte[] key = new String(DataGenUtils.createKey(50) + "testExceedsCompressLimit").getBytes();
                long lifetime = 25;

                JunoPropertiesProvider prop = new JunoPropertiesProvider(pConfig2);
                prop = new JunoPropertiesProvider(pConfig);
                int dataLength = Snappy.compress(data).length;
                LOGGER.info("data original length is:" + data.length + "compressed length is " + dataLength);

                try {
						junoClient.set(key, data, lifetime);
                }catch(Exception mex){
                        LOGGER.debug(mex.getMessage());
                        AssertJUnit.assertTrue(mex.getCause().getMessage().contains("The Document Value must not be larger than 204800"));
                        LOGGER.info("Exception", mex.getMessage());
                        LOGGER.info("2");
                        LOGGER.info("Completed");
                }
        }

        @Test
        public void testUpdateExceedsMax() throws Exception {
                LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
                LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));
                byte[] data = DataGenUtils.createCompressablePayload(300000).getBytes();
			byte[] updateD = DataGenUtils.createCompressablePayload(800000).getBytes();
                byte[] key = new String(DataGenUtils.createKey(50) + "testExceedsCompressLimit").getBytes();
                long lifetime = 25;

                JunoPropertiesProvider prop = new JunoPropertiesProvider(pConfig2);
                prop = new JunoPropertiesProvider(pConfig);
                int dataLength = Snappy.compress(data).length;
     			int updateDLength = Snappy.compress(updateD).length;
                LOGGER.info("data original length is:" + data.length + "compressed length is " + dataLength);
				LOGGER.info("updatedata original length is:" + data.length + "compressed length is " + updateDLength);

                JunoResponse junoResponse = junoClient.set(key, data, lifetime);
                AssertJUnit.assertEquals (OperationStatus.Success,junoResponse.getStatus());
                junoResponse = junoClient.get(key);
                AssertJUnit.assertEquals (OperationStatus.Success,junoResponse.getStatus());
                AssertJUnit.assertEquals(key, junoResponse.key());
                AssertJUnit.assertTrue(1 == junoResponse.getVersion());
                AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
                AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));

		try {
	                JunoResponse junoResponse1 = junoClient.update(key, updateD); 
                 }catch(Exception mex){
                        LOGGER.debug(mex.getMessage());
                        AssertJUnit.assertTrue(mex.getCause().getMessage().contains("The Document Value must not be larger than 204800"));
                        LOGGER.info("Exception", mex.getMessage());
                        LOGGER.info("2");
                        LOGGER.info("Completed");
                }
        }

//        @Test
//        public void testSetExceedsVersion() throws Exception {
//                LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
//                update(key, data1, lifetime1)
//                LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));
//                byte[] data = DataGenUtils.genBytes(2000);
//                byte[] key = DataGenUtils.createKey(50).getBytes();
//                long lifetime = 25;
//
//		try {
//			for (int i=0; i<40000; i++) {
//				JunoResponse junoResponse = junoClient.set(key, data, lifetime);
//				AssertJUnit.assertEquals (OperationStatus.Success,junoResponse.getStatus());
//
//                                JunoResponse junoResponse1 = junoClient.get(key);
//                                AssertJUnit.assertEquals (OperationStatus.Success,junoResponse1.getStatus());
//                                LOGGER.info("get version is " + junoResponse1.getVersion());
//			}
//                 }catch(Exception mex){
//                        LOGGER.debug(mex.getMessage());
//                        LOGGER.info("Exception", mex.getMessage());
//                        LOGGER.info("2");
//                        LOGGER.info("Completed");
//                }
//        }
}
