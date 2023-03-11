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
import java.util.Properties;
import java.util.Random;
import org.slf4j.Logger;import com.paypal.juno.client.JunoClient;
import org.slf4j.LoggerFactory;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class GetTest{
	private JunoClient junoClient;
	private JunoClient junoClient1;
	private JunoClient junoClient2;
	private JunoClient cryptClient2;
	private Properties pConfig;
	private Properties pConfig2;
	private Logger LOGGER;
	int syncFlag;

	@BeforeClass
	public void setup() throws JunoException, IOException, InterruptedException {
		LOGGER = LoggerFactory.getLogger(GetTest.class);
		URL url = GetTest.class.getResource("/com/paypal/juno/Juno.properties");
		pConfig = new Properties();
		pConfig.load(url.openStream());
		pConfig.setProperty(JunoProperties.APP_NAME, "QATestApp");
		pConfig.setProperty(JunoProperties.RECORD_NAMESPACE, "NS1");
		LOGGER.debug("Read syncFlag test to findout if we need to run test in sync/async mode");
	        String sync_flag = pConfig.getProperty("sync_flag_test", "0");
		LOGGER.debug("*********SYNC FLAG: " + sync_flag);
		syncFlag = Integer.parseInt(sync_flag.trim());
        
		//junoClient = new JunoTestClientImpl(new JunoPropertiesProvider(pConfig), SSLUtil.getSSLContext());
		URL url1 = GetTest.class.getResource("/com/paypal/juno/Juno.properties");
		junoClient1 = JunoClientFactory.newJunoClient(url1);

		URL url2 = GetTest.class.getResource("/com/paypal/juno/Juno.properties");
		pConfig2 = new Properties();
		pConfig2.load(url2.openStream());
		pConfig2.setProperty(JunoProperties.APP_NAME, "QATestApp2");
		pConfig2.setProperty(JunoProperties.RECORD_NAMESPACE, "NS1");

		try{
			junoClient = JunoClientFactory.newJunoClient(new JunoPropertiesProvider(pConfig), SSLUtil.getSSLContext());
			junoClient2 = JunoClientFactory.newJunoClient(new JunoPropertiesProvider(pConfig2), SSLUtil.getSSLContext());

		}catch (Exception e) {
			throw new RuntimeException(e);
		}

		Thread.sleep(1000);

	}

	/**
	 * Send a get request with key to the Juno 2.0 server and should not generate
	 * exception
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testGetWithKey() throws IOException{
		JunoPropertiesProvider prop = new JunoPropertiesProvider(pConfig);
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] key = DataGenUtils.genBytes(64);
		byte[] data = DataGenUtils.genBytes(10);
		long lifetime = 220000;
		JunoResponse mResponse = junoClient.create(key, data);
		AssertJUnit.assertEquals(OperationStatus.Success, mResponse.getStatus());
		JunoResponse junoResponse = junoClient.get(key);
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		AssertJUnit.assertEquals(key, junoResponse.key());
		AssertJUnit.assertTrue(1 == junoResponse.getVersion());
		AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
		AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));
		AssertJUnit.assertTrue(junoResponse.getTtl() <= prop.getDefaultLifetime() && junoResponse.getTtl() >= prop.getDefaultLifetime()-3);
		
		junoResponse = junoClient.get(key, 0);
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		AssertJUnit.assertTrue(1 == junoResponse.getVersion());
		AssertJUnit.assertTrue(junoResponse.getTtl() <= prop.getDefaultLifetime() && junoResponse.getTtl() >= prop.getDefaultLifetime()-3);
		
		junoResponse = junoClient.get(key, lifetime);
		LOGGER.debug("lifetime get is " + junoResponse.getTtl());
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		AssertJUnit.assertTrue(1 == junoResponse.getVersion());
		AssertJUnit.assertTrue(lifetime == junoResponse.getTtl());		
		AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
		AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));
		
		junoResponse = junoClient.get(key, 10);
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		AssertJUnit.assertTrue(1 == junoResponse.getVersion());
		AssertJUnit.assertTrue(lifetime == junoResponse.getTtl());
				
		LOGGER.info("0");
		LOGGER.info("Completed");
	}

	/**
	 * Send a get request with empty key and should generate IllegalArgumentException
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testGetWithEmptyKey() throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	    
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] key = "".getBytes();
		try{			
			JunoResponse junoResponse = junoClient.get(key);
			AssertJUnit.assertTrue ("Exception is not seen for empty key", false);
			AssertJUnit.assertEquals (OperationStatus.NoKey,junoResponse.getStatus());
		}catch(Exception mex){
			LOGGER.debug("Exception: " + mex.getMessage());
			AssertJUnit.assertTrue(mex.getCause().getMessage().contains("key must not be null"));
			LOGGER.info("Exception", mex.getMessage());
			LOGGER.info("2");			
			LOGGER.info("Completed");
		}
	}
	/**
	 * Send a get request with null key and should generate IllegalArgumentException
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testGetWithNullKey() throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	        
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		try{
			JunoResponse junoResponse = junoClient.get(null);
			AssertJUnit.assertTrue ("Exception is not seen for null key", false);
		}catch(Exception mex){
			LOGGER.debug("Exception: " + mex.getMessage());
			AssertJUnit.assertTrue(mex.getCause().getMessage().contains("The Document key must not be null or empty"));
			LOGGER.info("Exception", mex.getMessage());
			LOGGER.info("2");			
			LOGGER.info("Completed");
		}
	}
	
	/**
	 * Send a get request with no key exists on the server and should generate an
	 * exception	
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testGetNoKeyExists() throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	    
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] key = DataGenUtils.genBytes(64);
		try {
			JunoResponse mResponse = junoClient.get(key);
			AssertJUnit.assertEquals (OperationStatus.NoKey,mResponse.getStatus());
			LOGGER.info("0");
		} catch (JunoException mex) {
			AssertJUnit.assertTrue ("Exception is seen for no key", false);
			LOGGER.info("2");
		} finally {
			LOGGER.info("Completed");
		}		
	} 
	
	/**
	 * Send a get request after five seconds of creation of a record and should not
	 * generate an exception
	 * @throws JunoException
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Test
	public void testGetAfterFiveSeconds() throws JunoException, IOException, InterruptedException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	    
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] key = DataGenUtils.genBytes(64);
		byte[] data = DataGenUtils.genBytes(10);
		long lifetime = 10;
		junoClient.create(key, data, lifetime);
		Thread.sleep(5000);
		JunoResponse junoResponse = junoClient.get(key);
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		AssertJUnit.assertEquals(key, junoResponse.key());
		AssertJUnit.assertTrue(1 == junoResponse.getVersion());
		AssertJUnit.assertTrue(junoResponse.getTtl() <= 10-5 && junoResponse.getTtl() > 10-3-5);
		AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
		AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));
		LOGGER.info("0");
		LOGGER.info("Completed");
	}
	/**
	 * Send a get request after twelve seconds after creation of a record and should 
	 * generate an exception
	 * @throws JunoException
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Test
	public void testGetAfterTwelveSeconds() throws JunoException, IOException, InterruptedException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	    
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] key = DataGenUtils.genBytes(64);
		byte[] data = DataGenUtils.genBytes(10);
		JunoResponse junoResponse = junoClient.create(key, data, 12);
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		LOGGER.debug("default ttl is " + junoResponse.getTtl());
		Thread.sleep(5000);
		LOGGER.debug("Read should not extend lilfetime");
		junoResponse = junoClient.get(key);
		AssertJUnit.assertEquals (OperationStatus.Success,junoResponse.getStatus());
		AssertJUnit.assertTrue(junoResponse.getTtl() <= 12-5 && junoResponse.getTtl() > 12-3-5);
		AssertJUnit.assertTrue (junoResponse.getStatus().getCode() == OperationStatus.Success.getCode());
		AssertJUnit.assertEquals(key, junoResponse.key());
		AssertJUnit.assertTrue(1 == junoResponse.getVersion());
		AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
		AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));
		Thread.sleep(9000);
		
		try {
			JunoResponse mResponse = junoClient.get(key);
			AssertJUnit.assertEquals(OperationStatus.NoKey, mResponse.getStatus());
			LOGGER.info("0");
		} catch (JunoException mex) {
			AssertJUnit.assertTrue ("Exception is seen for no key", false);
			LOGGER.info("2");
		}		
		LOGGER.info("Completed");
	}
	/**
	 * Send a get request with 128bytes key and should not generate an exception
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testGetWith128BytesKey() throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	    
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] key = DataGenUtils.genBytes(128); 
		byte[] data = DataGenUtils.genBytes(10);
		long lifetime = 10;
		JunoResponse junoResponse = junoClient.create(key, data, lifetime);
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		junoResponse = junoClient.get(key);
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		AssertJUnit.assertEquals(key, junoResponse.key());
		AssertJUnit.assertTrue(1 == junoResponse.getVersion());
		AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
		AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));
		LOGGER.info("0");
		LOGGER.info("Completed");
	}
	/**
	 * Send a get request with 130 bytes key and should generate an exception
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testGetWith129BytesKey() throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	    
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] key = DataGenUtils.genBytes(129);
		try{
			JunoResponse junoResponse = junoClient.get(key);
			AssertJUnit.assertTrue ("Exception is not seen for key > 128 bytes length", false);
		}catch(Exception mex){
			LOGGER.debug(mex.getCause().getMessage());
			AssertJUnit.assertTrue(mex.getCause().getMessage().contains("The Document key must not be larger than 128 bytes"));
			LOGGER.info("Exception", mex.getMessage());
			LOGGER.info("2");			
			LOGGER.info("Completed");
		}
	}
	/**
	 * Send a get request with lifetime and should not generate exception
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testGetWithLifetime() throws JunoException, Exception{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	    
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] key = DataGenUtils.genBytes(64);
		byte[] data = DataGenUtils.genBytes(10);
		long lifetime = 5;
		JunoResponse junoResponse = junoClient.delete(key);
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		junoResponse = junoClient.create(key, data, lifetime);
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		junoResponse = junoClient.get(key, (long)5);
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		AssertJUnit.assertEquals(key, junoResponse.key());
		AssertJUnit.assertTrue(1 == junoResponse.getVersion());
		AssertJUnit.assertTrue(junoResponse.getTtl() <= 5 && junoResponse.getTtl() > 5-3);
		AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
		AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));

		Thread.sleep(3000);
		LOGGER.debug("Update key with new timeline vera add" );
		JunoResponse junoResponse1;
		try {
			junoResponse1 = junoClient.get(key, (long)5);
		} catch (JunoException mex) {
			AssertJUnit.assertTrue("\"get time out Exception?", false);
		}
		Thread.sleep(3000);
		junoResponse1 = junoClient.get(key);
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse1.getStatus());
		AssertJUnit.assertEquals(key, junoResponse1.key());
		AssertJUnit.assertTrue(1 == junoResponse1.getVersion());
		AssertJUnit.assertEquals(data.length, junoResponse1.getValue().length);

		LOGGER.debug("Sleep for key to expire");
		Thread.sleep(3000);
		try {
			JunoResponse junoResponse2 = junoClient.get(key);
			AssertJUnit.assertEquals(OperationStatus.NoKey,junoResponse2.getStatus());
			LOGGER.info("0");
		} catch (JunoException mex) {
			AssertJUnit.assertTrue ("Exception is seen for no key", false);
			LOGGER.info("2");
		} finally {
			LOGGER.info("Completed");
		}
	}
	
	/**
	 * Send a get request with zero life time and should generate an exception 
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testGetWithZeroLifetime() throws JunoException, Exception{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	    
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] key = DataGenUtils.genBytes(64);
		byte[] data = DataGenUtils.genBytes(10);
		long lifetime = 5;
		JunoResponse junoResponse = junoClient.create(key, data, lifetime);
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		junoResponse = junoClient.get(key);
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		AssertJUnit.assertEquals(key, junoResponse.key());
		AssertJUnit.assertTrue(1 == junoResponse.getVersion());
		AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
		AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));

		JunoResponse junoResponse1 = junoClient.get(key, (long)0);
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		AssertJUnit.assertTrue(junoResponse1.getTtl() <= 5 && junoResponse1.getTtl() > 5-3);
		Thread.sleep(6000);
		try {
			JunoResponse junoResponse2 = junoClient.get(key);
			AssertJUnit.assertEquals(OperationStatus.NoKey,junoResponse2.getStatus());
			LOGGER.info("0");
		} catch (JunoException mex) {
			AssertJUnit.assertTrue ("Exception is seen for no key", false);
			LOGGER.info("2");
		} finally {
			LOGGER.info("Completed");
		}	
	}

	/**
	 * Send a get request with zero life time and should generate an exception 
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testGetWithoutLifetime() throws JunoException, Exception{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	    
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] key = DataGenUtils.genBytes(64);
		byte[] data = DataGenUtils.genBytes(10);
		long lifetime = 8;
		JunoResponse junoResponse = junoClient.create(key, data, lifetime);
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		AssertJUnit.assertEquals(key, junoResponse.key());
		AssertJUnit.assertTrue(1 == junoResponse.getVersion());
		AssertJUnit.assertTrue(junoResponse.getTtl() > lifetime-3 && junoResponse.getTtl() <= lifetime);
		junoResponse = junoClient.get(key);
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
		AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));
		AssertJUnit.assertTrue(junoResponse.getTtl() > lifetime-3 && junoResponse.getTtl() <= lifetime);
		Thread.sleep(10000);
		try {
			JunoResponse junoResponse2 = junoClient.get(key);
			AssertJUnit.assertEquals(OperationStatus.NoKey,junoResponse2.getStatus());
			LOGGER.info("0");
		} catch (JunoException mex) {
			AssertJUnit.assertTrue ("Exception is seen for no key", false);
			LOGGER.info("2");
		} finally {
			LOGGER.info("Completed");
		}
	}
	
	/**
	 * Send a get request with max lifetime 3days and should not generate an exception
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testGetWithMaxLifetime() throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	    
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] key = DataGenUtils.genBytes(64);
		byte[] data = DataGenUtils.genBytes(10);
		long lifetime = 259200;
		JunoResponse mResponse = junoClient.delete(key);
		AssertJUnit.assertEquals(OperationStatus.Success, mResponse.getStatus());
		mResponse = junoClient.create(key, data, 10);
		AssertJUnit.assertEquals(OperationStatus.Success, mResponse.getStatus());
		AssertJUnit.assertTrue(10 == mResponse.getTtl());
		
		JunoResponse junoResponse = junoClient.get(key, 0);
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		AssertJUnit.assertTrue(1 == junoResponse.getVersion());
		AssertJUnit.assertTrue(10-3 <= junoResponse.getTtl() && junoResponse.getTtl() <= 10);
		
		junoResponse = junoClient.get(key, lifetime);
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		AssertJUnit.assertTrue(1 == junoResponse.getVersion());
		AssertJUnit.assertTrue(lifetime-2 <= junoResponse.getTtl() && junoResponse.getTtl() <= lifetime);		
		AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
		AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));
		
		junoResponse = junoClient.get(key, 10);
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		AssertJUnit.assertTrue(1 == junoResponse.getVersion());
		AssertJUnit.assertTrue(lifetime-3 <= junoResponse.getTtl() && junoResponse.getTtl() <= lifetime);
				
		LOGGER.info("0");
		LOGGER.info("Completed");
	}
	
	/**
	 * Send a get request with more than max lifetime and should generate an exception
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testGetWithMoreThanMaxLifetime() throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	        
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] key = DataGenUtils.genBytes(64);
		byte[] data = DataGenUtils.genBytes(10);
		long lifetime = 259200;
		long lifetime1 = 777000;
		JunoResponse junoResponse = junoClient.create(key, data, lifetime);
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		try{			 
			junoResponse = junoClient.get(key, lifetime1);
			AssertJUnit.assertTrue ("Exception is not seen for big lifetime = 777000", false);
		}catch(Exception mex){
			AssertJUnit.assertTrue(mex.getCause().getMessage().contains("Invalid lifetime. current lifetime=777000, max configured lifetime=259200"));
			LOGGER.info("Exception", mex.getMessage());
			LOGGER.info("2");			
			LOGGER.info("Completed");
		}
	}

	/**
	 * Send a get request with negative lifetime and should generate an exception
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testGetWithNegativeLifetime() throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	        
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] key = DataGenUtils.genBytes(64);
		byte[] data = DataGenUtils.genBytes(10);
		long lifetime = 10;
		JunoResponse junoResponse = junoClient.create(key, data, lifetime);
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		junoResponse = junoClient.get(key);
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		AssertJUnit.assertEquals(key, junoResponse.key());
		AssertJUnit.assertTrue(1 == junoResponse.getVersion());
		AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
		AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));
		try{
			JunoResponse junoResponse1 = junoClient.get(key, (long)-1);
			AssertJUnit.assertTrue ("Exception is not seen for negative lifetime", false);
		}catch(Exception mex){
			LOGGER.debug(mex.getMessage());
			AssertJUnit.assertTrue(mex.getCause().getMessage().contains("The Document's TTL cannot be negative"));
			LOGGER.info("Exception", mex.getMessage());
			LOGGER.info("2");			
			LOGGER.info("Completed");
		}
	}

	/***********************************************
	 * insert 2 client objects with different 
	 * application names but same namespace. insert 
	 * record with key k1 using one client object. 
	 * Do a get for record with k1 using the second 
	 * client object.
	 *
	 ************************************************/
	@Test
	public void testGetWithSameNameSpaceDiffAPP_NAME() throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	    
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] data = DataGenUtils.genBytes(10);
		byte[] key = DataGenUtils.genBytes(64);

		JunoResponse junoResponse = junoClient.create(key, data);
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		junoResponse = junoClient.get(key);
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		JunoResponse junoResponse1 = junoClient2.get(key);
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse1.getStatus());
		AssertJUnit.assertEquals(junoResponse.key(), junoResponse1.key());
		LOGGER.debug("BUGGY : response ttl is " + junoResponse.getTtl() + " respnose1 ttl is " + junoResponse1.getTtl());
		AssertJUnit.assertTrue(junoResponse.getTtl() >= junoResponse1.getTtl());
		AssertJUnit.assertEquals(junoResponse.getVersion(), junoResponse1.getVersion());
		AssertJUnit.assertEquals(junoResponse.getValue().length, junoResponse1.getValue().length);
		AssertJUnit.assertEquals(new String(junoResponse.getValue()), new String(junoResponse1.getValue()));	
		LOGGER.info("0");
		LOGGER.info("Completed");
	}

	/***********************************************
	 * insert 2 client objects with namespaces NS1 & 
	 * NS2. insert records with different keys and 
	 * payloads under both clients. Perform 'Get' 
	 * for key insertd under NS1 using NS2's client 
	 * object and vice-versa. 
	 * Result: Should get "key not found"
	 *
	 ************************************************/
	@Test
	public void testGetWithDiffNameSpaceAndDiffKey() throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	    
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] data = DataGenUtils.genBytes(10);
		byte[] data1 = DataGenUtils.genBytes(20);
		byte[] key = DataGenUtils.genBytes(64);
		byte[] key1 = DataGenUtils.genBytes(64);

		long lifetime = 3600;

		JunoResponse junoResponse = junoClient.create(key, data, lifetime);
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		JunoResponse junoResponse1 = junoClient1.create(key1, data1, lifetime);
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse1.getStatus());

		//client1 get key from a different name space NS
		junoResponse = junoClient.get(key);
		AssertJUnit.assertEquals (OperationStatus.Success,junoResponse.getStatus());
		AssertJUnit.assertTrue (junoResponse.getTtl() <= lifetime && junoResponse.getTtl() > lifetime-3);

		try {
			junoResponse1 = junoClient1.get(key);
			AssertJUnit.assertEquals (OperationStatus.NoKey, junoResponse1.getStatus());	
		} catch (JunoException mex) {
			AssertJUnit.assertTrue ("Exception is seen for no key", false);
		}
		
		//client get key from a different name space
		try {
			junoResponse = junoClient.get(key1);
			AssertJUnit.assertEquals (OperationStatus.NoKey, junoResponse.getStatus());
		} catch (JunoException mex) {
			AssertJUnit.assertTrue ("Exception is seen for no key", false);	
		}

		junoResponse1 = junoClient1.get(key1);	
		AssertJUnit.assertEquals (OperationStatus.Success,junoResponse1.getStatus());
		AssertJUnit.assertTrue (junoResponse1.getTtl() <= lifetime && junoResponse1.getTtl() > lifetime-3);			

		LOGGER.info("0");
		LOGGER.info("Completed");
	}	

	/***********************************************
	 * insert 2 client objects with namespaces NS1 & 
	 * NS2. insert records with same key but different 
	 * payloads under both clients. Perform a 'Get' 
	 * for the key under NS1 and NS2.
	 *  
	 ************************************************/
	@Test
	public void testGetWithDiffNameSpaceDiffPayloadSameKey() throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	    
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] data = DataGenUtils.genBytes(10);
		byte[] data1 = DataGenUtils.genBytes(20);
		byte[] key = DataGenUtils.genBytes(10);

		junoClient.delete(key);
		try {
			Thread.sleep(2000);
		} catch (Exception ex) {
			LOGGER.debug(ex.getMessage());
		}
		junoClient.create(key, data);
		try {
			JunoResponse res = junoClient1.create(key, data1);
			AssertJUnit.assertEquals(OperationStatus.Success,res.getStatus());
		} catch (JunoException mex) {
			AssertJUnit.assertTrue ("Exception is seen for duplicate key", false);
		}
		JunoResponse junoResponse = junoClient.get(key);
		AssertJUnit.assertEquals (OperationStatus.Success,junoResponse.getStatus());
		JunoResponse junoResponse1 = junoClient1.get(key);
		AssertJUnit.assertEquals (OperationStatus.Success,junoResponse1.getStatus());
		AssertJUnit.assertEquals(junoResponse.key(), junoResponse1.key());
		AssertJUnit.assertTrue(junoResponse.getVersion() == junoResponse1.getVersion());
		AssertJUnit.assertTrue(junoResponse.getValue().length < junoResponse1.getValue().length);
		AssertJUnit.assertTrue(new String(junoResponse.getValue()) != new String(junoResponse1.getValue()));
		LOGGER.info("0");
		LOGGER.info("Completed");
	}

	@Test
	public void testinsertChineseKeyForCpp() throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	        
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		//byte[] key = "123456";
		byte[] key = "Q:������������A:���������. Q:�������������".getBytes(); 
		byte[] key_utf8 = new String("UTF-8,UTF-8").getBytes();
		byte[] data = "Q:������������A:���������".getBytes("UTF-8");
		byte[] data2 = "Q:������������A:���������. Q:���������������123".getBytes("UTF-8");

		JunoResponse junoResponse = junoClient.delete(key);
		AssertJUnit.assertEquals (OperationStatus.Success,junoResponse.getStatus());
		junoResponse = junoClient.delete(key_utf8);
		AssertJUnit.assertEquals (OperationStatus.Success,junoResponse.getStatus());
		junoResponse = junoClient.create(key, data, 100);
		AssertJUnit.assertEquals (OperationStatus.Success,junoResponse.getStatus());
		AssertJUnit.assertEquals(1, junoResponse.getVersion());
		junoResponse = junoClient.get(key, 0);
		AssertJUnit.assertEquals (OperationStatus.Success,junoResponse.getStatus());
		AssertJUnit.assertEquals(1, junoResponse.getVersion());
		AssertJUnit.assertTrue(junoResponse.getTtl() <= 100 && junoResponse.getTtl() > 100-3);
		AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
		AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));
		
		junoResponse = junoClient.get(key, 115);
		AssertJUnit.assertEquals (OperationStatus.Success,junoResponse.getStatus());
		AssertJUnit.assertEquals(1, junoResponse.getVersion());
		AssertJUnit.assertTrue(junoResponse.getTtl() <= 115 && junoResponse.getTtl() > 115-3);
		AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
		AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));
		
		LOGGER.debug("\nUpdate key: ");
		byte[] data1 = "Q:������������A:���������. Q:���������������".getBytes("UTF8");
		long lifetime1 = 120L;
		JunoResponse junoResponse1 = junoClient.update(key, data1, lifetime1);
		AssertJUnit.assertEquals (OperationStatus.Success,junoResponse1.getStatus());
		junoResponse1 = junoClient.get(key);
		LOGGER.debug("New Data1: " + new String(junoResponse1.getValue()));
		AssertJUnit.assertEquals(new String(data1), new String(junoResponse1.getValue()));

		JunoPropertiesProvider prop = new JunoPropertiesProvider(pConfig);
		JunoResponse junoResponse2 = junoClient.create(key_utf8, data);
		AssertJUnit.assertEquals (OperationStatus.Success,junoResponse2.getStatus());
		AssertJUnit.assertEquals(1, junoResponse2.getVersion());
		AssertJUnit.assertTrue(junoResponse2.getTtl() <= prop.getDefaultLifetime() && junoResponse2.getTtl() > prop.getDefaultLifetime()-3);
		junoResponse2 = junoClient2.get(key_utf8);
		AssertJUnit.assertEquals (OperationStatus.Success,junoResponse2.getStatus());
		AssertJUnit.assertEquals(data.length, junoResponse2.getValue().length);
		AssertJUnit.assertEquals(new String(data), new String(junoResponse2.getValue()));
		 	
		//conditional update old response will fail, ttl etc. shouldn't be updated
		LOGGER.debug("\nConditional Update: ");		
		try {
			JunoResponse junoResponse3 = junoClient.compareAndSet(junoResponse.getRecordContext(), data2, (long)600L);
			AssertJUnit.assertEquals(OperationStatus.ConditionViolation, junoResponse3.getStatus());
		} catch (JunoException mex) {
			AssertJUnit.assertTrue ("Exception is seen for Condition Violation", false);
		}
		
		JunoResponse gResponse = junoClient2.get(key);
		AssertJUnit.assertEquals (OperationStatus.Success,gResponse.getStatus());
		AssertJUnit.assertEquals(2, gResponse.getVersion());
		LOGGER.debug("junoResponse3 lifetime is " + gResponse.getTtl());
		AssertJUnit.assertTrue(gResponse.getTtl() <= lifetime1 && gResponse.getTtl() > lifetime1 - 5);
		AssertJUnit.assertEquals(data1.length, gResponse.getValue().length);
		AssertJUnit.assertEquals(new String(data1), new String(gResponse.getValue()));
		
		//conditional update successful
		gResponse = junoClient.compareAndSet(junoResponse1.getRecordContext(), data2, (long)600L);
		AssertJUnit.assertEquals (OperationStatus.Success,gResponse.getStatus());
		gResponse = junoClient2.get(key);
		AssertJUnit.assertEquals (OperationStatus.Success,gResponse.getStatus());
		AssertJUnit.assertEquals(key, gResponse.key());
		AssertJUnit.assertEquals(3, gResponse.getVersion());
		AssertJUnit.assertTrue(gResponse.getTtl() <= 600 && gResponse.getTtl() > 600-3);
		AssertJUnit.assertEquals(new String(data2), new String(gResponse.getValue()));

		LOGGER.info("0");
		LOGGER.info("Completed");
	}

	//@Test
	public void testGetEncrypt() throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	        
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] key = "Thisisanewkey".getBytes();
		LOGGER.debug("Crypt client1 inserts key");
		//cryptClient1.delete (key);
		//cryptClient1.insert(key, data, 7200L);

		LOGGER.debug("Crypt client2 gets key");
		JunoResponse junoResponse = cryptClient2.get(key);
		AssertJUnit.assertEquals (OperationStatus.Success,junoResponse.getStatus());
		AssertJUnit.assertEquals(key, junoResponse.key());
		LOGGER.debug("Data from Infra: " + new String(junoResponse.getValue()));

		//			LOGGER.debug("Crypt client1 updates key");
		//			byte[] data1 = "Hello testing data".getBytes();
		//			JunoResponse junoResponse2 = cryptClient1.update (key, data1);
		//			assertEquals (junoResponse2.getStatus(), OperationStatus.Success);
		//					
		//			junoResponse = cryptClient2.get(key);
		//			assertEquals (new String(data1), new String(junoResponse.getValue()));
		//			LOGGER.debug("Data: " + new String(junoResponse.getValue()));
		//			
		//			LOGGER.debug("Crypt client2 do a Conditional Update on key");
		//
		//			JunoResponse junoResponse3 = cryptClient1.cas (junoResponse, data2, (long)15);
		//			assertEquals (junoResponse3.getStatus(), OperationStatus.Success);
		//			assertEquals (new String(data2), new String(junoResponse3.getValue()));
		//			LOGGER.debug("Data: " + new String(junoResponse3.getValue()));

		//			LOGGER.debug("Crypt client1 delete key");
		//			cryptClient1.delete(key);
		//			junoResponse = cryptClient2.get(key);
		//			assertEquals (junoResponse.getStatus(), OperationStatus.NoKey);

	}

	@Test
	public void testGetWithKey3() throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	        
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] key =  "EbZaSXjUi3IHef52XSCe".getBytes();
		byte[] data2 = "This is a new test data testing".getBytes();
		JunoResponse res = junoClient.delete(key);
		AssertJUnit.assertEquals(OperationStatus.Success, res.getStatus());
		res = junoClient.create(key,data2,160L);
		AssertJUnit.assertEquals(OperationStatus.Success, res.getStatus());
		AssertJUnit.assertTrue(res.getTtl() == 160);
		JunoResponse junoResponse = junoClient.get(key);
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		LOGGER.info("0");
		LOGGER.info("Completed");
	}

	public String generateRandomString (int len) {
		String chars =
				"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-";

		int max = chars.length();
		LOGGER.debug(String.valueOf(max));
		StringBuffer strBuf = new StringBuffer();
		for (int i = 0 ; i < len; i++) {
			double index = Math.random() * max;
			//LOGGER.debug(Math.random());
			strBuf.append(chars.charAt((int) index)); 
		}
		LOGGER.debug(strBuf.toString()); 
		return strBuf.toString();
	}
	
//	@Test
//	public void testinsertWithEncryptData() throws JunoException, IOException{
//		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());		  
//		//byte[] key = DataGenUtils.genBytes(64);
//		//byte[] data = DataGenUtils.genBytes(10);
//		byte[] key = "ccctest";
//		byte[] data = "This is a special day of year 2015".getBytes();
//		//byte[] data = DataGenUtils.genBytes(10);
//		LOGGER.debug("Crypt client1 inserts key");
//	}
	/**
	 * Send a get request with key to the Juno 2.0 server and should not generate
	 * exception
	 * @throws JunoException
	 * @throws IOException
	 */
//	@Test 
//	public void testGetWithEncryptData() throws JunoException, IOException{
//		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());		  
//		byte[] key = DataGenUtils.genBytes(64);
//		byte[] data = DataGenUtils.genBytes(10);
//		//byte[] key = "abctest";
//		//byte[] data = "This is second day of year 2015".getBytes();
//		//byte[] data = DataGenUtils.genBytes(10);
//		LOGGER.debug("Crypt client1 inserts key");
//		cryptClient1.insert(key, data);
//
//		LOGGER.debug("Crypt client2 gets key");
//		JunoResponse junoResponse = cryptClient2.get(key);
//		AssertJUnit.assertEquals(key, junoResponse.key());
//		AssertJUnit.assertEquals("NS1", junoResponse.getNamespace());
//		//org.junit.Assert.assertTrue(1 == junoResponse.getVersion());
//
//		AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
//		AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));
//		LOGGER.debug("Data from Infra: " + new String(junoResponse.getValue()));
//
//		LOGGER.debug("Crypt client1 updates key");
//		byte[] data1 = "Hello testing data".getBytes();
//		JunoResponse junoResponse2 = cryptClient1.update (key, data1);
//		AssertJUnit.assertEquals (junoResponse2.getStatus(), OperationStatus.Success);
//
//		junoResponse = cryptClient2.get(key);
//		AssertJUnit.assertEquals (new String(data1), new String(junoResponse.getValue()));
//		LOGGER.debug("Data: " + new String(junoResponse.getValue()));
//
//		LOGGER.debug("Crypt client2 do a Conditional Update on key");
//		byte[] data2 = "Hello, updating testing data".getBytes();
//		JunoResponse junoResponse3 = cryptClient1.cas (junoResponse, data2, (long)15);
//		AssertJUnit.assertEquals (junoResponse3.getStatus(), OperationStatus.Success);
//		AssertJUnit.assertEquals (new String(data2), new String(junoResponse3.getValue()));
//		LOGGER.debug("Data: " + new String(junoResponse3.getValue()));
//
//		LOGGER.debug("Crypt client1 delete key");
//		cryptClient1.delete(key);
//		junoResponse = cryptClient2.get(key);
//		AssertJUnit.assertEquals (junoResponse.getStatus(), OperationStatus.NoKey);
//	}
}
