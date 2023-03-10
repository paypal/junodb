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
import com.paypal.juno.client.JunoReactClient;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class ReactorUpdateTest{
	private JunoClient junoClient;
	private JunoReactClient junoReactClient;
	private JunoReactClient junoReactClient1;
	private JunoReactClient junoReactClient2;
	private Properties pConfig;
	private Properties pConfig2;
	private Logger LOGGER;

	@BeforeClass
	public void setup() throws JunoException, IOException {
		URL url = JunoClientFactory.class.getResource("/com/paypal/juno/Juno.properties");
		LOGGER = LoggerFactory.getLogger(UpdateTest.class);
		pConfig = new Properties();
		pConfig.load(url.openStream());
		pConfig.setProperty(JunoProperties.APP_NAME, "QATestApp");
		pConfig.setProperty(JunoProperties.RECORD_NAMESPACE, "NS1");

		junoReactClient1 = JunoClientFactory.newJunoReactClient(url);
		URL url2 = JunoClientFactory.class.getResource("/com/paypal/juno/Juno.properties");
		pConfig2 = new Properties();
		pConfig2.load(url2.openStream());
		pConfig2.setProperty(JunoProperties.APP_NAME, "QATestApp2");
		pConfig2.setProperty(JunoProperties.RECORD_NAMESPACE, "NS1");
		
		try {
			Thread.sleep(3000);
			junoReactClient2 = JunoClientFactory.newJunoReactClient(new JunoPropertiesProvider(pConfig2), SSLUtil.getSSLContext());
			junoClient = JunoClientFactory.newJunoClient(new JunoPropertiesProvider(pConfig), SSLUtil.getSSLContext());
			junoReactClient = JunoClientFactory.newJunoReactClient(new JunoPropertiesProvider(pConfig), SSLUtil.getSSLContext());
		} catch (Exception ex) {
			LOGGER.debug(ex.getMessage());
		}
	}

	
	@Test
	public void testUpdateWithoutTTL() throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());

		 LOGGER.info(new Object(){}.getClass().getEnclosingMethod().getName()); 
		 LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
			  
		byte[] key = DataGenUtils.genBytes(10);
		byte[] data = DataGenUtils.genBytes(10);

		JunoResponse junoResponse = junoReactClient.create(key, data).block(); // It uses the default TTL 1800sec
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse.getStatus());
		junoResponse = junoReactClient.get(key).block();
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse.getStatus());
			AssertJUnit.assertEquals(key, junoResponse.key());
			AssertJUnit.assertTrue(1 == junoResponse.getVersion());
			AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			JunoResponse junoResponse1 = junoReactClient.update(key, data).block(); // Update without TTL
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse1.getStatus());
			AssertJUnit.assertTrue(2 == junoResponse1.getVersion());
			AssertJUnit.assertFalse(1800 == junoResponse1.getTtl()); // It cannot be 1800 as we waited for 10sec
			LOGGER.info("Completed");
	}
	
	/**
	 * Send a update request with different payload and should not generate an exception
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testUpdateWithDiffPayload() throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));			  
		byte[] key = DataGenUtils.genBytes(64);
		byte[] data = DataGenUtils.genBytes(10);
		byte[] data1 = DataGenUtils.genBytes(11);
		byte[] data2 = DataGenUtils.genBytes(20);
		byte[] data3 = DataGenUtils.genBytes(40);
		byte[] data4 = DataGenUtils.genBytes(800);		
		long lifetime = 22000;
		
		JunoResponse junoResponse = junoReactClient.create(key, data).block();
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse.getStatus());
		junoResponse = junoReactClient.get(key).block();
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse.getStatus());
		AssertJUnit.assertEquals(key, junoResponse.key());
		AssertJUnit.assertTrue(1 == junoResponse.getVersion());
		AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
		AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));
		
		JunoResponse mResponse = junoReactClient.update(key, data1).block(); //ttl won't be updated
		AssertJUnit.assertEquals (OperationStatus.Success,mResponse.getStatus());
		assert (mResponse.getStatus().getCode() == OperationStatus.Success.getCode());
		
		junoResponse = junoReactClient.update(key,data2, (long)lifetime).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		AssertJUnit.assertTrue(3 == junoResponse.getVersion());
		AssertJUnit.assertTrue(lifetime-3 <= junoResponse.getTtl() && junoResponse.getTtl() <= lifetime);
		
		junoResponse = junoClient.update(key, data3, (long)10);
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		AssertJUnit.assertTrue(4 == junoResponse.getVersion());
		AssertJUnit.assertTrue(lifetime-3 <= junoResponse.getTtl() && junoResponse.getTtl() <= lifetime);
		
		junoResponse = junoReactClient.update(key, data4, (long)0).block();
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse.getStatus());
		AssertJUnit.assertTrue(5 == junoResponse.getVersion());
		AssertJUnit.assertTrue(junoResponse.getTtl() <= lifetime && junoResponse.getTtl() >= lifetime-5);		

		JunoResponse junoResponse1 = junoReactClient.get(key).block();
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse1.getStatus());
		AssertJUnit.assertEquals(key, junoResponse1.key());
		AssertJUnit.assertTrue(5 == junoResponse1.getVersion());
		AssertJUnit.assertEquals(data4.length, junoResponse1.getValue().length);
		AssertJUnit.assertEquals(new String(data4), new String(junoResponse1.getValue()));
		LOGGER.info("0");
		LOGGER.info("Completed");
	}
	
	/**
	 * Send a update request with same payload
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testUpdateWithSamePayload() throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] key = DataGenUtils.genBytes(64);
		byte[] data = DataGenUtils.genBytes(10);
		
		JunoResponse junoResponse = junoReactClient.create(key, data).block();
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse.getStatus());
		junoResponse = junoReactClient.get(key).block();
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse.getStatus());
		AssertJUnit.assertEquals(key, junoResponse.key());
		AssertJUnit.assertTrue(1 == junoResponse.getVersion());
		AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
		AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));
			
		JunoResponse junoResponse1 = junoReactClient.update(key, data).block();
		AssertJUnit.assertTrue(OperationStatus.Success == junoResponse.getStatus());
		AssertJUnit.assertEquals(key, junoResponse1.key());
		AssertJUnit.assertTrue(2 == junoResponse1.getVersion());

		junoResponse1 = junoReactClient.get(key).block();
		AssertJUnit.assertTrue(OperationStatus.Success == junoResponse.getStatus());
		AssertJUnit.assertEquals(junoResponse.key(), junoResponse1.key());
		AssertJUnit.assertTrue(2 == junoResponse1.getVersion());
		AssertJUnit.assertEquals(junoResponse.getValue().length, junoResponse1.getValue().length);
		AssertJUnit.assertEquals(new String(junoResponse.getValue()), new String(junoResponse1.getValue()));
		AssertJUnit.assertTrue(1800-8 <= junoResponse1.getTtl() && junoResponse1.getTtl() <= 1800);
		LOGGER.info("0");
		LOGGER.info("Completed");		
	}
	
	/**
	 * Send a update with empty key and should generate an exception
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testUpdateWithEmptyKey() throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] key = "".getBytes();
		byte[] data = DataGenUtils.genBytes(10);
		try{
			junoReactClient.update(key, data).block();
			AssertJUnit.assertTrue ("Exception should happen for empty key", false);
		}catch(Exception mex){
			AssertJUnit.assertTrue(mex.getCause().getMessage().contains("The Document key must not be null or empty"));
			LOGGER.info("Exception", mex.getMessage());
			LOGGER.info("2");			
			LOGGER.info("Completed");
		}
	}
	
	/**
	 * Send a update request with null key and should generate an exception
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testUpdateWithNullKey() throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] key = null;
		byte[] data = DataGenUtils.genBytes(10);
		try{
			junoReactClient.update(key, data).block();
			AssertJUnit.assertTrue ("Exception should happen for null key", false);
		}catch(Exception mex){
			AssertJUnit.assertTrue(mex.getCause().getMessage().contains("The Document key must not be null or empty"));
			LOGGER.info("Exception", mex.getMessage());
			LOGGER.info("2");			
			LOGGER.info("Completed");
		}
	}
	
	/**
	 * Send a update request with no key exists on server and should generate an 
	 * exception
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testUpdateKeyWhichDoesNotExist() throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));			  
		byte[] key = DataGenUtils.genBytes(64);
		byte[] data = DataGenUtils.genBytes(10);
		
		try {
			JunoResponse mResponse = junoReactClient.update(key, data).block();
			AssertJUnit.assertEquals (OperationStatus.NoKey, mResponse.getStatus());
		} catch (JunoException mex) {
			AssertJUnit.assertTrue ("Exception is seen for no key", false);
			LOGGER.info("0");
		} finally {
			LOGGER.info("Completed");	
		}	
	}
	
	/**
	 * Send a update request with 128 bytes key and should not generate an exception
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testUpdateWith128BytesKey() throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	

		byte[] key1 = DataGenUtils.genBytes(128);
		byte[] data = DataGenUtils.genBytes(10);

		JunoResponse junoResponse = junoReactClient.create(key1, data).block();
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse.getStatus());
		junoResponse = junoReactClient.update(key1, data).block();
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse.getStatus());
		junoResponse = junoReactClient.get(key1).block();
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse.getStatus());
		AssertJUnit.assertEquals(key1, junoResponse.getKey());
		AssertJUnit.assertTrue(2 == junoResponse.getVersion());
		AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
		AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));
		
		LOGGER.info("0");
		LOGGER.info("Completed");		
	}
	
	/**
	 * Send a update request with 130 bytes key and should generate an exception
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testUpdateWith129BytesKey() throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] key1 = DataGenUtils.genBytes(129);
		byte[] data = DataGenUtils.genBytes(10);
		try{
			junoReactClient.update(key1, data).block();
			AssertJUnit.assertTrue ("Exception should happen for key > 128 bytes", false);
		}catch(Exception mex){
			LOGGER.debug(mex.getCause().getMessage());
			AssertJUnit.assertTrue(mex.getCause().getMessage().contains("The Document key must not be larger than 128 bytes"));
		}
	}
	/**
	 * Send a update request with mix of special chars and should not generate an exception
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testUpdateWithSpecialCharsKey() throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		JunoPropertiesProvider prop = new JunoPropertiesProvider(pConfig);
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	

		byte[] key1 = "@@#$%^&*()_+?>,<|}{[]~abc780=.".getBytes();
		byte[] data = DataGenUtils.genBytes(10);
		JunoResponse junoResponse = junoReactClient.delete(key1).block();
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse.getStatus());
		junoResponse = junoReactClient.create(key1, data).block();
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse.getStatus());
		junoResponse = junoReactClient.update(key1, data).block();
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse.getStatus());
		AssertJUnit.assertEquals(key1, junoResponse.key());
		AssertJUnit.assertTrue(2 == junoResponse.getVersion());
		junoResponse = junoReactClient.get(key1).block();
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse.getStatus());
		AssertJUnit.assertTrue(2 == junoResponse.getVersion());
		AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
		AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));		
		AssertJUnit.assertTrue(junoResponse.getTtl() <= prop.getDefaultLifetime() && junoResponse.getTtl() >= prop.getDefaultLifetime()-3);
		
		LOGGER.info("0");
		LOGGER.info("Completed");
		
	}
	/**
	 * Send a update request with zero lifetime, the original lifetime for the 
	 * object will be retained.
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testUpdateWithZeroLifetime() throws JunoException, Exception{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] key = DataGenUtils.genBytes(10);
		byte[] data = DataGenUtils.genBytes(10);
		byte[] data2 = DataGenUtils.genBytes(10);
		JunoResponse junoResponse = junoReactClient.create(key, data, (long)3).block();
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse.getStatus());
		junoResponse = junoReactClient.get(key).block();
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse.getStatus());
		AssertJUnit.assertEquals(key, junoResponse.key());
		AssertJUnit.assertTrue(1 == junoResponse.getVersion());
		AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
		AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));

		junoResponse = junoReactClient.update(key, data2, (long)0).block();
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse.getStatus());
		Thread.sleep (4000);
		try {
			JunoResponse mResponse = junoReactClient.update(key, data).block();
			AssertJUnit.assertEquals (OperationStatus.NoKey, mResponse.getStatus());
		} catch (JunoException mex) {
			AssertJUnit.assertTrue ("Exception is seen for no key", false);
			LOGGER.info("0");
		} finally {
			LOGGER.info("Completed");
		}		
	}
	
	/**
	 * Send a update request with negative lifetime, exception will be throw
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testUpdateWithNegativeLifetime() throws JunoException, Exception{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] key = DataGenUtils.genBytes(10);
		byte[] data = DataGenUtils.genBytes(10);
		byte[] data2 = DataGenUtils.genBytes(10);
		JunoResponse junoResponse = junoReactClient.create(key, data, (long)3).block();
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse.getStatus());
		
		try {
			junoReactClient.update(key, data2, (long)-1).block();
			AssertJUnit.assertTrue(false);
		} catch (JunoException mex) {
			AssertJUnit.assertTrue(mex.getCause().getMessage().contains("The Document's TTL cannot be negative"));
			LOGGER.info("0");
			LOGGER.info("Completed");
		}
	}
				
	/**
	 * Send a update request without lifetime, default lifetime will be updated for existing Juno
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testUpdateWithoutLifetime() throws JunoException, Exception{ //TODO: ???
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] key = DataGenUtils.genBytes(10);
		byte[] data = DataGenUtils.genBytes(10);
		byte[] data2 = DataGenUtils.genBytes(10);
		
		JunoResponse junoResponse = junoReactClient.create(key, data, (long)3).block();
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse.getStatus());
		junoResponse = junoReactClient.get(key).block();
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse.getStatus());
		AssertJUnit.assertEquals(key, junoResponse.key());
		AssertJUnit.assertTrue(1 == junoResponse.getVersion());
		AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
		AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));

		JunoResponse mResponse=junoReactClient.update(key, data2).block(); // This should not update the Life time
		AssertJUnit.assertEquals (OperationStatus.Success, mResponse.getStatus());
		Thread.sleep (4000); // Sleep for 2 sec so that the key expires
		mResponse = junoReactClient.update(key, data).block();
		AssertJUnit.assertEquals (OperationStatus.NoKey, mResponse.getStatus());
		mResponse = junoReactClient.get(key).block();
		AssertJUnit.assertEquals (OperationStatus.NoKey,mResponse.getStatus()); 				
		LOGGER.info("0");
		LOGGER.info("Completed");
	}

	/**
	 * Send a update request with 3 days lifetime and should not generate an exception
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testUpdateWith3DaysLifetime() throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] key = DataGenUtils.genBytes(10);
		byte[] data = DataGenUtils.genBytes(10);
		long lifetime = 259200;

		JunoResponse junoResponse = junoReactClient.create(key, data, (long)10).block();
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse.getStatus());
		junoResponse = junoReactClient.get(key).block();
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse.getStatus());
		AssertJUnit.assertEquals(key, junoResponse.key());
		AssertJUnit.assertTrue(1 == junoResponse.getVersion());
		AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
		AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));
		junoResponse = junoReactClient.update(key, data, lifetime).block();
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse.getStatus());
		JunoResponse junoResponse1 = junoReactClient.get(key).block();
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse1.getStatus());
		AssertJUnit.assertEquals(key, junoResponse1.key());
		AssertJUnit.assertTrue(2 == junoResponse1.getVersion());
		AssertJUnit.assertEquals(data.length, junoResponse1.getValue().length);
		AssertJUnit.assertEquals(new String(data), new String(junoResponse1.getValue()));
		
		LOGGER.info("0");
		LOGGER.info("Completed");
	}
	/**
	 * Send a update request with more than 3 days lifetime and should generate an 
	 * exception
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testUpdateWithMorethan3DaysLifetime() throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] key = DataGenUtils.genBytes(10);
		byte[] data = DataGenUtils.genBytes(10);
		long lifetime = 259201;
		try{
			JunoResponse junoResponse = junoReactClient.create(key, data, (long)10).block();
			AssertJUnit.assertEquals (OperationStatus.Success, junoResponse.getStatus());
			junoResponse = junoReactClient.get(key).block();
			AssertJUnit.assertEquals(key, junoResponse.key());
			AssertJUnit.assertTrue(1 == junoResponse.getVersion());
			AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
			AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));
			@SuppressWarnings("unused")
			JunoResponse junoResponse1 = junoReactClient.update(key, data, lifetime).block();
			AssertJUnit.assertTrue ("Exception should happen for life time > 3 days", false);
		}catch(Exception mex){
			LOGGER.debug(mex.getCause().getMessage());
			AssertJUnit.assertTrue(mex.getCause().getMessage().contains("Invalid lifetime. current lifetime=259201, max configured lifetime=259200"));
		}
	}
	
	/**
	 * Send a update request with null payload and should generate an exception
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testUpdateWithNullPayload() throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] data = DataGenUtils.genBytes(10);
		byte[] key = DataGenUtils.genBytes(64);
		try{
			JunoResponse junoResponse = junoReactClient.create(key, data).block();
			AssertJUnit.assertEquals (OperationStatus.Success, junoResponse.getStatus());
			junoResponse = junoReactClient.get(key).block();
			AssertJUnit.assertEquals (OperationStatus.Success, junoResponse.getStatus());
			AssertJUnit.assertEquals(key, junoResponse.key());
			AssertJUnit.assertTrue(1 == junoResponse.getVersion());
			AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
			AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));
			@SuppressWarnings("unused")
			JunoResponse junoResponse1 = junoReactClient.update(key, null).block();
			AssertJUnit.assertEquals (OperationStatus.Success, junoResponse1.getStatus());
			AssertJUnit.assertEquals(key, junoResponse1.key());
			AssertJUnit.assertTrue(2 == junoResponse1.getVersion());
			AssertJUnit.assertEquals(0, junoResponse1.getValue().length);
		}catch(Exception mex){
			LOGGER.debug(mex.getCause().getMessage());
			AssertJUnit.assertFalse(true);
		}
	}
	
	/**
	 * Send a update request with empty payload and should not generate an exception
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testUpdateWithEmptyPayload() throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] data = DataGenUtils.genBytes(64);
		byte[] key = DataGenUtils.genBytes(64);
		byte[] data1 = "".getBytes();
		try{
			JunoResponse junoResponse = junoReactClient.create(key, data).block();
			AssertJUnit.assertEquals (OperationStatus.Success, junoResponse.getStatus());
			junoResponse = junoReactClient.get(key).block();
			AssertJUnit.assertEquals (OperationStatus.Success, junoResponse.getStatus());
			AssertJUnit.assertEquals(key, junoResponse.key());
			AssertJUnit.assertTrue(1 == junoResponse.getVersion());
			AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
			AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));
			junoResponse = junoReactClient.update(key, data1).block();
			AssertJUnit.assertEquals (OperationStatus.Success, junoResponse.getStatus());
			LOGGER.info("0");
		}catch(Exception mex){
			AssertJUnit.assertTrue("empty payload should be allowed, why come to exception?", false);
			LOGGER.info("2");
		} finally {
			LOGGER.info("Completed");
		}
	}
	
	/**
	 * Send a update request with 200KB payload and should not generate an exception
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testUpdateWith200KBPayload() throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] data = DataGenUtils.genBytes(10);
		byte[] data1 = DataGenUtils.genBytes(204800);
		byte[] key = DataGenUtils.genBytes(64);
		
		JunoResponse junoResponse = junoReactClient.create(key, data).block();
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse.getStatus());
		junoResponse = junoReactClient.get(key).block();
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse.getStatus());
		AssertJUnit.assertEquals(key, junoResponse.key());
		AssertJUnit.assertTrue(1 == junoResponse.getVersion());
		AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
		AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));
		JunoResponse junoResponse1 = null;
		try {
			junoResponse1 = junoReactClient.update(key, data1).block();
		} catch (JunoException ex) {
			LOGGER.debug("Exception occured: " + ex.getMessage());
			AssertJUnit.assertTrue(false);
		}
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse1.getStatus());			
		AssertJUnit.assertEquals(key, junoResponse1.key());
		AssertJUnit.assertTrue(2 == junoResponse1.getVersion());
		junoResponse1 = junoReactClient.get(key).block();
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse1.getStatus());
		AssertJUnit.assertEquals(data1.length, junoResponse1.getValue().length);
		AssertJUnit.assertEquals(new String(data1), new String(junoResponse1.getValue()));
	}
	
	/**
	 * Send a update request with more than 200KB payload and should generate an exception
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testUpdateWithMorethan200KBPayload() throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] data = DataGenUtils.genBytes(10);
		byte[] data1 = DataGenUtils.genBytes(204801);
		byte[] key = DataGenUtils.genBytes(64);
		try{
			JunoResponse junoResponse =junoReactClient.create(key, data).block();
			AssertJUnit.assertEquals (OperationStatus.Success, junoResponse.getStatus());
			junoResponse = junoReactClient.get(key).block();
			AssertJUnit.assertEquals (OperationStatus.Success, junoResponse.getStatus());
			AssertJUnit.assertEquals(key, junoResponse.key());
			AssertJUnit.assertTrue(1 == junoResponse.getVersion());
			AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
			AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));
			@SuppressWarnings("unused")
			JunoResponse junoResponse1 = junoReactClient.update(key, data1).block();
			AssertJUnit.assertTrue ("Exception should happen for big payload > 200KB", false);
		}catch(Exception mex){
			LOGGER.debug(mex.getCause().getMessage());
			AssertJUnit.assertTrue(mex.getCause().getMessage().contains("The Document Value must not be larger than 204800 bytes"));
			LOGGER.info("Exception", mex.getMessage());
			LOGGER.info("2");			
			LOGGER.info("Completed");
		}
	}
	/**
	 * Send a update request with Lifetime and payload and should not generate an exception
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testUpdateWithLifeTimeAndPayload() throws JunoException, Exception{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] data = DataGenUtils.genBytes(10);
		byte[] data1 = DataGenUtils.genBytes(1000);
		long lifetime =5;
		long lifetime1 = lifetime+2;
		long lifetime2 = lifetime1+5;
		byte[] key = DataGenUtils.genBytes(64);

		JunoResponse junoResponse = junoReactClient.create(key, data, lifetime).block();
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse.getStatus());
		junoResponse = junoReactClient.get(key).block();
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse.getStatus());
		AssertJUnit.assertEquals(key, junoResponse.key());
		AssertJUnit.assertTrue(1 == junoResponse.getVersion());
		AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
		AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));
		junoResponse = junoReactClient.update(key, data1, lifetime1).block();
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse.getStatus());

		//Sleep 11 seconds, key should not expired
		Thread.sleep (6000);
		JunoResponse junoResponse1 = junoReactClient.get(key).block();
		AssertJUnit.assertEquals (OperationStatus.Success,junoResponse1.getStatus());
		AssertJUnit.assertEquals(key, junoResponse1.key());
		AssertJUnit.assertTrue(2 == junoResponse1.getVersion());
		AssertJUnit.assertEquals(data1.length, junoResponse1.getValue().length);
		AssertJUnit.assertEquals(new String(data1), new String(junoResponse1.getValue()));
		Thread.sleep (4000);
		//Sleep 2 more seconds for a total of 13 seconds
		try {
			JunoResponse mResponse = junoReactClient.update(key, data1, lifetime2).block();
			AssertJUnit.assertEquals (OperationStatus.NoKey,mResponse.getStatus());
		} catch (JunoException mex) {
			AssertJUnit.assertTrue ("Exception is seen for key", false);
			LOGGER.info("0");
		} finally {
			LOGGER.info("Completed");	
		}
	}
	
	//Below tests needs to be reviewed This is important.
	/*************************************************
	 * Create 2 client objects with namespaces NS1 & NS2.
	 * Create records with same key under both clients.
	 *  Update the record under NS1 only. 
	 *************************************************/
	@Test
	public void testUpdateInOneOfTheNameSpaces() throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] data = DataGenUtils.genBytes(10);
		byte[] data1 = DataGenUtils.genBytes(11);
		byte[] key = DataGenUtils.genBytes(64);

		JunoResponse junoResponse = junoReactClient.create(key, data, (long)20).block();
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse.getStatus());
		junoResponse = junoReactClient1.create(key, data).block(); //same appname, different namespace
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse.getStatus());
			
		junoResponse = junoReactClient.get(key).block();
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse.getStatus());
		AssertJUnit.assertEquals(key, junoResponse.key());
		AssertJUnit.assertTrue(1 == junoResponse.getVersion());
		AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
		AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));
			
		JunoResponse junoResponse1 = junoReactClient1.get(key).block();
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse1.getStatus());
		AssertJUnit.assertEquals(key, junoResponse1.key());
		AssertJUnit.assertTrue(1 == junoResponse1.getVersion());
		AssertJUnit.assertTrue(OperationStatus.Success == junoResponse1.getStatus());

		JunoResponse junoResponse2 = junoReactClient.compareAndSet(junoResponse.getRecordContext(), data1, (long)5).block();
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse2.getStatus());
		AssertJUnit.assertEquals(junoResponse.key(), junoResponse2.key());
		AssertJUnit.assertTrue(2 == junoResponse2.getVersion());
		junoResponse2 = junoReactClient.get(key).block();
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse2.getStatus());
		AssertJUnit.assertEquals(data1.length, junoResponse2.getValue().length);
		AssertJUnit.assertEquals(new String(data1), new String(junoResponse2.getValue()));
		AssertJUnit.assertTrue(junoResponse.getValue().length < junoResponse2.getValue().length);
		AssertJUnit.assertTrue(new String(junoResponse.getValue()) != new String(junoResponse2.getValue()));
			
		junoResponse2 = junoReactClient1.get(key).block();
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse2.getStatus());
		AssertJUnit.assertTrue(1 == junoResponse2.getVersion());

		LOGGER.info("0");
		LOGGER.info("Completed");			
	}
	
	/*************************************************
	 * Create two clients with the same namespaces but 
	 * different application names. Create a record 
	 * using client1 with key k1.Update the record 
	 * with key k1 using client2.
	 *************************************************/
	@Test
	public void testUpdateWithSameNameSpaceDiffAppname() throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] data = DataGenUtils.genBytes(10);
		byte[] data1 = DataGenUtils.genBytes(20);
		byte[] key = DataGenUtils.genBytes(64);

		JunoResponse junoResponse = junoReactClient.create(key, data).block();
		AssertJUnit.assertEquals(OperationStatus.Success,junoResponse.status());
		junoResponse = junoReactClient.get(key).block();
		AssertJUnit.assertEquals(OperationStatus.Success,junoResponse.status());
		AssertJUnit.assertEquals(key, junoResponse.key());
		AssertJUnit.assertTrue(1 == junoResponse.getVersion());
		AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
		AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));

		JunoResponse junoResponse1 = junoReactClient2.update(key, data1).block();
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse1.getStatus());
		AssertJUnit.assertEquals(key, junoResponse1.key());
		AssertJUnit.assertTrue(2 == junoResponse1.getVersion());
		junoResponse1 = junoReactClient.get(key).block();
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse1.getStatus());
		AssertJUnit.assertTrue(2 == junoResponse1.getVersion());
		AssertJUnit.assertEquals(data1.length, junoResponse1.getValue().length);
		AssertJUnit.assertEquals(new String(data1), new String(junoResponse1.getValue()));
		LOGGER.info("0");
		LOGGER.info("Completed");
	}
	
	/*************************************************
	 * Attempting update expired data
	 * @throws InterruptedException 
	 *************************************************/
	@Test
	public void testUpdateExpiredData() throws JunoException, IOException, InterruptedException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	        
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] data = DataGenUtils.genBytes(10);
		byte[] data1 = DataGenUtils.genBytes(10);
		byte[] key = DataGenUtils.genBytes(64);

		JunoResponse junoResponse = junoReactClient.create(key, data, (long)7).block();
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse.getStatus());
		junoResponse = junoReactClient.get(key).block();
		AssertJUnit.assertEquals (OperationStatus.Success, junoResponse.getStatus());
		AssertJUnit.assertEquals(key, junoResponse.key());
		AssertJUnit.assertTrue(1 == junoResponse.getVersion());
		AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
		AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));
		Thread.sleep(10000);
		try {
			JunoResponse mResponse = junoReactClient1.update(key, data1).block();
			AssertJUnit.assertEquals (OperationStatus.NoKey,mResponse.getStatus());
		} catch (JunoException mex) {
			AssertJUnit.assertTrue ("Exception is seen for key", false);
			LOGGER.info("0");
		} finally {
			LOGGER.info("Completed");
		}
	}
}
