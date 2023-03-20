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
import com.paypal.juno.client.JunoReactClient;
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
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class ReactorConditionUpdateTest{
	private JunoReactClient junoReactClient;
	private JunoReactClient junoReactClient1;
	private JunoReactClient junoReactClient2;
	private Properties pConfig;
	private Properties pConfig1;
	private Properties pConfig2;
	private Logger LOGGER;

	@BeforeClass
	public void setup() throws JunoException, IOException, InterruptedException {
		LOGGER = LoggerFactory.getLogger(ConditionalUpdateTest.class);

		URL url = ConditionalUpdateTest.class.getResource("/com/paypal/juno/Juno.properties");
		pConfig = new Properties();
		pConfig.load(url.openStream());
		pConfig.setProperty(JunoProperties.APP_NAME, "QATestApp");
		pConfig.setProperty(JunoProperties.RECORD_NAMESPACE, "NS");

		URL url1 = ConditionalUpdateTest.class.getResource("/com/paypal/juno/Juno.properties");
		pConfig1 = new Properties();
		pConfig1.load(url1.openStream());
		pConfig1.setProperty(JunoProperties.APP_NAME, "QATestApp");
		pConfig1.setProperty(JunoProperties.RECORD_NAMESPACE, "NS1");
		URL url2 = ConditionalUpdateTest.class.getResource("/com/paypal/juno/Juno.properties");
		pConfig2 = new Properties();
		pConfig2.load(url2.openStream());
		pConfig2.setProperty(JunoProperties.APP_NAME, "QATestApp2");
		pConfig2.setProperty(JunoProperties.RECORD_NAMESPACE, "NS");

		try{
			junoReactClient = JunoClientFactory.newJunoReactClient(new JunoPropertiesProvider(pConfig), SSLUtil.getSSLContext());
			junoReactClient1 = JunoClientFactory.newJunoReactClient(new JunoPropertiesProvider(pConfig1), SSLUtil.getSSLContext());
			junoReactClient2 = JunoClientFactory.newJunoReactClient(new JunoPropertiesProvider(pConfig2), SSLUtil.getSSLContext());
		}catch (Exception e) {
			throw new RuntimeException(e);
		}

		Thread.sleep(1000);

	}
	/**
	 * Send a cas request with different payload and should not generate an exception
	 * @throws JunoException
	 * @throws IOException 
	 */
	@Test
	public void testCUpdateWithDiffPayload() throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] key = DataGenUtils.genBytes(64);
		byte[] data = DataGenUtils.genBytes(10);
		byte[] data1 = DataGenUtils.genBytes(11);
		JunoResponse junoResponse = junoReactClient.create(key, data, (long)20).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		junoResponse = junoReactClient.get(key).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		AssertJUnit.assertTrue(1 == junoResponse.getVersion());
		AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
		AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));
		JunoResponse junoResponse1 = junoReactClient.compareAndSet(junoResponse.getRecordContext(), data1, (long)5).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse1.getStatus());
		AssertJUnit.assertTrue(2 == junoResponse1.getVersion());
		AssertJUnit.assertTrue(junoResponse.getTtl() >= 20-3 && junoResponse.getTtl() <= 20);	
		
		junoResponse1 = junoReactClient.get(key).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse1.getStatus());
		AssertJUnit.assertTrue(2 == junoResponse1.getVersion());
		AssertJUnit.assertEquals(data1.length, junoResponse1.getValue().length);
		AssertJUnit.assertEquals(new String(data1), new String(junoResponse1.getValue()));
		AssertJUnit.assertTrue(junoResponse.getTtl() >= 20-3 && junoResponse.getTtl() <= 20);

		LOGGER.info("SUCCESS");
		LOGGER.info("Completed");
	}
	
	/**
	 * Send a cas request with same payload - should be successful
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testCUpdateWithSamePayload() throws JunoException, IOException {
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] key = DataGenUtils.genBytes(64);
		byte[] data = DataGenUtils.genBytes(10);

		JunoResponse junoResponse = junoReactClient.create(key, data).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());

		junoResponse = junoReactClient.get(key).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		AssertJUnit.assertEquals(key, junoResponse.key());
		AssertJUnit.assertTrue(1 == junoResponse.getVersion());
		AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
		AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));

		JunoResponse junoResponse1 = junoReactClient.compareAndSet(junoResponse.getRecordContext(), data, (long)5).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse1.getStatus());

		junoResponse1 = junoReactClient.get(key).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse1.getStatus());
		AssertJUnit.assertEquals(key, junoResponse1.key());
		AssertJUnit.assertTrue(2 == junoResponse1.getVersion());
		AssertJUnit.assertEquals(data.length, junoResponse1.getValue().length);
		AssertJUnit.assertEquals(new String(data), new String(junoResponse1.getValue()));
		AssertJUnit.assertEquals(junoResponse.key(), junoResponse1.key());
		AssertJUnit.assertEquals(junoResponse.getValue().length, junoResponse1.getValue().length);
		AssertJUnit.assertEquals(new String(junoResponse.getValue()), new String(junoResponse1.getValue()));
		LOGGER.info("SUCCESS");
		LOGGER.info("Completed");
	}
	
	/**
	 * Send a cas with empty key and should generate an exception
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testCUpdateWithEmptyKey() throws JunoException, IOException {
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] data = DataGenUtils.genBytes(10);
		RecordContext rcx = new RecordContext("".getBytes(),(long)1,(long)5,(long)1);
		try {
			junoReactClient.compareAndSet(rcx, data, (long)5).block();
			AssertJUnit.assertTrue ("Exception is not seen for empty key", false);
		}catch(Exception mex){
			LOGGER.debug("Exception :"+mex.getMessage());
			AssertJUnit.assertTrue(mex.getCause().getMessage().contains("The Document key must not be null or empty"));
			LOGGER.info("Exception", mex.getMessage());
			LOGGER.info("SUCCESS");
			LOGGER.info("Completed");
		}
	}
	
	/**
	 * Send a cas request with null key and should generate an exception
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testCUpdateWithNullKey() throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	    
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] data = DataGenUtils.genBytes(10);
		RecordContext rcx=null;
		try {	
			junoReactClient.compareAndSet(rcx, data, (long)5).block();
			AssertJUnit.assertTrue ("Exception is not seen for null key", false);
		}catch(Exception mex){
			LOGGER.debug("Exception :"+mex.getMessage());
			AssertJUnit.assertTrue(mex.getCause().getMessage().contains("Record Context cannot be null"));
			LOGGER.info("Exception", mex.getMessage());
			LOGGER.info("ERROR");
			LOGGER.info("Completed");
		}
	}
	
	/**
	 * Send a cas request with no key exists on server and should generate an 
	 * exception
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testCUpdateKeyWhichDoesNotExist() throws JunoException, IOException {
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	    
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] data = DataGenUtils.genBytes(10);
		RecordContext rcx = new RecordContext("nonexist".getBytes(), (long)1, (long)5, (long)1);

		try {
			JunoResponse mResponse = junoReactClient.compareAndSet(rcx, data, (long)5).block();	
			AssertJUnit.assertEquals (OperationStatus.NoKey,mResponse.getStatus());
		} catch (JunoException mex) {
			AssertJUnit.assertTrue ("Exception is seen for key", false);
			LOGGER.info("SUCCESS");
		} finally {
			LOGGER.info("Completed");
		}
	}
	
	/**
	 * Send a cas request with 128 bytes key and should not generate an exception
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testCUpdateWith128BytesKey() throws JunoException, Exception {
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	        
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] key = DataGenUtils.genBytes(128);
		byte[] data = DataGenUtils.genBytes(10);

		JunoResponse junoResponse = junoReactClient.create(key, data).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		junoResponse = junoReactClient.get(key).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		AssertJUnit.assertEquals(key, junoResponse.key());
		AssertJUnit.assertTrue(1 == junoResponse.getVersion());
		AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
		AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));
		junoResponse = junoReactClient.compareAndSet(junoResponse.getRecordContext(), data, (long)5).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		JunoResponse junoResponse1 = junoReactClient.get(key).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse1.getStatus());
		AssertJUnit.assertEquals(key, junoResponse1.key());
		AssertJUnit.assertTrue(2 == junoResponse1.getVersion());
		AssertJUnit.assertEquals(data.length, junoResponse1.getValue().length);
		AssertJUnit.assertEquals(new String(data), new String(junoResponse1.getValue()));
		LOGGER.info("SUCCESS");
		LOGGER.info("Completed");
	}
	
	/**
	 * Send a cas request with 130 bytes key and should generate an exception
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testCUpdateWith129BytesKey() throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] key = DataGenUtils.genBytes(129);
		byte[] data = DataGenUtils.genBytes(10);
		try{
			JunoResponse junoResponse = junoReactClient.create(key, data).block();
			AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
			junoResponse = junoReactClient.get(key).block();
			AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
			AssertJUnit.assertEquals(key, junoResponse.key());
			AssertJUnit.assertTrue(1 == junoResponse.getVersion());
			AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
			AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));
			junoReactClient.compareAndSet(junoResponse.getRecordContext(), data, (long)5).block();
			AssertJUnit.assertTrue ("Exception is NOT seen for Key = 129 bytes length", false);
		}catch(Exception mex){
			LOGGER.debug(mex.getCause().getMessage());
			AssertJUnit.assertTrue(mex.getCause().getMessage().contains("The Document key must not be larger than 128 bytes"));
			LOGGER.info("Exception", mex.getMessage());
			LOGGER.info("ERROR");
			LOGGER.info("Completed");
		}
	}
	
	/**
	 * Send a cas request with mix of special chars and should not generate an exception
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testCUpdateWithSpecialCharsKey() throws JunoException, IOException{	
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] key1 = "@@#$%^&*()_+?>,<|}{[]~abc780=.".getBytes();
		byte[] data = DataGenUtils.genBytes(10);
		JunoResponse junoResponse = junoReactClient.delete(key1).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		junoResponse = junoReactClient.create(key1, data).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());

		byte[] data2 = DataGenUtils.genBytes(20);
		JunoPropertiesProvider prop = new JunoPropertiesProvider(pConfig);
		junoResponse = junoReactClient.get(key1).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		AssertJUnit.assertEquals(key1, junoResponse.key());
		AssertJUnit.assertTrue(1 == junoResponse.getVersion());
		AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
		AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));

		junoResponse = junoReactClient.compareAndSet(junoResponse.getRecordContext(), data, (long)2000).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		AssertJUnit.assertTrue(2 == junoResponse.getVersion());
		AssertJUnit.assertTrue(junoResponse.getTtl() <= 2000 && junoResponse.getTtl() >= 2000-3);
		
		junoResponse = junoReactClient.get(key1, 2200).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		AssertJUnit.assertTrue(2 == junoResponse.getVersion());
		AssertJUnit.assertEquals(data, junoResponse.getValue());
		AssertJUnit.assertTrue(junoResponse.getTtl() <= 2200 && junoResponse.getTtl() >= 2200-3);

		junoResponse = junoReactClient.update(key1, data2, (long)2000).block();
		AssertJUnit.assertEquals (OperationStatus.Success,junoResponse.getStatus());	
                AssertJUnit.assertTrue(3 == junoResponse.getVersion());

		junoResponse = junoReactClient.get(key1).block();
		AssertJUnit.assertTrue(3 == junoResponse.getVersion());
                AssertJUnit.assertEquals(data2, junoResponse.getValue());
                AssertJUnit.assertTrue(junoResponse.getTtl() <= 2200 && junoResponse.getTtl() >= 2200-3);

		LOGGER.info("SUCCESS");
		LOGGER.info("Completed");
	}
	
	/**
	 * Send a cas request with zero lifetime and should generate an exception
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testCUpdateWithZeroNegLifetime() throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] key = DataGenUtils.genBytes(10);
		byte[] data = DataGenUtils.genBytes(10);
		byte[] data1 = DataGenUtils.genBytes(20);

		JunoResponse junoResponse = junoReactClient.create(key, data, (long)10).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		junoResponse = junoReactClient.get(key).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		AssertJUnit.assertEquals(key, junoResponse.key());
		AssertJUnit.assertTrue(1 == junoResponse.getVersion());
		AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
		AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));			
		JunoResponse junoResponse1 = junoReactClient.compareAndSet(junoResponse.getRecordContext(), data1, (long)0).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse1.getStatus());
		AssertJUnit.assertTrue(2 == junoResponse1.getVersion());
		AssertJUnit.assertTrue(junoResponse1.getTtl() <= 10 && junoResponse1.getTtl() > 10-3);			
		junoResponse1 = junoReactClient.get(key).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse1.getStatus());
		AssertJUnit.assertEquals(data1.length, junoResponse1.getValue().length);	
		AssertJUnit.assertTrue(junoResponse1.getTtl() <= 10 && junoResponse1.getTtl() > 10-3);

		try {
			junoResponse = junoReactClient.compareAndSet(junoResponse1.getRecordContext(), data1, (long)-1).block();
			AssertJUnit.assertTrue(false);
		} catch (JunoException mex) {
			LOGGER.debug("Exception occured: " + mex.getCause().getMessage());
			AssertJUnit.assertTrue(mex.getCause().getMessage().contains("The Document's TTL cannot be negative"));
		}

		LOGGER.info("SUCCESS");
		LOGGER.info("Completed");
	}
	
	/**
	 * Send a cas request with 3 days lifetime and should not generate an exception
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testCUpdateWith3DaysLifetime() throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	        
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] key = DataGenUtils.genBytes(10);
		byte[] data = DataGenUtils.genBytes(10);
		byte[] data1 = DataGenUtils.genBytes(20);
		long lifetime = 259200;

		JunoResponse junoResponse = junoReactClient.create(key, data, (long)10).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		junoResponse = junoReactClient.get(key).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		AssertJUnit.assertEquals(key, junoResponse.key());
		AssertJUnit.assertTrue(1 == junoResponse.getVersion());
		AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
		AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));
		JunoResponse junoResponse1 = junoReactClient.compareAndSet(junoResponse.getRecordContext(), data1, lifetime).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse1.getStatus());
		AssertJUnit.assertEquals(key, junoResponse1.key());
		AssertJUnit.assertTrue(2 == junoResponse1.getVersion());
		AssertJUnit.assertTrue(junoResponse1.getTtl() <= lifetime && junoResponse1.getTtl() > lifetime-3);
			
		junoResponse1 = junoReactClient.get(key).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse1.getStatus());
		AssertJUnit.assertEquals(data1.length, junoResponse1.getValue().length);
		AssertJUnit.assertEquals(new String(data1), new String(junoResponse1.getValue()));
		AssertJUnit.assertTrue(junoResponse1.getTtl() <= lifetime && junoResponse1.getTtl() > lifetime-3);
		LOGGER.info("SUCCESS");
		LOGGER.info("Completed");
	}
	
	/**
	 * Send a cas request with more than 3 days lifetime and should generate an 
	 * exception
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testCUpdateWithMorethan3DaysLifetime() throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] key = DataGenUtils.genBytes(10);
		byte[] data = DataGenUtils.genBytes(10);
		long lifetime = 259201;
		try{
			JunoResponse junoResponse = junoReactClient.create(key, data, (long)10).block();
			AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
			junoResponse = junoReactClient.get(key).block();
			AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
			AssertJUnit.assertEquals(key, junoResponse.key());
			AssertJUnit.assertTrue(1 == junoResponse.getVersion());
			AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
			AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));
			@SuppressWarnings("unused")
			JunoResponse junoResponse1 = junoReactClient.compareAndSet(junoResponse.getRecordContext(), data,lifetime).block();
			AssertJUnit.assertTrue ("Exception is NOT seen for when lifetime is set to > 3 days", false);
		}catch(Exception mex){
			LOGGER.debug(mex.getCause().getMessage());
			System.out.println("Exception :"+mex.getCause().getMessage());
			AssertJUnit.assertTrue(mex.getCause().getMessage().contains("Invalid lifetime. current lifetime=259201, max configured lifetime=259200"));
			LOGGER.info("Exception", mex.getMessage());
			LOGGER.info("ERROR");
			LOGGER.info("Completed");
		}
	}
	
	/**
	 * Send a cas request with null payload and should not generate an exception
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testCUpdateWithNullPayload() throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] data = DataGenUtils.genBytes(10);
		byte[] key = DataGenUtils.genBytes(64);
		try{
			JunoResponse junoResponse = junoReactClient.create(key, data).block();
			AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
			junoResponse = junoReactClient.get(key).block();
			AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
			AssertJUnit.assertEquals(key, junoResponse.key());
			AssertJUnit.assertTrue(1 == junoResponse.getVersion());
			AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
			AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));
			@SuppressWarnings("unused")
			JunoResponse junoResponse1 = junoReactClient.compareAndSet(junoResponse.getRecordContext(), null, (long)5).block();
			AssertJUnit.assertEquals(OperationStatus.Success, junoResponse1.getStatus());
			AssertJUnit.assertTrue(2 == junoResponse1.getVersion());
			AssertJUnit.assertEquals(0, junoResponse1.getValue().length);
		}catch(Exception mex){
			LOGGER.debug(mex.getCause().getMessage());
			AssertJUnit.assertFalse(true); // Expect no exception
			LOGGER.info("Exception", mex.getMessage());
			LOGGER.info("ERROR");
			LOGGER.info("Completed");
		}
	}

	/**
	 * Send a cas request with 0 payload and should not generate an exception
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testCUpdateWithZeroPayload() throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] data = DataGenUtils.genBytes(10);
		byte[] key = DataGenUtils.genBytes(64);
		try{
			JunoResponse junoResponse = junoReactClient.create(key, data).block();
			AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
			junoResponse = junoReactClient.get(key).block();
			AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
			AssertJUnit.assertEquals(key, junoResponse.key());
			AssertJUnit.assertTrue(1 == junoResponse.getVersion());
			AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
			AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));
			byte[] data1 = DataGenUtils.genBytes(0);
			JunoResponse junoResponse1 = junoReactClient.compareAndSet(junoResponse.getRecordContext(), data1, (long)5).block();
			AssertJUnit.assertEquals(OperationStatus.Success, junoResponse1.getStatus());
      AssertJUnit.assertTrue(2 == junoResponse1.getVersion());
      AssertJUnit.assertEquals(0, junoResponse1.getValue().length);
			LOGGER.info("SUCCESS");
		}catch(Exception mex){		
			LOGGER.debug(mex.getCause().getMessage());
			AssertJUnit.assertFalse(true); // Expect no exception
			LOGGER.info("Exception", mex.getMessage());
			LOGGER.info("ERROR");
		} finally {
			LOGGER.info("Completed");
		}
	}

	/**
	 * Send a cas request with 200KB payload and should not generate an exception
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testCUpdateWith200KBPayload() throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] data = DataGenUtils.genBytes(10);
		byte[] data1 = DataGenUtils.genBytes(204800);
		byte[] key = DataGenUtils.genBytes(64);

		JunoResponse junoResponse = junoReactClient.create(key, data).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		junoResponse = junoReactClient.get(key).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		AssertJUnit.assertEquals(key, junoResponse.key());
		AssertJUnit.assertTrue(1 == junoResponse.getVersion());
		AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
		AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));
		try {
		JunoResponse junoResponse1 = junoReactClient.compareAndSet(junoResponse.getRecordContext(), data1, (long)5).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse1.getStatus());
		AssertJUnit.assertEquals(key, junoResponse1.key());
		AssertJUnit.assertTrue(2 == junoResponse1.getVersion());
		} catch (JunoException mex) {
			LOGGER.debug("exception get is " + mex.getCause().getMessage());
		}

		JunoResponse junoResponse1 = junoReactClient.get(key).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse1.getStatus());
		AssertJUnit.assertTrue(2 == junoResponse1.getVersion());
		AssertJUnit.assertEquals(data1.length, junoResponse1.getValue().length);
		AssertJUnit.assertEquals(new String(data1), new String(junoResponse1.getValue()));
		LOGGER.info("SUCCESS");
		LOGGER.info("Completed");
	}
	
	/**
	 * Send a cas request with more than 200KB payload and should generate an exception
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testCUpdateWithMorethan200KBPayload() throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] data = DataGenUtils.genBytes(10);
		byte[] data1 = DataGenUtils.genBytes(204801);
		byte[] key = DataGenUtils.genBytes(64);
		try{
			JunoResponse junoResponse = junoReactClient.create(key, data).block();
			AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
			junoResponse = junoReactClient.get(key).block();
			AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
			AssertJUnit.assertEquals(key, junoResponse.key());
			AssertJUnit.assertTrue(1 == junoResponse.getVersion());
			AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
			AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));			
			JunoResponse junoResponse1 = junoReactClient.compareAndSet(junoResponse.getRecordContext(), data1, (long)5).block();
			AssertJUnit.assertTrue ("Exception is NOT seen for big payload > 200 KB", false);
			AssertJUnit.assertTrue(2 == junoResponse1.getVersion());
		}catch(Exception mex){
			LOGGER.debug(mex.getCause().getMessage());			
			AssertJUnit.assertTrue(mex.getCause().getMessage().contains("The Document Value must not be larger than 204800 bytes"));
			LOGGER.info("Exception", mex.getMessage());
			LOGGER.info("ERROR");
			LOGGER.info("Completed");
		}
	}
	
	/**
	 * Send a cas request with Lifetime and payload and should not generate an exception
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testCUpdateWithLifeTimeAndPayload() throws JunoException, Exception{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] data = DataGenUtils.genBytes(10);
		byte[] data1 = DataGenUtils.genBytes(1000);
		long lifetime =5;
		long lifetime1 = lifetime+2;
		long lifetime2 = lifetime+5;
		byte[] key = DataGenUtils.genBytes(64);

		JunoResponse junoResponse = junoReactClient.create(key, data, lifetime).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		AssertJUnit.assertEquals(key, junoResponse.key());
		AssertJUnit.assertTrue(1 == junoResponse.getVersion());

		JunoResponse junoResponse1 = junoReactClient.compareAndSet(junoResponse.getRecordContext(), data1, lifetime1).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse1.getStatus());
		Thread.sleep(6000);
		AssertJUnit.assertEquals(key, junoResponse1.key());
		AssertJUnit.assertTrue(2 == junoResponse1.getVersion());

		junoResponse1 = junoReactClient.get(key).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse1.getStatus());
		AssertJUnit.assertEquals(data1.length, junoResponse1.getValue().length);
		AssertJUnit.assertEquals(new String(data1), new String(junoResponse1.getValue()));

		Thread.sleep (2000);
		//Sleep 2 more seconds for a total of 13 seconds
		try {
			JunoResponse mResponse = junoReactClient.compareAndSet(junoResponse.getRecordContext(), data1, lifetime2).block();
			AssertJUnit.assertEquals (OperationStatus.NoKey,mResponse.getStatus());
		} catch (JunoException mex) {
			AssertJUnit.assertTrue ("Exception is seen for key", false);
			LOGGER.info("SUCCESS");
		}
		LOGGER.info("Completed");
	}
	
	//Below tests needs to be reviewed This is important.
	/*************************************************
	 * Create 2 client objects with namespaces NS & NS1.
	 * Create records with same key under both clients.
	 *  Update the record under NS only. 
	 *************************************************/
	@Test
	public void testCUpdateInOneOfTheNameSpaces() throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	        
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] data = DataGenUtils.genBytes(10);
		byte[] data1 = DataGenUtils.genBytes(11);
		byte[] key = DataGenUtils.genBytes(64);

		JunoResponse junoResponse = junoReactClient.create(key, data, (long)20).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		junoResponse = junoReactClient.get(key).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		AssertJUnit.assertEquals(key, junoResponse.key());
		AssertJUnit.assertTrue(1 == junoResponse.getVersion());
		AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
		AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));

		JunoResponse junoResponse1 = junoReactClient1.create(key, data1).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse1.getStatus());
		AssertJUnit.assertTrue(1 == junoResponse1.getVersion());
		junoResponse1 = junoReactClient1.get(key).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse1.getStatus());
		AssertJUnit.assertEquals(junoResponse1.getValue(), data1);
		AssertJUnit.assertTrue(1 == junoResponse1.getVersion());
		AssertJUnit.assertTrue(junoResponse.getValue() != junoResponse1.getValue());
		
		JunoResponse junoResponse2 = junoReactClient.compareAndSet(junoResponse.getRecordContext(), data1, (long)5).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse2.getStatus());
		AssertJUnit.assertEquals(junoResponse.key(), junoResponse2.key());
		AssertJUnit.assertTrue(2 == junoResponse2.getVersion());
		junoResponse2 = junoReactClient.get(key).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse2.getStatus());
		AssertJUnit.assertTrue(junoResponse.getValue().length < junoResponse2.getValue().length);
		AssertJUnit.assertEquals(new String(data1), new String(junoResponse2.getValue()));
		AssertJUnit.assertTrue(new String(junoResponse.getValue()) != new String(junoResponse2.getValue()));

		LOGGER.info("SUCCESS");
		LOGGER.info("Completed");
	}
	
	/*************************************************
	 * Create two clients with the same namespaces but 
	 * different application names. Create a record 
	 * using client1 with key k1.Update the record 
	 * with key k1 using client2.
	 *************************************************/
	@Test
	public void testCUpdateWithSameNameSpaceDiffAppname() throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		JunoPropertiesProvider prop = new JunoPropertiesProvider(pConfig);
		byte[] data = DataGenUtils.genBytes(10);
		byte[] data1 = DataGenUtils.genBytes(20);
		byte[] key = DataGenUtils.genBytes(64);

		JunoResponse junoResponse = junoReactClient.create(key, data).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		junoResponse = junoReactClient.get(key).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		AssertJUnit.assertEquals(key, junoResponse.key());
		AssertJUnit.assertTrue(1 == junoResponse.getVersion());
		AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
		AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));
		AssertJUnit.assertTrue(junoResponse.getTtl() <= prop.getDefaultLifetime() && 
					junoResponse.getTtl() > prop.getDefaultLifetime()-3);

		JunoResponse junoResponse1 = junoReactClient2.compareAndSet(junoResponse.getRecordContext(), data1, (long)5).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse1.getStatus());
		AssertJUnit.assertEquals(key, junoResponse1.key());
		AssertJUnit.assertTrue(2 == junoResponse1.getVersion());
			
		junoResponse1 = junoReactClient.get(key).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse1.getStatus());
		AssertJUnit.assertTrue(2 == junoResponse1.getVersion());
		AssertJUnit.assertEquals(data1.length, junoResponse1.getValue().length);
		AssertJUnit.assertEquals(new String(data1), new String(junoResponse1.getValue()));
		AssertJUnit.assertTrue(junoResponse.getTtl() <= prop.getDefaultLifetime() && 
					junoResponse.getTtl() > prop.getDefaultLifetime()-3);
			
		LOGGER.info("SUCCESS");
		LOGGER.info("Completed");
	}
	
	/*************************************************
	 * Attempting cas expired data
	 *************************************************/
	@Test
	public void testCUpdateExpiredData() throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	        
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] data = DataGenUtils.genBytes(10);
		byte[] key = DataGenUtils.genBytes(64);
		try{
			JunoResponse junoResponse = junoReactClient.create(key, data, (long)5).block();
			AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
			junoResponse = junoReactClient.get(key).block();
			AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
			AssertJUnit.assertEquals(key, junoResponse.key());
			AssertJUnit.assertTrue(1 == junoResponse.getVersion());
			AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
			AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));
			Thread.sleep(6000);
			try {
				JunoResponse mResponse = junoReactClient.compareAndSet(junoResponse.getRecordContext(), data, (long)5).block();
				AssertJUnit.assertEquals (OperationStatus.NoKey,mResponse.getStatus());
			} catch (JunoException mex) {
				AssertJUnit.assertTrue ("Exception is seen for key", false);
				LOGGER.info("SUCCESS");
			} finally {
				LOGGER.info("Completed");
			}
		}catch(IllegalArgumentException iaex){
			LOGGER.debug(iaex.getMessage());
			Assert.fail ("Unexpected exception when calling cas on an expired key");
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			Assert.fail ("Unexpected exception when calling cas on an expired key");
		}
	}

	/*************************************************
	 * Attempting cas on key from a different namespace
	 *************************************************/
	@Test
	public void testCUpdateDifferentNamespace() throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	        
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] data = DataGenUtils.genBytes(10);
		byte[] key = DataGenUtils.genBytes(64);
		byte[] key1 = DataGenUtils.genBytes(10);
		try{
			JunoResponse junoResponse = junoReactClient.create(key, data, (long)5).block();
			AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
			junoResponse = junoReactClient1.create(key1, data, (long)5).block();
			AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
			junoResponse = junoReactClient.get(key).block();
			AssertJUnit.assertEquals(key, junoResponse.key());
			AssertJUnit.assertTrue(1 == junoResponse.getVersion());
			AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
			AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));
			try {
				JunoResponse mResponse = junoReactClient1.compareAndSet(junoResponse.getRecordContext(), data, (long)10).block();
				AssertJUnit.assertEquals (OperationStatus.NoKey, mResponse.getStatus());
			} catch (JunoException mex) {
				AssertJUnit.assertTrue ("Exception is seen for key", false);
				LOGGER.info("SUCCESS");
				LOGGER.info("Completed");
			}			
		}catch(Exception mex){
			LOGGER.debug(mex.getCause().getMessage());
			AssertJUnit.assertTrue("CAS can have no key error instead of exception", false);
		} 
	}

	/*************************************************
	 * Attempting cas to get VersionToOld status
	 *************************************************/
	@Test
	public void testCUpdateVersionTooOld() throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	        
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] data = DataGenUtils.genBytes(10);
		byte[] key = DataGenUtils.genBytes(64);		
		try{
			JunoResponse junoResponse = junoReactClient.create(key, data, 10).block();
			AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());

			//check for the record
			junoResponse = junoReactClient.get(key).block();
			AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
			AssertJUnit.assertEquals(key, junoResponse.key());
			AssertJUnit.assertTrue(1 == junoResponse.getVersion());
			AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
			AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));

			String data2 = "New test data to update";
			LOGGER.debug("Use compareAndSet to update data");
			JunoResponse junoResponse1 = junoReactClient.compareAndSet(junoResponse.getRecordContext(), data2.getBytes(), new Long (15)).block();
			LOGGER.debug("Check for update data");
			junoResponse1 = junoReactClient.get(key).block();
			AssertJUnit.assertEquals(OperationStatus.Success, junoResponse1.getStatus());
			AssertJUnit.assertEquals (data2, new String (junoResponse1.getValue()));
			AssertJUnit.assertTrue(2 == junoResponse1.getVersion());
			LOGGER.debug("Use cas to update with old version");
			String data3 = "Update with modified data";

			//Use old response version, update shouldn't be successful
			try {
				JunoResponse mResponse1 = junoReactClient.compareAndSet(junoResponse.getRecordContext(), data3.getBytes(), new Long(1)).block();
				AssertJUnit.assertEquals (OperationStatus.ConditionViolation,mResponse1.getStatus());
			} catch (JunoException mex) {
				//LOGGER.debug("OperationStatus is " + mex.getOperationStatus());
				AssertJUnit.assertTrue ("Exception is seen for Condition violation :"+mex.getMessage(), false);
			}
				
			JunoResponse mResponse1 = junoReactClient.get(key).block();
			AssertJUnit.assertEquals(OperationStatus.Success, mResponse1.getStatus());
			AssertJUnit.assertEquals (2,mResponse1.getVersion());
			AssertJUnit.assertTrue (mResponse1.getTtl() <= 15 && mResponse1.getTtl() > 15-3);	
			AssertJUnit.assertEquals (data2.getBytes().length,mResponse1.getValue().length);
			AssertJUnit.assertEquals (new String(data2.getBytes()), new String(mResponse1.getValue()));
			LOGGER.info("SUCCESS");
			LOGGER.info("Completed");
		}catch(IllegalArgumentException iaex){
			AssertJUnit.assertTrue("shouldn't get expcetion ", false);			
		} 
	}

	/*************************************************
	 * Attempting cas() with version too old to update
	 *************************************************/
	@Test
	public void testCUpdateVersionTooOld2() throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	        
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		JunoPropertiesProvider prop = new JunoPropertiesProvider(pConfig1);
		byte[] data = DataGenUtils.genBytes(10);
		byte[] key = DataGenUtils.genBytes(64);		
		try{
			JunoResponse junoResponse = junoReactClient.create(key, data).block();
			AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());

			//check for the record
			junoResponse = junoReactClient.get(key).block();
			AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
			AssertJUnit.assertTrue(1 == junoResponse.getVersion());			

			String data2 = "New test data to update";
			LOGGER.debug("Use cas()to update data");
			JunoResponse junoResponse2 = junoReactClient.compareAndSet(junoResponse.getRecordContext(), data2.getBytes(), new Long (15)).block();
			AssertJUnit.assertEquals (OperationStatus.Success,junoResponse2.getStatus());
			AssertJUnit.assertTrue (2 == junoResponse2.getVersion());

			String data3 = "Other new test data to update again";
			LOGGER.debug("Use cas()to update data");
			try {
				JunoResponse junoResponse3 = junoReactClient.compareAndSet(junoResponse.getRecordContext(), data3.getBytes(), new Long (15)).block();
				AssertJUnit.assertEquals (OperationStatus.ConditionViolation, junoResponse3.getStatus());
			} catch (JunoException mex) {
				AssertJUnit.assertTrue ("Exception is seen for Condition Violation", false);
			}

			//Read again to ensure that right data is returned
			JunoResponse junoResponse4 = junoReactClient.get(key).block();
			AssertJUnit.assertEquals(OperationStatus.Success, junoResponse4.getStatus());
			AssertJUnit.assertEquals(key, junoResponse4.key());
			AssertJUnit.assertTrue(2 == junoResponse4.getVersion());			
			AssertJUnit.assertEquals(data2, new String(junoResponse4.getValue()));
			AssertJUnit.assertTrue(junoResponse4.getTtl() <= prop.getDefaultLifetime() && junoResponse4.getTtl() > prop.getDefaultLifetime()-3);
			LOGGER.info("SUCCESS");
			LOGGER.info("Completed");
		}catch(IllegalArgumentException iaex){
			LOGGER.debug(iaex.getMessage());			
		} 
	}

	/**
	 * Send a cas request with different payload and should not generate an exception
	 * @throws JunoException
	 * @throws IOException 
	 * Seeing negative version print out
	 * Version: -8
	 */
	//@Test
	public void testCUpdateVersionOverflow() throws JunoException, IOException{
		byte[] key = DataGenUtils.genBytes(64);
		byte[] data = DataGenUtils.genBytes(10);
		byte[] data1 = DataGenUtils.genBytes(11);
		JunoResponse junoResponse = junoReactClient.create(key, data, (long)20).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		long lifetime = 5L;
		//after 65536, should be overflow
		for (int i = 0; i < 65540; i++) {
			lifetime +=1;
			junoResponse = junoReactClient.get(key, lifetime).block();
			AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
			JunoResponse junoResponse1 = junoReactClient.compareAndSet(junoResponse.getRecordContext(), data1, (long)5).block();
			AssertJUnit.assertEquals(OperationStatus.Success, junoResponse1.getStatus());
			//org.junit.Assert.assertEquals(key, junoResponse1.key());
			//org.junit.Assert.assertEquals("NS", junoResponse1.getNamespace());
			LOGGER.debug("Version: " + junoResponse1.getVersion());
			AssertJUnit.assertEquals(new String(data1), new String(junoResponse1.getValue()));
		}
	}
}
