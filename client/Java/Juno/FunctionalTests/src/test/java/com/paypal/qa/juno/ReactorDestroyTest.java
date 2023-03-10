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

public class ReactorDestroyTest{
	private JunoReactClient junoReactClient;
	private JunoReactClient junoReactClient1;
	private JunoReactClient junoReactClient2;
	private Properties pConfig;
	private Properties pConfig1;
	private Properties pConfig2;
	private Logger LOGGER;
	int syncFlag;

	@BeforeClass
	public void setup() throws JunoException, IOException, InterruptedException {
		URL url = DestroyTest.class.getResource("/com/paypal/juno/Juno.properties");
		LOGGER = LoggerFactory.getLogger(DestroyTest.class);
		pConfig = new Properties();
		pConfig.load(url.openStream());
		pConfig.setProperty(JunoProperties.APP_NAME, "QATestApp");
		pConfig.setProperty(JunoProperties.RECORD_NAMESPACE, "NS1");

		URL url1 = DestroyTest.class.getResource("/com/paypal/juno/Juno.properties");
		pConfig1 = new Properties();
		pConfig1.load(url1.openStream());
		pConfig1.setProperty(JunoProperties.APP_NAME, "QATestApp");
		pConfig1.setProperty(JunoProperties.RECORD_NAMESPACE, "NS2");
		
		URL url2 = DestroyTest.class.getResource("/com/paypal/juno/Juno.properties");
		pConfig2 = new Properties();
		pConfig2.load(url2.openStream());
		pConfig2.setProperty(JunoProperties.APP_NAME, "QATestApp2");
		pConfig2.setProperty(JunoProperties.RECORD_NAMESPACE, "NS1");

		try{
			junoReactClient = JunoClientFactory.newJunoReactClient(new JunoPropertiesProvider(pConfig), SSLUtil.getSSLContext());
			junoReactClient1 = JunoClientFactory.newJunoReactClient(new JunoPropertiesProvider(pConfig1), SSLUtil.getSSLContext());
			junoReactClient2 = JunoClientFactory.newJunoReactClient(new JunoPropertiesProvider(pConfig2), SSLUtil.getSSLContext());
		}catch (Exception e) {
			throw new RuntimeException(e);
		}

		Thread.sleep(3000);
		
	}

	/**
	 * Send a delete request with key to juno 2.0 server and should not generate
	 * an exception
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testDestroyWithKey() throws JunoException, IOException{		
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
		JunoResponse deleteResponse = junoReactClient.delete(key).block();		
		AssertJUnit.assertEquals (OperationStatus.Success, deleteResponse.getStatus());
		AssertJUnit.assertTrue ( deleteResponse.getStatus().getCode() == OperationStatus.Success.getCode());
		AssertJUnit.assertEquals(key, deleteResponse.key());
		LOGGER.debug("data: " + deleteResponse.getValue());
		AssertJUnit.assertEquals(new String(deleteResponse.getValue()), "");
		
		junoResponse = junoReactClient.create(key, data).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		junoResponse = junoReactClient.update(key, data).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		junoResponse = junoReactClient.set(key, data).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		junoResponse = junoReactClient.compareAndSet(junoResponse.getRecordContext(), data, (long)5).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		AssertJUnit.assertEquals(4, junoResponse.getVersion());
		deleteResponse = junoReactClient.delete(key).block();
		AssertJUnit.assertEquals(OperationStatus.Success, deleteResponse.getStatus());
		AssertJUnit.assertEquals(key, deleteResponse.key());

		try {
			JunoResponse result1 = junoReactClient.get(key).block();
			AssertJUnit.assertEquals (OperationStatus.NoKey, result1.getStatus());
		} catch (JunoException mex) {
			AssertJUnit.assertTrue ("Exception is seen for no key", false);
			LOGGER.info("0");
		} finally {
			LOGGER.info("Completed");
		}		
	}
	
	/**
	 * Send a delete request for existing key and insert a record with same key
	 * after delete and should not generate an exception
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testDestroyAndCreateWithSameKey() throws JunoException, IOException{
		JunoPropertiesProvider prop = new JunoPropertiesProvider(pConfig);
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] key = "juno_test1".getBytes();
		byte[] data = "testing data".getBytes();

		JunoResponse junoResponse = junoReactClient.delete(key).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		junoResponse = junoReactClient.create(key, data).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		junoResponse = junoReactClient.get(key).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		AssertJUnit.assertEquals(key, junoResponse.key());
		AssertJUnit.assertTrue(1 == junoResponse.getVersion());
		AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
		AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));
		JunoResponse deleteResponse = junoReactClient.delete(key).block();
		AssertJUnit.assertEquals(OperationStatus.Success, deleteResponse.getStatus());
		AssertJUnit.assertEquals(key, deleteResponse.key());
		AssertJUnit.assertEquals(new String(deleteResponse.getValue()), "");
		AssertJUnit.assertTrue ( deleteResponse.getStatus().getCode() == OperationStatus.Success.getCode());
		
		try {
			JunoResponse result1 = junoReactClient.get(key).block();
			AssertJUnit.assertEquals (OperationStatus.NoKey, result1.getStatus());
		} catch (JunoException mex) {
			AssertJUnit.assertTrue ("Exception is seen for key", false);
		}
		junoResponse = junoReactClient.create(key, data).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		JunoResponse junoResponse1 = junoReactClient.get(key).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse1.getStatus());
		AssertJUnit.assertEquals(key, junoResponse1.key());
		AssertJUnit.assertTrue(1 == junoResponse1.getVersion());
		AssertJUnit.assertEquals(data.length, junoResponse1.getValue().length);
		AssertJUnit.assertTrue(junoResponse.getTtl() <= prop.getDefaultLifetime() && junoResponse.getTtl() > prop.getDefaultLifetime()-3);
		AssertJUnit.assertEquals(new String(data), new String(junoResponse1.getValue()));
		LOGGER.info("0");
		LOGGER.info("Completed");
	}
	
	/**
	 * Send delete request with empty key and should generate and exception
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testDestroyWithEmptyKey() {
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	        
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] key = "".getBytes();
		try{
			junoReactClient.delete(key).block();
			AssertJUnit.assertTrue ("***Error: null key", false);
		}catch(Exception mex){		
			AssertJUnit.assertTrue(mex.getCause().getMessage().contains("The Document key must not be null or empty"));
			LOGGER.info("Exception", mex.getMessage());
			LOGGER.info("2");			
			LOGGER.info("Completed");
		}
	}
	/**
	 * Send a delete request with null key and should generate an exception
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testDestroyWithNullKey() throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	        
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		try{
			junoReactClient.delete(null).block();
			AssertJUnit.assertTrue ("***Error: null key not throwing exception", false);
		}catch(Exception mex){	
			AssertJUnit.assertTrue(mex.getCause().getMessage().contains("The Document key must not be null or empty"));
			LOGGER.info("Exception", mex.getMessage());
			LOGGER.info("2");			
			LOGGER.info("Completed");
		}
	}
	/**
	 * Send a delete request after five seconds if creation record lifetime 10 seconds
	 * and should not generate an exception
	 * @throws JunoException
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Test
	public void testDestroyAfterFiveSeconds() throws JunoException, IOException, InterruptedException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] key = DataGenUtils.genBytes(64);
		byte[] data = DataGenUtils.genBytes(10);
		long lifetime = 10;
		JunoResponse junoResponse = junoReactClient.create(key, data, (long)lifetime).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		junoResponse = junoReactClient.get(key).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		AssertJUnit.assertEquals(key, junoResponse.key());
		AssertJUnit.assertTrue(1 == junoResponse.getVersion());
		AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
		AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));
		Thread.sleep(5000);
		JunoResponse deleteResponse = junoReactClient.delete(key).block();
		AssertJUnit.assertEquals(OperationStatus.Success, deleteResponse.getStatus());
		AssertJUnit.assertEquals(key, deleteResponse.key());
		AssertJUnit.assertEquals(new String(deleteResponse.getValue()), "");		

		try {
			JunoResponse junoResponse1 = junoReactClient.get(key).block();
			AssertJUnit.assertEquals (OperationStatus.NoKey, junoResponse1.getStatus());
		} catch (JunoException mex) {
			AssertJUnit.assertTrue ("Exception is seen for no key", false);
			LOGGER.info("0");
		} finally {
			LOGGER.info("Completed");
		}		
	}
	
	/**
	 * Send a delete request after twelve seconds if creation record lifetime is ten seconds
	 * and should generate an exception
	 * @throws JunoException
	 * @throws IOException
	 * @throws InterruptedException
	 */
	//This test can be used to test default juno.idleConnectionsTimeoutMillis = 9000 ( 9 seconds)
	@Test
	public void testDestroyAfterTwelveSeconds() throws JunoException, IOException, InterruptedException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] key = DataGenUtils.genBytes(64);
		byte[] key2 = DataGenUtils.genBytes(64);
		byte[] data = DataGenUtils.genBytes(10);
		long lifetime = 5;
		JunoResponse junoResponse = junoReactClient.create(key, data, (long)lifetime).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		junoResponse = junoReactClient.get(key).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		AssertJUnit.assertEquals(key, junoResponse.key());
		AssertJUnit.assertTrue(1 == junoResponse.getVersion());
		AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
		AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));
		Thread.sleep(6000);
		
		JunoResponse deleteResponse = junoReactClient.delete(key).block();
		AssertJUnit.assertEquals(OperationStatus.Success, deleteResponse.getStatus());
		junoResponse = junoReactClient.create(key2, data, (long)lifetime).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		junoResponse = junoReactClient.create(key, data, (long)lifetime).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		LOGGER.info("0");
		LOGGER.info("Completed");
	}
	
	/**
	 * Send a request with 128 bytes key and should not generate an exception
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testDestroyWith128BytesKey() throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] key = DataGenUtils.genBytes(128);
		byte[] data = DataGenUtils.genBytes(10);
		long lifetime = 10;
		JunoResponse junoResponse = junoReactClient.create(key, data, (long)lifetime).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		JunoResponse deleteResponse = junoReactClient.delete(key).block();
		AssertJUnit.assertEquals(OperationStatus.Success, deleteResponse.getStatus());
		AssertJUnit.assertEquals(key, deleteResponse.key());
		LOGGER.debug("data: " + deleteResponse.getValue());
		AssertJUnit.assertEquals(new String(deleteResponse.getValue()), "");
			
		try {
			JunoResponse junoResponse1 = junoReactClient.get(key).block();
			AssertJUnit.assertEquals(OperationStatus.NoKey, junoResponse1.getStatus());
		} catch (JunoException mex) {
			AssertJUnit.assertTrue ("Exception is seen for key", false);
			LOGGER.info("0");
		} finally {
			LOGGER.info("Completed");
		}
	}
	
	/**
	 * Send a delete request with 129 bytes key and should generate an exception
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testDestroyWith129BytesKey() throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] key = DataGenUtils.genBytes(129);
		try{
			junoReactClient.delete(key).block();
			AssertJUnit.assertTrue ("Exception should happen for long key > 128bytes ", false);
		}catch(Exception mex){
			LOGGER.debug(mex.getCause().getMessage());
			AssertJUnit.assertTrue(mex.getCause().getMessage().contains("The Document key must not be larger than 128 bytes"));
			LOGGER.info("Exception", mex.getMessage());
			LOGGER.info("2");			
			LOGGER.info("Completed");
		}
	}
	/**
	 * Send a delete request with no key exists on server and should generate an exception
	 * @throws JunoException
	 * @throws IOException
	 */
	@Test
	public void testDestroyNoKeyExists() throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] key = DataGenUtils.genBytes(64);
		String key1 = new String("haha, nohaoma");
		String key2 = new String("Q:������������������������������������A:���");
		
		JunoResponse deleteResponse = junoReactClient.delete(key).block();
		AssertJUnit.assertEquals(OperationStatus.Success, deleteResponse.getStatus());
		JunoResponse deleteResponse1 = junoReactClient.delete(key1.getBytes()).block();
		AssertJUnit.assertEquals(OperationStatus.Success, deleteResponse1.getStatus());
		System.out.println("Key size:"+key2.getBytes().length);
		JunoResponse deleteResponse2 = junoReactClient.delete(key2.getBytes()).block();
		AssertJUnit.assertEquals (OperationStatus.Success, deleteResponse2.getStatus());
		AssertJUnit.assertEquals(key, deleteResponse.getKey());
		AssertJUnit.assertEquals(key1.getBytes(), deleteResponse1.getKey());
		AssertJUnit.assertEquals(key2.getBytes(), deleteResponse2.getKey());
		AssertJUnit.assertEquals(new String(deleteResponse.getValue()), "");
		AssertJUnit.assertEquals(new String(deleteResponse1.getValue()), "");
		AssertJUnit.assertEquals(new String(deleteResponse2.getValue()), "");
		LOGGER.info("0");
		LOGGER.info("Completed");
	}
	
	/***********************************************
	 * Create two records under different namespaces 
	 * but with same key. Destroy one of the records.
	 * Verify that the other record still exists.
	 ************************************************/
	@Test
	public void testDestroyWithDiffNamespacesWithSameKey() throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	        
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] key = "juno_test3".getBytes();
		byte[] data = DataGenUtils.genBytes(10);
		JunoResponse junoResponse = junoReactClient.delete(key).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		junoResponse = junoReactClient1.delete(key).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		junoResponse = junoReactClient.create(key, data).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		junoResponse = junoReactClient1.create(key, data).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());

		JunoResponse deleteResponse = junoReactClient.delete(key).block();
		AssertJUnit.assertEquals(OperationStatus.Success, deleteResponse.getStatus());
		AssertJUnit.assertEquals(key, deleteResponse.key());
		LOGGER.debug("data: " + deleteResponse.getValue());
		AssertJUnit.assertEquals(new String(deleteResponse.getValue()), "");		
		AssertJUnit.assertTrue ( deleteResponse.getStatus().getCode() == OperationStatus.Success.getCode());

		LOGGER.debug("Verify the other namspace still has the key");
		junoResponse = junoReactClient1.get(key).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		AssertJUnit.assertEquals(key, junoResponse.key());
		AssertJUnit.assertTrue(1 == junoResponse.getVersion());
		AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
		AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));

		LOGGER.info("0");
		LOGGER.info("Completed");
	}
	
	/*****************************************
	 * Create one records, delete the record
	 * and get the same key with same app name 
	 *****************************************/
	@Test
	public void testDestroyWithDiffAppnameWithSameKey() throws JunoException, IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		byte[] key = "juno_test_sameappname".getBytes();
		byte[] data = DataGenUtils.genBytes(10);
		JunoResponse junoResponse = junoReactClient.delete(key).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		junoResponse = junoReactClient2.delete(key).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		junoResponse = junoReactClient.create(key, data).block();
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		try {
	        Thread.sleep(2000);	
	        JunoResponse junoResponse2 = junoReactClient2.create(key, data).block();
	        AssertJUnit.assertEquals(OperationStatus.UniqueKeyViolation,junoResponse2.getStatus());
		} catch (JunoException mex) {
			AssertJUnit.assertTrue ("Exception is seen for key :"+mex.getMessage(), false);
		} catch (Exception e) {
			LOGGER.debug(e.getMessage());
		}
		
		JunoResponse deleteResponse = junoReactClient.delete(key).block();
		AssertJUnit.assertEquals(OperationStatus.Success, deleteResponse.getStatus());
		AssertJUnit.assertEquals(key, deleteResponse.key());
		LOGGER.debug("data: " + deleteResponse.getValue());
		AssertJUnit.assertEquals(new String(deleteResponse.getValue()), "");

		LOGGER.debug("Verify the other appname does not have key");
		try {
			junoResponse = junoReactClient2.get(key).block();
			AssertJUnit.assertEquals(OperationStatus.NoKey, junoResponse.getStatus());
		} catch (JunoException mex) {
			AssertJUnit.assertTrue ("Exception is seen for key", false);
			LOGGER.info("0");
		} finally {
			LOGGER.info("Completed");
		}
	}
}
