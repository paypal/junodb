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
package com.paypal.qa.juno.usf;

import com.paypal.juno.client.JunoClient;
import com.paypal.juno.client.JunoReactClient;
import com.paypal.juno.client.io.JunoResponse;
import com.paypal.juno.client.io.OperationStatus;
import com.paypal.juno.exception.JunoException;
import com.paypal.qa.juno.DataGenUtils;
import java.util.Date;
import java.util.UUID;
import javax.inject.Inject;
import javax.inject.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

/**
 * Test multiple JunoClient implementations being instantiated
 * into the Spring Bean Context.  The config xml references a Commons
 * Configuration implementation that references a properties
 * file with more than 1 set of Juno client configuration
 * properties.
 */

@ContextConfiguration("classpath:spring-config.xml")
@Component
public class JunoUSFFactoryMultiClientTest extends AbstractTestNGSpringContextTests {

	@Inject
	@Named("junoClient1")
	JunoClient junoClient1;

	@Inject
	@Named("junoClient2")
	JunoClient junoClient2;

	@Inject
	@Named("junoClient3")
	JunoClient junoClient3;

	@Inject
	@Named("junoClnt4")
	JunoClient junoClient4;

	@Inject
	@Named("junoRClient1")
	JunoReactClient junoRClient1;

	@Inject
	@Named("junoRClient3")
	JunoReactClient junoRClient3;

	@Inject
	JunoClient junoClient;

	private static final Logger LOGGER = LoggerFactory.getLogger(JunoUSFFactoryMultiClientTest.class);

	@Test
	public void testJunoClientNotNull() {
		AssertJUnit.assertNotNull(junoClient1);
		AssertJUnit.assertNotNull(junoClient2);
		AssertJUnit.assertNotNull(junoClient3);
		AssertJUnit.assertNotNull(junoClient4);
	}

	//This test has this setting in  juno_usf_multiple_configuration.properties
	//juno.connection.reCycleDuration = 1000
	//PASS in 6/29 - only 1 connection is seen
	@Test
	public void testCreateKey() throws Exception {
		//System.out.println( "\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		byte[] key;

		for (int i = 0; i < 100; i++) {
			key = UUID.randomUUID().toString().getBytes();

			String data = "data to store in juno on " + new Date(System.currentTimeMillis()).toString();
			byte[] bytes = data.getBytes();

			JunoResponse response = junoClient1.create(key, bytes);
			AssertJUnit.assertTrue (response.getStatus().getCode() == OperationStatus.Success.getCode());
			// Check for the record
			response = junoClient2.get(key);
			AssertJUnit.assertTrue (response.getStatus().getCode() == OperationStatus.Success.getCode());
			String dataOut = new String(response.getValue());
			//System.out.println ("Data: " + dataOut);
			assert (data.equals(dataOut));
			//Thread.sleep(1500);

			JunoResponse response2 = junoClient2.get(key);
			String dataOut2 = new String(response2.getValue());
			//System.out.println ("Data: " + dataOut2);
			assert (data.equals(dataOut2));

			//Check log to make sure that recyle happening here
			JunoResponse response3 = junoClient2.get(key);
			String dataOut3 = new String(response3.getValue());
			//System.out.println ("Data: " + dataOut3);
			assert (data.equals(dataOut3));
		}
		LOGGER.info("SUCCESS");
	}
	
	/**
	 * This test has this setting in  juno_usf_multiple_configuration.properties
	 * juno.connection.reCycleDuration = 1000
	 *
	 */
	@Test
	//Due to recycle, make sure 3 connections are created
	public void testCreateReadUpdateMultClients() throws Exception {
		//System.out.println( "\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		byte[] key = UUID.randomUUID().toString().getBytes();

		String data = "data to store in juno on " + new Date(System.currentTimeMillis()).toString();
		byte[] bytes = data.getBytes();
		Long lifetime = 10L;
		JunoResponse response = junoClient1.create(key, bytes,lifetime);

		// Check for the record from client2
		JunoResponse junoResponse = junoClient2.get(key);	
		AssertJUnit.assertEquals(key, junoResponse.key());
		AssertJUnit.assertTrue(1 == junoResponse.getVersion());		
		AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));

		String data2 = "New data to store " + new Date(System.currentTimeMillis()).toString();
		byte[] bytes2 = data2.getBytes();
		lifetime = 12L;
		junoResponse = junoClient2.update(key, bytes2, lifetime);

		junoResponse = junoClient1.get(key);
		AssertJUnit.assertEquals(key, junoResponse.key());
		AssertJUnit.assertTrue(2 == junoResponse.getVersion());		
		AssertJUnit.assertEquals(new String(data2), new String(junoResponse.getValue()));

		Thread.sleep (15000);
		String data3 = "Beautiful weather " + new Date(System.currentTimeMillis()).toString();
		byte[] bytes3 = data3.getBytes();
		try {
			JunoResponse mResponse = junoClient1.update(key, bytes3);
			AssertJUnit.assertEquals (OperationStatus.NoKey,mResponse.getStatus());
		} catch (JunoException mex) {
			AssertJUnit.assertTrue ("Exception is seen for no key", false);
		}
		
		LOGGER.info("SUCCESS");
	}

	/**
	 * Client2 creates a key 
	 * Client3 which has different namespace reads the key, should get NoKey.
	 * Before the key expires, Client1 invokes CAS to update the key with new data and version
	 * Verify CAS is successfull
	 * Client2 delete the key
	 * Client1 read the key, verify NoKey status is returned	
	 * @throws Exception
	 */
	@Test
	public void testCreateCASDestroyMultiClients() throws Exception {
		//System.out.println( "\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		LOGGER.info("CorrID : ","id12345");

		byte[] key = DataGenUtils.createKey(64).getBytes();

		String data = "Test data - happy testing " + new Date(System.currentTimeMillis()).toString();
		byte[] bytes = data.getBytes();

		JunoResponse response = junoClient2.create(key, bytes);   
		AssertJUnit.assertEquals (OperationStatus.Success,response.getStatus());
		try {
			JunoResponse mResponse = junoClient3.get(key);
			AssertJUnit.assertEquals (OperationStatus.NoKey,mResponse.getStatus());
		} catch (JunoException mex) {
			AssertJUnit.assertTrue ("Exception is seen for no key", false);	
		}

		Thread.sleep(8000);
		String data1 = "Test data - happy Friday " + new Date(System.currentTimeMillis()).toString();
		byte[] bytes1 = data1.getBytes();
		Long lifetime = 15000L;
		JunoResponse junoResponse1 = junoClient2.compareAndSet(response.getRecordContext(), bytes1, lifetime);

		AssertJUnit.assertEquals(key, junoResponse1.key());
		AssertJUnit.assertTrue(2 == junoResponse1.getVersion());
		junoResponse1 = junoClient2.get(key);
		AssertJUnit.assertEquals(new String(data1), new String(junoResponse1.getValue()));

		JunoResponse mResponse = junoClient2.delete (key);
		AssertJUnit.assertEquals(key, mResponse.key());
		AssertJUnit.assertTrue(new String(mResponse.getValue()).equals(""));

		try {
			mResponse = junoClient1.get(key);
			AssertJUnit.assertEquals (OperationStatus.NoKey,mResponse.getStatus());
		} catch (JunoException mex) {
			AssertJUnit.assertTrue ("Exception is seen for no key", false);	
		}
		
		LOGGER.info("SUCCESS");
	}

        @Test
        public void testCreateCASDestroyMultiCAndRlients() throws Exception {
                //System.out.println( "\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
                LOGGER.info("CorrID : ","id12345");

                byte[] key = DataGenUtils.createKey(64).getBytes();

                String data = "Test data - happy testing " + new Date(System.currentTimeMillis()).toString();
                byte[] bytes = data.getBytes();

                JunoResponse response = junoRClient1.create(key, bytes).block();
                AssertJUnit.assertEquals (OperationStatus.Success,response.getStatus());
		JunoResponse mResponse = junoClient1.get(key);
		AssertJUnit.assertEquals (OperationStatus.Success,mResponse.getStatus());

                try {
                        mResponse = junoRClient3.get(key).block();
                        AssertJUnit.assertEquals (OperationStatus.NoKey,mResponse.getStatus());
                } catch (JunoException mex) {
                        AssertJUnit.assertTrue ("Exception is seen for no key", false);
                }

                Thread.sleep(8000);
                String data1 = "Test data - happy Friday " + new Date(System.currentTimeMillis()).toString();
                byte[] bytes1 = data1.getBytes();
                long lifetime = 15000;
                JunoResponse junoResponse1 = junoRClient1.compareAndSet(response.getRecordContext(), bytes1, lifetime).block();

                AssertJUnit.assertEquals(key, junoResponse1.key());
                AssertJUnit.assertTrue(2 == junoResponse1.getVersion());
                junoResponse1 = junoClient2.get(key);
                AssertJUnit.assertEquals(new String(data1), new String(junoResponse1.getValue()));

                mResponse = junoClient1.delete (key);
                AssertJUnit.assertEquals(key, mResponse.key());
                AssertJUnit.assertTrue(new String(mResponse.getValue()).equals(""));

                try {
                        mResponse = junoClient2.get(key);
                        AssertJUnit.assertEquals (OperationStatus.NoKey,mResponse.getStatus());
                } catch (JunoException mex) {
                        AssertJUnit.assertTrue ("Exception is seen for no key", false);
                }

                LOGGER.info("SUCCESS");
        }

	@Test
	public void testGetKey() {	
		//System.out.println( "\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());

		byte[] key = "mytesting1234".getBytes();

		String data = "Test data - happy Friday " + new Date(System.currentTimeMillis()).toString();
		byte[] bytes = data.getBytes();
		junoClient4.delete(key);
		JunoResponse response = junoClient4.create(key, bytes);
		response = junoClient4.get(key);
		AssertJUnit.assertEquals (OperationStatus.Success,response.getStatus());
		response = junoClient.delete(key);
		AssertJUnit.assertEquals (OperationStatus.Success,response.getStatus());       
	}

//	@Test
//	public void testGetProperties(){
//
//		AssertJUnit.assertEquals("{juno.server.port=5080, juno.connection.pool_size=3, juno.operation.retry=false, juno.default_record_lifetime_sec=259200, juno.record_namespace=NS1, prefix=, juno.server.host=10.183.38.11, juno.response.timeout_msec=1000, juno.connection.timeout_msec=500, juno.usePayloadCompression=false, juno.connection.byPassLTM=true, juno.application_name=mayflyng}",junoClient.getProperties().toString());
//		AssertJUnit.assertEquals("{juno.server.port=5080, juno.connection.pool_size=3, juno.operation.retry=false, juno.default_record_lifetime_sec=259200, juno.record_namespace=NS1, prefix=junoClient1, juno.server.host=10.183.38.11, juno.response.timeout_msec=1000, juno.connection.timeout_msec=500, juno.usePayloadCompression=false, juno.connection.byPassLTM=true, juno.application_name=mayflyng}",junoClient1.getProperties().toString());
//		AssertJUnit.assertEquals("{juno.server.port=8080, juno.connection.pool_size=1, juno.operation.retry=false, juno.default_record_lifetime_sec=259200, juno.record_namespace=NS1, prefix=junoClient2, juno.server.host=10.183.38.11, juno.response.timeout_msec=1000, juno.connection.timeout_msec=500, juno.usePayloadCompression=false, juno.connection.byPassLTM=true, juno.application_name=mayflyng}",junoClient2.getProperties().toString());
//		AssertJUnit.assertEquals("{juno.server.port=5080, juno.connection.pool_size=1, juno.operation.retry=false, juno.default_record_lifetime_sec=259200, juno.record_namespace=JunoNS1, prefix=junoClient3, juno.server.host=10.183.38.11, juno.response.timeout_msec=1000, juno.connection.timeout_msec=500, juno.usePayloadCompression=false, juno.connection.byPassLTM=true, juno.application_name=mayflyng}",junoClient3.getProperties().toString());
//		AssertJUnit.assertEquals("{juno.server.port=5080, juno.connection.pool_size=1, juno.operation.retry=false, juno.default_record_lifetime_sec=259200, juno.record_namespace=NS1, prefix=junoClnt4, juno.server.host=10.183.38.11, juno.response.timeout_msec=1000, juno.connection.timeout_msec=500, juno.usePayloadCompression=false, juno.connection.byPassLTM=true, juno.application_name=mayflyng}",junoClient4.getProperties().toString());
//
//	}

}