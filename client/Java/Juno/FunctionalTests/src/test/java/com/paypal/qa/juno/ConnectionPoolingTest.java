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
import java.util.Date;
import java.util.Properties;
import java.util.UUID;
import org.apache.commons.lang.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class ConnectionPoolingTest{
	private JunoClient junoClient1;
	private JunoClient junoClient2;
	private JunoClient junoClient3;
	private JunoClient junoClient4;
	private JunoClient junoClient5;
	private JunoClient junoClient6;
	private JunoClient junoClient7;
	private Logger LOGGER;
	private Properties pConfig;

	@BeforeClass
	public void setup() throws JunoException, IOException, InterruptedException, CertificateException, KeyStoreException, NoSuchAlgorithmException, KeyManagementException {
		URL url = ConnectionPoolingTest.class.getResource("/com/paypal/juno/Juno.properties");
		LOGGER = LoggerFactory.getLogger(ConnectionPoolingTest.class);
		junoClient1 = JunoClientFactory.newJunoClient(url);
		junoClient2 = JunoClientFactory.newJunoClient(url);
		junoClient3 = JunoClientFactory.newJunoClient(url);
		junoClient4 = JunoClientFactory.newJunoClient(url);
		
		pConfig = new Properties();
		pConfig.load(url.openStream());
		pConfig.setProperty(JunoProperties.PORT, "8080");
		pConfig.setProperty(JunoProperties.USE_SSL, "false");
		junoClient5 = JunoClientFactory.newJunoClient(new JunoPropertiesProvider(pConfig), SSLUtil.getSSLContext());

		pConfig.setProperty(JunoProperties.PORT, "5080");
		pConfig.setProperty(JunoProperties.USE_SSL, "true");
		pConfig.setProperty(JunoProperties.BYPASS_LTM, "true");
		pConfig.setProperty(JunoProperties.CONNECTION_LIFETIME, "5000");
		try{
			junoClient6 = JunoClientFactory.newJunoClient(new JunoPropertiesProvider(pConfig), SSLUtil.getSSLContext());
		}catch(Exception e){
			System.out.println("Exception in constructor :"+e.getMessage());
		}
		
		pConfig.setProperty(JunoProperties.PORT, "8080");
		pConfig.setProperty(JunoProperties.USE_SSL, "false");
		pConfig.setProperty(JunoProperties.RESPONSE_TIMEOUT,"1");
		pConfig.setProperty(JunoProperties.ENABLE_RETRY, "true");
		junoClient7 = JunoClientFactory.newJunoClient(new JunoPropertiesProvider(pConfig), SSLUtil.getSSLContext());

	

		Thread.sleep(3000);
	}
	

	
	@Test(enabled = false)
	private void testLoadCreates(int records, int loops ) throws JunoException,  Exception {

		long totalCreateTime = 0L;
		String[] key = new String[loops];
		StopWatch clock = new StopWatch();
		String data[] = new String[100];
		for (int iter = 0; iter < loops; ++iter) {
			key[iter] = DataGenUtils.createKey(loops+20);
			data[iter] = "testing datadasfkkkkkksd'''''''8!@#$*((())-/?:;677777777fggggggggggggggggh~`" + iter;		

			byte[] payload = data[iter].getBytes();
			clock.start();
			junoClient1.create(key[iter].getBytes(), payload, 3600L);
			clock.stop();
			totalCreateTime += clock.getTime();
			clock.reset();
		}
		LOGGER.debug("Time per insert: " + totalCreateTime / loops + " ms/insert");
		LOGGER.debug("Total time for " + loops + " loops creating "
				+ records + "KB of data: " + totalCreateTime / 1000 + " seconds");

		for (int iter = 0; iter < loops; ++iter){
			JunoResponse response = junoClient1.get(key[iter].getBytes());
			AssertJUnit.assertEquals( key[iter], new String(response.key()));
			LOGGER.debug(new String (response.getValue()));
			AssertJUnit.assertEquals( data[iter], new String (response.getValue()));

			junoClient1.delete(key[iter].getBytes());
		}
	}
	
	@Test(enabled = false)
	private void testLoadGets(int records, int loops ) throws JunoException,  IOException {

		String[] key = new String[loops];
		for (int iter = 0; iter < loops; ++iter){
			key[iter] = DataGenUtils.createKey(1024*iter+1);
			byte[] payload = DataGenUtils.genBytes(1024 * records);
			junoClient2.create(key[iter].getBytes(), payload);
		}
		JunoResponse[] junoRecords = new JunoResponse[loops];
		long totalGetTime = 0L;
		StopWatch clock = new StopWatch();
		for (int iter = 0; iter < loops; ++iter) {
			clock.start();
			junoRecords[iter] = junoClient2.get(key[iter].getBytes());
			clock.stop();
			totalGetTime += clock.getTime();
			clock.reset();
		}
		LOGGER.debug("Time per Get: " + totalGetTime / loops + " ms/get");
		LOGGER.debug("Total time for " + loops + " loops getting "
				+ records + "KB of data: " + totalGetTime / 1000 + " seconds");
		//for(int iter = 0; iter < loops; ++iter){
		//	LOGGER.debug("Record " + iter +":" + junoRecords[iter]);
		//}
		for (int iter = 0; iter < loops; ++iter){
			junoClient2.delete(key[iter].getBytes());
		}
	}
	
	@Test(enabled = false)
	private void testLoadUpdates(int records, int loops ) throws JunoException,  IOException {

		byte[] data = DataGenUtils.genBytes(10);
		String key[] = new String[loops]; 
		for (int iter = 0; iter < loops; ++iter){
			key[iter] = DataGenUtils.createKey(iter+1);
			junoClient1.create(key[iter].getBytes(), data);
		}

		long totalUpdateTime = 0L;
		StopWatch clock = new StopWatch();
		for (int iter = 0; iter < loops; ++iter) {
			byte[] payload = DataGenUtils.genBytes(1024 * records);
			clock.start();
			junoClient1.update(key[iter].getBytes(), payload);
			clock.stop();
			totalUpdateTime += clock.getTime();
			clock.reset();
		}
		LOGGER.debug("Time per update: " + totalUpdateTime / loops + " ms/update");
		LOGGER.debug("Total time for " + loops + " loops updating "
				+ records + "KB of data: " + totalUpdateTime / 1000 + " seconds");
		for (int iter = 0; iter < loops; ++iter){
			junoClient1.delete(key[iter].getBytes());
		}
	}

	@Test(enabled = false)
	private void testLoadCAS (int records, int loops ) throws JunoException,  IOException {

		byte[] data = DataGenUtils.genBytes(10);
		String key[] = new String[loops]; 
		JunoResponse mResponse[] = new JunoResponse[loops];
		for (int iter = 0; iter < loops; ++iter){
			key[iter] = DataGenUtils.createKey(iter+1);
			mResponse[iter] = junoClient1.create(key[iter].getBytes(), data);
		}

		LOGGER.debug("\nKey creation is done for all");
		long totalUpdateTime = 0L;
		long lifetime = 120;
		StopWatch clock = new StopWatch();
		for (int iter = 0; iter < loops; ++iter) {
			byte[] payload = DataGenUtils.genBytes(1024 * records);
			clock.start();
			junoClient1.compareAndSet(mResponse[iter].getRecordContext(), payload, lifetime);
			clock.stop();
			totalUpdateTime += clock.getTime();
			clock.reset();
		}
		LOGGER.debug("Time per cas: " + totalUpdateTime / loops + " ms/update");
		LOGGER.debug("Total time for " + loops + " loops updating "
				+ records + "KB of data: " + totalUpdateTime / 1000 + " seconds");
		for (int iter = 0; iter < loops; ++iter){
			junoClient1.delete(key[iter].getBytes());
		}
	}

	@Test(enabled = false)
	private void testLoadDestroys(int records, int loops ) throws JunoException,  IOException {

		byte[] payload = DataGenUtils.genBytes(1024 * records);
		String[] key = new String[loops];
		for (int iter = 0; iter < loops; ++iter){
			key[iter] = DataGenUtils.createKey(iter+1);
			junoClient1.create(key[iter].getBytes(), payload);
		}
		long totalDestroyTime = 0L;
		StopWatch clock = new StopWatch();
		for (int iter = 0; iter < loops; ++iter) {	
			clock.start();
			junoClient1.delete(key[iter].getBytes());
			clock.stop();
			totalDestroyTime += clock.getTime();
			clock.reset();
		}
		LOGGER.debug("Time per delete: " + totalDestroyTime / loops + " ms/delete");
		LOGGER.debug("Total time for " + loops + " loops deleteing "
				+ records + "KB of data: " + totalDestroyTime / 1000 + " seconds");
	}
	
	@Test
	private void testLoadAll() throws JunoException,  IOException {

		long totalCreateTime = 0L;
		int loops = 100;
		int records = 10;
		byte[][] key = new byte[loops][];
		for (int iter = 0; iter < loops; ++iter){
			key[iter] = DataGenUtils.genBytes(20);
		}
		StopWatch clock = new StopWatch();
		for (int iter = 0; iter < loops; ++iter) {
			clock.start();
			byte[] payload = DataGenUtils.genBytes(1024 * records);
			byte[] payload1 = DataGenUtils.genBytes(1024 * records);
			junoClient5.create(key[iter], payload);
			junoClient5.update(key[iter], payload1);
			junoClient5.get(key[iter]);
			junoClient5.delete(key[iter]);
			junoClient6.delete(key[iter]);
			clock.stop();
			totalCreateTime += clock.getTime();
			clock.reset();
		}
		LOGGER.debug("Time per all(insert+update+get+delete): " + totalCreateTime / loops + " ms/all");
		LOGGER.debug("Total time for " + loops + " loops for all "
				+ records + "KB of data: " + totalCreateTime / 1000 + " seconds");
	}
	
	@Test
	private void testResponseTimeout() throws JunoException,  IOException {

		byte[] key = DataGenUtils.genBytes(20);
		byte[] payload = DataGenUtils.genBytes(1024);
		try{
			junoClient7.create(key, payload);
		}catch(Exception e){
			AssertJUnit.assertTrue(e.getMessage().contains("Response Timed out"));
		}
	}
	
	@Test(enabled = false)
	public void testCreateGetKey(int loops) throws Exception {
		String key, key2, key3;

		for (int i = 0; i < loops; i++) {
			key = UUID.randomUUID().toString();
			key2 = UUID.randomUUID().toString();
			key3 = UUID.randomUUID().toString();

			String data = "data to store in juno on "
					+ new Date(System.currentTimeMillis()).toString();
			byte[] bytes = data.getBytes();

			JunoResponse response = junoClient1.create(key.getBytes(), bytes);
			assert(response.getStatus() == OperationStatus.Success);
			// Check for the record
			response = junoClient1.get(key.getBytes());
			assert(response.getStatus() == OperationStatus.Success);
			String dataOut = new String(response.getValue());
			LOGGER.debug("Data: " + dataOut);
			assert (data.equals(dataOut));

			String data2 = "testing data to store in juno on "
					+ new Date(System.currentTimeMillis()).toString();
			byte[] bytes2 = data2.getBytes();
			JunoResponse response2 = junoClient2.create(key2.getBytes(), bytes2);
			assert(response2.getStatus() == OperationStatus.Success);
			response2 = junoClient2.get(key2.getBytes());
			assert(response2.getStatus() == OperationStatus.Success);
			String dataOut2 = new String(response2.getValue());
			LOGGER.debug("Data: " + dataOut2);
			assert (data2.equals(dataOut2));

			String data3 = "testing juno on "
					+ new Date(System.currentTimeMillis()).toString();
			byte[] bytes3 = data3.getBytes();
			JunoResponse response3 = junoClient3.create(key3.getBytes(), bytes3);
			assert(response3.getStatus() == OperationStatus.Success);
			response3 = junoClient3.get(key3.getBytes());
			assert(response3.getStatus() == OperationStatus.Success);
			String dataOut3 = new String(response3.getValue());
			LOGGER.debug("Data: " + dataOut3);
			assert (data3.equals(dataOut3));
		}

		LOGGER.info("SUCCESS");
	}
	
	//juno.use.persistent.connection = true
	//juno.connection.reCycleDuration = 500
	//Verify log has:
	//"Recycling connection as connection lifetime expired"
	//Should have "The pool size is:2" as we use 2 threads.
	@Test(threadPoolSize = 2, invocationCount = 2)	
	//Passed on 6/29
	public void testRecyleAfter1second1() throws JunoException,  Exception {
		testLoadCreates(5, 1);
		Thread.sleep (1000);
		testLoadCreates(5, 1);
	}
	
	//juno.use.persistent.connection = true
	//juno.connection.reCycleDuration = 500
	//Verify log has:
	//"Recycling connection as connection lifetime expired"
	//Should always see "The pool size is:1" as we use 1 threads.
	@Test(threadPoolSize = 1, invocationCount = 2)	
	//Passed on 6/29
	public void testCreateWith1thread() throws JunoException,  Exception {
		testLoadCreates(5, 10);
	}
	
	//juno.use.persistent.connection = true
	//juno.connection.reCycleDuration = 500
	//Verify log that the recycle happens only before the 3rd request
	@Test(threadPoolSize = 1, invocationCount = 1)	
	//Passed on 6/29
	public void testRecycleAfter9second() throws JunoException,  Exception {
		LOGGER.debug("run testLoadGet");
		testLoadGets(5, 1);
		LOGGER.debug("sleep 8 secs");
		Thread.sleep (9000);
		LOGGER.debug("run testLoadUpdates");
		testLoadUpdates(5, 1);
		LOGGER.debug("sleep 1 secs");
		Thread.sleep (1000);
		LOGGER.debug("run testLoadCAS");
		testLoadCAS(5, 1);
	}	//juno.use.persistent.connection = true
	//Do operations on multiple pools with insert and get operation in
	//Parallel. Should not see any error. Verify that the log shows
	//3 different connections insertd for each pool and the pool should
	//have total 6 connections after the fist operation is done. 
	@Test(threadPoolSize = 2, invocationCount = 2)
	public void testMultiPoolMultithread() throws JunoException,  Exception {
		testCreateGetKey(1);
		Thread.sleep (100);
		testCreateGetKey(1);
	}
	
	//juno.use.persistent.connection = true
	//Do operations on multiple pools with insert and get operation in
	//Parallel. Should not see any error. Verify that the log shows
	//3 different connections insertd for each pool and the pool should
	//have total of 3 connections after the fist operation is done. 
	@Test(threadPoolSize = 1, invocationCount = 2)
	public void testMultiPoolSinglethread() throws JunoException,  Exception {
		testCreateGetKey(1);
		Thread.sleep (100);
		testCreateGetKey(1);
	}
	
	@Test
	public void testReusingConnection() throws JunoException,  Exception {
		
		String key = UUID.randomUUID().toString();
		String data = "data to store in juno on "
				+ new Date(System.currentTimeMillis()).toString();
		byte[] bytes = data.getBytes();

		//Create a key using junoClient3 object on stage2t4663.
		JunoResponse response = junoClient3.create(key.getBytes(), bytes);
		assert(response.getStatus() == OperationStatus.Success);
		
		//Read the Key using  junoClient4 object which is pointing to a unknown stage as primary.
		//But the port will be the same so this get operation has to use the same connection which was insertd
		//for the above insert operation and get the key successfully.
		response = junoClient4.get(key.getBytes());
		assert(response.getStatus() == OperationStatus.Success);
		String dataOut = new String(response.getValue());
		assert (data.equals(dataOut));
	}
	
	@Test
	public void testColoFailover() throws JunoException,  Exception {
		
		String key = UUID.randomUUID().toString();
		String data = "data to store in juno on "
				+ new Date(System.currentTimeMillis()).toString();
		byte[] bytes = data.getBytes();

		//Create a key using junoClient4 object. The connection to primary server will fail.
		//But the secondary server will succeed.
		JunoResponse response = junoClient4.create(key.getBytes(), bytes);
		assert(response.getStatus() == OperationStatus.Success);
		
		//Read the same key using junoClient3 object. The operation has to be successful.
		//There should be any new connection insertd.It has to use the existing connection.
		response = junoClient3.get(key.getBytes());
		assert(response.getStatus() == OperationStatus.Success);
		String dataOut = new String(response.getValue());
		assert (data.equals(dataOut));
		
	}

}
