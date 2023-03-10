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

import com.paypal.juno.client.JunoAsyncClient;
import com.paypal.juno.client.JunoClient;
import com.paypal.juno.client.JunoClientFactory;
import com.paypal.juno.client.io.JunoRequest;
import com.paypal.juno.client.io.JunoResponse;
import com.paypal.juno.client.io.OperationStatus;
import com.paypal.juno.conf.JunoProperties;
import com.paypal.juno.conf.JunoPropertiesProvider;
import com.paypal.juno.exception.JunoException;
import com.paypal.juno.util.SSLUtil;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class BatchGetTest{
	private JunoAsyncClient junoClient;
	private JunoAsyncClient asyncJunoClient;
	private JunoClient junoClient1;
	private JunoClient junoClient2;
	private Properties pConfig;
	private Properties pConfig1;
	private Properties pConfig2;	
	private Logger LOGGER;
	private int syncFlag;

	@BeforeClass
	public void setup() throws  IOException, InterruptedException {
		LOGGER = LoggerFactory.getLogger(BatchGetTest.class);
		
		URL url = BatchGetTest.class.getResource("/com/paypal/juno/Juno.properties");
		pConfig = new Properties();
		pConfig.load(url.openStream());
		pConfig.setProperty(JunoProperties.APP_NAME, "QATestApp");
		pConfig.setProperty(JunoProperties.RECORD_NAMESPACE, "junotoken");
		LOGGER.debug("Read syncFlag test to findout what needs to be run");
		String sync_flag = pConfig.getProperty("sync_flag_test", "0");
		LOGGER.debug("*********SYNC FLAG: " + sync_flag);
		syncFlag = Integer.parseInt(sync_flag.trim());

		URL url1 = BatchGetTest.class.getResource("/com/paypal/juno/Juno_batch.properties");
		pConfig1 = new Properties();
		pConfig1.load(url1.openStream());
		pConfig1.setProperty(JunoProperties.APP_NAME, "QATestApp");
		pConfig1.setProperty(JunoProperties.RECORD_NAMESPACE, "NS2");
		URL url2 = BatchGetTest.class.getResource("/com/paypal/juno/Juno_batch.properties");
		pConfig2 = new Properties();
		pConfig2.load(url2.openStream());
		pConfig2.setProperty(JunoProperties.APP_NAME, "QATestApp2");
		pConfig2.setProperty(JunoProperties.RECORD_NAMESPACE, "junotoken");

		try {
			junoClient = JunoClientFactory.newJunoAsyncClient(new JunoPropertiesProvider(pConfig), SSLUtil.getSSLContext());
			asyncJunoClient = JunoClientFactory.newJunoAsyncClient(new JunoPropertiesProvider(pConfig), SSLUtil.getSSLContext());
			junoClient1 = JunoClientFactory.newJunoClient(new JunoPropertiesProvider(pConfig1), SSLUtil.getSSLContext());
			junoClient2 = JunoClientFactory.newJunoClient(new JunoPropertiesProvider(pConfig2), SSLUtil.getSSLContext());
		} catch (Exception e){
			throw new RuntimeException(e);
		}

		Thread.sleep(3000);
		
	}

	@AfterClass
	public  void cleanSetup() throws Exception{
		
	}
	
	/**
	 * Create and get multiple keys 
	 * @throws JunoException
	 */
	@Test
	public void testBatchGet() throws JunoException, InterruptedException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	    
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		int numKeys = 10;
		byte[][] key = new byte[numKeys][];
		long[] ttl = new long[numKeys];
		byte[][] payload = new byte[numKeys][];
		HashMap<String, byte[]> hmap = new HashMap<String, byte[]>();
		LOGGER.debug("Create " + numKeys + " keys using batch Create");
		List<JunoRequest> list = new ArrayList<>();
		for (int i = 0; i < numKeys; i ++) {
			key[i] = DataGenUtils.createKey(10).getBytes();
			key[numKeys-1] = DataGenUtils.createKey(20).getBytes();
			LOGGER.debug("key" + i + " is " + new String(key[i]));
			payload[i] = DataGenUtils.createKey(100).getBytes();;
			payload[9] = DataGenUtils.createKey(204800).getBytes();
			ttl[i] = 200;
			ttl[6] = 259200;
			hmap.put(new String(key[i]), payload[i]);
			long createTime = System.currentTimeMillis();
			JunoRequest item = new JunoRequest(key[i], payload[i], (long)0, ttl[i],createTime, JunoRequest.OperationType.Create);
			list.add(item);
		}
		try {
			Iterable<JunoResponse> batchResp = junoClient.doBatch(list).toBlocking().toIterable();
			int i = 0;
			for (JunoResponse mResponse: batchResp) {	
				LOGGER.debug("Key: " + i + ": "+ new String(mResponse.getKey()));
				AssertJUnit.assertEquals (OperationStatus.Success, mResponse.getStatus());
				AssertJUnit.assertTrue(1 == mResponse.getVersion());
				i++;
			}
		} catch (JunoException mex) {			
			AssertJUnit.assertTrue ("Exception is thrown for batch Create", false);	
		}
		List<JunoRequest> list1 = new ArrayList<>();
		for (int i = 0; i < numKeys; i ++) {
			JunoRequest item;
			LOGGER.debug("key " + i + " at get is " + new String(key[i]));
			item = new JunoRequest(key[i], null, 0, 0, JunoRequest.OperationType.Get);
			list1.add(item);
		}
		LOGGER.debug("Read " + numKeys + " keys using batch Get()");
		Iterable<JunoResponse> getBatchResp = null;
		if (syncFlag == 1) {
			getBatchResp = junoClient.doBatch(list1).toBlocking().toIterable();
		} else {
			getBatchResp = BatchTestSubscriber.async_dobatch(asyncJunoClient, list1);
		}
		for (JunoResponse response: getBatchResp) {		
			String mkey = new String(response.getKey());
			AssertJUnit.assertEquals(OperationStatus.Success, response.getStatus());
			AssertJUnit.assertEquals(new String(hmap.get(mkey)), new String(response.getValue()));
		}
		LOGGER.info("0");
		LOGGER.info("Completed");
	}

	/**
	 * Create and get multiple keys with two keys expired
	 * @throws JunoException
	 */
	@Test
	public void testBatchGetExpiredRecord() throws JunoException, InterruptedException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	    
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		int numKeys = 10;
		byte[][] key = new byte[numKeys][];
		long[] ttl = new long[numKeys];
		byte[][] payload = new byte[numKeys][];
		HashMap<String, byte[]> hmap = new HashMap<String, byte[]>();
		LOGGER.debug("Create " + numKeys + " keys using batch Create");
		List<JunoRequest> list = new ArrayList<>();
		for (int i = 0; i < numKeys; i ++) {
			key[i] = DataGenUtils.createKey(10).getBytes();
			String str = "Hello Testing " + i;
			payload[i] = str.getBytes();
			ttl[i] = 20000;
			ttl[3] = 3;
			ttl[8] = 4;
			hmap.put(new String(key[i]), payload[i]);
			long createTime = System.currentTimeMillis();
			JunoRequest item = new JunoRequest(key[i], payload[i], (long)0, ttl[i],createTime, JunoRequest.OperationType.Create);
			list.add(item);
		}
		try {
			Iterable<JunoResponse> batchResp;
			if (syncFlag == 1) {
				batchResp = junoClient.doBatch(list).toBlocking().toIterable();
			} else {
				batchResp = BatchTestSubscriber.async_dobatch(asyncJunoClient, list);
			}
			int i = 1;
			for (JunoResponse mResponse: batchResp) {	
				LOGGER.debug("Key: " + i + ": "+ new String(mResponse.getKey()));
				AssertJUnit.assertEquals (OperationStatus.Success, mResponse.getStatus());
				AssertJUnit.assertTrue(1 == mResponse.getVersion());
				i++;
			}
		} catch (JunoException mex) {			
			LOGGER.debug("Exception occurs: " + mex.getMessage());
			AssertJUnit.assertTrue ("Exception is thrown for batch Create", false);	
		}
		LOGGER.debug("Sleep for key to expire");
		Thread.sleep (5000);
		List<JunoRequest> list1 = new ArrayList<>();
		for (int i = 0; i < numKeys; i ++) {
			JunoRequest item = new JunoRequest(key[i], null, 0, 0, JunoRequest.OperationType.Get);
			list1.add(item);
		}
		LOGGER.debug("Read " + numKeys + " keys using batch Get(), no lifetime");
		Iterable<JunoResponse> getBatchResp = null;
		if (syncFlag == 1) {
			getBatchResp = junoClient.doBatch(list1).toBlocking().toIterable();
		} else {
			getBatchResp = BatchTestSubscriber.async_dobatch(asyncJunoClient, list1);
		}
		for (JunoResponse response: getBatchResp) {		
			String mkey = new String(response.getKey());
			if (mkey.equals (new String(key[3])) || mkey.equals(new String(key[8]))) {
				LOGGER.debug("Check for key to expire");
				if (response.getStatus() != OperationStatus.NoKey ) {
					AssertJUnit.assertTrue ("Key not expired", false);
				} else {
					LOGGER.debug("Key expired as expected");
				}
			} else {
				AssertJUnit.assertEquals(OperationStatus.Success, response.getStatus());
				AssertJUnit.assertEquals(new String(hmap.get(mkey)), new String(response.getValue()));	
			}
		}
		LOGGER.info("0");
		LOGGER.info("Completed");
	}

	/**
	 * Get batch keys with lifetime extended
	 * Verify appropriate JunoException is thrown
	 * @throws JunoException
	 */
	@Test
	public void testGetWithLifeTimeExtented() throws JunoException, InterruptedException {
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	    
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		int numKeys = 10;
		byte[][] key = new byte[numKeys][];
		long[] ttl = new long[numKeys];
		byte[][] payload = new byte[numKeys][];
		HashMap<String, String> hmapTTL = new HashMap<String, String>();
		HashMap<String, byte[]> hmap = new HashMap<String, byte[]>();
		LOGGER.debug("Create " + numKeys + " keys using batch Create");

		List<JunoRequest> list = new ArrayList<>();
		for (int i = 0; i < numKeys; i ++) {
			key[i] = DataGenUtils.createKey(10).getBytes();
			String str = "Hello Testing, Happy Friday" + i;
			payload[i] = str.getBytes();		
			ttl[i] = 4;
			hmap.put(new String(key[i]), payload[i]);
			hmapTTL.put(new String(key[i]), String.valueOf(ttl[i]));
			JunoRequest item = new JunoRequest(key[i], payload[i], (long)0, ttl[i], JunoRequest.OperationType.Create);
			list.add(item);
		}
		try {
			Iterable<JunoResponse> batchResp = null;
			if (syncFlag == 1) {
				batchResp = junoClient.doBatch(list).toBlocking().toIterable();
			} else {
				batchResp = BatchTestSubscriber.async_dobatch(asyncJunoClient, list);
			}
			int j = 0;
			for (JunoResponse mResponse: batchResp) {				
				LOGGER.debug("Key: " + j + ": "+ new String(mResponse.getKey()));
				AssertJUnit.assertEquals (OperationStatus.Success,mResponse.getStatus());
				AssertJUnit.assertTrue(1 == mResponse.getVersion());
				AssertJUnit.assertTrue(ttl[j] == mResponse.getTtl());
				j++;
			}

		} catch (JunoException mex) {
			LOGGER.debug("Exception occurs: " + mex.getMessage());
		}
		List<JunoRequest> list1 = new ArrayList<>();
		LOGGER.debug("Set and passing lifetime, some will have lifetime 0 passing");
		for (int i = 0; i < numKeys; i ++) {
			ttl[i] = 0;
			ttl[2] = 30;
			ttl[9] = 20;
			hmapTTL.put(new String(key[i]), String.valueOf(ttl[i]));
			LOGGER.debug("Key: " + key[i]);
			JunoRequest item1 = new JunoRequest(key[i], null, 0, ttl[i], JunoRequest.OperationType.Get);
			list1.add(item1);
		}

		LOGGER.debug("Read " + numKeys + " keys using batch Get()");
		Iterable<JunoResponse> getBatchResp = null;
		if (syncFlag == 1) {
			getBatchResp = junoClient.doBatch(list1).toBlocking().toIterable();
		} else {
			getBatchResp = BatchTestSubscriber.async_dobatch(asyncJunoClient, list1);
		}	
		for (JunoResponse response: getBatchResp) {		
			String mkey = new String(response.getKey());
			AssertJUnit.assertTrue(1 == response.getVersion());
			AssertJUnit.assertEquals (OperationStatus.Success, response.getStatus());
			AssertJUnit.assertEquals(new String(hmap.get(mkey)), new String(response.getValue()));			
		}

		Thread.sleep (5000); //Most keys will expired
		LOGGER.debug("Verify keys have lifetime=0 passing are expired");
		Iterable<JunoResponse> getBatchResp1 = junoClient.doBatch(list1).toBlocking().toIterable();	
		for (JunoResponse response: getBatchResp1) {		
			String mkey = new String(response.getKey());
			if (!mkey.equals (new String(key[2])) && !mkey.equals(new String(key[9]))) {
				if (response.getStatus() != OperationStatus.NoKey ) {
					AssertJUnit.assertTrue ("Key not expired", false);
				} else {
					LOGGER.debug("Key expired as expected");
				}
			} else {
				AssertJUnit.assertEquals(OperationStatus.Success, response.getStatus());
				AssertJUnit.assertEquals(new String(hmap.get(mkey)), new String(response.getValue()));
				AssertJUnit.assertTrue(1 == response.getVersion());
				AssertJUnit.assertEquals(hmapTTL.get(mkey), String.valueOf(response.getTtl()));
			}
		}
		LOGGER.info("0");
		LOGGER.info("Completed");
	}
	/**
	 * Verify appropriate JunoException is thrown when getting a key with zero length
	 * @throws JunoException
	 */
	@Test
	public void testBatchGetEmptyKey() throws JunoException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	        
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		int numKeys = 10;
		byte[][] key = new byte[numKeys][];
		long[] ttl = new long[numKeys];
		byte[][] payload = new byte[numKeys][];
		Random r = new Random();

		LOGGER.debug("Create request item with 1 key having zero length");
		List<JunoRequest> list = new ArrayList<>();
                List<JunoRequest> getList = new ArrayList<>();
		
		for (int i = 0; i < numKeys; i ++) {
			key[i] = DataGenUtils.createKey(DataGenUtils.rand(r, 1, 128)).getBytes();
			key[9] = "".getBytes();		
			ttl[i]=10;
			payload[i] = DataGenUtils.genBytes(DataGenUtils.rand(r, 1, 4048));
                        JunoRequest item = new JunoRequest(key[i], payload[i], (long)0, ttl[i], JunoRequest.OperationType.Create);
			JunoRequest getItem = new JunoRequest(key[i], payload[i], (long)0, ttl[i], JunoRequest.OperationType.Get);
                        list.add(item);
                        getList.add(getItem);
		}
                try{
                        Iterable<JunoResponse> batchResp;
                        if (syncFlag == 1) {
                                batchResp=junoClient.doBatch(list).toBlocking().toIterable();
                        } else {
                                batchResp=BatchTestSubscriber.async_dobatch(asyncJunoClient, list);
                        }       
                        for (JunoResponse response: batchResp) {
                            if (response != null && response.getKey() != null && response.getKey() != key[9]) {
                                AssertJUnit.assertEquals (OperationStatus.Success, response.getStatus());
                            } else {
                                AssertJUnit.assertEquals (OperationStatus.IllegalArgument, response.getStatus());
                            }
                        }
                } catch (IllegalArgumentException mex) {
                        LOGGER.debug("Exception occurs: " + mex.getMessage());
                        AssertJUnit.assertTrue(mex.getMessage().contains("key must not be null"));
                        LOGGER.info("Exception", mex.getMessage());
                        LOGGER.info("2");                   
                        LOGGER.info("Completed");
                }       
                try {
                        Iterable<JunoResponse> getResp;
                        LOGGER.debug("\n===Batch Get is sent ");
                        if (syncFlag == 1) {
                                getResp=junoClient.doBatch(getList).toBlocking().toIterable();
                        } else {
                                getResp=BatchTestSubscriber.async_dobatch(asyncJunoClient, getList);
                        }
                        for (JunoResponse response: getResp) {
                            if (response != null && response.getKey() != null && response.getKey() != key[9]) {
                                AssertJUnit.assertEquals (OperationStatus.Success, response.getStatus());
                                AssertJUnit.assertTrue(1 == response.getVersion());
                            } else {
                                AssertJUnit.assertEquals (OperationStatus.IllegalArgument, response.getStatus());
                            }
                        }
                } catch (IllegalArgumentException mex) {
                        LOGGER.debug("Exception occurs: " + mex.getMessage());
                        AssertJUnit.assertTrue(mex.getMessage().contains("Key must not be null"));
                        LOGGER.info("Exception", mex.getMessage());
                        LOGGER.info("2");
                        LOGGER.info("Completed");
                }
	}

	/**
	 * Verify appropriate JunoException is thrown when keys not exist
	 * @throws JunoException //TODO: check So we will still have key info in response even we didn't insert at all?
	 */
	@Test
	public void testBatchGetNotExist() throws JunoException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	    
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		int numKeys = 4;
		long[] ttl = new long[numKeys];
		byte[][] key = new byte[numKeys][];
		byte[][] payload = new byte[numKeys][];
		HashMap<String, String> hmapTTL = new HashMap<String, String>();
		HashMap<String, OperationStatus> hmapStatus = new HashMap<String, OperationStatus>();
		HashMap<String, byte[]> hmap = new HashMap<String, byte[]>();		
		LOGGER.debug("Create 2 keys using Existing Juno Sync Client");
		LOGGER.debug("Two keys are created with NS1, two key created with different Namespace");
		key[0] = "56789".getBytes();    //key not exist in Juno
		key[1] = DataGenUtils.createKey(20).getBytes();
		key[2] = "23456".getBytes();  //key not exist in Juno
		key[3] = DataGenUtils.createKey(10).getBytes();
		byte[] data = DataGenUtils.genBytes(10);
		byte[] data1 = DataGenUtils.genBytes(10);		
		payload[1] = data;
		payload[3] = data1;
		payload[0] = data1;
		payload[2] = data1;
		for (int i = 0; i < numKeys; i ++) {
			ttl[i]=(long)20;
			LOGGER.debug("initial key " + i + "put is " + new String(key[i]));
		}
		junoClient1.delete("23456".getBytes());
		junoClient1.delete("56789".getBytes());
		try {
			Thread.sleep(2000);
		} catch (Exception ex) {
			LOGGER.debug(ex.getMessage());
		}
		
		try {
			junoClient2.create(key[1], payload[1], ttl[1]);
			junoClient2.create(key[3], payload[3], ttl[3]);
			junoClient1.create(key[0], payload[0]);
			junoClient1.create(key[2], payload[2]);
		} catch (JunoException ex) {
			AssertJUnit.assertEquals("shouldn't come here ", false);			
		}

		LOGGER.debug("Create Juno request items ");
		List<JunoRequest> list = new ArrayList<>();
		for (int i = 0; i < numKeys; i ++) {
			hmapStatus.put(new String(key[i]), OperationStatus.Success);
			hmapStatus.put(new String(key[0]), OperationStatus.NoKey);
			hmapStatus.put(new String(key[2]), OperationStatus.NoKey);			
			hmap.put(new String(key[i]), payload[i]);
			hmapTTL.put(new String(key[i]), String.valueOf(ttl[i]));
			LOGGER.info("hmapstatus key " + i + "is " + new String(key[i]));
			JunoRequest item = new JunoRequest(key[i], null, 0, 0, JunoRequest.OperationType.Get);
			list.add(item);
		}

		Iterable<JunoResponse> getBatchResp1 = null;
		if (syncFlag == 1) {
			getBatchResp1 = junoClient.doBatch(list).toBlocking().toIterable();
		} else {
			getBatchResp1 = BatchTestSubscriber.async_dobatch(asyncJunoClient, list);
		}
		for (JunoResponse response: getBatchResp1) {		
			String mkey = new String(response.getKey());
			LOGGER.info("Key is??: " + mkey + "response status " + response.getStatus() + "hmap get key " + hmapStatus.get(mkey));
			AssertJUnit.assertEquals (hmapStatus.get(mkey), response.getStatus());	
			if (response.getStatus() == OperationStatus.Success) {
				AssertJUnit.assertEquals(hmap.get(mkey), response.getValue());	
				AssertJUnit.assertTrue(Integer.parseInt(hmapTTL.get(mkey)) - 10 <= response.getTtl() && response.getTtl() <= Integer.parseInt(hmapTTL.get(mkey)));
				AssertJUnit.assertTrue(1 == response.getVersion());
			}
		}
		LOGGER.info("0");
		LOGGER.info("Completed");
	}

	/**
	 * Verify appropriate JunoException is thrown when batch get to read key=null
	 * @throws JunoException
	 */
	@Test
	public void testBatchGetNullKey() throws JunoException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	        
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		int numKeys = 5;
		byte[][] key = new byte[numKeys][];
                long[] ttl = new long[numKeys];
		byte[][] payload = new byte[numKeys][];

		LOGGER.debug("Create request item with key = null");
		List<JunoRequest> list = new ArrayList<>();
                List<JunoRequest> getList = new ArrayList<>();
		for (int i = 0; i < numKeys; i ++) {
			key[i] = DataGenUtils.createKey(10).getBytes();
			key[0] = null;
                        String str = "Hello testBatchGetNullKey " + i;
                        payload[i] = str.getBytes();
			ttl[i] = 20;
			JunoRequest item = new JunoRequest(key[i], payload[i], (long)0, ttl[i], JunoRequest.OperationType.Create);
			JunoRequest getItem = new JunoRequest(key[i], payload[i], (long)0, ttl[i], JunoRequest.OperationType.Get);
                        list.add(item);
			getList.add(getItem);
		}
		try{
                        Iterable<JunoResponse> batchResp;
			if (syncFlag == 1) {
				batchResp=junoClient.doBatch(list).toBlocking().toIterable();
			} else {
				batchResp=BatchTestSubscriber.async_dobatch(asyncJunoClient, list);
			}	
                        for (JunoResponse response: batchResp) {
                            if (response != null && response.getKey() != null) {
                                AssertJUnit.assertEquals (OperationStatus.Success, response.getStatus());
                            } else {
                                AssertJUnit.assertEquals (OperationStatus.IllegalArgument, response.getStatus());
                            }
                        }
		} catch (IllegalArgumentException mex) {
			LOGGER.debug("Exception occurs: " + mex.getMessage());
			AssertJUnit.assertTrue(mex.getMessage().contains("key must not be null"));
			LOGGER.info("Exception", mex.getMessage());
			LOGGER.info("2");			
			LOGGER.info("Completed");
		}	
                try {
                        Iterable<JunoResponse> getResp;
                        LOGGER.debug("\n===Batch Get is sent ");
                        if (syncFlag == 1) {
                                getResp=junoClient.doBatch(getList).toBlocking().toIterable();
                        } else {
                                getResp=BatchTestSubscriber.async_dobatch(asyncJunoClient, getList);
                        }
                        for (JunoResponse response: getResp) {
                            if (response != null && response.getKey() != null) {
                                AssertJUnit.assertEquals (OperationStatus.Success, response.getStatus());
                                AssertJUnit.assertTrue(1 == response.getVersion());
                            } else {
                                AssertJUnit.assertEquals (OperationStatus.IllegalArgument, response.getStatus());
                            }
                        }
                } catch (IllegalArgumentException mex) {
                        LOGGER.debug("Exception occurs: " + mex.getMessage());
                        AssertJUnit.assertTrue(mex.getMessage().contains("Key must not be null"));
                        LOGGER.info("Exception", mex.getMessage());
                        LOGGER.info("2");
                        LOGGER.info("Completed");
                }
	}

	/**
	 * Get batch keys to pass TTL > 3 days
	 * Verify appropriate JunoException is thrown
	 * @throws JunoException
	 */
	@Test
	public void testBatchGetLifetimeLongerThan3Days() throws JunoException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	    
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		int numKeys = 5;
		byte[][] key = new byte[numKeys][];
		byte[][] payload = new byte[numKeys][];
		long[] ttl = new long[numKeys];
		Random r = new Random();

		LOGGER.debug("Create request item with key = null");
		List<JunoRequest> list = new ArrayList<>();
		for (int i = 0; i < numKeys; i ++) {
			key[i] = DataGenUtils.createKey(DataGenUtils.rand(r, 1, 128)).getBytes();
			payload[i] = DataGenUtils.genBytes(DataGenUtils.rand(r, 1, 4048));
			ttl[i] = 100;
			ttl[4] = 259201;
			JunoRequest item = new JunoRequest(key[i], payload[i], (long)0, (long)ttl[i], JunoRequest.OperationType.Get);
			list.add(item);
		}

		Iterable <JunoResponse> gResp = new ArrayList<>();
		try {
			LOGGER.debug("Read " + numKeys + " keys using batch Get()");
			if (syncFlag == 1) {
				gResp = junoClient.doBatch(list).toBlocking().toIterable();
			} else {
				gResp = BatchTestSubscriber.async_dobatch(asyncJunoClient, list);
			}

		} catch (JunoException mex) {
			AssertJUnit.assertTrue ("Exception should not thrown for one item with NULL key", false);
		}	
		
		int i=0;
		for (JunoResponse response: gResp) {	
			String mkey = new String(response.getKey());
			if (mkey.equals(new String(key[4]))) {
				AssertJUnit.assertEquals (OperationStatus.IllegalArgument, response.getStatus());
			} else {
				AssertJUnit.assertEquals (OperationStatus.NoKey, response.getStatus());
				i++;
			}
		}
		AssertJUnit.assertEquals(i, numKeys-1);	
		LOGGER.info("0");
		LOGGER.info("Completed");
	
	}

	/**
	 * Get batch keys with keys >=256 bytes
	 * Verify appropriate JunoException is thrown
	 * @throws JunoException
	 */
	@Test
	public void testBatchGetKey257Bytes() throws JunoException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	    
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		int numKeys = 10;
		byte[][] key = new byte[numKeys][];
		byte[][] payload = new byte[numKeys][];
		Random r = new Random();

		LOGGER.debug("Create request item with key = null");
		key[9] = DataGenUtils.createKey(258).getBytes();
		key[4] = DataGenUtils.createKey(129).getBytes();
		List<JunoRequest> list = new ArrayList<>();
		for (int i = 0; i < numKeys; i ++) {
			if ( i != 4 && i != 9 ) {
				key[i] = DataGenUtils.createKey(DataGenUtils.rand(r, 1, 128)).getBytes();
			}
			payload[i] = DataGenUtils.genBytes(DataGenUtils.rand(r, 1, 4048));
			JunoRequest item = new JunoRequest(key[i], null, (long)0, (long)0, JunoRequest.OperationType.Get);
			list.add(item);
		}

		Iterable <JunoResponse> gResp = new ArrayList<>();
		try {
			LOGGER.debug("Read " + numKeys + " keys using batch Get()");
			if (syncFlag == 1) {
				gResp = junoClient.doBatch(list).toBlocking().toIterable();
			} else {
				gResp = BatchTestSubscriber.async_dobatch(asyncJunoClient, list);
			}
		} catch (JunoException mex) {
			AssertJUnit.assertTrue ("Exception should not thrown for only one item key with long length", false);
		}
		
		int i=0;
		for (JunoResponse response: gResp) {	
			String mkey = new String(response.getKey());
			if ( mkey.equals(new String(key[9])) || mkey.equals(new String(key[4]))) {
				AssertJUnit.assertEquals (OperationStatus.IllegalArgument, response.getStatus());
			} else {
				AssertJUnit.assertEquals (OperationStatus.NoKey, response.getStatus());
				i++;
			}
		}
		AssertJUnit.assertEquals(i, numKeys-2);	
		LOGGER.info("0");
		LOGGER.info("Completed");
	}

	/**
	 * Batch keys with no Item in the JunoRequest list
	 * Verify appropriate JunoException is thrown 
	 * @throws JunoException  //TODO: ask no exception for empty list? test case has real failure for now
	 */
	//@Test
	public void testBatchGetZeroItem() throws JunoException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	        
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		LOGGER.debug("Send 0 item to Batch create");
		List<JunoRequest> list = new ArrayList<>();
		LOGGER.debug("\n===Batch Create is sent ");
		try {
			junoClient.doBatch(list).toBlocking().toIterable();
			AssertJUnit.assertTrue ("Exception is not thrown for no key in Juno Request", false);
		} catch (IllegalArgumentException mex) {
			LOGGER.debug("Exception occurs: " + mex.getMessage());
			AssertJUnit.assertTrue(mex.getMessage().contains("Empty juno request list"));
			LOGGER.info("Exception", mex.getMessage());
			LOGGER.info("2");			
			LOGGER.info("Completed");
		}
	}
	
	/**
	 * Create batch keys with a key having TTL more than 3 days
	 * Verify get correct response for each item including long TTL one
	 */
	@Test
	public void testBatchSetTTLmorethan3days() throws JunoException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	    
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		int numKeys = 5;
		LOGGER.debug("Create " + numKeys + " keys with a key having > lifetime > 3 days");
		byte[][] key = new byte[numKeys][];
		long[] ttl = new long[numKeys];
		byte[][] payload = new byte[numKeys][];
		List<JunoRequest> list = new ArrayList<>();
		HashMap <String, Long> hmapTTL = new HashMap <String, Long>();
		for (int i = 0; i < numKeys; i ++) {
			key[i] = DataGenUtils.createKey(25).getBytes();
			String str = "Hello Testing testing " + i;
			payload[i] = str.getBytes();
			ttl[i] = 20;
			ttl[1] = 259201;
			hmapTTL.put(new String(key[i]), ttl[i]);
			JunoRequest item = new JunoRequest(key[i], null, (long)0, ttl[i], System.currentTimeMillis(), JunoRequest.OperationType.Get);
			list.add(item);
		}
		LOGGER.debug("\n===Batch Get is sent ");
		Iterable <JunoResponse> resp = new ArrayList<JunoResponse>();
		try {
			if (syncFlag == 1) {
				resp = junoClient.doBatch(list).toBlocking().toIterable();
			} else {
				resp = BatchTestSubscriber.async_dobatch(asyncJunoClient, list);
			}
		} catch (JunoException mex) {
			AssertJUnit.assertTrue ("Exception is not thrown for TTL > 3 days", false);
		}
		
		int i = 0;
		for (JunoResponse response: resp) {				
			String mkey = new String(response.getKey());
			if (mkey.equals(new String(key[1]))) {
				AssertJUnit.assertEquals(OperationStatus.IllegalArgument, response.getStatus());
			} else {
				AssertJUnit.assertTrue(OperationStatus.NoKey == response.getStatus());
				i++;
			}
		}
		AssertJUnit.assertEquals(i, numKeys-1);
	}
}
