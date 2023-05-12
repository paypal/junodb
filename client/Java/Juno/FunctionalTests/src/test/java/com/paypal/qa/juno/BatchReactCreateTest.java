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
import org.xerial.snappy.Snappy;

public class BatchReactCreateTest{
	private JunoReactClient junoActClient;
	private JunoReactClient junoReactClient;
	private JunoReactClient junoReactClient3;
	private Properties pConfig;
	private Properties pConfig1;
	private Properties pConfig2;
	private Properties pConfig3;
	private enum syncFlag {reactAsync, reactSync};
	private int flag;
	
	private Logger LOGGER;
	
	@BeforeClass
	public void setup() throws  IOException, InterruptedException {
		LOGGER = LoggerFactory.getLogger(BatchCreateTest.class);

		URL url = BatchCreateTest.class.getResource("/com/paypal/juno/Juno_batch.properties");
		pConfig = new Properties();
		pConfig.load(url.openStream());
		LOGGER.debug("Read syncFlag");
		String sync_flag = pConfig.getProperty("sync_flag_test", "0");
		LOGGER.debug("*********SYNC FLAG: " + sync_flag);
		flag = Integer.parseInt(sync_flag.trim());
		pConfig.setProperty(JunoProperties.APP_NAME, "QATestApp");
		pConfig.setProperty(JunoProperties.RECORD_NAMESPACE, "NS1");

		URL url2 = BatchCreateTest.class.getResource("/com/paypal/juno/Juno_batch.properties");
		pConfig2 = new Properties();
		pConfig2.load(url2.openStream());
		pConfig2.setProperty(JunoProperties.APP_NAME, "QATestApp2");
		pConfig2.setProperty(JunoProperties.RECORD_NAMESPACE, "NS1");
		
		pConfig3 = new Properties();
		pConfig3.load(url2.openStream());
		pConfig3.setProperty(JunoProperties.ENABLE_RETRY, "true");
		pConfig3.setProperty(JunoProperties.RESPONSE_TIMEOUT, "10");

		try{
			junoReactClient = JunoClientFactory.newJunoReactClient(new JunoPropertiesProvider(pConfig), SSLUtil.getSSLContext());
			junoActClient = JunoClientFactory.newJunoReactClient(new JunoPropertiesProvider(pConfig2), SSLUtil.getSSLContext());
			junoReactClient3 = JunoClientFactory.newJunoReactClient(new JunoPropertiesProvider(pConfig3), SSLUtil.getSSLContext());
		} catch (Exception e ){
			throw new RuntimeException(e);
		}

		Thread.sleep(3000);
	}

	@AfterClass
	public  void cleanSetup() throws Exception{		
		
	}

	@Test
	public void testBatchRetry() throws JunoException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	

		int numKeys = 50;
		byte[][] key = new byte[numKeys][];
		byte[][] payload = new byte[numKeys][];
		List<JunoRequest> list = new ArrayList<>();
		for (int i = 0; i < numKeys; i ++) {
			key[i] = DataGenUtils.createKey(10).getBytes();
			payload[i] = DataGenUtils.createKey(1024).getBytes();
			JunoRequest item = new JunoRequest(key[i], payload[i], (long)0, 180, System.currentTimeMillis(), JunoRequest.OperationType.Create);
			list.add(item);
		}

		try {
			Iterable<JunoResponse> batchResp = null;
			if (syncFlag.reactSync.ordinal() == flag) {	//sync react call
				batchResp = junoReactClient3.doBatch(list).toIterable();				
			} else {	//async react call
				batchResp = BatchReactSubscriber.async_dobatch(junoReactClient3, list);
			}

			int i = 0;
			for (JunoResponse mResponse: batchResp) {	
				AssertJUnit.assertTrue (OperationStatus.Success.getCode() == mResponse.getStatus().getCode() || 
					OperationStatus.ResponseTimeout.getCode() ==  mResponse.getStatus().getCode());
				if (OperationStatus.Success.getCode() == mResponse.getStatus().getCode()) {
					AssertJUnit.assertTrue(1 == mResponse.getVersion());
				}
			}

		} catch (JunoException mex) {
			LOGGER.debug("Exception occurs: " + mex.getMessage());
			AssertJUnit.assertTrue(false);
		}
	}
	
	/**
	 * Create and get multiple keys using batch create
	 * @throws JunoException
	 */
	@Test
	public void testBatchCreate() throws JunoException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		int numKeys = 10;
		byte[][] key = new byte[numKeys][];
		long[] ttl = new long[numKeys];
		byte[][] payload = new byte[numKeys][];
		HashMap<String, byte[]> hmap = new HashMap<String, byte[]>();
		HashMap<String, String> hmapTTL = new HashMap<String, String>();
		LOGGER.debug("Create " + numKeys + " keys using batch Create");
		List<JunoRequest> list = new ArrayList<>();
		List<JunoRequest> list1 = new ArrayList<>();

		key[numKeys-1] = DataGenUtils.createKey(128).getBytes();
		payload[numKeys-1] = DataGenUtils.createKey(204700).getBytes();
		ttl[numKeys-1] = 100;
		hmap.put(new String(key[numKeys-1]), payload[numKeys-1]);
		hmapTTL.put(new String(key[numKeys-1]), String.valueOf(ttl[numKeys-1]));
		JunoRequest item = new JunoRequest(key[numKeys-1], payload[numKeys-1], (long)0, 100, System.currentTimeMillis(), JunoRequest.OperationType.Create);
		list.add(item);
		Random r = new Random();
		for (int i = 0; i < numKeys-1; i ++) {
			key[i] = DataGenUtils.createKey(10).getBytes();
			int payloadlen = DataGenUtils.rand(r, 200, 204700);
			payload[i] = DataGenUtils.createKey(payloadlen).getBytes();
			ttl[i] = 100;			
			hmap.put(new String(key[i]), payload[i]);
			hmapTTL.put(new String(key[i]), String.valueOf(ttl[i]));
			item = new JunoRequest(key[i], payload[i], (long)0, ttl[i], System.currentTimeMillis(), JunoRequest.OperationType.Create);
			list.add(item);
		}
		try {
			Iterable<JunoResponse> batchResp = null;
                        if (syncFlag.reactSync.ordinal() == flag) {     //sync react call
				LOGGER.debug("come into sync batch create -- vera");
                                batchResp = junoActClient.doBatch(list).toIterable();
                        } else {        //async react call
				LOGGER.debug("come into async batch create -- vera");
                                batchResp = BatchReactSubscriber.async_dobatch(junoActClient, list);
                        }
			int i = 0;
			for (JunoResponse mResponse: batchResp) {	
				String mKey=new String(mResponse.getKey());
				AssertJUnit.assertEquals (OperationStatus.Success,mResponse.getStatus());
				AssertJUnit.assertEquals (hmapTTL.get(mKey), String.valueOf(mResponse.getTtl()));
				AssertJUnit.assertTrue(1 == mResponse.getVersion());
				i++;
			}			
			AssertJUnit.assertEquals(i, numKeys);
						
		} catch (JunoException mex) {
			LOGGER.debug("Exception occurs: " + mex.getMessage());
			AssertJUnit.assertTrue(false);
		}
		LOGGER.debug("Read " + numKeys + " keys using batch Get()");
		for (int i = 0; i < numKeys; i ++) {
			item = new JunoRequest(key[i], null, (long)0, (long)0, JunoRequest.OperationType.Get);
			list1.add(item);
		}
		Iterable<JunoResponse> getBatchResp = null;
		try {
			LOGGER.debug("before blocking mode batch get " + System.currentTimeMillis());
                        if (syncFlag.reactSync.ordinal() == flag) {     //sync react call
				LOGGER.debug("come into sync batch create -- vera");
                                getBatchResp = junoActClient.doBatch(list1).toIterable();
                        } else {        //async react call
				LOGGER.debug("come into async batch get -- vera");
                                getBatchResp = BatchReactSubscriber.async_dobatch(junoActClient, list1);
                        }
			LOGGER.debug("after batch get " + System.currentTimeMillis());
			int i = 0;
			for (JunoResponse response: getBatchResp) {	
				AssertJUnit.assertEquals (OperationStatus.Success, response.getStatus());			
				String mkey = new String(response.getKey());
				AssertJUnit.assertTrue(1 == response.getVersion());
				AssertJUnit.assertTrue(Integer.parseInt(hmapTTL.get(mkey))-16 <= response.getTtl()  && response.getTtl() <= Integer.parseInt(hmapTTL.get(mkey)));
				AssertJUnit.assertEquals (new String(hmap.get(mkey)), new String(response.getValue()));
				i++;
			}
			AssertJUnit.assertEquals(i, numKeys);
		} catch (JunoException ex) {
			LOGGER.debug("Exception occured:" + ex.getMessage() );
			AssertJUnit.assertTrue(false);
		}

		LOGGER.info("0");
		LOGGER.info("Completed");
	}	/**
	 * Create batch keys with 1 key duplicated
	 * Verify the batch create give correct errror for duplicated key
	 * Verify other keys are successful
	 * @throws JunoException
	 */
	@Test
	public void testBatchCreateDuplicateKeys() throws JunoException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		LOGGER.debug("Create a key first ");
		byte[] new_key = DataGenUtils.createKey(64).getBytes();
		byte[] data = DataGenUtils.genBytes(10);
		JunoResponse resp = junoReactClient.create(new_key,data).block();
		AssertJUnit.assertEquals (OperationStatus.Success, resp.getStatus());

		int numKeys = 10;
		LOGGER.debug("Create " + numKeys + " keys with key 7 a duplicated key");
		byte[][] key = new byte[numKeys][];
		long[] ttl = new long[numKeys];
		byte[][] payload = new byte[numKeys][];
		HashMap<String, byte[]> hmap = new HashMap<String, byte[]>();
		HashMap<String, Integer> hmapVersion = new HashMap<String, Integer>();
		HashMap<String, OperationStatus> hmapStatus = new HashMap<String, OperationStatus>();
		LOGGER.debug("Create " + numKeys + " keys using batch Create");
		List<JunoRequest> list = new ArrayList<>();
		List<JunoRequest> list1 = new ArrayList<>();
		for (int i = 0; i < numKeys; i ++) {
			key[i] = DataGenUtils.createKey(20).getBytes();
			key[7] = new_key;
			hmapStatus.put (new String(key[i]), OperationStatus.Success );
			hmapStatus.put (new String(key[7]), OperationStatus.UniqueKeyViolation);
			hmapVersion.put(new String(key[i]), 1);
			hmapVersion.put(new String(key[7]), 0);
			String str = "Hello Testing " + i;
			payload[i] = str.getBytes();
			ttl[i] = 20;		

			//Expected status;
			hmap.put(new String(key[i]), payload[i]);
			hmap.put(new String(key[7]),data);
			JunoRequest item = new JunoRequest(key[i], payload[i], (long)0, ttl[i], System.currentTimeMillis(), JunoRequest.OperationType.Create);
			list.add(item);
		}
		LOGGER.debug("\n===Batch Create is sent ");
		try {
			Iterable<JunoResponse> batchResp = null;
                       if (syncFlag.reactSync.ordinal() == flag) {     //sync react call
                                batchResp = junoReactClient.doBatch(list).toIterable();
                        } else {        //async react call
                                batchResp = BatchReactSubscriber.async_dobatch(junoReactClient, list);
                        }
			int j=0; 
			for (JunoResponse mResponse: batchResp) {	
				String mKey=new String(mResponse.getKey());
				AssertJUnit.assertEquals (hmapStatus.get(mKey), mResponse.getStatus());
				AssertJUnit.assertEquals ((long)hmapVersion.get(mKey), mResponse.getVersion());
				j++;
			}
			AssertJUnit.assertEquals(j, numKeys);

		} catch (JunoException mex) {
			//LOGGER.debug("Error code: " + mex.getOperationStatus().getCode());
			LOGGER.debug("Exception occurs: " + mex.getMessage());
		}
		LOGGER.debug("\n===Batch Read " + numKeys + " keys using batch Get()");
		Iterable<JunoResponse> getResp = null;		
		
		for (int i = 0; i < numKeys; i ++) {
			JunoRequest item = new JunoRequest(key[i], null, (long)0, (long)0, JunoRequest.OperationType.Get);
			list1.add(item);
		}
                if (syncFlag.reactSync.ordinal() == flag) {     //sync react call
                        getResp = junoReactClient.doBatch(list1).toIterable();
                } else {        //async react call
                        getResp = BatchReactSubscriber.async_dobatch(junoReactClient, list1);
                }
		int j=0; 
		for (JunoResponse mResponse: getResp) {	
			String mKey=new String(mResponse.getKey());
			AssertJUnit.assertEquals (OperationStatus.Success,mResponse.getStatus());
			AssertJUnit.assertTrue (1 == mResponse.getVersion());
			AssertJUnit.assertEquals(hmap.get(mKey), mResponse.getValue());
			j++;
		}
		AssertJUnit.assertEquals(j, numKeys);
		
		LOGGER.info("0");
		LOGGER.info("Completed");
	}

	/**
	 * Create batch keys with 1 empty key
	 * Verify appropriate JunoException is thrown
	 * @throws JunoException
	 */
	@Test
	public void testBatchCreateEmptyKeys() throws JunoException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		int numKeys = 5;
		LOGGER.debug("Create " + numKeys + " keys with one zero-length key");
		byte[][] key = new byte[numKeys][];
		long[] ttl = new long[numKeys];
		byte[][] payload = new byte[numKeys][];
		List<JunoRequest> list = new ArrayList<>();
		
		for (int i = 0; i < numKeys; i ++) {
			key[i] = DataGenUtils.createKey(127).getBytes();
			key[4] = "".getBytes();
			String str = "Hello Testing testing " + i;
			payload[i] = str.getBytes();
			ttl[i] = 20;		

			JunoRequest item = new JunoRequest(key[i], payload[i], (long)0, ttl[i], System.currentTimeMillis(), JunoRequest.OperationType.Create);
			list.add(item);
		}
		try{
                        Iterable<JunoResponse> batchResp;
                        LOGGER.error( "\n===Batch Create is sent ");
                        if (syncFlag.reactSync.ordinal() == flag) {
                                batchResp=junoActClient.doBatch(list).toIterable();
                        } else {
                                batchResp=BatchReactSubscriber.async_dobatch(junoActClient, list);
                        }
                        for (JunoResponse response: batchResp) {
                            if (response != null && response.getKey() != null && response.getKey() != key[4]) {
                                AssertJUnit.assertEquals (OperationStatus.Success, response.getStatus());
                            } else {
                                AssertJUnit.assertEquals (OperationStatus.IllegalArgument, response.getStatus());
                            }
                        }
		} catch (IllegalArgumentException mex) {
			LOGGER.debug("Exception occurs: " + mex.getMessage());
			AssertJUnit.assertTrue(mex.getMessage().contains("key must not be null or empty"));
			LOGGER.info("Exception", mex.getMessage());
			LOGGER.info("2");			
			LOGGER.info("Completed");
		}
	}

	/**
	 * Create batch keys with 2 NULL keys
	 * Verify appropriate JunoException is thrown
	 * @throws JunoException
	 */
	@Test
	public void testBatchCreateNullKeys() throws JunoException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		int numKeys = 10;
		LOGGER.debug("Create " + numKeys + " keys with two null keys");
		byte[][] key = new byte[numKeys][];
		long[] ttl = new long[numKeys];
		byte[][] payload = new byte[numKeys][];
		List<JunoRequest> list = new ArrayList<>();
		for (int i = 0; i < numKeys; i ++) {
			key[i] = DataGenUtils.createKey(10).getBytes();
			key[3] = null;
			key[9] = null;
			String str = "Hello Testing testing " + i;
			payload[i] = str.getBytes();
			ttl[i] = 20;		

			JunoRequest item = new JunoRequest(key[i], payload[i], (long)0, ttl[i], JunoRequest.OperationType.Create);
			list.add(item);
		}
		try{
                        Iterable<JunoResponse> batchResp;
                        LOGGER.error( "\n===Batch Create is sent ");
                        if (syncFlag.reactSync.ordinal() == flag) {
                                batchResp=junoActClient.doBatch(list).toIterable();
                        } else {
                                batchResp=BatchReactSubscriber.async_dobatch(junoActClient, list);
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
	}

	/**
	 * Create batch keys with keys >=257 keys
	 * Verify appropriate JunoException is thrown
	 * @throws JunoException
	 */
	@Test
	public void testBatchCreate129BytesKeys() throws JunoException
	{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		int numKeys = 10;
		LOGGER.debug("Create " + numKeys + " keys with more than 1 key >=  129 bytes");
		byte[][] key = new byte[numKeys][];
		long[] ttl = new long[numKeys];
		byte[][] payload = new byte[numKeys][];
		List<JunoRequest> list = new ArrayList<>();
		HashMap <String, byte[]> hmap = new HashMap <String, byte[]>();
		HashMap <String, Long> hmapTTL = new HashMap <String, Long> ();
				
		for (int i = 0; i < numKeys; i ++) {
			key[i] = DataGenUtils.createKey(128).getBytes();
			key[9] = DataGenUtils.createKey(129).getBytes();
			payload[i] = DataGenUtils.createKey(30).getBytes();
			ttl[i] = 20;	
			hmap.put(new String(key[i]),payload[i]);
			hmapTTL.put(new String(key[i]), ttl[i]);
			JunoRequest item = new JunoRequest(key[i], payload[i], (long)0, ttl[i], JunoRequest.OperationType.Create);
			list.add(item);
		}
		LOGGER.debug("\n===Batch Create is sent ");
		Iterable <JunoResponse> resp; 
                if (syncFlag.reactSync.ordinal() == flag) {     //sync react call
                        resp = junoActClient.doBatch(list).toIterable();
                } else {        //async react call
                        resp = BatchReactSubscriber.async_dobatch(junoActClient, list);
                }
			
		int i = 0;
		for (JunoResponse response: resp) {	
			String mkey = new String(response.getKey());
			if ( mkey.equals(new String(key[9]))) {
				AssertJUnit.assertEquals (OperationStatus.IllegalArgument, response.getStatus());
			} else {
				AssertJUnit.assertEquals (OperationStatus.Success, response.getStatus());
				AssertJUnit.assertTrue(1 == response.getVersion());
				AssertJUnit.assertTrue(response.getTtl() <= hmapTTL.get(mkey) &&  response.getTtl() >= hmapTTL.get(mkey)-3 );
				i++;
			}
		}
		AssertJUnit.assertEquals(i, numKeys-1);	
		
		//batch get
		List<JunoRequest> list1 = new ArrayList<>();
		Iterable <JunoResponse> gResp = new ArrayList<>();
		for (int j = 0; j < numKeys; j++) {
			JunoRequest item1 = new JunoRequest(key[j], (long)0, (long)0, JunoRequest.OperationType.Get);
			list1.add(item1);
		}	
		try {
			if (syncFlag.reactSync.ordinal() == flag) {     //sync react call
				gResp = junoActClient.doBatch(list1).toIterable();
			} else {        //async react call
				gResp = BatchReactSubscriber.async_dobatch(junoActClient, list1);
			} 
			i=0;
			for (JunoResponse response: gResp) {	
				String mkey = new String(response.getKey());
				LOGGER.debug("mkey in get is " + mkey);
				if ( mkey.equals(new String(key[9]))) {
					AssertJUnit.assertEquals (OperationStatus.IllegalArgument, response.getStatus());
				} else {
					AssertJUnit.assertEquals (OperationStatus.Success, response.getStatus());
					AssertJUnit.assertTrue(1 == response.getVersion());
					AssertJUnit.assertTrue(response.getTtl() <= hmapTTL.get(mkey) &&  response.getTtl() >= hmapTTL.get(mkey)-3 );
					AssertJUnit.assertEquals(response.getValue(), hmap.get(mkey));
					i++;
				}
			}
			AssertJUnit.assertEquals(i, numKeys-1);	
		} catch (Exception ex) {
			AssertJUnit.assertTrue(false);	
		}
		
		LOGGER.info("0");			
		LOGGER.info("Completed");		
	}

	/**
	 * Create batch keys with keys having 0 or null payload
	 * Verify JunoException does not throw
	 * @throws JunoException
	 */
	@Test
	public void testBatchCreateZeroPayload() throws JunoException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	    
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		int numKeys = 10;
		LOGGER.debug("Create " + numKeys + " keys with some keys having zero payload");
		byte[][] key = new byte[numKeys][];
		long[] ttl = new long[numKeys];
		byte[][] payload = new byte[numKeys][];
		ArrayList <JunoRequest> list = new ArrayList <JunoRequest>();
		HashMap <String, byte[]> hmap = new HashMap<String, byte[]>();
		for (int i = 0; i < numKeys; i ++) {
			key[i] = DataGenUtils.createKey(25).getBytes();
			String str = "Hello Testing testing " + i;
			payload[i] = str.getBytes();
			payload[5] = "".getBytes();
			ttl[i] = 20;		
			hmap.put(new String(key[i]), payload[i]);

			JunoRequest item = new JunoRequest(key[i], payload[i], (long)0, ttl[i], JunoRequest.OperationType.Create);
			list.add(item);
		}
		LOGGER.debug("\n===Batch Create is sent ");
		try {
			int i = 0;
			Iterable <JunoResponse> resp = null;
                        if (syncFlag.reactSync.ordinal() == flag) {     //sync react call
                                resp = junoActClient.doBatch(list).toIterable();
                        } else {        //async react call
                                resp = BatchReactSubscriber.async_dobatch(junoActClient, list);
                        }
			for (JunoResponse mResponse: resp) {
                                AssertJUnit.assertEquals (OperationStatus.Success,mResponse.getStatus());
                                AssertJUnit.assertTrue(1 == mResponse.getVersion());
				i++;
                        }
			AssertJUnit.assertEquals(i, numKeys);
		} catch (JunoException mex) {
			AssertJUnit.assertTrue ("Exception should not throw for zero payload", false);
		}
		
		ArrayList <JunoRequest> list1 = new ArrayList<JunoRequest>();
		Iterable <JunoResponse> gResp = new ArrayList<>();
		for (int j = 0; j < numKeys; j++) {
			JunoRequest item1 = new JunoRequest(key[j], (long)0, (long)0, JunoRequest.OperationType.Get);
			list1.add(item1);
		}	
		try {
	                if (syncFlag.reactSync.ordinal() == flag) {     //sync react call
				gResp = junoActClient.doBatch(list1).toIterable();
			} else {        //async react call
				gResp = BatchReactSubscriber.async_dobatch(junoActClient, list1);
			} 
			int i=0;
			for (JunoResponse response: gResp) {	
				String mkey = new String(response.getKey());
				LOGGER.debug("mkey in get is " + mkey);
				AssertJUnit.assertEquals (OperationStatus.Success, response.getStatus());
				AssertJUnit.assertTrue(1 == response.getVersion());
				LOGGER.debug("value get is " + new String(response.getValue()));
				AssertJUnit.assertEquals(response.getValue(), hmap.get(mkey));
				i++;
			}
			AssertJUnit.assertEquals(i, numKeys);	
			LOGGER.info("0");
			LOGGER.info("Completed");
		} catch (Exception ex) {
			AssertJUnit.assertTrue(false);	
		}
	}

	/**
	 * Create batch keys with keys having 0 or null payload
	 * Verify JunoException does not throw
	 * @throws JunoException
	 */
	@Test
	public void testBatchCreateNullPayload() throws JunoException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		int numKeys = 10;
		LOGGER.debug("Create " + numKeys + " keys with some keys having null payload");
		byte[][] key = new byte[numKeys][];
		long[] ttl = new long[numKeys];
		byte[][] payload = new byte[numKeys][];
		HashMap <String, Long> hmapTTL = new HashMap<String, Long>();
		List<JunoRequest> list = new ArrayList<>();
		for (int i = 0; i < numKeys; i ++) {
			key[i] = DataGenUtils.createKey(25).getBytes();
			String str = "Hello Testing testing " + i;
			payload[i] = str.getBytes();
			payload[0] = null;
			payload[4] = "".getBytes();
			ttl[i] = 20;		
			hmapTTL.put(new String(key[i]), ttl[i]);

			JunoRequest item = new JunoRequest(key[i], payload[i], (long)0, ttl[i], JunoRequest.OperationType.Create);
			list.add(item);
		}
		LOGGER.debug("\n===Batch Create is sent ");
		Iterable <JunoResponse> batchResp = null;
		try {
                        if (syncFlag.reactSync.ordinal() == flag) {     //sync react call
                                batchResp = junoActClient.doBatch(list).toIterable();
                        } else {        //async react call
                                batchResp = BatchReactSubscriber.async_dobatch(junoActClient, list);
                        }
		} catch (JunoException mex) {
			AssertJUnit.assertTrue("batch create for one item null payload shouldn't fail", false);
		}
		
		int i = 0;
		for (JunoResponse response: batchResp) {	
			String mkey = new String(response.getKey());
			if ( mkey.equals(new String(key[0]))) {
				AssertJUnit.assertEquals (OperationStatus.Success, response.getStatus());
				AssertJUnit.assertEquals (0, response.getValue().length);
			} else {
				AssertJUnit.assertEquals (OperationStatus.Success, response.getStatus());
				AssertJUnit.assertTrue(1 == response.getVersion());
				AssertJUnit.assertTrue(response.getTtl() <= hmapTTL.get(mkey) &&  response.getTtl() >= hmapTTL.get(mkey)-3 );
				i++;
			}
		}
		AssertJUnit.assertEquals(i, numKeys-1);	
		
		//Batch Get
		ArrayList <JunoRequest> list1 = new ArrayList<JunoRequest>();
		Iterable <JunoResponse> gResp = new ArrayList<>();
		for (int j = 0; j < numKeys; j++) {
			JunoRequest item1 = new JunoRequest(key[j], (long)0, (long)0, JunoRequest.OperationType.Get);
			list1.add(item1);
		}	
		try {
                        if (syncFlag.reactSync.ordinal() == flag) {     //sync react call
                                gResp = junoActClient.doBatch(list1).toIterable();
                        } else {        //async react call
                                gResp = BatchReactSubscriber.async_dobatch(junoActClient, list1);
                        }
			i=0;
			for (JunoResponse response: gResp) {	
				String mkey = new String(response.getKey());
				if (mkey.equals(new String(key[0]))) {
					AssertJUnit.assertEquals (OperationStatus.Success, response.getStatus());
					AssertJUnit.assertEquals (0, response.getValue().length);
				} else {
					AssertJUnit.assertEquals (OperationStatus.Success, response.getStatus());
					AssertJUnit.assertTrue(1 == response.getVersion());					
					AssertJUnit.assertTrue(response.getTtl() <= hmapTTL.get(mkey) &&  response.getTtl() >= hmapTTL.get(mkey)-3);
					i++;
				}
			}
			AssertJUnit.assertEquals(i, numKeys-1);	
			LOGGER.info("0");
			LOGGER.info("Completed");
			
		} catch (Exception ex) {
			AssertJUnit.assertTrue(false);	
		}
		
		LOGGER.info("0");			
		LOGGER.info("Completed");
	}

	/**
	 * Create batch keys with a key having > 200KB payload
	 * Verify JunoException does not throw 
	 * @throws JunoException
	 */
	@Test
	public void testBatchCreateMoreThan200KPayload() throws JunoException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		int numKeys = 10;
		LOGGER.debug("Create " + numKeys + " keys with a key having > 200KB payload");
		byte[][] key = new byte[numKeys][];
		long[] ttl = new long[numKeys];
		byte[][] payload = new byte[numKeys][];
		List<JunoRequest> list = new ArrayList<>();
		HashMap <String, byte[]> hmap = new HashMap <String, byte[]>();
		HashMap <String, Long> hmapTTL = new HashMap <String, Long>();
		
		for (int i = 0; i < numKeys; i ++) {
			key[i] = DataGenUtils.createKey(25).getBytes();
			payload[i] = DataGenUtils.genBytes(30);
			byte[] data = DataGenUtils.genBytes(204801);
			payload[8] = data;
			ttl[i] = 20;		
			LOGGER.debug("key " + i + " is " + new String(key[i]));
			hmap.put(new String(key[i]), payload[i]);
			hmapTTL.put(new String(key[i]), ttl[i]);

			JunoRequest item = new JunoRequest(key[i], payload[i], (long)0, ttl[i], JunoRequest.OperationType.Create);
			list.add(item);
		}
		LOGGER.debug("\n===Batch Create is sent ");
		try {			
			Iterable <JunoResponse> resp = null;
			resp = BatchReactSubscriber.async_dobatch(junoActClient, list);
			for (JunoResponse mResponse: resp) {
				String mkey = new String(mResponse.getKey());
				if ( mkey.equals(new String(key[8]))) {
					AssertJUnit.assertEquals (OperationStatus.IllegalArgument, mResponse.getStatus());
				}else {
					AssertJUnit.assertEquals (OperationStatus.Success,mResponse.getStatus());
					AssertJUnit.assertTrue(1 == mResponse.getVersion());
				}
			}
		} catch (Exception mex) {
			AssertJUnit.assertTrue("batch create shouldn't fail", false);
		}
		
		//batch get
		List<JunoRequest> list1 = new ArrayList<>();
		Iterable <JunoResponse> gResp = new ArrayList<>();
		for (int j = 0; j < numKeys; j++) {
			JunoRequest item1 = new JunoRequest(key[j], (long)0, (long)0, JunoRequest.OperationType.Get);
			list1.add(item1);
		}	
		try {
                        if (syncFlag.reactSync.ordinal() == flag) {     //sync react call
                                gResp = junoActClient.doBatch(list1).toIterable();
                        } else {        //async react call
                                gResp = BatchReactSubscriber.async_dobatch(junoActClient, list1);
                        }
			int i=0;
			for (JunoResponse response: gResp) {	
				String mkey = new String(response.getKey());
				LOGGER.debug("mkey in get is " + mkey);
				if ( mkey.equals(new String(key[8]))) {
					AssertJUnit.assertEquals (OperationStatus.NoKey, response.getStatus());
				} else {
					AssertJUnit.assertEquals (OperationStatus.Success, response.getStatus());
					AssertJUnit.assertTrue(1 == response.getVersion());
					AssertJUnit.assertTrue(response.getTtl() <= hmapTTL.get(mkey) &&  response.getTtl() >= hmapTTL.get(mkey)-3 );
					AssertJUnit.assertEquals(response.getValue(), hmap.get(mkey));
					i++;
				}
			}
			AssertJUnit.assertEquals(i, numKeys-1);	
		} catch (Exception ex) {
			AssertJUnit.assertTrue(false);	
		}
		
		LOGGER.info("0");
		LOGGER.info("Completed");
	}	/**
	 * Create batch keys with a key having TTL more than 3 days
	 * Verify appropriate JunoException is thrown
	 * @throws JunoException
	 */
	@Test
	public void testBatchCreateTTLmorethan3days() throws JunoException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		int numKeys = 5;
		LOGGER.debug("Create " + numKeys + " keys with a key having > lifetime > 3 days");
		byte[][] key = new byte[numKeys][];
		long[] ttl = new long[numKeys];
		byte[][] payload = new byte[numKeys][];
		List<JunoRequest> list = new ArrayList<>();
		HashMap <String, byte[]> hmap = new HashMap<String, byte[]>();
		HashMap <String, Long> hmapTTL = new HashMap<String, Long>();
		
		for (int i = 0; i < numKeys; i ++) {
			key[i] = DataGenUtils.createKey(25).getBytes();
			payload[i] = DataGenUtils.createKey(40).getBytes();
			ttl[i] = 20;
			ttl[1] = 259201;	
			hmap.put(new String(key[i]), payload[i]);
			hmapTTL.put(new String(key[i]), ttl[i]);
			LOGGER.error( "Vera: added ttl haha1" + ttl[i]);		
			JunoRequest item = new JunoRequest(key[i], payload[i], (long)0, ttl[i], JunoRequest.OperationType.Create);
			LOGGER.error( "Vera: added ttl haha2" + ttl[i]);
			list.add(item);
		}
		LOGGER.debug("\n===Batch Create is sent ");
		try {
			int i=0;
			Iterable <JunoResponse> resp = null;
                        if (syncFlag.reactSync.ordinal() == flag) {     //sync react call
                                resp = junoActClient.doBatch(list).toIterable();
                        } else {        //async react call
                                resp = BatchReactSubscriber.async_dobatch(junoActClient, list);
                        }
                        for (JunoResponse mResponse: resp) {
				String mkey = new String(mResponse.getKey());
				if ( mkey.equals(new String(key[1]))) {
					AssertJUnit.assertEquals (OperationStatus.IllegalArgument, mResponse.getStatus());
				} else {
					//System.out.println("Record version:"+mResponse.getVersion());
					//AssertJUnit.assertTrue(1 == mResponse.getVersion());
					AssertJUnit.assertEquals (OperationStatus.Success,mResponse.getStatus());
					i++;
				}
                        }
			AssertJUnit.assertEquals(i, numKeys-1);
		} catch (JunoException mex) {
			AssertJUnit.assertTrue ("Exception should not thrown for one item exceeds max lifetime", false);
		}
		
		//batch get
		List<JunoRequest> list1 = new ArrayList<>();
		Iterable <JunoResponse> gResp = new ArrayList<>();
		for (int j = 0; j < numKeys; j++) {
			JunoRequest item1 = new JunoRequest(key[j], (long)0, (long)0, JunoRequest.OperationType.Get);
			list1.add(item1);
		}	
		try {
                        if (syncFlag.reactSync.ordinal() == flag) {     //sync react call
                                gResp = junoActClient.doBatch(list1).toIterable();
                        } else {        //async react call
                                gResp = BatchReactSubscriber.async_dobatch(junoActClient, list1);
                        }
			int i=0;
			for (JunoResponse response: gResp) {	
				String mkey = new String(response.getKey());
				LOGGER.debug("mkey in get is " + mkey);
				if ( mkey.equals(new String(key[1]))) {
					AssertJUnit.assertEquals (OperationStatus.NoKey, response.getStatus());
				} else {
					AssertJUnit.assertEquals (OperationStatus.Success, response.getStatus());
					AssertJUnit.assertTrue(1 == response.getVersion());
					AssertJUnit.assertTrue(response.getTtl() <= hmapTTL.get(mkey) &&  response.getTtl() >= hmapTTL.get(mkey)-3 );
					AssertJUnit.assertEquals(response.getValue(), hmap.get(mkey));
					i++;
				}
			}
			AssertJUnit.assertEquals(i, numKeys-1);	
		} catch (Exception ex) {
			AssertJUnit.assertTrue(false);	
		}
		
		LOGGER.info("0");
		LOGGER.info("Completed");
	}

	/**
	 * Create batch keys with different TTL and payload
	 * This test is used to test connection error 
	 * @throws JunoException
	 */
	@Test
	public void testBatchCreateDiffLifeTimePayloads(){
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	    
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		int numKeys = 10;
		byte[][] key = new byte[numKeys][];
		long[] ttl = new long[numKeys];
		byte[][] payload = new byte[numKeys][];
		HashMap<String, byte[]> hmap = new HashMap<String, byte[]>();
		HashMap<String, String> hmapTTL = new HashMap<String, String>();
		LOGGER.debug("Create " + numKeys + " keys using batch Create");
		Random r = new Random();
		long ttl1 = DataGenUtils.rand(r, 200, 86400);
		LOGGER.debug("ttl1: " + ttl1);

		List<JunoRequest> list = new ArrayList<>();
		for (int i = 0; i < numKeys; i ++) {			
			key[i] = DataGenUtils.createKey(DataGenUtils.rand(r, 1, 128)).getBytes();
			payload[i] = DataGenUtils.genBytes(DataGenUtils.rand(r, 1, 2048));
			payload[5] = DataGenUtils.genBytes(500);
			ttl[i] = DataGenUtils.rand(r, 200, 8600);	
			ttl[5] = 800;
			hmap.put(new String(key[i]), payload[i]);
			hmapTTL.put(new String(key[i]), String.valueOf(ttl[i]));
			
			JunoRequest item = new JunoRequest(key[i], payload[i], (long)0, ttl[i], JunoRequest.OperationType.Create);
			list.add(item);
		}
		try {
			Iterable<JunoResponse> batchResp = null;
                        if (syncFlag.reactSync.ordinal() == flag) {     //sync react call
                                batchResp = junoActClient.doBatch(list).toIterable();
                        } else {        //async react call
                                batchResp = BatchReactSubscriber.async_dobatch(junoActClient, list);
                        }
			int i = 0;
			for (JunoResponse mResponse: batchResp) {				
				AssertJUnit.assertTrue(1 == mResponse.getVersion());
				AssertJUnit.assertEquals (OperationStatus.Success,mResponse.getStatus());
				i++;
			}
			AssertJUnit.assertTrue (i == numKeys);
		} catch (JunoException mex) {
			//LOGGER.debug(String.valueOf(mex.getOperationStatus().getCode()));
			//LOGGER.debug(mex.getOperationStatus().getErrorText());
			//LOGGER.debug(mex.getOperationStatus().name());
			LOGGER.debug("Exception:  " + mex.getMessage());
			AssertJUnit.assertTrue (false);
		}
		LOGGER.debug("Read " + numKeys + " keys using batch Get()");
		List<JunoRequest> list1 = new ArrayList<>();
		for (int i = 0; i < numKeys; i ++) {
			JunoRequest item = new JunoRequest(key[i], (long)0, (long)0, JunoRequest.OperationType.Get);
			list1.add(item);
		}
		try {
			Iterable<JunoResponse> getBatchResp = null;
                        if (syncFlag.reactSync.ordinal() == flag) {     //sync react call
                                getBatchResp = junoActClient.doBatch(list1).toIterable();
                        } else {        //async react call
                                getBatchResp = BatchReactSubscriber.async_dobatch(junoActClient, list1);
                        }

			for (JunoResponse response: getBatchResp) {		
				String mkey = new String(response.getKey());
				AssertJUnit.assertEquals (OperationStatus.Success, response.getStatus());
				AssertJUnit.assertTrue(1 == response.getVersion());
				AssertJUnit.assertEquals(new String(hmap.get(mkey)), new String(response.getValue()));
				AssertJUnit.assertTrue(Integer.parseInt(hmapTTL.get(mkey))-10 <= response.getTtl()  && response.getTtl() <= Integer.parseInt(hmapTTL.get(mkey)));
			}
			LOGGER.info("0");
			LOGGER.info("Completed");
		} catch (JunoException mex) {
			AssertJUnit.assertTrue (false);
		}
	}

	/**
	 * Create batch keys without TTL
	 * Verify appropriate JunoException is thrown
	 * @throws JunoException //TODO: ask is there anyway to passin default TTL
	 */
	@Test
	public void testBatchCreateNoLifeTime() throws JunoException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	    
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		int numKeys = 10;
		byte[][] key = new byte[numKeys][];
		byte[][] payload = new byte[numKeys][];
		long[] ttl = new long[numKeys];
		HashMap<String, Long> hmapTTL = new HashMap<String, Long>();
		LOGGER.debug("Create " + numKeys + " keys using batch Create");
		Random r = new Random();

		List<JunoRequest> list = new ArrayList<>();
		for (int i = 0; i < numKeys; i ++) {
			key[i] = DataGenUtils.createKey(DataGenUtils.rand(r, 1, 128)).getBytes();
			payload[i] = DataGenUtils.genBytes(DataGenUtils.rand(r, 1, 4048));
			payload[5] = DataGenUtils.genBytes(204800);
			ttl[i]=100;
			ttl[8] = 0; 
			hmapTTL.put(new String(key[i]), ttl[i]);
			JunoRequest item = new JunoRequest(key[i], payload[i], (long)0, ttl[i], JunoRequest.OperationType.Create);
			list.add(item);
		}
		
		Iterable<JunoResponse> batchResp = null;
		try {
                        if (syncFlag.reactSync.ordinal() == flag) {     //sync react call
                                batchResp = junoActClient.doBatch(list).toIterable();
                        } else {        //async react call
                                batchResp = BatchReactSubscriber.async_dobatch(junoActClient, list);
                        }
		} catch (JunoException mex) {
			AssertJUnit.assertTrue("create without 0 ttl for one item shouldn't fail", false);			
		}
		
		int i = 0;
		for (JunoResponse response: batchResp) {	
			String mkey = new String(response.getKey());
			if ( mkey.equals(new String(key[8]))) {
				AssertJUnit.assertEquals (OperationStatus.IllegalArgument, response.getStatus());
			} else {
				AssertJUnit.assertEquals (OperationStatus.Success, response.getStatus());
				AssertJUnit.assertTrue(1 == response.getVersion());
				AssertJUnit.assertTrue(response.getTtl() <= hmapTTL.get(mkey) &&  response.getTtl() >= hmapTTL.get(mkey)-3 );
				i++;
			}
		}
		AssertJUnit.assertEquals(i, numKeys-1);	
		
		LOGGER.info("0");			
		LOGGER.info("Completed");
	}

	/**
	 * Create batch keys with no Item in the JunoRequest list
	 * Verify appropriate JunoException is thrown
	 * @throws JunoException
	 */
	@Test
	public void testBatchCreateZeroItem() throws JunoException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		LOGGER.debug("Send 0 item to Batch create");

		List<JunoRequest> list = new ArrayList<>();
		LOGGER.debug("\n===Batch Create is sent ");
		Iterable<JunoResponse> batchResp = null;
		try {
                        if (syncFlag.reactSync.ordinal() == flag) {     //sync react call
                                batchResp = junoActClient.doBatch(list).toIterable();
                        } else {        //async react call
                                batchResp = BatchReactSubscriber.async_dobatch(junoActClient, list);
                        }
			AssertJUnit.assertTrue ("Exception is not thrown for no key in Juno Request", false);
			LOGGER.info("0");
		} catch (JunoException mex) {
			LOGGER.debug("Exception occurs: " + mex.getMessage());
			//AssertJUnit.assertTrue(OperationStatus.IllegalArgument == mex.getOperationStatus());
			LOGGER.info("Exception", mex.getMessage());
			LOGGER.info("2");			
			LOGGER.info("Completed");
		}
	}

	/**
	 * Create batch keys, many params passed to Juno Request Item list
	 * Verify keys are created successfully
	 * @throws JunoException
	 */
	@Test
	public void testBatchCreateManyParams() throws JunoException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	    
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		int numKeys = 10;
		byte[][] key = new byte[numKeys][];
		long[] ttl = new long[numKeys];
		byte[][] payload = new byte[numKeys][];
		HashMap<String, byte[]> hmap = new HashMap<String, byte[]>();
		HashMap <String, String> hmapTTL = new HashMap <String, String>();
		LOGGER.debug("Create " + numKeys + " keys using batch Create");
		Random r = new Random();
		long ttl1 = DataGenUtils.rand(r, 200, 86400);
		LOGGER.debug("ttl1: " + ttl1);

		List<JunoRequest> list = new ArrayList<>();
		for (int i = 0; i < numKeys; i ++) {
			key[i] = DataGenUtils.createKey(DataGenUtils.rand(r, 1, 128)).getBytes();
			payload[i] = DataGenUtils.genBytes(DataGenUtils.rand(r, 1, 2048));
			payload[5] = DataGenUtils.genBytes(5000);			
			ttl[i] = DataGenUtils.rand(r, 200, 86400);	
			ttl[5] = 259200;
			hmap.put(new String(key[i]), payload[i]);
			hmapTTL.put(new String(key[i]), String.valueOf(ttl[i]));
			//Passing version in the Juno Request Item list
			JunoRequest item = new JunoRequest(key[i], payload[i], (long)0, ttl[i], JunoRequest.OperationType.Create);
			list.add(item);
		}
		try {
			Iterable<JunoResponse> batchResp = null;
                        if (syncFlag.reactSync.ordinal() == flag) {     //sync react call
                                batchResp = junoActClient.doBatch(list).toIterable();
                        } else {        //async react call
                                batchResp = BatchReactSubscriber.async_dobatch(junoActClient, list);
                        }
			int i = 0;
			for (JunoResponse mResponse: batchResp) {				
				LOGGER.debug("Key: haha" + i + ": "+ mResponse.getKey());
				AssertJUnit.assertTrue(1 == mResponse.getVersion());
				AssertJUnit.assertEquals (OperationStatus.Success,mResponse.getStatus());
				i++;
			}
			AssertJUnit.assertTrue ( i == numKeys);
		} catch (JunoException mex) {
			LOGGER.debug("Exception occurs: " + mex.getMessage());
			AssertJUnit.assertTrue (false);
		}
		LOGGER.debug("Read " + numKeys + " keys using batch Get()");
		Iterable<JunoResponse> getBatchResp = null;
		
		List<JunoRequest> list1 = new ArrayList<>();
		for (int i = 0; i < numKeys; i ++) {
			JunoRequest item = new JunoRequest(key[i], null, (long)0, (long)0, JunoRequest.OperationType.Get);
			list1.add(item);				
		}
                if (syncFlag.reactSync.ordinal() == flag) {     //sync react call
                        getBatchResp = junoActClient.doBatch(list1).toIterable();
                } else {        //async react call
                        getBatchResp = BatchReactSubscriber.async_dobatch(junoActClient, list1);
                }
		for (JunoResponse response: getBatchResp) {		
			String mkey = new String(response.getKey());
			AssertJUnit.assertTrue(1 == response.getVersion());
			AssertJUnit.assertEquals (OperationStatus.Success, response.getStatus());
			AssertJUnit.assertEquals(new String(hmap.get(mkey)), new String(response.getValue()));
			AssertJUnit.assertTrue(Integer.parseInt(hmapTTL.get(mkey)) - 10 <= response.getTtl() && response.getTtl() <= Integer.parseInt(hmapTTL.get(mkey)));
		}
		LOGGER.info("0");
		LOGGER.info("Completed");
	}
	
	/**
	 * Check response Timeout exception
	 * @throws JunoException
	 */
	//@Test
	public void testBatchCreateResponseTimeout() throws IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		URL url1 = BatchCreateTest.class.getResource("/com/paypal/juno/Juno.properties");
		pConfig1 = new Properties();
		pConfig1.load(url1.openStream());
		pConfig1.setProperty(JunoProperties.APP_NAME, "QATestApp");
		pConfig1.setProperty(JunoProperties.RECORD_NAMESPACE, "NS2");
		pConfig1.setProperty(JunoProperties.RESPONSE_TIMEOUT, "1");
		JunoReactClient junoReactClient1 = null;
		try {
			junoReactClient1 = JunoClientFactory.newJunoReactClient(new JunoPropertiesProvider(pConfig1), SSLUtil.getSSLContext());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		int numKeys = 100;
		byte[][] key = new byte[numKeys][];
		byte[][] payload = new byte[numKeys][];
		long[] ttl = new long[numKeys];
		Random r = new Random();
		long ttl1 = DataGenUtils.rand(r, 200, 86400);
		List<JunoRequest> list = new ArrayList<>();
		LOGGER.debug("Create " + numKeys + " keys using batch Create");						
		for (int i = 0; i < numKeys; i ++) {
			key[i] = DataGenUtils.createKey(DataGenUtils.rand(r, 1, 128)).getBytes();
			payload[i] = DataGenUtils.genBytes(204800);
			ttl[i] = DataGenUtils.rand(r, 200, 86400);	
			ttl[5] = 259200;
			JunoRequest item = new JunoRequest(key[i], payload[i], (long)0, ttl[i], JunoRequest.OperationType.Create);
			list.add(item);
		}
		try {
			Iterable <JunoResponse> batchResp = null;
                        if (syncFlag.reactSync.ordinal() == flag) {     //sync react call
                                batchResp = junoReactClient1.doBatch(list).toIterable();
                        } else {        //async react call
                                batchResp = BatchReactSubscriber.async_dobatch(junoReactClient1, list);
                        }
			int i = 0;
			for (JunoResponse mResponse: batchResp) {				
				AssertJUnit.assertEquals (OperationStatus.ResponseTimeout,mResponse.getStatus());
				i++;
			}
			AssertJUnit.assertTrue ( i == numKeys);
		} catch (JunoException mex) {
			//mex.printStackTrace();
			AssertJUnit.assertTrue(false);
			LOGGER.debug("Exception occurs: " + mex.getMessage());
			AssertJUnit.assertTrue(mex.getMessage().contains("Response Timed out"));
			LOGGER.info("Exception", mex.getMessage());
			LOGGER.info("2");			
			LOGGER.info("Completed");
		}
	}

	/**
	 * Check Connection Timeout exception
	 * @throws JunoException
	 */
	//@Test
	public void testBatchCreateConnectionError() throws IOException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		URL url1 = BatchCreateTest.class.getResource("/com/paypal/juno/Juno.properties");
		pConfig1 = new Properties();
		pConfig1.load(url1.openStream());
		pConfig1.setProperty(JunoProperties.APP_NAME, "QATestApp");
		pConfig1.setProperty(JunoProperties.RECORD_NAMESPACE, "NS2");
		pConfig1.setProperty(JunoProperties.CONNECTION_TIMEOUT, "1");
		JunoReactClient junoReactClient1 = null;
		try {
			junoReactClient1 = JunoClientFactory.newJunoReactClient(new JunoPropertiesProvider(pConfig1), SSLUtil.getSSLContext());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		int numKeys = 100;
		byte[][] key = new byte[numKeys][];
		byte[][] payload = new byte[numKeys][];
		LOGGER.debug("Create " + numKeys + " keys using batch Create");
		List<JunoRequest> list = new ArrayList<>();
		for (int i = 0; i < numKeys; i ++) {
			key[i] = DataGenUtils.createKey(10).getBytes();
			payload[i] = DataGenUtils.genBytes(204000);				
			JunoRequest item = new JunoRequest(key[i], payload[i], (long)0, (long)20, JunoRequest.OperationType.Create);
			list.add(item);
		}
		try {
			Iterable <JunoResponse> batchResp = null;
                        if (syncFlag.reactSync.ordinal() == flag) {     //sync react call
                                batchResp = junoReactClient1.doBatch(list).toIterable();
                        } else {        //async react call
                                batchResp = BatchReactSubscriber.async_dobatch(junoReactClient1, list);
                        }
			AssertJUnit.assertTrue("Connection Error Exception not happening", false);
		} catch (JunoException mex) {
			LOGGER.debug("Exception occurs: " + mex.getMessage());
			AssertJUnit.assertTrue(mex.getMessage().contains("connection timed out"));
		}
	}
	
	/**
	 * This is negative test case only enable when junoserv config and juno property set to
	 * allow 10M payload and we'd like to see how large payload batch behave
	 * Verify appropriate JunoException is thrown
	 * @throws JunoException
	 */
	//@Test
	public void testBatchCreateMoreThan8MPayload() throws JunoException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		int numKeys = 10;
		LOGGER.debug("Create " + numKeys + " keys with a key having > 8MB payload");
		byte[][] key = new byte[numKeys][];
		long[] ttl = new long[numKeys];
		byte[][] payload = new byte[numKeys][];
		List<JunoRequest> list = new ArrayList<>();
		HashMap <String, byte[]> hmap = new HashMap <String, byte[]>();
		HashMap <String, Long> hmapTTL = new HashMap <String, Long>();
		
		for (int i = 0; i < numKeys; i ++) {
			key[i] = DataGenUtils.createKey(25).getBytes();
			payload[i] = DataGenUtils.genBytes(30);
			byte[] data = DataGenUtils.genBytes(88480100);
			payload[8] = data;
			ttl[i] = 20;		
			LOGGER.debug("key " + i + " is " + new String(key[i]));
			hmap.put(new String(key[i]), payload[i]);
			hmapTTL.put(new String(key[i]), ttl[i]);

			JunoRequest item = new JunoRequest(key[i], payload[i], (long)0, ttl[i], JunoRequest.OperationType.Create);
			list.add(item);
		}
		LOGGER.debug("\n===Batch Create is sent ");
		try {			
			Iterable <JunoResponse> resp = null;	
                        if (syncFlag.reactSync.ordinal() == flag) {     //sync react call
                                resp = junoActClient.doBatch(list).toIterable();
                        } else {        //async react call
                                resp = BatchReactSubscriber.async_dobatch(junoActClient, list);
                        }
		} catch (Exception mex) {
			LOGGER.debug("Exception occured: batch create fail due to large payload");
			AssertJUnit.assertTrue("batch create shouldn't fail", true);
		}
		
		//batch get
		List<JunoRequest> list1 = new ArrayList<>();
		Iterable <JunoResponse> gResp = new ArrayList<>();
		for (int j = 0; j < numKeys; j++) {
			JunoRequest item1 = new JunoRequest(key[j], (long)0, (long)0, JunoRequest.OperationType.Get);
			list1.add(item1);
		}	
		try {
                        if (syncFlag.reactSync.ordinal() == flag) {     //sync react call
                                gResp = junoActClient.doBatch(list1).toIterable();
                        } else {        //async react call
                                gResp = BatchReactSubscriber.async_dobatch(junoActClient, list1);
                        }
			int i=0;
			for (JunoResponse response: gResp) {	
				String mkey = new String(response.getKey());
				LOGGER.debug("mkey in get is " + mkey);
				if ( mkey.equals(new String(key[8]))) {
					AssertJUnit.assertEquals (OperationStatus.NoKey, response.getStatus());
				} else {
					AssertJUnit.assertEquals (OperationStatus.Success, response.getStatus());
					AssertJUnit.assertTrue(1 == response.getVersion());
					AssertJUnit.assertTrue(response.getTtl() <= hmapTTL.get(mkey) &&  response.getTtl() >= hmapTTL.get(mkey)-3 );
					AssertJUnit.assertEquals(response.getValue(), hmap.get(mkey));
					i++;
				}
			}
			AssertJUnit.assertEquals(i, numKeys-1);	
		} catch (Exception ex) {
			LOGGER.debug("Exception occured: batch get fail due to large payload?, shouldn't");
			AssertJUnit.assertTrue(false);	
			LOGGER.info("2");
		}
		
		LOGGER.info("0");
		LOGGER.info("Completed");
	}

	/**
	 * Create batch and update with compressed payload larger than max 2048000
	 * Verify appropriate JunoException is thrown
	 * @throws JunoException
	 */
	@Test
	public void testBatchUpdateExceedsCompressMax() throws Exception{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		int numKeys = 10;
		byte[][] key = new byte[numKeys][];
		long[] ttl = new long[numKeys];
		byte[][] payload = new byte[numKeys][];
		HashMap<String, byte[]> hmap = new HashMap<String, byte[]>();
		HashMap<String, String> hmapTTL = new HashMap<String, String>();
		HashMap<String, String> hmapTTL2 = new HashMap<String, String>();
		LOGGER.debug("Create " + numKeys + " keys using batch Set");
		List<JunoRequest> list = new ArrayList<>();
		for (int i = 0; i < numKeys; i ++) {
			key[i] = DataGenUtils.createKey(10).getBytes();
			String str = "Hello Testing, Happy Friday" + i;
			payload[i] = str.getBytes();
			payload[5] = DataGenUtils.createCompressablePayload(100000).getBytes();
			ttl[i]=1200;
			hmapTTL.put(new String(key[i]), String.valueOf(ttl[i]));
			JunoRequest item = new JunoRequest(key[i], payload[i], (long)0, ttl[i], JunoRequest.OperationType.Set);
			list.add(item);
		}
		try {
			Iterable<JunoResponse> batchResp = null;
                        if (syncFlag.reactSync.ordinal() == flag) {     //sync react call
                                batchResp = junoActClient.doBatch(list).toIterable();
                        } else {        //async react call
                                batchResp = BatchReactSubscriber.async_dobatch(junoActClient, list);
                        }
			int i = 0;
			for (JunoResponse mResponse: batchResp) {	
				LOGGER.debug("Key: " + i + ": "+ mResponse.getKey());
				String mKey = new String(mResponse.getKey());
				AssertJUnit.assertEquals (OperationStatus.Success,mResponse.getStatus());
				AssertJUnit.assertTrue(1 == mResponse.getVersion());
				AssertJUnit.assertTrue(Integer.parseInt(hmapTTL.get(mKey)) - 10 <= mResponse.getTtl() &&  mResponse.getTtl() <= Integer.parseInt(hmapTTL.get(mKey)));
				i++;
			}
			AssertJUnit.assertTrue(i == numKeys);
		} catch (JunoException mex) {			
			LOGGER.debug("Exception occurs: " + mex.getMessage());
			AssertJUnit.assertTrue ("Exception is thrown for batch create", false);	
		}
		byte[][] upayload = new byte[numKeys][];
		LOGGER.debug("Update " + numKeys + " keys using batch Update()");
		List<JunoRequest> ulist = new ArrayList<>();
		for (int i = 0; i < numKeys; i ++) {
			Random r = new Random();
	        int payloadlen = DataGenUtils.rand(r, 200, 204800);
			String str = "Update Hello Testing, Happy Friday" + i;
			upayload[i] = str.getBytes();
			if (i == 6) {
				upayload[i] = DataGenUtils.createCompressablePayload(800000).getBytes();
			}
			ttl[i] = 1200;			
			hmap.put(new String(key[i]), upayload[i]);
			hmapTTL2.put(new String(key[i]), String.valueOf(ttl[i]));
			JunoRequest uitem = new JunoRequest(key[i], upayload[i], (long)1, ttl[i], JunoRequest.OperationType.Update);
			ulist.add(uitem);
		}

		int dataLength = Snappy.compress(upayload[6]).length;
		LOGGER.info("upayload6 original length is " + upayload[6].length + "compressed length is " + dataLength);
			
		Iterable<JunoResponse> batchResp = null;
                if (syncFlag.reactSync.ordinal() == flag) {     //sync react call
                        batchResp = junoActClient.doBatch(ulist).toIterable();
                } else {        //async react call
                        batchResp = BatchReactSubscriber.async_dobatch(junoActClient, ulist);
                }
		for (JunoResponse mResponse: batchResp) {
			String mKey=new String(mResponse.getKey());
			String upayloadStr = new String(upayload[6]);
			String mkeyStr = new String(hmap.get(mKey));
			if ( new String(hmap.get(mKey)).equals(new String(upayload[6])) ) {			
				LOGGER.info("enter into if, value is " + upayloadStr.length() + ", payload6 is " +  mkeyStr.length());
				AssertJUnit.assertEquals (OperationStatus.IllegalArgument, mResponse.getStatus());
				LOGGER.info("2");
			} else {
				AssertJUnit.assertEquals (OperationStatus.Success,mResponse.getStatus());
				AssertJUnit.assertEquals (hmapTTL2.get(mKey), String.valueOf(mResponse.getTtl()));
				AssertJUnit.assertTrue(2 == mResponse.getVersion());
				LOGGER.info("0");
			}  
		}
		LOGGER.info("Completed");
	}
}	

