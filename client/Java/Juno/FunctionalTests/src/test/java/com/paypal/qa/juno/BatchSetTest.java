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
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
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
import rx.observables.BlockingObservable;

public class BatchSetTest{
	private JunoAsyncClient junoClient;
	private JunoAsyncClient asyncJunoClient;
	private JunoAsyncClient asyncJunoClient1;
	private JunoClient junoClient1;
	private JunoClient junoClient2;
	private Properties pConfig;
	private Properties pConfig1;
	private Properties pConfig2;	
	private Logger LOGGER;
	private int syncFlag;

	@BeforeClass
	public void setup() throws IOException, InterruptedException, CertificateException, KeyStoreException, NoSuchAlgorithmException, KeyManagementException {
		
		LOGGER = LoggerFactory.getLogger(BatchSetTest.class);

		URL url = BatchSetTest.class.getResource("/com/paypal/juno/Juno_batch.properties");
		pConfig = new Properties();
		pConfig.load(url.openStream());
		pConfig.setProperty(JunoProperties.APP_NAME, "QATestApp");
		pConfig.setProperty(JunoProperties.RECORD_NAMESPACE, "NS1");
		LOGGER.debug("Read syncFlag test to findout what needs to be run");
		String sync_flag = pConfig.getProperty("sync_flag_test", "0");
		LOGGER.debug("*********SYNC FLAG: " + sync_flag);
		syncFlag = Integer.parseInt(sync_flag.trim());
		junoClient = JunoClientFactory.newJunoAsyncClient(new JunoPropertiesProvider(pConfig), SSLUtil.getSSLContext());
		asyncJunoClient = JunoClientFactory.newJunoAsyncClient(url);

		URL url1 = BatchSetTest.class.getResource("/com/paypal/juno/Juno_batch.properties");
		pConfig1 = new Properties();
		pConfig1.load(url1.openStream());
		pConfig1.setProperty(JunoProperties.APP_NAME, "QATestApp");
		pConfig1.setProperty(JunoProperties.RECORD_NAMESPACE, "NS2");
		junoClient1 = JunoClientFactory.newJunoClient(new JunoPropertiesProvider(pConfig1), SSLUtil.getSSLContext());
		URL url2 = BatchSetTest.class.getResource("/com/paypal/juno/Juno_batch.properties");
		pConfig2 = new Properties();
		pConfig2.load(url2.openStream());
		pConfig2.setProperty(JunoProperties.APP_NAME, "QATestApp2");
		pConfig2.setProperty(JunoProperties.RECORD_NAMESPACE, "NS1");
		junoClient2 = JunoClientFactory.newJunoClient(new JunoPropertiesProvider(pConfig2), SSLUtil.getSSLContext());

	

		Thread.sleep(3000);
		
	}

	@AfterClass
	public  void cleanSetup() throws Exception{
		
	}

	/**
	 * Create and get multiple keys using batch create
	 * @throws JunoException  //TODO: Ask so during set, what version passed in doesn't matter? 
	 */
	@Test
	public void testBatchCreateSet() throws JunoException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	    
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		int numKeys = 10;
		byte[][] key = new byte[numKeys][];
		Long[] ttl = new Long[numKeys];
		byte[][] payload = new byte[numKeys][];
		HashMap<String, byte[]> hmap = new HashMap<String, byte[]>();
		LOGGER.debug("Create " + numKeys + " keys using existing Juno Create, no lifetime");
		for (int i = 0; i < numKeys; i ++) {
			key[i] = DataGenUtils.createKey(10).getBytes();
			key[numKeys-1] = DataGenUtils.createKey(128).getBytes();
			Random r = new Random();
	        int payloadlen = DataGenUtils.rand(r, 200, 204800);
			payload[i] = DataGenUtils.createKey(payloadlen).getBytes();
			payload[numKeys-1] = DataGenUtils.createKey(204800).getBytes();
			JunoResponse mResponse = junoClient2.create(key[i], payload[i]);
			AssertJUnit.assertEquals (OperationStatus.Success,mResponse.getStatus());			
		}

		Long[] ttl1 = new Long[numKeys];
		byte[][] payload1 = new byte[numKeys][];
		LOGGER.debug("Calling Batch set with lifetime");
		List<JunoRequest> list = new ArrayList<>();
		for (int i = 0; i < numKeys; i ++) {
			Random r = new Random();
	        int payloadlen = DataGenUtils.rand(r, 200, 204800);
			payload1[i] = DataGenUtils.createKey(payloadlen).getBytes();
			payload1[numKeys-1] = DataGenUtils.createKey(204800).getBytes();
			ttl1[i] = 20L;			
			hmap.put(new String(key[i]), payload1[i]);
			JunoRequest item1 = new JunoRequest(key[i], payload1[i], (long)0, ttl1[i], System.currentTimeMillis(), JunoRequest.OperationType.Set);
			list.add(item1);
		}
		try {
			Iterable<JunoResponse> batchResp = null;
			if (syncFlag == 1) {
				batchResp = junoClient.doBatch(list).toBlocking().toIterable();
			} else {
				batchResp = BatchTestSubscriber.async_dobatch(junoClient, list);
			}
			int i = 1;
			for (JunoResponse mResponse: batchResp) {				
				LOGGER.debug("Key: " + i + ": "+ new String(mResponse.getKey()));
				AssertJUnit.assertEquals (OperationStatus.Success,mResponse.getStatus());
				AssertJUnit.assertTrue(2 == mResponse.getVersion());
				i++;
			}
		} catch (JunoException mex) {
			LOGGER.debug("Exception occurs: " + mex.getMessage());
		}
		//Read Item List
		List<JunoRequest> list2 = new ArrayList<>();
		for (int i = 0; i < numKeys; i ++) {
			JunoRequest item2 = new JunoRequest(key[i],(long)0, (long)0, JunoRequest.OperationType.Get );
			list2.add(item2);
		}

		LOGGER.debug("Read " + numKeys + " keys using batch Get()");
		Iterable<JunoResponse> getBatchResp = junoClient.doBatch(list2).toBlocking().toIterable();	
		if (syncFlag == 1) {
			getBatchResp = junoClient.doBatch(list2).toBlocking().toIterable();
		} else {
			getBatchResp = BatchTestSubscriber.async_dobatch(junoClient, list2);
		}
		int i = 0;
		for (JunoResponse response: getBatchResp) {		
			String mkey = new String(response.getKey());
			LOGGER.debug("Key: " + i + ": "+ response.getKey());
			LOGGER.debug("Data: " + new String(response.getValue()));
			AssertJUnit.assertTrue(2 == response.getVersion());
			AssertJUnit.assertEquals (OperationStatus.Success, response.getStatus());
			AssertJUnit.assertEquals(new String(hmap.get(mkey)), new String(response.getValue()));
			i++;
		}
		AssertJUnit.assertEquals(i,numKeys);
		LOGGER.info("0");
		LOGGER.info("Completed");
	}

	/**
	 * Set batch keys with 1 empty key
	 * Verify appropriate JunoException is thrown
	 * @throws JunoException
	 */
	@Test
	public void testBatchSetEmptyKeys() throws JunoException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		int numKeys = 10;
		LOGGER.debug("Batch set " + numKeys + " keys with one zero-length key");
		byte[][] key = new byte[numKeys][];
                long[] ttl = new long[numKeys];
		byte[][] payload = new byte[numKeys][];
		List<JunoRequest> list = new ArrayList<>();
		for (int i = 0; i < numKeys; i ++) {
			key[i] = DataGenUtils.createKey(127).getBytes();
                        ttl[i] = 20;
                        key[9] = "".getBytes();
			String str = "Hello Testing testing " + i;
			payload[i] = str.getBytes();
			//System.out.println(" i is " + i + ", key[i] is " + new String(key[i]));	
			JunoRequest item = new JunoRequest(key[i], payload[i], (long)0, ttl[i], JunoRequest.OperationType.Set);
			list.add(item);
		}
		try{
			for (int i = 0; i < numKeys; i ++) {
				//System.out.println(" i is " + i + ", key[i] is " + new String(key[i]));
			}
			LOGGER.debug("\n===Batch Set is sent ");
                        Iterable<JunoResponse> batchResp;
			if (syncFlag == 1) {
				batchResp=junoClient.doBatch(list).toBlocking().toIterable();
			} else {
				batchResp=BatchTestSubscriber.async_dobatch(asyncJunoClient, list);
			}
                        for (JunoResponse response: batchResp) {
                            if (response != null && response.getKey() != null && response.getKey() != key[9]) {
				//System.out.println(" mkey is " + new String(response.getKey()));
                                AssertJUnit.assertEquals (OperationStatus.Success, response.getStatus());
                            } else {
                                AssertJUnit.assertEquals (OperationStatus.IllegalArgument, response.getStatus());
                            }
                        }
		} catch (IllegalArgumentException mex) {
			LOGGER.debug("Exception occurs: " + mex.getMessage());
			AssertJUnit.assertTrue(mex.getMessage().contains("The Document key must not be null or empty"));
			LOGGER.info("Exception", mex.getMessage());
			LOGGER.info("2");			
			LOGGER.info("Completed");
		}
	}

	/**
	 * Set batch keys with 2 NULL keys
	 * Verify appropriate JunoException is thrown
	 * @throws JunoException
	 */
	@Test
	public void testBatchSetNullKeys() throws JunoException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		int numKeys = 10;
		LOGGER.debug("Batch Set " + numKeys + " keys with two null keys");
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
			//intentionally use below value for set
			JunoRequest item = new JunoRequest(key[i], payload[i], (long)0, (long)ttl[i], System.currentTimeMillis(), JunoRequest.OperationType.Set);
			list.add(item);
		}
		LOGGER.debug("\n===Batch Set is sent ");
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
			AssertJUnit.assertTrue(mex.getMessage().contains("Key must not be null"));
			LOGGER.info("Exception", mex.getMessage());
			LOGGER.info("2");			
			LOGGER.info("Completed");
		}
	}

	/**
	 * Set batch keys with keys >=257 keys
	 * Verify appropriate JunoException is thrown
	 * @throws JunoException
	 */
	@Test
	public void testBatchSet257BytesKeys() throws JunoException
	{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	    
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		int numKeys = 10;
		LOGGER.debug("Batch set " + numKeys + " keys with more than 1 key >=  257 bytes");
		byte[][] key = new byte[numKeys][];
		long[] ttl = new long[numKeys];
		byte[][] payload = new byte[numKeys][];
		List<JunoRequest> list = new ArrayList<>();
		HashMap <String, byte[]> hmap = new HashMap<String, byte[]>();
		
		key[1] = DataGenUtils.createKey(259).getBytes();
		key[9] = DataGenUtils.createKey(257).getBytes();
		for (int i = 0; i < numKeys; i ++) {
			if ( i != 1 && i != 9 ) {
				key[i] = DataGenUtils.createKey(128).getBytes();
			}
			String str = "Hello Testing testing " + i;
			payload[i] = str.getBytes();
			ttl[i] = 20;		
			hmap.put(new String(key[i]), payload[i]);
			JunoRequest item = new JunoRequest(key[i], payload[i], (long)0, ttl[i], JunoRequest.OperationType.Set);
			list.add(item);
		}
		LOGGER.debug("\n===Batch Create is sent ");
		try {
			if (syncFlag == 1) {
				BlockingObservable resp = junoClient.doBatch(list).toBlocking();
				resp.subscribe();
			} else {
				BatchTestSubscriber.async_dobatch(asyncJunoClient, list);
			}
		} catch (JunoException mex) {
			AssertJUnit.assertTrue ("Exception should not thrown for key with > 256 bytes long happen on one item only", false);
		}
		
		ArrayList <JunoRequest> getList = new ArrayList<>();
		for (int i = 0; i < numKeys; i ++) {
			JunoRequest item = new JunoRequest(key[i], null, (long)0, (long)0, JunoRequest.OperationType.Get);
			getList.add(item);
		}
		Iterable<JunoResponse> getBatchResponse;
		try {
			if (syncFlag == 1) {
				getBatchResponse = junoClient.doBatch(getList).toBlocking().toIterable();
			} else {
				getBatchResponse = BatchTestSubscriber.async_dobatch(asyncJunoClient, getList);
			}
			int i = 0;
			for (JunoResponse response: getBatchResponse) {		
				String mkey = new String(response.getKey());
				if (mkey.equals(new String(key[1])) || mkey.equals(new String(key[9]))) {
					AssertJUnit.assertEquals (OperationStatus.IllegalArgument, response.getStatus());
				} else {
					AssertJUnit.assertEquals (OperationStatus.Success, response.getStatus());
					AssertJUnit.assertTrue(1 == response.getVersion());
					AssertJUnit.assertEquals(hmap.get(mkey), response.getValue());
					i++;
				}
			}
			AssertJUnit.assertEquals(i,numKeys-2);
			LOGGER.info("0");			
			LOGGER.info("Completed");
		} catch (JunoException mex) {
			LOGGER.debug("Exception occurs at reset: " + mex.getCause().getMessage());
			AssertJUnit.assertTrue(false);
			LOGGER.info("2");			
			LOGGER.info("Completed");
		}	
	}

	/**
	 * Set batch keys with keys having 0 or null payload
	 * Verify appropriate JunoException is thrown
	 * @throws JunoException
	 */
	@Test
	public void testBatchSetZeroPayload() throws JunoException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	    
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		int numKeys = 10;
		LOGGER.debug("Create " + numKeys + " keys with some keys having zero payload");
		byte[][] key = new byte[numKeys][];
		long[] ttl = new long[numKeys];
		byte[][] payload = new byte[numKeys][];
		List<JunoRequest> list = new ArrayList<>();
		HashMap<String, byte[]> hmap = new HashMap<String, byte[]>();
		
		for (int i = 0; i < numKeys; i ++) {
			key[i] = DataGenUtils.createKey(25).getBytes();
			String str = DataGenUtils.createKey(25);
			payload[i] = str.getBytes();
			payload[5] = "".getBytes();
			ttl[i] = 20;	
			hmap.put(new String(new String(key[i])), payload[i]);

			JunoRequest item = new JunoRequest(key[i], payload[i], (long)0, ttl[i], JunoRequest.OperationType.Set);
			list.add(item);
		}
		LOGGER.debug("\n===Batch Set is sent ");
		if (syncFlag == 1) {				
			BlockingObservable<JunoResponse> resp = junoClient.doBatch(list).toBlocking();
			resp.subscribe();
		} else {
			BatchTestSubscriber.async_dobatch(asyncJunoClient, list);
		}
		
		List<JunoRequest> list1 = new ArrayList<>();
		for (int i = 0; i < numKeys; i ++) {		
			JunoRequest item1 = new JunoRequest(key[i], null, 0, 0, JunoRequest.OperationType.Get);
			list1.add(item1);
		}
		
		Iterable <JunoResponse> getBatchResponse;
		if (syncFlag == 1) {				
			getBatchResponse = junoClient.doBatch(list1).toBlocking().toIterable();
		} else {
			getBatchResponse = BatchTestSubscriber.async_dobatch(asyncJunoClient, list1);
		}
		
		int j = 0;
		for (JunoResponse response: getBatchResponse) {	
			byte[] mkey=response.getKey();
			AssertJUnit.assertEquals (OperationStatus.Success, response.getStatus());
			AssertJUnit.assertTrue(1 == response.getVersion());
			AssertJUnit.assertEquals(hmap.get(new String(mkey)), response.getValue());
			j++;
		} 
		AssertJUnit.assertEquals(j, numKeys);	
		LOGGER.info("0");
		LOGGER.info("Completed");
	}

	/**
	 * Set batch keys with keys having null payload
	 * Verify appropriate JunoException is thrown
	 * @throws JunoException
	 */
	@Test
	public void testBatchSetNullPayload() throws JunoException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	    
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		int numKeys = 10;
		LOGGER.debug("Set " + numKeys + " keys with some keys having null payload");
		byte[][] key = new byte[numKeys][];
		long[] ttl = new long[numKeys];
		byte[][] payload = new byte[numKeys][];
		HashMap <String, byte[]> hmap = new HashMap <String, byte[]>();
		List<JunoRequest> list = new ArrayList<>();
		List<JunoRequest> list1 = new ArrayList<>();
		List<JunoRequest> getList = new ArrayList<>();
		for (int i = 0; i < numKeys; i ++) {
			key[i] = DataGenUtils.createKey(25).getBytes();
			String str = "Hello Testing testing " + i;
			payload[i] = str.getBytes();
			payload[0] = null;
			payload[4] = "".getBytes();
			hmap.put(new String(key[i]), payload[i]);
			ttl[i] = 20;		

			JunoRequest item = new JunoRequest(key[i], payload[i], (long)0, ttl[i], JunoRequest.OperationType.Set);
			list.add(item);
		}
		LOGGER.debug("\n===Batch Set is sent ");
		try {
			if (syncFlag == 1) {
				BlockingObservable <JunoResponse> resp = junoClient.doBatch(list).toBlocking();
				resp.subscribe();
			} else {
				BatchTestSubscriber.async_dobatch(asyncJunoClient, list);
			}
		} catch (JunoException mex) {
			AssertJUnit.assertTrue ("Exception should not thrown for null payload on just one item", false);
		}
		
		for (int i = 0; i < numKeys; i ++) {
			JunoRequest item = new JunoRequest(key[i], payload[i], (long)0, ttl[i], JunoRequest.OperationType.Set);
			list1.add(item);
		}
		Iterable<JunoResponse> setBatchResponse;
		try {
			if (syncFlag == 1) {
				setBatchResponse = junoClient.doBatch(list1).toBlocking().toIterable();
			} else {
				setBatchResponse = BatchTestSubscriber.async_dobatch(asyncJunoClient, list1);
			}
			int i = 0;
			for (JunoResponse response: setBatchResponse) {		
				String mkey = new String(response.getKey());
				if (mkey.equals(new String(key[0]))) {
					AssertJUnit.assertEquals (OperationStatus.Success, response.getStatus());
					AssertJUnit.assertEquals (0, response.getValue().length);
				} else {
					AssertJUnit.assertEquals (OperationStatus.Success, response.getStatus());
					AssertJUnit.assertTrue(2 == response.getVersion());
					i++;
				}
			}
			AssertJUnit.assertEquals(i,numKeys-1);
			LOGGER.info("0");			
		} catch (JunoException mex) {
			LOGGER.debug("Exception occurs at reset: " + mex.getCause().getMessage());
			AssertJUnit.assertTrue(false);
			LOGGER.info("2");			
		}	
		
		for (int i = 0; i < numKeys; i ++) {
			JunoRequest item = new JunoRequest(key[i], null, (long)0, (long)0, JunoRequest.OperationType.Get);
			getList.add(item);
		}
		Iterable<JunoResponse> getBatchResponse;
		try {
			if (syncFlag == 1) {
				getBatchResponse = junoClient.doBatch(getList).toBlocking().toIterable();
			} else {
				getBatchResponse = BatchTestSubscriber.async_dobatch(asyncJunoClient, getList);
			}
			int i = 0;
			for (JunoResponse response: getBatchResponse) {		
				String mkey = new String(response.getKey());
				if (mkey.equals(new String(key[0]))) {
					AssertJUnit.assertEquals (OperationStatus.Success, response.getStatus());
					AssertJUnit.assertEquals (0, response.getValue().length);
				} else {
					AssertJUnit.assertEquals (OperationStatus.Success, response.getStatus());
					AssertJUnit.assertTrue(2 == response.getVersion());
					LOGGER.debug("hmap value is " + hmap.get(mkey) + " " + "response value is " + response.getValue() + " i is " + i);
					AssertJUnit.assertEquals(hmap.get(mkey), response.getValue());
					i++;
				}
			}
			AssertJUnit.assertEquals(i,numKeys-1);
			LOGGER.info("0");			
			LOGGER.info("Completed");
		} catch (JunoException mex) {
			LOGGER.debug("Exception occurs at reset: " + mex.getCause().getMessage());
			AssertJUnit.assertTrue(false);
			LOGGER.info("2");			
			LOGGER.info("Completed");
		}		
	}

	/**
	 * Set batch keys with a key having > 200KB payload
	 * Verify appropriate JunoException is thrown
	 * @throws JunoException
	 */
	@Test
	public void testBatchSetMoreThan200KPayload() throws JunoException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		int numKeys = 10;
		LOGGER.debug("Set " + numKeys + " keys with a key having > 200KB payload");
		byte[][] key = new byte[numKeys][];
		long[] ttl = new long[numKeys];
		byte[][] payload = new byte[numKeys][];
		List<JunoRequest> list = new ArrayList<>();
		HashMap <String, byte[]> hmap = new HashMap<String, byte[]>();
		
		for (int i = 0; i < numKeys; i ++) {
			key[i] = DataGenUtils.createKey(25).getBytes();
			payload[i] = DataGenUtils.genBytes(204801);
			payload[numKeys-1] = DataGenUtils.genBytes(20);
			ttl[i] = 20;		
			hmap.put(new String(key[i]), payload[i]);
			JunoRequest item = new JunoRequest(key[i], payload[i], (long)0, (long)0, JunoRequest.OperationType.Set);
			list.add(item);
		}
		LOGGER.debug("\n===Batch Set is sent ");
		try {
			if (syncFlag == 1) {
				BlockingObservable resp = junoClient.doBatch(list).toBlocking();
				resp.subscribe();
			} else {
				BatchTestSubscriber.async_dobatch(asyncJunoClient, list);
			}
		} catch (JunoException mex) {
			AssertJUnit.assertTrue ("Exception should not thrown for big payload happened on one item", false);
		}
		
		ArrayList <JunoRequest> getList = new ArrayList<JunoRequest>();
		for (int i = 0; i < numKeys; i ++) {
			JunoRequest item = new JunoRequest(key[i], null, (long)0, (long)0, JunoRequest.OperationType.Get);
			getList.add(item);
		}
		Iterable<JunoResponse> getBatchResponse;
		try {
			if (syncFlag == 1) {
				getBatchResponse = junoClient.doBatch(getList).toBlocking().toIterable();
			} else {
				getBatchResponse = BatchTestSubscriber.async_dobatch(asyncJunoClient, getList);
			}
			int i = 0;
			for (JunoResponse response: getBatchResponse) {		
				String mkey = new String(response.getKey());
				if (mkey.equals(new String(key[numKeys-1]))) {
					AssertJUnit.assertEquals (OperationStatus.Success, response.getStatus());
					AssertJUnit.assertTrue(1 == response.getVersion());
					AssertJUnit.assertEquals(hmap.get(mkey), response.getValue());
				} else {
					AssertJUnit.assertEquals (OperationStatus.NoKey, response.getStatus());
					i++;
				}
			}
			AssertJUnit.assertEquals(i,numKeys-1);
			LOGGER.info("0");			
			LOGGER.info("Completed");
		} catch (JunoException mex) {
			LOGGER.debug("Exception occurs at reset: " + mex.getCause().getMessage());
			AssertJUnit.assertTrue(false);
			LOGGER.info("2");			
			LOGGER.info("Completed");
		}
	}	/**
	 * Create batch keys with a key having TTL more than 3 days
	 * Verify appropriate JunoException is thrown
	 * @throws JunoException
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
			payload[i] = DataGenUtils.createKey(25).getBytes();
			ttl[i] = 20;
			ttl[1] = 259201;
			hmapTTL.put(new String(key[i]), ttl[i]);
			JunoRequest item = new JunoRequest(key[i], payload[i], (long)0, ttl[i], System.currentTimeMillis(), JunoRequest.OperationType.Set);
			list.add(item);
		}
		LOGGER.debug("\n===Batch Set is sent ");
		try {
			if (syncFlag == 1) {
				BlockingObservable resp = junoClient.doBatch(list).toBlocking();
				resp.subscribe();
			} else {
				BatchTestSubscriber.async_dobatch(asyncJunoClient, list);
			}
		} catch (JunoException mex) {
			AssertJUnit.assertTrue ("Exception is not thrown for TTL > 3 days", false);
		}
		
		ArrayList <JunoRequest> list1 = new ArrayList<> ();
		LOGGER.debug("Read " + numKeys + " keys using batch Get()");
		for (int i = 0; i < numKeys; i ++) {
			JunoRequest item1 = new JunoRequest(key[i], null, (long)0, ttl[i], JunoRequest.OperationType.Get);
			list1.add(item1);
		}
		Iterable<JunoResponse> getBatchResp = null;
		try {
			if (syncFlag == 1) {
				LOGGER.debug("before blocking mode batch get " + System.currentTimeMillis());
				getBatchResp = junoClient.doBatch(list1).toBlocking().toIterable();					
			} else {
				LOGGER.debug("before non-blocking mode batch get " + System.currentTimeMillis());
				getBatchResp = BatchTestSubscriber.async_dobatch(asyncJunoClient, list1);
			}
			LOGGER.debug("after batch get " + System.currentTimeMillis());
			int i = 0;
			for (JunoResponse response: getBatchResp) {				
				String mkey = new String(response.getKey());
				if (mkey.equals(new String(key[1]))) {
					AssertJUnit.assertEquals(OperationStatus.IllegalArgument, response.getStatus());
				} else {
					AssertJUnit.assertTrue(OperationStatus.Success == response.getStatus());
					AssertJUnit.assertTrue(1 == response.getVersion());					
					AssertJUnit.assertTrue(hmapTTL.get(mkey)-3 <= response.getTtl()  && response.getTtl() <= hmapTTL.get(mkey));
					i++;
				}
			}
			AssertJUnit.assertEquals(i, numKeys-1);
		} catch (JunoException ex) {
			LOGGER.debug("Exception occured:" + ex.getMessage() );
			AssertJUnit.assertTrue(false);
		}
	}

	/**
	 * Create batch keys with different TTL and payload
	 * One key has max payload and TTL
	 * Verify appropriate JunoException is thrown
	 * @throws JunoException
	 */
	@Test
	public void testBatchSetDiffLifeTimePayloads() throws JunoException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	    
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		int numKeys = 10;
		byte[][] key = new byte[numKeys][];
		long[] ttl = new long[numKeys];
		byte[][] payload = new byte[numKeys][];
		HashMap<String, byte[]> hmap = new HashMap<String, byte[]>();
		LOGGER.debug("Create " + numKeys + " keys using batch Create");
		Random r = new Random();
		long ttl1 = DataGenUtils.rand(r, 200, 86400);
		LOGGER.debug("ttl1: " + ttl1);

		List<JunoRequest> list = new ArrayList<>();
		for (int i = 0; i < numKeys; i ++) {
			key[i] = DataGenUtils.createKey(DataGenUtils.rand(r, 1, 128)).getBytes();
			payload[i] = DataGenUtils.genBytes(DataGenUtils.rand(r, 1, 2048));
			payload[5] = DataGenUtils.genBytes(204800);
			ttl[i] = DataGenUtils.rand(r, 200, 86400);	
			ttl[5] = 259200;
			hmap.put(new String(key[i]), payload[i]);
			JunoRequest item = new JunoRequest(key[i], payload[i], (long)0, ttl[i], JunoRequest.OperationType.Set);
			list.add(item);
		}
		try {
			Iterable<JunoResponse> batchResp = null;
			if (syncFlag == 1) {
				batchResp = junoClient.doBatch(list).toBlocking().toIterable();
			} else {
				batchResp = BatchTestSubscriber.async_dobatch(asyncJunoClient, list);
			}

			int i = 0;
			for (JunoResponse mResponse: batchResp) {				
				//LOGGER.debug("Key: " + i + ": "+ mResponse.getKey());
				AssertJUnit.assertTrue(1 == mResponse.getVersion());
				AssertJUnit.assertEquals (OperationStatus.Success,mResponse.getStatus());
				i++;
			}
			AssertJUnit.assertEquals(i, numKeys);
		} catch (JunoException mex) {
			//LOGGER.debug("Error code: " + mex.getOperationStatus().getCode());
			LOGGER.debug("Exception occurs: " + mex.getMessage());
		}
		LOGGER.debug("Read " + numKeys + " keys using batch Get()");
		List<JunoRequest> list1 = new ArrayList<>();
		for (int i = 0; i < numKeys; i ++) {
			JunoRequest item = new JunoRequest(key[i], null, (long)0, 0, JunoRequest.OperationType.Get);
			list1.add(item);
		}
		Iterable<JunoResponse> getBatchResp;
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
		LOGGER.info("0");
		LOGGER.info("Completed");
	}

	/**
	 * Perform set batch keys without TTL
	 * Verify appropriate JunoException is thrown
	 * @throws JunoException  //TODO: ask create can not have 0 TTL while set can? Why it's 3600 not 1800??
	 */
	@Test
	public void testBatchSetNoLifeTime() throws JunoException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		int numKeys = 10;
		byte[][] key = new byte[numKeys][];
		byte[][] payload = new byte[numKeys][];
		HashMap<String, byte[]> hmap = new HashMap<String, byte[]>();
		LOGGER.debug("Create " + numKeys + " keys using batch Create");
		Random r = new Random();

		List<JunoRequest> list = new ArrayList<>();
		for (int i = 0; i < numKeys; i ++) {
			key[i] = DataGenUtils.createKey(DataGenUtils.rand(r, 1, 128)).getBytes();
			payload[i] = DataGenUtils.genBytes(DataGenUtils.rand(r, 1, 4048));			
			hmap.put(new String(key[i]), payload[i]);
			JunoRequest item = new JunoRequest(key[i], payload[i], (long)0, (long)0, JunoRequest.OperationType.Set);
			list.add(item);
		}
		try {
			Iterable<JunoResponse> batchResp = null;
			if (syncFlag == 1) {
				batchResp = junoClient.doBatch(list).toBlocking().toIterable();
			} else {
				batchResp = BatchTestSubscriber.async_dobatch(junoClient, list);
			}
			int i = 0;
			for (JunoResponse mResponse: batchResp) {				
				AssertJUnit.assertEquals (OperationStatus.Success,mResponse.getStatus());
				AssertJUnit.assertTrue(1 == mResponse.getVersion());
				i++;
			}
			AssertJUnit.assertEquals(i, numKeys);
		} catch (JunoException mex) {
			AssertJUnit.assertTrue (false);

		}
		LOGGER.debug("Read " + numKeys + " keys using existing Juno read()");
		List<JunoRequest> list1 = new ArrayList<>();
		for (int i = 0; i < numKeys; i++) {		
			JunoResponse response = junoClient2.get(key[i]);	
			AssertJUnit.assertTrue(1 == response.getVersion());
			AssertJUnit.assertEquals (OperationStatus.Success, response.getStatus());
			AssertJUnit.assertEquals (new String(payload[i]), new String (response.getValue()));
			//System.out.println("TTL in Response :"+response.getTtl());
			AssertJUnit.assertTrue (1800 - 8 <= response.getTtl() && response.getTtl() <= 1800);
		}
		LOGGER.info("0");
		LOGGER.info("Completed");
	}

	/**
	 * Perform batch set keys with no Item in the JunoRequest list
	 * Verify appropriate JunoException is thrown
	 * @throws JunoException //TODO: same question as testBatchGetZeroItem
	 */
	//@Test
	public void testBatchSetZeroItem() throws JunoException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	        
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		LOGGER.debug("Send 0 item to Batch create");

		List<JunoRequest> list = new ArrayList<>();
		LOGGER.debug("\n===Batch Create is sent ");
		try {
			junoClient.doBatch (list).toBlocking().toIterable();
			AssertJUnit.assertTrue ("Exception is not thrown for no key in the Juno Request", false);
		} catch (IllegalArgumentException mex) {
			LOGGER.debug("Exception occurs: " + mex.getMessage());
			AssertJUnit.assertTrue(mex.getMessage().contains("Empty juno request list"));
		}
	}
}
