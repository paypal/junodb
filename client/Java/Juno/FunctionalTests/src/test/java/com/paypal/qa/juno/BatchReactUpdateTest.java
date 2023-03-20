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

public class BatchReactUpdateTest{
	private JunoReactClient junoActClient;
	private JunoReactClient junoReactClient;
	private JunoClient junoClient2;
	private Properties pConfig;
	private Properties pConfig2;	
        private enum syncFlag {reactAsync, reactSync};
        private int flag;

	private Logger LOGGER;

	@BeforeClass
	public void setup() throws IOException, InterruptedException, CertificateException, KeyStoreException, NoSuchAlgorithmException, KeyManagementException {
		
		LOGGER = LoggerFactory.getLogger(BatchUpdateTest.class);
		URL url = BatchUpdateTest.class.getResource("/com/paypal/juno/Juno_batch.properties");
		pConfig = new Properties();
		pConfig.load(url.openStream());
                LOGGER.debug("Read syncFlag");
                String sync_flag = pConfig.getProperty("sync_flag_test", "0");
                LOGGER.debug("*********SYNC FLAG: " + sync_flag);
                flag = Integer.parseInt(sync_flag.trim());
		pConfig.setProperty(JunoProperties.APP_NAME, "QATestApp");
		pConfig.setProperty(JunoProperties.RECORD_NAMESPACE, "junotoken");
		junoActClient = JunoClientFactory.newJunoReactClient(new JunoPropertiesProvider(pConfig), SSLUtil.getSSLContext());
		junoReactClient = JunoClientFactory.newJunoReactClient(url);

		URL url2 = BatchSetTest.class.getResource("/com/paypal/juno/Juno_batch.properties");
		pConfig2 = new Properties();
		pConfig2.load(url2.openStream());
		pConfig2.setProperty(JunoProperties.APP_NAME, "QATestApp2");
		pConfig2.setProperty(JunoProperties.RECORD_NAMESPACE, "junotoken");
		junoClient2 = JunoClientFactory.newJunoClient(new JunoPropertiesProvider(pConfig2), SSLUtil.getSSLContext());

	

		Thread.sleep(3000);

	}

	@AfterClass
	public  void cleanSetup() throws Exception{
		
	}

	/**
	 * Batch Create and Batch Update to update lifetime and payload
	 * @throws JunoException
	 */
	@Test
	public void testBatchUpdatePayloadLifetime() throws JunoException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		int numKeys = 5;
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
			ttl[i]=50;
			hmapTTL.put(new String(key[i]), String.valueOf(ttl[i]));
			JunoRequest item = new JunoRequest(key[i], payload[i], (long)0, ttl[i], JunoRequest.OperationType.Set);
			list.add(item);
		}
		try {
			Iterable<JunoResponse> batchResp;
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
	        upayload[i] = DataGenUtils.createKey(payloadlen).getBytes();
	        upayload[numKeys-1] = DataGenUtils.createKey(204800).getBytes();
			ttl[i] = 100;			
			hmap.put(new String(key[i]), upayload[i]);
			hmapTTL2.put(new String(key[i]), String.valueOf(ttl[i]));
			
			JunoRequest uitem = new JunoRequest(key[i], upayload[i], (long)1, ttl[i], JunoRequest.OperationType.Update);
			ulist.add(uitem);
		}

		try {
			Iterable<JunoResponse> batchResp = null;
                        if (syncFlag.reactSync.ordinal() == flag) {     //sync react call
                                batchResp = junoActClient.doBatch(ulist).toIterable();
                        } else {        //async react call
                                batchResp = BatchReactSubscriber.async_dobatch(junoActClient, ulist);
                        }

			int i = 0;
			for (JunoResponse mResponse: batchResp) {	
				LOGGER.debug("Key: " + i + ": "+ mResponse.getKey());
				AssertJUnit.assertEquals (OperationStatus.Success,mResponse.getStatus());
				AssertJUnit.assertTrue(2 == mResponse.getVersion());
				i++;
			}
			AssertJUnit.assertTrue(i == numKeys);			
		} catch (JunoException mex) {
			AssertJUnit.assertTrue ("Exception is thrown for batch update", false);	
		}

		LOGGER.debug("Get key using exisitng exising Juno read()");
		for (int i = 0; i < numKeys; i ++) {
			JunoResponse junoResponse = junoClient2.get(key[i]);
			String mKey = new String(junoResponse.getKey());
			AssertJUnit.assertEquals(key[i], junoResponse.getKey());
			AssertJUnit.assertTrue(2 == junoResponse.getVersion());
			AssertJUnit.assertEquals(new String(upayload[i]), new String(junoResponse.getValue()));
			AssertJUnit.assertTrue(Integer.parseInt(hmapTTL2.get(mKey)) - 10 <= junoResponse.getTtl() &&  junoResponse.getTtl() <= Integer.parseInt(hmapTTL2.get(mKey)));
		}
		LOGGER.info("0");
		LOGGER.info("Completed");	
	}

	/**
	 * Verify appropriate JunoException is thrown when doing Batch Update with a key having zero length
	 * @throws JunoException
	 */
	@Test
	public void testBatchUpdateEmptyKey() throws JunoException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		int numKeys = 5;
		byte[][] key = new byte[numKeys][];
                long[] ttl = new long[numKeys];
		byte[][] payload = new byte[numKeys][];

		LOGGER.debug("Create request item with 1 key having zero length");
		List<JunoRequest> list = new ArrayList<>();
                List<JunoRequest> updateList = new ArrayList<>();
		
		for (int i = 0; i < numKeys; i ++) {
			Random r = new Random();
			key[i] = DataGenUtils.createKey(DataGenUtils.rand(r, 1, 128)).getBytes();
			payload[i] = DataGenUtils.genBytes(DataGenUtils.rand(r, 1, 1028));
			key[4] = "".getBytes();
                        ttl[i] = 20;
			JunoRequest item = new JunoRequest(key[i], payload[i], (long)0, ttl[i], JunoRequest.OperationType.Create);
			JunoRequest updateItem = new JunoRequest(key[i], payload[i], (long)0, ttl[i], JunoRequest.OperationType.Update);
			list.add(item);
			updateList.add(updateItem);
		}
		try{
                        Iterable<JunoResponse> batchResp;	
                        if (syncFlag.reactSync.ordinal() == flag) {     //sync react call
                                batchResp=junoActClient.doBatch(list).toIterable();
                        } else {        //async react call
                                batchResp=BatchReactSubscriber.async_dobatch(junoActClient, list);
                        }
                        for (JunoResponse response: batchResp) {
                            if (response != null && response.getKey() != null && response.getKey() != key[4]) {
                                    AssertJUnit.assertEquals(OperationStatus.Success, response.getStatus());
                            } else {
                                AssertJUnit.assertEquals(OperationStatus.IllegalArgument, response.getStatus());
                            }
                        }
		} catch (IllegalArgumentException mex) {
			LOGGER.debug("Exception occurs: " + mex.getMessage());
			AssertJUnit.assertTrue(mex.getMessage().contains("The Document key must not be null or empty"));
			LOGGER.info("Exception", mex.getMessage());
			LOGGER.info("2");			
			LOGGER.info("Completed");
		}
                try{
                        Iterable<JunoResponse> updateResp;
                        if (syncFlag.reactSync.ordinal() == flag) {     //sync react call
                               updateResp=junoActClient.doBatch(updateList).toIterable();
                        } else {        //async react call
                                updateResp=BatchReactSubscriber.async_dobatch(junoActClient, updateList);
                        }
                        for (JunoResponse response: updateResp) {
                            if (response != null && response.getKey() != null && response.getKey() != key[4]) {
                                    AssertJUnit.assertEquals(OperationStatus.Success, response.getStatus());
				    AssertJUnit.assertEquals(2, response.getVersion());
                            } else {
                                AssertJUnit.assertEquals(OperationStatus.IllegalArgument, response.getStatus());
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
	 * Create batch keys with 2 NULL keys
	 * Verifyy JunoException does not thrown
	 * @throws JunoException
	 */
	@Test
	public void testBatchUpdateNullKeys() throws JunoException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		int numKeys = 10;
		LOGGER.debug("Create " + numKeys + " keys with two null keys");
		byte[][] key = new byte[numKeys][];
		long[] ttl = new long[numKeys];
		byte[][] payload = new byte[numKeys][];
		List<JunoRequest> list = new ArrayList<>();
		List<JunoRequest> updateList = new ArrayList<>();
		for (int i = 0; i < numKeys; i ++) {
			key[i] = DataGenUtils.createKey(10).getBytes();
			String str = "Hello Testing testing " + i;
			payload[i] = str.getBytes();
			ttl[i] = 20;		
			key[3] = null;
			key[9] = null;
			JunoRequest item = new JunoRequest(key[i], payload[i], (long)0, ttl[i], JunoRequest.OperationType.Set);
			JunoRequest updateItem = new JunoRequest(key[i], payload[i], (long)0, ttl[i], JunoRequest.OperationType.Update);
			list.add(item);
			updateList.add(updateItem);
		}
		try{
			LOGGER.debug("\n===Batch Set is sent ");
                        Iterable<JunoResponse> batchResp;
                        if (syncFlag.reactSync.ordinal() == flag) {     //sync react call
                                batchResp=junoActClient.doBatch(list).toIterable();
                        } else {        //async react call
                                batchResp=BatchReactSubscriber.async_dobatch(junoActClient, list);
                        }
                        for (JunoResponse response: batchResp) {
                            if (response != null && response.getKey() != null) {
                                    AssertJUnit.assertEquals(OperationStatus.Success, response.getStatus());
                            } else {
                                AssertJUnit.assertEquals(OperationStatus.IllegalArgument, response.getStatus());
                            }
                        }
		} catch (IllegalArgumentException mex) {
			LOGGER.debug("Exception occurs: " + mex.getMessage());
			AssertJUnit.assertTrue(mex.getMessage().contains("key must not be null"));
			LOGGER.info("Exception", mex.getMessage());
			LOGGER.info("2");			
			LOGGER.info("Completed");
		}
                try{    
                        LOGGER.debug("\n===Batch Update is sent ");
                        Iterable<JunoResponse> updateResp;
                        if (syncFlag.reactSync.ordinal() == flag) {     //sync react call
                                updateResp=junoActClient.doBatch(updateList).toIterable();
                        } else {        //async react call
                                updateResp=BatchReactSubscriber.async_dobatch(junoActClient, updateList);
                        }
                        for (JunoResponse response: updateResp) {
                            if (response != null && response.getKey() != null) {
                                    AssertJUnit.assertEquals(OperationStatus.Success, response.getStatus());
                            } else {
                                AssertJUnit.assertEquals(OperationStatus.IllegalArgument, response.getStatus());
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
	 * Update batch keys with keys >=257 keys
	 * Verify appropriate JunoException is thrown
	 * @throws JunoException
	 */
	@Test
	public void testBatchUpdate257BytesKey() throws JunoException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		int numKeys = 5;
		LOGGER.debug("Create " + numKeys + " keys with more than 1 key >=  257 bytes");
		byte[][] key = new byte[numKeys][];
		long[] ttl = new long[numKeys];
		byte[][] payload = new byte[numKeys][];
		List<JunoRequest> list = new ArrayList<>();
		for (int i = 0; i < numKeys; i ++) {
			key[i] = DataGenUtils.createKey(256).getBytes();
			key[1] = DataGenUtils.createKey(259).getBytes();
			key[4] = DataGenUtils.createKey(127).getBytes();
			String str = "Hello Testing testing " + i;
			payload[i] = str.getBytes();
			ttl[i] = 20;		

			JunoRequest item = new JunoRequest(key[i], payload[i], (long)0, ttl[i],System.currentTimeMillis(), JunoRequest.OperationType.Update);
			list.add(item);
		}
		LOGGER.debug("\n===Batch Update is sent ");
		try {
			Iterable<JunoResponse> resp;	
                        if (syncFlag.reactSync.ordinal() == flag) {     //sync react call
                                resp = junoActClient.doBatch(list).toIterable();
                        } else {        //async react call
                                resp = BatchReactSubscriber.async_dobatch(junoActClient, list);
                        }
		} catch (JunoException mex) {
			AssertJUnit.assertTrue ("Exception should not thrown for key with > 256 key length happened for only one item", false);
		}
		
		ArrayList <JunoRequest> getList = new ArrayList<>();
		for (int i = 0; i < numKeys; i ++) {
			JunoRequest item = new JunoRequest(key[i], null, (long)0, (long)0, JunoRequest.OperationType.Get);
			getList.add(item);
		}
		Iterable<JunoResponse> getBatchResponse;
		try {
			if (syncFlag.reactSync.ordinal() == flag) {     //sync react call
				getBatchResponse = junoActClient.doBatch(getList).toIterable();
			} else {        //async react call
				getBatchResponse = BatchReactSubscriber.async_dobatch(junoActClient, getList);
			}
			int i = 0;
			for (JunoResponse response: getBatchResponse) {		
				String mkey = new String(response.getKey());
					if ( mkey.equals(new String(key[4])) ) {
						AssertJUnit.assertTrue(OperationStatus.NoKey == response.getStatus());
					} else {
						AssertJUnit.assertTrue(OperationStatus.IllegalArgument == response.getStatus());
					}
					i++;
			}
			AssertJUnit.assertEquals(i,numKeys);
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
	 * Update batch keys with keys having 0 or null payload
	 * Verify JunoException does not thrown
	 * @throws JunoException
	 */
	@Test
	public void testBatchUpdateZeroPayload() throws JunoException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		int numKeys = 8;
		LOGGER.debug("Create " + numKeys + " keys");
		byte[][] key = new byte[numKeys][];
		byte[][] payload = new byte[numKeys][];
		long ttl[] = new long[numKeys];
		List<JunoRequest> list = new ArrayList<>();
		for (int i = 0; i < numKeys; i ++) {
			key[i] = DataGenUtils.createKey(25).getBytes();
			String str = "Hello Testing testing " + i;
			payload[i] = str.getBytes();
			ttl[i] = 50;
			JunoRequest item = new JunoRequest(key[i], payload[i],(long)0, ttl[i], JunoRequest.OperationType.Create);
			list.add(item);
		}
		LOGGER.debug("\n===Batch Create is sent ");
		Iterable<JunoResponse> batchResp; 
                if (syncFlag.reactSync.ordinal() == flag) {     //sync react call
                        batchResp = junoActClient.doBatch(list).toIterable();
                } else {        //async react call
                        batchResp = BatchReactSubscriber.async_dobatch(junoActClient, list);
                }
                int g = 0;
                for (JunoResponse mResponse: batchResp) {
                        LOGGER.debug("Key: " + g + ": "+ mResponse.getKey());
                        String mKey = new String(mResponse.getKey());
                        AssertJUnit.assertEquals (OperationStatus.Success,mResponse.getStatus());
                        AssertJUnit.assertTrue(1 == mResponse.getVersion());
                        g++;
                }
                AssertJUnit.assertTrue(g == numKeys);

		LOGGER.debug("Create Juno request list for Batch Update with some keys having zero payload");
		Iterable <JunoResponse> resp;
                if (syncFlag.reactSync.ordinal() == flag) {     //sync react call
                        resp = junoActClient.doBatch(list).toIterable();
                } else {        //async react call
                        resp = BatchReactSubscriber.async_dobatch(junoActClient, list);
                }
		
		List<JunoRequest> list1 = new ArrayList<>();
		HashMap <String, byte[]> hmap = new HashMap<String, byte[]>();		
		for (int h = 0; h < numKeys; h++) {
			String str = "New Hello Testing testing " + h;
			payload[h] = str.getBytes();
			payload[5] = "".getBytes();
			hmap.put(new String(key[h]), payload[h]);
			JunoRequest item1 = new JunoRequest(key[h], payload[h], (long)1, (long)0, JunoRequest.OperationType.Update);
			list1.add(item1);
		}
		LOGGER.debug("\n===Batch Update is sent ");
                if (syncFlag.reactSync.ordinal() == flag) {     //sync react call
                        resp = junoActClient.doBatch(list1).toIterable();
                } else {        //async react call
                        resp = BatchReactSubscriber.async_dobatch(junoActClient, list1);
                }
                int k = 0;
                for (JunoResponse mResponse: resp) {
                        String mKey = new String(mResponse.getKey());
                        AssertJUnit.assertEquals (OperationStatus.Success,mResponse.getStatus());
                        AssertJUnit.assertTrue(2 == mResponse.getVersion());
                        k++;
                }
                AssertJUnit.assertTrue(k == numKeys);
		
		List<JunoRequest> list2 = new ArrayList<>();
		for (int h = 0; h < numKeys; h++) {		
			JunoRequest item2 = new JunoRequest(key[h], null, 0, 0, JunoRequest.OperationType.Get);
			list2.add(item2);
		}
		
		Iterable <JunoResponse> getBatchResponse;
                if (syncFlag.reactSync.ordinal() == flag) {     //sync react call
                        getBatchResponse = junoActClient.doBatch(list2).toIterable();
                } else {        //async react call
                        getBatchResponse = BatchReactSubscriber.async_dobatch(junoActClient, list2);
                }
		
		int j = 0;
		for (JunoResponse response: getBatchResponse) {	
			byte[] mkey=response.getKey();
			AssertJUnit.assertEquals (OperationStatus.Success, response.getStatus());
			AssertJUnit.assertTrue(2 == response.getVersion());
			AssertJUnit.assertEquals(hmap.get(new String(mkey)), response.getValue());
			j++;
		} 
		AssertJUnit.assertEquals(j, numKeys);	
		LOGGER.info("0");
		LOGGER.info("Completed");
	}

	/**
	 * Update batch keys with keys having null payload
	 * Verify appropriate JunoException is thrown
	 * @throws JunoException //TODO: ask if 0 can be considered as default ttl
	 */
	@Test
	public void testBatchUpdateNullPayload() throws JunoException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		int numKeys = 5;
		LOGGER.debug("Create " + numKeys + " keys");
		byte[][] key = new byte[numKeys][];
		long[] ttl = new long[numKeys];
		byte[][] payload = new byte[numKeys][];
		List<JunoRequest> list = new ArrayList<>();
		HashMap <String, byte[]> hmap = new HashMap<String, byte[]>();
		
		for (int i = 0; i < numKeys; i ++) {
			key[i] = DataGenUtils.createKey(25).getBytes();
			String str = "Hello Testing testing " + i;
			payload[i] = str.getBytes();		
			ttl[i] = 50; 
			hmap.put(new String(key[i]), payload[i]);		
			LOGGER.debug("key " + i + " is " + new String(key[i]));
			JunoRequest item = new JunoRequest(key[i], payload[i], (long)0, ttl[i], JunoRequest.OperationType.Create);
			list.add(item);
		}
		LOGGER.debug("\n===Batch Create is sent ");
		Iterable<JunoResponse> resp;	
                if (syncFlag.reactSync.ordinal() == flag) {     //sync react call
                        resp = junoActClient.doBatch(list).toIterable();
                } else {        //async react call
                        resp = BatchReactSubscriber.async_dobatch(junoActClient, list);
                }
                int h = 0;
                for (JunoResponse mResponse: resp) {
                        LOGGER.debug("Key: " + h + ": "+ mResponse.getKey());
                        String mKey = new String(mResponse.getKey());
                        AssertJUnit.assertEquals (OperationStatus.Success,mResponse.getStatus());
                        AssertJUnit.assertTrue(1 == mResponse.getVersion());
                        h++;
                }
                AssertJUnit.assertTrue(h == numKeys);
		LOGGER.debug("Create Juno request list for Batch Update with some keys having zero payload");
		List<JunoRequest> list1 = new ArrayList<>();
		for (int i = 0; i < numKeys; i ++) {
			String str = "New Hello Testing testing " + i;
			payload[i] = str.getBytes();
			payload[2] = null; 
			payload[4] = null;
			ttl[i] = 20;
			if ( i != 2 && i != 4) {
				hmap.put(new String(key[i]), payload[i]);
			}
						
			JunoRequest item1 = new JunoRequest(key[i], payload[i], (long)0, ttl[i], JunoRequest.OperationType.Update);
			list1.add(item1);
		}
		LOGGER.debug("\n===Batch Update is sent ");
		try {			
                        if (syncFlag.reactSync.ordinal() == flag) {     //sync react call
                                resp = junoActClient.doBatch(list1).toIterable();
                        } else {        //async react call
                                resp = BatchReactSubscriber.async_dobatch(junoActClient, list1);
                        }
			int k = 0;
			for (JunoResponse mResponse: resp) {
				LOGGER.debug("Key: " + k + ": "+ mResponse.getKey());
				String mKey = new String(mResponse.getKey());
				AssertJUnit.assertEquals (OperationStatus.Success,mResponse.getStatus());
				AssertJUnit.assertTrue(2 == mResponse.getVersion());
				k++;
			}
	                AssertJUnit.assertTrue(k == numKeys);
		} catch (JunoException mex) {
			AssertJUnit.assertTrue ("Exception should not thrown for null payload happened only for one item", false);
		}
		
		ArrayList <JunoRequest> getList = new ArrayList<>();
		for (int i = 0; i < numKeys; i ++) {
			JunoRequest item = new JunoRequest(key[i], null, (long)0, (long)0, JunoRequest.OperationType.Get);
			getList.add(item);
		}
		Iterable<JunoResponse> getBatchResponse;
		try {
			if (syncFlag.reactSync.ordinal() == flag) {     //sync react call
				getBatchResponse = junoActClient.doBatch(getList).toIterable();
			} else {        //async react call
				getBatchResponse = BatchReactSubscriber.async_dobatch(junoActClient, getList);
			}
			int i = 0;
			for (JunoResponse response: getBatchResponse) {		
				String mkey = new String(response.getKey());
					AssertJUnit.assertEquals (OperationStatus.Success, response.getStatus());
					if ( mkey.equals(new String(key[2])) || mkey.equals(new String(key[4])) ) {
						AssertJUnit.assertTrue(2 == response.getVersion());
						AssertJUnit.assertEquals ( 0, response.getValue().length);
					} else {
						LOGGER.debug("mkey is " + mkey + " version is " + response.getVersion());
						AssertJUnit.assertTrue(2 == response.getVersion());
						AssertJUnit.assertEquals(hmap.get(mkey), response.getValue());
					}
					i++;
			}
			AssertJUnit.assertEquals(i,numKeys);
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
	 * Update batch keys with keys having > 200KB payload
	 * Verify appropriate JunoException is thrown
	 * @throws JunoException
	 */
	@Test
	public void testBatchUpdateMoreThan200KPayLoad() throws JunoException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		int numKeys = 5;
		LOGGER.debug("Create " + numKeys + " keys");
		byte[][] key = new byte[numKeys][];
		long[] ttl = new long[numKeys];
		byte[][] payload = new byte[numKeys][];
		List<JunoRequest> list = new ArrayList<>();
		HashMap <String, Long> hmapTTL = new HashMap <String, Long>();
		HashMap <String, String> hmapData = new HashMap <String, String>();
		
		for (int i = 0; i < numKeys; i ++) {
			key[i] = DataGenUtils.createKey(25).getBytes();
			String str = "Hello Testing testing " + i;
			payload[i] = str.getBytes();
			ttl[i]=30;

			JunoRequest item = new JunoRequest(key[i], payload[i], (long)0, ttl[i], System.currentTimeMillis(), JunoRequest.OperationType.Create);
			list.add(item);
		}
		LOGGER.debug("\n===Batch Create is sent ");
		Iterable<JunoResponse> resp;
                if (syncFlag.reactSync.ordinal() == flag) {     //sync react call
                        resp = junoActClient.doBatch(list).toIterable();
                } else {        //async react call
                        resp = BatchReactSubscriber.async_dobatch(junoActClient, list);
                }
                int h = 0;
                for (JunoResponse mResponse: resp) {
                        LOGGER.debug("Key: " + h + ": "+ mResponse.getKey());
                        String mKey = new String(mResponse.getKey());
                        AssertJUnit.assertEquals (OperationStatus.Success,mResponse.getStatus());
                        AssertJUnit.assertTrue(1 == mResponse.getVersion());
                        h++;
                }
                AssertJUnit.assertTrue(h == numKeys);

		LOGGER.debug("Create Juno request list for Batch Update with some keys having zero payload");
		List<JunoRequest> list1 = new ArrayList<>();
		for (int i = 0; i < numKeys; i ++) {
			String str = "New Hello Testing testing " + i;
			payload[i] = str.getBytes();
			byte[] data = DataGenUtils.genBytes(204801);			
			payload[3] = data;
			ttl[i]=50;
			hmapData.put(new String(key[i]), new String(payload[i]));
			hmapTTL.put(new String(key[i]), ttl[i]);
			JunoRequest item1 = new JunoRequest(key[i], payload[i], (long)0, ttl[i], JunoRequest.OperationType.Update);
			list1.add(item1);
		}
		
		LOGGER.debug("\n===Batch Update is sent ");
		try {
                        if (syncFlag.reactSync.ordinal() == flag) {     //sync react call
                                resp = junoActClient.doBatch(list1).toIterable();
                        } else {        //async react call
                                resp = BatchReactSubscriber.async_dobatch(junoActClient, list1);
                        }
			int k = 0;
			for (JunoResponse mResponse: resp) {
				String mKey = new String(mResponse.getKey());
				if ( mKey.equals(new String(key[3]))) {
					AssertJUnit.assertEquals (OperationStatus.IllegalArgument, mResponse.getStatus());
				} else {
					AssertJUnit.assertEquals (OperationStatus.Success,mResponse.getStatus());
					AssertJUnit.assertTrue(2 == mResponse.getVersion());
					k++;
				}
			}
			AssertJUnit.assertTrue(k == numKeys-1);
		} catch (JunoException mex) {
			AssertJUnit.assertTrue ("Exception should not thrown for > 200K payload happened only on one item", false);
		}
		
		ArrayList <JunoRequest> getList = new ArrayList<>();
		for (int i = 0; i < numKeys; i ++) {
			JunoRequest item = new JunoRequest(key[i], null, (long)0, (long)0, JunoRequest.OperationType.Get);
			getList.add(item);
		}
		Iterable<JunoResponse> getBatchResponse;
		try {
			if (syncFlag.reactSync.ordinal() == flag) {     //sync react call
				getBatchResponse = junoActClient.doBatch(getList).toIterable();
			} else {        //async react call
				getBatchResponse = BatchReactSubscriber.async_dobatch(junoActClient, getList);
			}
			int i = 0;
			for (JunoResponse response: getBatchResponse) {		
				String mkey = new String(response.getKey());
					AssertJUnit.assertEquals (OperationStatus.Success, response.getStatus());
					if ( mkey.equals(new String(key[3])) ) {
						AssertJUnit.assertTrue(1 == response.getVersion());
					} else {
						LOGGER.debug("mkey is " + mkey + " version is " + response.getVersion());
						AssertJUnit.assertTrue(2 == response.getVersion());
						AssertJUnit.assertEquals(hmapData.get(new String(mkey)), new String(response.getValue()));
					}
					AssertJUnit.assertTrue(response.getTtl() <= hmapTTL.get(mkey) && response.getTtl() >= response.getTtl()-5);
					i++;
			}
			AssertJUnit.assertEquals(i,numKeys);
			LOGGER.info("0");			
			LOGGER.info("Completed");
		} catch (JunoException mex) {
			LOGGER.debug("Exception occurs at reset: " + mex.getCause().getMessage());
			AssertJUnit.assertTrue(false);
			LOGGER.info("2");			
			LOGGER.info("Completed");
		}	
	}	/**
	 * Update batch keys TTL > 3 days
	 * Verify appropriate JunoException is thrown
	 * @throws JunoException
	 */
	@Test
	public void testBatchUpdateMoreThan3Days() throws JunoException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		int numKeys = 5;
		LOGGER.debug("Create " + numKeys + " keys");
		byte[][] key = new byte[numKeys][];
		long[] ttl = new long[numKeys];
		byte[][] payload = new byte[numKeys][];
		List<JunoRequest> list = new ArrayList<>();
		HashMap <String, Long> hmapTTL = new HashMap <String, Long>();
		
		for (int i = 0; i < numKeys; i ++) {
			key[i] = DataGenUtils.createKey(25).getBytes();
			String str = "Hello Testing testing " + i;
			payload[i] = str.getBytes();
			ttl[i] = (i + 11) * 2;
			hmapTTL.put(new String(key[i]), ttl[i]);
			JunoRequest item = new JunoRequest(key[i], payload[i], (long)0, ttl[i], JunoRequest.OperationType.Create);
			list.add(item);
		}
		LOGGER.debug("\n===Batch Create is sent ");
		Iterable<JunoResponse> resp;
                if (syncFlag.reactSync.ordinal() == flag) {     //sync react call
                        resp = junoActClient.doBatch(list).toIterable();
                } else {        //async react call
                        resp = BatchReactSubscriber.async_dobatch(junoActClient, list);
                }
                int a = 0;
                for (JunoResponse mResponse: resp) {
                        LOGGER.debug("Key: " + a + ": "+ mResponse.getKey());
                        String mKey = new String(mResponse.getKey());
                        AssertJUnit.assertEquals (OperationStatus.Success,mResponse.getStatus());
                        AssertJUnit.assertTrue(1 == mResponse.getVersion());
                        a++;
                }
                AssertJUnit.assertTrue(a == numKeys);

		LOGGER.debug("Create Juno request list for Batch Update with some keys having zero payload");
		List<JunoRequest> list1 = new ArrayList<>();
		for (int g = 0; g < numKeys; g++) {
			String str = "New Hello Testing testing " + g;
			payload[g] = str.getBytes();
			ttl[g] = 100;			
			ttl[0] = 259201; 
			if ( g != 0 ) {
				hmapTTL.put(new String(key[g]), ttl[g]);
			}
			JunoRequest item1 = new JunoRequest(key[g], payload[g], (long)1, ttl[g], JunoRequest.OperationType.Update);
			list1.add(item1);
		}
		LOGGER.debug("\n===Batch Update is sent ");
		try {
                        if (syncFlag.reactSync.ordinal() == flag) {     //sync react call
                                resp = junoActClient.doBatch(list1).toIterable();
                        } else {        //async react call
                                resp = BatchReactSubscriber.async_dobatch(junoActClient, list1);
                        }
                        int k = 0;
                        for (JunoResponse mResponse: resp) {
                                String mkey = new String(mResponse.getKey());
                                if ( mkey.equals(new String(key[0])) ) {
					AssertJUnit.assertEquals (OperationStatus.IllegalArgument, mResponse.getStatus());
                                } else {
	                                AssertJUnit.assertEquals (OperationStatus.Success,mResponse.getStatus());
					AssertJUnit.assertTrue(2 == mResponse.getVersion());
					k++;
				}
                        }
                        AssertJUnit.assertTrue(k == numKeys-1);
		} catch (JunoException mex) {
			AssertJUnit.assertTrue ("Exception should not thrown for TTL > 3days happened on one item ", false);
		}
		
		//Batch Get
		ArrayList <JunoRequest> getList = new ArrayList<>();
		for (int i = 0; i < numKeys; i ++) {
			JunoRequest item = new JunoRequest(key[i], null, (long)0, (long)0, JunoRequest.OperationType.Get);
			getList.add(item);
		}
		Iterable<JunoResponse> getBatchResponse;
		try {
			if (syncFlag.reactSync.ordinal() == flag) {     //sync react call
				getBatchResponse = junoActClient.doBatch(getList).toIterable();
			} else {        //async react call
				getBatchResponse = BatchReactSubscriber.async_dobatch(junoActClient, getList);
			}
			int i = 0;
			for (JunoResponse response: getBatchResponse) {		
				String mkey = new String(response.getKey());
				AssertJUnit.assertEquals (OperationStatus.Success, response.getStatus());
				if ( mkey.equals(new String(key[0])) ) {
					AssertJUnit.assertTrue(1 == response.getVersion());
				} else {
					AssertJUnit.assertTrue(2 == response.getVersion());
				}
				AssertJUnit.assertTrue(response.getTtl() <= hmapTTL.get(mkey) && response.getTtl() >= response.getTtl()-5);
				i++;
			}
			AssertJUnit.assertEquals(i,numKeys);
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
	 * Create and get multiple keys using batch create
	 * @throws JunoException //TODO: so updte with empty lifetime is fine?
	 */
	@Test
	public void testBatchUpdateNoLifetime() throws JunoException , InterruptedException {
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		int numKeys = 5;
		byte[][] key = new byte[numKeys][];
		Long[] ttl = new Long[numKeys];
		long[] updatettl = new long[numKeys];
		byte[][] payload = new byte[numKeys][];
		HashMap<String, byte[]> hmap = new HashMap<String, byte[]>();
		LOGGER.debug("Create " + numKeys + " keys using existing Juno Create");
		for (int i = 0; i < numKeys; i ++) {
			key[i] = DataGenUtils.createKey(10).getBytes();
			String str = "Hello Testing, Happy Friday" + i;
			payload[i] = str.getBytes();
			ttl[i] = 9L;
			JunoResponse mResponse = junoClient2.create(key[i], payload[i], ttl[i]);
			AssertJUnit.assertEquals (OperationStatus.Success,mResponse.getStatus());			
		}

		byte[][] upayload = new byte[numKeys][];
		LOGGER.debug("Update " + numKeys + " keys without lifetime using batch Update()");
		List<JunoRequest> ulist = new ArrayList<>();
		for (int i = 0; i < numKeys; i ++) {
			String str = "Hello Testing, Happy Friday! Testing again" + i;
			upayload[i] = str.getBytes();
			hmap.put(new String(key[i]), upayload[i]);
			JunoRequest uitem = new JunoRequest(key[i], upayload[i], (long)1, updatettl[i], JunoRequest.OperationType.Update);
			ulist.add(uitem);
		}

		try {
			Iterable<JunoResponse> batchResp= null;
                        if (syncFlag.reactSync.ordinal() == flag) {     //sync react call
                                batchResp = junoActClient.doBatch(ulist).toIterable();
                        } else {        //async react call
                                batchResp = BatchReactSubscriber.async_dobatch(junoActClient, ulist);
                        }
			int i = 0;
			for (JunoResponse mResponse: batchResp) {	
				LOGGER.debug("Key: " + i + ": "+ new String(mResponse.getKey()));
				AssertJUnit.assertEquals (OperationStatus.Success,mResponse.getStatus());
				AssertJUnit.assertTrue(2 == mResponse.getVersion());
				LOGGER.debug("ttl for item " + i + " is " + mResponse.getTtl());
				i++;
			}
			AssertJUnit.assertTrue(i == numKeys);
		} catch (JunoException mex) {
			LOGGER.debug("Exception occurs: " + mex.getMessage());
			AssertJUnit.assertTrue ("Exception is thrown for batch update", false);	
		}

		Thread.sleep (5000);
		LOGGER.debug("Get key using existing Juno read(), keys still exist");
		for (int i = 0; i < numKeys; i ++) {
			try {
			JunoResponse junoResponse = junoClient2.get(key[i]);
			AssertJUnit.assertEquals(key[i], junoResponse.getKey());
			AssertJUnit.assertTrue(2 == junoResponse.getVersion());
			AssertJUnit.assertEquals(new String(upayload[i]), new String(junoResponse.getValue()));
			} catch (JunoException ex) {
				LOGGER.debug("Exception occured at get " + ex.getMessage());
			}
		}

		LOGGER.debug("Sleep for keys to expire");
		Thread.sleep (10000);
		for (int i = 0; i < numKeys; i ++) {			
			try {
				JunoResponse response = junoClient2.get(key[i]);
			} catch (JunoException mex) {
				LOGGER.debug("why exception occured here? " + mex.getMessage());
			}
		}
		
		List<JunoRequest> glist = new ArrayList<>();
		for (int i = 0; i < numKeys; i ++) {
			JunoRequest gitem = new JunoRequest(key[i], upayload[i], (long)1, updatettl[i], JunoRequest.OperationType.Update);
			glist.add(gitem);
		}		
		Iterable<JunoResponse> getBatchResponse;
                if (syncFlag.reactSync.ordinal() == flag) {     //sync react call
                        getBatchResponse = junoActClient.doBatch(glist).toIterable();
                } else {        //async react call
                        getBatchResponse = BatchReactSubscriber.async_dobatch(junoActClient, glist);
                }
		int i = 0;
		for (JunoResponse response: getBatchResponse) {		
			AssertJUnit.assertEquals (OperationStatus.NoKey, response.getStatus());
		}
				
		LOGGER.info("0");
		LOGGER.info("Completed");
	}

	/**
	 * Perform a batch Update on non existing keys 
	 * Verify batch update returns correct status for non-key
	 * @throws JunoException
	 */
	@Test
	public void testBatchUpdateKeyNotExists() throws JunoException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	    
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		int numKeys = 5;
		byte[][] key = new byte[numKeys][];
		byte[][] payload = new byte[numKeys][];
		byte[][] upayload = new byte[numKeys][];
		long ttl[] = new long[numKeys];
		HashMap<String, OperationStatus> hmapStatus = new HashMap<String, OperationStatus>();
		Random r = new Random();

		LOGGER.debug("Create request items and create keys");
		List<JunoRequest> list = new ArrayList<>();
		for (int i = 0; i < numKeys; i ++) {
			key[i] = DataGenUtils.createKey(DataGenUtils.rand(r, 1, 128)).getBytes();
			payload[i] = DataGenUtils.genBytes(DataGenUtils.rand(r, 1, 100));
			ttl[i] = 30;
			JunoRequest item = new JunoRequest(key[i], payload[i], (long)0, ttl[i], JunoRequest.OperationType.Create);
			list.add(item);
		}

		try {
			Iterable<JunoResponse> batchResp; 
                        if (syncFlag.reactSync.ordinal() == flag) {     //sync react call
                                batchResp = junoActClient.doBatch(list).toIterable();
                        } else {        //async react call
                                batchResp = BatchReactSubscriber.async_dobatch(junoActClient, list);
                        }
			int i = 0;
			for (JunoResponse mResponse: batchResp) {				
				LOGGER.debug("Key: " + i + ": "+ mResponse.getKey());
				AssertJUnit.assertEquals (OperationStatus.Success,mResponse.getStatus());
				i++;
			}  
			AssertJUnit.assertTrue( i == numKeys);
		}catch (JunoException mex) {
			LOGGER.debug("Exception occurs: " + mex.getMessage());
			AssertJUnit.assertTrue(false);
		}	

		//Juno Request Item for update
		List<JunoRequest> list1 = new ArrayList<>();
		for (int i = 0; i < numKeys; i ++) {
			key[3] = "12345".getBytes();
			hmapStatus.put(new String(key[i]), OperationStatus.Success);
			hmapStatus.put(new String(key[3]), OperationStatus.NoKey);
			upayload[i] = DataGenUtils.genBytes(DataGenUtils.rand(r, 1, 20));
			JunoRequest item1 = new JunoRequest(key[i], payload[i], (long)1, (long)0, JunoRequest.OperationType.Update);
			list1.add(item1);
		}
		try {
			LOGGER.debug("Update " + numKeys + " keys with version 0 using batch Update");
			Iterable<JunoResponse> batchResp= null;
                        if (syncFlag.reactSync.ordinal() == flag) {     //sync react call
                                batchResp = junoActClient.doBatch(list1).toIterable();
                        } else {        //async react call
                                batchResp = BatchReactSubscriber.async_dobatch(junoActClient, list1);
                        }
			for (JunoResponse mResponse: batchResp) {	
				String mkey = new String(mResponse.getKey());
				AssertJUnit.assertEquals (hmapStatus.get(mkey), mResponse.getStatus());
				if (mResponse.getStatus() == OperationStatus.Success) {
					AssertJUnit.assertTrue(2 == mResponse.getVersion());
				}			    
			}
		} catch (JunoException mex) {
			LOGGER.debug("Exception occurs: " + mex.getMessage());
			AssertJUnit.assertTrue(false);
		}	
		LOGGER.info("0");
		LOGGER.info("Completed");
	}

	/**
	 * Perform a batch Update with version = 0
	 * Verify batch update is successful and return the next version
	 * @throws JunoException //TODO: TTL 0 for set is allowable and will set default ttl 3600?
	 * 						 //TODO: so update does not have response value back? 
	 */
	@Test
	public void testBatchUpdateVersion0() throws JunoException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	    
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		int numKeys = 5;
		byte[][] key = new byte[numKeys][];
		byte[][] payload = new byte[numKeys][];
		byte[][] upayload = new byte[numKeys][];
		HashMap <String, byte[]> hmap = new HashMap<String, byte[]>();
		int[] version = new int [numKeys];
		Random r = new Random();

		LOGGER.debug("Create request item and create items using batch create");
		List<JunoRequest> list = new ArrayList<>();
		for (int i = 0; i < numKeys; i ++) {
			key[i] = DataGenUtils.createKey(DataGenUtils.rand(r, 1, 128)).getBytes();
			payload[i] = DataGenUtils.genBytes(DataGenUtils.rand(r, 1, 100));
			JunoRequest item = new JunoRequest(key[i], payload[i], (long)0, (long)0, JunoRequest.OperationType.Set);
			list.add(item);
		}

		try {
			Iterable<JunoResponse> batchResp= null;
                        if (syncFlag.reactSync.ordinal() == flag) {     //sync react call
                                batchResp = junoActClient.doBatch(list).toIterable();
                        } else {        //async react call
                                batchResp = BatchReactSubscriber.async_dobatch(junoActClient, list);
                        }
			int i = 0;
			for (JunoResponse mResponse: batchResp) {				
				LOGGER.debug("Key: " + i + ": "+ mResponse.getKey());
				AssertJUnit.assertEquals (OperationStatus.Success,mResponse.getStatus());
				i++;
			}  
			AssertJUnit.assertEquals(i, numKeys);
		}catch (JunoException mex) {
			AssertJUnit.assertTrue(false);
		}	

		//Juno Request Item for update
		List<JunoRequest> list1 = new ArrayList<>();
		for (int i = 0; i < numKeys; i ++) {
			version[i] = 0;
			upayload[i] = DataGenUtils.genBytes(DataGenUtils.rand(r, 1, 20));
			hmap.put(new String(key[i]), upayload[i]);
			JunoRequest item1 = new JunoRequest(key[i], upayload[i], version[0], (long)0, JunoRequest.OperationType.Update);
			list1.add(item1);
		}
		try {
			LOGGER.debug("Update " + numKeys + " keys with version 0 using batch Update");
			Iterable<JunoResponse> batchResp= null;
                        if (syncFlag.reactSync.ordinal() == flag) {     //sync react call
                                batchResp = junoActClient.doBatch(list1).toIterable();
                        } else {        //async react call
                                batchResp = BatchReactSubscriber.async_dobatch(junoActClient, list1);
                        }
			for (JunoResponse mResponse: batchResp) {	
				AssertJUnit.assertEquals (OperationStatus.Success, mResponse.getStatus());
				String mKey = new String(mResponse.getKey());
				AssertJUnit.assertTrue(2 == mResponse.getVersion());	
				AssertJUnit.assertTrue(1800 - 8 <= mResponse.getTtl() && mResponse.getTtl() <= 1800);
			}
		} catch (JunoException mex) {
			AssertJUnit.assertTrue(false);
		}	
		
		List<JunoRequest> list2 = new ArrayList<>();
		for (int i = 0; i < numKeys; i ++) {
			JunoRequest item = new JunoRequest(key[i], null, (long)0, (long)0, JunoRequest.OperationType.Get);
			list2.add(item);
		}		
		try {
			Iterable<JunoResponse> batchResp=null;
                        if (syncFlag.reactSync.ordinal() == flag) {     //sync react call
                                batchResp = junoActClient.doBatch(list2).toIterable();
                        } else {        //async react call
                                batchResp = BatchReactSubscriber.async_dobatch(junoActClient, list2);
                        }
			for (JunoResponse mResponse: batchResp) {
				AssertJUnit.assertEquals (OperationStatus.Success, mResponse.getStatus());
				String mKey = new String(mResponse.getKey());
				AssertJUnit.assertTrue(2 == mResponse.getVersion());	
				AssertJUnit.assertEquals (hmap.get(mKey), mResponse.getValue());
				LOGGER.debug("ttl of " + mKey +  " is " + mResponse.getTtl());
				AssertJUnit.assertTrue(1800 - 11 <= mResponse.getTtl() && mResponse.getTtl() <= 1800);
			}
		} catch (JunoException mex) {
			AssertJUnit.assertTrue(false);
		}
		LOGGER.info("0");
		LOGGER.info("Completed");
	}

	/**
	 * Perform a batch Update with version < current version
	 * Verify batch update is successful and return the next version
	 * @throws JunoException //TODO: check with Stephane should version update behave like that? 
	 * 						//TODO: check with Joseph, update seems just ignore the pass in version number but 1??
	 * 						//ask update doesn't get value back?
	 * 	
	 */
	@Test
	public void testBatchUpdateInvalidVersion() throws JunoException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	    
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		int numKeys = 10;
		byte[][] key = new byte[numKeys][];
		byte[][] payload = new byte[numKeys][];
		byte[][] upayload = new byte[numKeys][];
		byte[][] udate_payload = new byte[numKeys][];
		int[] version = new int [numKeys];
		long[] ttl = new long [numKeys];
		for (int i = 0; i < numKeys; i ++) {
			key[i] = DataGenUtils.createKey(10).getBytes();
			String str = "Hello Testing, Happy Friday" + i;
			payload[i] = str.getBytes();
			ttl[i] = 20L;
			JunoResponse mResponse = junoClient2.create(key[i], payload[i], ttl[i]);
			AssertJUnit.assertEquals (OperationStatus.Success,mResponse.getStatus());			
		}

		LOGGER.debug("Calling batch update to update keys without passing version");
		List<JunoRequest> list1 = new ArrayList<>();
		for (int i = 0; i < numKeys; i ++) {
			String str = "Hello Testing, Happy Friday, Try again" + i;
			upayload[i] = str.getBytes();
			version[i] = 5;
			JunoRequest item1 = new JunoRequest(key[i], upayload[i], version[i], (long)0, JunoRequest.OperationType.Update);
			list1.add(item1);
		}
		try {
			Iterable<JunoResponse> batchResp=null;
                        if (syncFlag.reactSync.ordinal() == flag) {     //sync react call
                                batchResp = junoActClient.doBatch(list1).toIterable();
                        } else {        //async react call
                                batchResp = BatchReactSubscriber.async_dobatch(junoActClient, list1);
                        }

			for (JunoResponse mResponse: batchResp) {	
				AssertJUnit.assertEquals (OperationStatus.Success, mResponse.getStatus());
				LOGGER.debug("version is " + mResponse.getVersion());
				AssertJUnit.assertTrue(2 == mResponse.getVersion());			   
			}
		} catch (JunoException mex) {
			LOGGER.debug("Exception occurs for update without version passing: " + mex.getMessage());
			AssertJUnit.assertTrue(false);
		}	

		LOGGER.debug("Calling batch update to update keys, some having invalid version");
		List<JunoRequest> list2 = new ArrayList<>();
		HashMap<String, byte[]> hmap = new HashMap<String, byte[]>();
		HashMap<String, OperationStatus> hmapStatus = new HashMap<String, OperationStatus>();
		for (int i = 0; i < numKeys; i ++) {
			String str = "Hello Testing, Happy Friday, Try again. Have a good weekend" + i;
			udate_payload[i] = str.getBytes();
			ttl[i] = 30;
			version[i] = 6; //next version should be 7
			version[3] = 4;
			version[4] = 0; //OK
			version[5] = -1; //OK
			version[6] = 65535; //OK
			version[7] = 1;
			hmapStatus.put(new String(key[i]), OperationStatus.Success);
			hmapStatus.put(new String(key[7]), OperationStatus.ConditionViolation);
			hmap.put(new String(key[i]), udate_payload[i]);
			hmap.put(new String(key[7]), upayload[7]);
			JunoRequest item2 = new JunoRequest(key[i], udate_payload[i], version[i], ttl[i], JunoRequest.OperationType.Update);
			list2.add(item2);
		}
		try {
			Iterable<JunoResponse> batchResp; 
                        if (syncFlag.reactSync.ordinal() == flag) {     //sync react call
                                batchResp = junoActClient.doBatch(list2).toIterable();
                        } else {        //async react call
                                batchResp = BatchReactSubscriber.async_dobatch(junoActClient, list2);
                        }
			int i = 0; 
			for (JunoResponse mResponse: batchResp) {	
				String mkey = new String(mResponse.getKey());
				LOGGER.debug("=======Status: for " + mkey + " is " +   mResponse.getStatus() + " - Version: " + mResponse.getVersion());
				AssertJUnit.assertEquals(hmapStatus.get(mkey), mResponse.getStatus());
				if (mResponse.getStatus() == OperationStatus.Success) {					
					AssertJUnit.assertTrue(3 == mResponse.getVersion());	
					i++;
				}   
			}
			AssertJUnit.assertTrue ( i == numKeys - 1);
		} catch (JunoException mex) {
			LOGGER.debug("Exception occurs: " + mex.getMessage());
			AssertJUnit.assertTrue(false);
		}	
		
		List<JunoRequest> list3 = new ArrayList<>();
		for (int i = 0; i < numKeys; i ++) {
			JunoRequest item = new JunoRequest(key[i], null, (long)0, (long)0, JunoRequest.OperationType.Get);
			list3.add(item);
		}	
		try {
			Iterable<JunoResponse> batchResp = junoActClient.doBatch(list3).toIterable();
			int i = 0; 
			for (JunoResponse mResponse: batchResp) {	
				String mkey = new String(mResponse.getKey());
				AssertJUnit.assertEquals (new String (hmap.get(mkey)), new String (mResponse.getValue()));
				i++;			
			}		
			AssertJUnit.assertTrue ( i == numKeys);
		} catch (JunoException mex) {
			LOGGER.debug("Exception occurs: " + mex.getMessage());
			AssertJUnit.assertTrue(false);
		}
		LOGGER.info("0");
		LOGGER.info("Completed");
	}

	/**
	 * Create batch keys with no Item in the JunoRequest list
	 * Verify appropriate JunoException is thrown
	 * @throws JunoException //TODO: aost realsed
	 */
	//@Test
	public void testBatchUpdateZeroItem() throws JunoException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		LOGGER.debug("Send 0 item to Batch Update");
		List<JunoRequest> list = new ArrayList<>();
		LOGGER.debug("\n===Batch Create is sent ");
		try {
			junoActClient.doBatch(list).toIterable();
			AssertJUnit.assertTrue ("Exception is not thrown for no key in Juno Request", false);
		} catch (IllegalArgumentException mex) {
			LOGGER.debug("Exception occurs: " + mex.getMessage());
			AssertJUnit.assertTrue(mex.getMessage().contains("Empty juno request list"));
		}
		LOGGER.info("0");
		LOGGER.info("Completed");
	}

	/**
	 * Batch Create and Batch Update to update lifetime and payload
	 * @throws JunoException
	 */
	@Test
	public void testBatchUpdatePayloadLifetimeVersion() throws JunoException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		//int numKeys = 100;
		int numKeys = 5;
		byte[][] key = new byte[numKeys][];
		long[] ttl = new long[numKeys];
		long[]creationTime = new long[numKeys];
		long[]getTime = new long[numKeys];
		byte[][] payload = new byte[numKeys][];
		HashMap <String, byte[]> hmap = new HashMap <String, byte[]>();
		LOGGER.debug("Create " + numKeys + " keys using batch Set");

		List<JunoRequest> list = new ArrayList<>();
		for (int i = 0; i < numKeys; i ++) {
			key[i] = DataGenUtils.createKey(10).getBytes();
			String str = "Hello Testing, Happy Friday" + i;
			payload[i] = str.getBytes();
			creationTime[i] = System.currentTimeMillis();
			JunoRequest item = new JunoRequest(key[i], payload[i], (long)0, (long)0, JunoRequest.OperationType.Set);
			list.add(item);
		}
		try {
			Iterable<JunoResponse> batchResp = junoActClient.doBatch(list).toIterable();
			int i = 0;
			for (JunoResponse mResponse: batchResp) {	
				LOGGER.debug("Key: " + i + ": "+ mResponse.getKey());
				AssertJUnit.assertEquals (OperationStatus.Success,mResponse.getStatus());
				AssertJUnit.assertTrue(1 == mResponse.getVersion());
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
			String str = "Hello Testing, Happy Friday! Testing again" + i;
			upayload[i] = str.getBytes();
			ttl[i] = 100;			
			hmap.put(new String(key[i]), upayload[i]);
			JunoResponse gResponse = junoClient2.get(key[i]);
			AssertJUnit.assertEquals (OperationStatus.Success,gResponse.getStatus());
			JunoRequest uitem = new JunoRequest(key[i], upayload[i], ttl[i], gResponse.getVersion(), JunoRequest.OperationType.Update);
			ulist.add(uitem);
		}

		try {
			Iterable<JunoResponse> batchResp= null;
			batchResp = junoActClient.doBatch(ulist).toIterable();

			int i = 0;	
			for (JunoResponse mResponse: batchResp) {	
				LOGGER.debug("Key: " + i + ": "+ mResponse.getKey());
				AssertJUnit.assertEquals (OperationStatus.Success,mResponse.getStatus());
				AssertJUnit.assertTrue(2 == mResponse.getVersion());
				i++;
			}
			AssertJUnit.assertTrue(i == numKeys);
		} catch (JunoException mex) {
			LOGGER.debug("Exception occurs: " + mex.getMessage());
			AssertJUnit.assertTrue ("Exception is thrown for batch update", false);	
		}

		LOGGER.debug("Get key using existing Juno read()");
		for (int i = 0; i < numKeys; i ++) {
			getTime[i] = System.currentTimeMillis();
			JunoResponse junoResponse = junoClient2.get(key[i]);
			AssertJUnit.assertEquals(key[i], junoResponse.getKey());
			AssertJUnit.assertTrue(2 == junoResponse.getVersion());
			AssertJUnit.assertEquals(new String(upayload[i]), new String(junoResponse.getValue()));
			AssertJUnit.assertTrue(1800 - ((getTime[i] - creationTime[i])/1000)-3 <= junoResponse.getTtl() && junoResponse.getTtl() <= 1800 -((getTime[i]-creationTime[i])/1000));
		}
		LOGGER.info("0");
		LOGGER.info("Completed");
	}
}
