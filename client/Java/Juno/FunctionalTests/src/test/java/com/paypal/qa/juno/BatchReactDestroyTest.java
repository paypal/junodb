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
import java.util.List;
import java.util.Properties;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;import com.paypal.juno.client.JunoClient;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class BatchReactDestroyTest{
	private JunoReactClient junoActClient;
	private JunoReactClient JunoReactClient;
	private JunoClient junoClient2;
	private Properties pConfig;
	private Properties pConfig2;	
        private enum syncFlag {reactAsync, reactSync};
        private int flag;

	private Logger LOGGER;

	@BeforeClass
	public void setup() throws  IOException, InterruptedException {
		
		LOGGER = LoggerFactory.getLogger(BatchDestroyTest.class);

		URL url = BatchDestroyTest.class.getResource("/com/paypal/juno/Juno_batch.properties");
		pConfig = new Properties();
		pConfig.load(url.openStream());
                LOGGER.debug("Read syncFlag");
                String sync_flag = pConfig.getProperty("sync_flag_test", "0");
                LOGGER.debug("*********SYNC FLAG: " + sync_flag);
                flag = Integer.parseInt(sync_flag.trim());
		pConfig.setProperty(JunoProperties.APP_NAME, "QATestApp");
		pConfig.setProperty(JunoProperties.RECORD_NAMESPACE, "NS1");

		URL url2 = BatchDestroyTest.class.getResource("/com/paypal/juno/Juno_batch.properties");
		pConfig2 = new Properties();
		pConfig2.load(url2.openStream());
		pConfig2.setProperty(JunoProperties.APP_NAME, "QATestApp2");
		pConfig2.setProperty(JunoProperties.RECORD_NAMESPACE, "NS1");
		try {
			junoActClient = JunoClientFactory.newJunoReactClient(new JunoPropertiesProvider(pConfig), SSLUtil.getSSLContext());
			JunoReactClient = JunoClientFactory.newJunoReactClient(new JunoPropertiesProvider(pConfig), SSLUtil.getSSLContext());
			junoClient2 = JunoClientFactory.newJunoClient(new JunoPropertiesProvider(pConfig2), SSLUtil.getSSLContext());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}


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
	public void testBatchCreateDestroy() throws JunoException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		int numKeys = 10;
		byte[][] key = new byte[numKeys][];
		byte[][] payload = new byte[numKeys][];
		long[] ttl = new long[numKeys];
		LOGGER.debug("Create " + numKeys + " keys using batch Create");
		List<JunoRequest> list = new ArrayList<>();
		for (int i = 0; i < numKeys; i ++) {
			key[i] = DataGenUtils.createKey(10).getBytes();
			String str = "Hello Testing, Happy Friday" + i;
			payload[i] = str.getBytes();
			ttl[i] = (i+10) *20;
			JunoRequest item = new JunoRequest(key[i], payload[i], (long)0, ttl[i], System.currentTimeMillis(), JunoRequest.OperationType.Create);
			list.add(item);
		}
		try {
			Iterable<JunoResponse> batchResp;
                        if (syncFlag.reactSync.ordinal() == flag) {     //sync react call
				LOGGER.debug("enter into sync call" );
                                batchResp = junoActClient.doBatch(list).toIterable();
                        } else {        //async react call
				LOGGER.debug("enter into async call" );
                                batchResp = BatchReactSubscriber.async_dobatch(junoActClient, list);
                        }
			
			int i = 0;
			for (JunoResponse mResponse: batchResp) {	
				AssertJUnit.assertEquals (OperationStatus.Success,mResponse.getStatus());
				AssertJUnit.assertTrue(1 == mResponse.getVersion());
				i++;
			}
			AssertJUnit.assertTrue( i == numKeys);
		} catch (IllegalArgumentException mex) {			
			LOGGER.debug("Exception occurs: " + mex.getMessage());
			AssertJUnit.assertTrue ("Exception is thrown for batch create", false);	
		}

		List<JunoRequest> dlist = new ArrayList<>();
		for (int i = 0; i < numKeys; i ++) {
			JunoRequest uitem = new JunoRequest(key[i], null, 0, 0, JunoRequest.OperationType.Destroy);
			dlist.add(uitem);
		}
		LOGGER.debug("Destroy key using batch destroy in async mode");
		try {
			Iterable<JunoResponse> batchResp;
                        if (syncFlag.reactSync.ordinal() == flag) {     //sync react call
                                batchResp = junoActClient.doBatch(dlist).toIterable();
                        } else {        //async react call
                                batchResp = BatchReactSubscriber.async_dobatch(junoActClient, dlist);
                        }
			int i = 0;
			for (JunoResponse mResponse: batchResp) {	
				LOGGER.debug("Key: " + i + ": "+ mResponse.getKey());
				AssertJUnit.assertEquals (OperationStatus.Success,mResponse.getStatus());	
				i++;
			}
			AssertJUnit.assertTrue( i == numKeys);
		} catch (JunoException mex) {
			AssertJUnit.assertTrue ("Exception is thrown for batch destroy", false);	
		}

		LOGGER.debug("Get key using existing Juno read()");
		for (int i = 0; i < numKeys; i ++) {
			try {
				JunoResponse junoResponse = junoClient2.get(key[i]);
			} catch (JunoException mex) {
				//AssertJUnit.assertEquals (OperationStatus.NoKey,mex.getOperationStatus());		
				LOGGER.info("0");
			} finally {
				LOGGER.info("Completed");
			}
		}		
	}

	/**
	 * Verify appropriate JunoException is thrown when doing Batch Destroy with a key having zero length
	 * @throws JunoException
	 */
	@Test
	public void testBatchDestroyEmptyKey() throws JunoException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		int numKeys = 5;
		byte[][] key = new byte[numKeys][];
		byte[][] payload = new byte[numKeys][];
		Random r = new Random();

		LOGGER.debug("Create request item with 1 key having zero length");
		//Juno Request Item for update
		List<JunoRequest> list1 = new ArrayList<>();
		for (int i = 0; i < numKeys; i ++) {
			key[i] = DataGenUtils.createKey(DataGenUtils.rand(r, 1, 128)).getBytes();
			payload[i] = DataGenUtils.genBytes(DataGenUtils.rand(r, 1, 1028));
			key[4] = "".getBytes();
			JunoRequest item1 = new JunoRequest(key[i], payload[i], (long)0, (long)0, JunoRequest.OperationType.Destroy);
			list1.add(item1);
		}
		try{
                        Iterable<JunoResponse> batchResp;
			LOGGER.debug("Destroy " + numKeys + " keys using batch destroy");
                        if (syncFlag.reactSync.ordinal() == flag) {     //sync react call
                                batchResp=junoActClient.doBatch(list1).toIterable();
                        } else {        //async react call
                                batchResp=BatchReactSubscriber.async_dobatch(junoActClient, list1);
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
			AssertJUnit.assertTrue(mex.getMessage().contains("The Document key must not be null or empty"));
			LOGGER.info("Exception", mex.getMessage());
			LOGGER.info("2");			
			LOGGER.info("Completed");
		}	
	}

	/**
	 * Bull Destroy for null keys  
	 * Verify appropriate JunoException is thrown
	 * @throws JunoException
	 */
	@Test
	public void testBatchDestroyNullKeys() throws JunoException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
	        
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		int numKeys = 10;
		LOGGER.debug("Create " + numKeys + " keys with two null keys");
		byte[][] key = new byte[numKeys][];
		long[] ttl = new long[numKeys];
		byte[][] payload = new byte[numKeys][];

		List<JunoRequest> list1 = new ArrayList<>();
		for (int i = 0; i < numKeys; i ++) {
			key[i] = DataGenUtils.createKey(10).getBytes();
			key[3] = null;
			key[9] = null; 
			JunoRequest item1 = new JunoRequest(key[i], payload[i], (long)0, ttl[i], JunoRequest.OperationType.Destroy);
			list1.add(item1);
		}
		try{
			LOGGER.debug("\n===Batch destroy is sent ");
                        Iterable<JunoResponse> batchResp;				
                        if (syncFlag.reactSync.ordinal() == flag) {     //sync react call
                                batchResp=junoActClient.doBatch(list1).toIterable();
                        } else {        //async react call
                                batchResp=BatchReactSubscriber.async_dobatch(junoActClient, list1);
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
	 * Verify appropriate JunoException is thrown when doing Batch Destroy a key > 256bytes length
	 * @throws JunoException
	 */
	@Test
	public void testBatchDestroy129BytesKey() throws JunoException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		int numKeys = 5;
		byte[][] key = new byte[numKeys][];
		byte[][] payload = new byte[numKeys][];
		Random r = new Random();

		LOGGER.debug("Create request item with 1 key having zero length");
		List<JunoRequest> list = new ArrayList<>();
		for (int i = 0; i < numKeys; i ++) {
			key[i] = DataGenUtils.createKey(DataGenUtils.rand(r, 1, 128)).getBytes();
			key[4] = DataGenUtils.createKey(129).getBytes();

			System.out.println("key " + i + "is " + new String(key[i]));
			payload[i] = DataGenUtils.genBytes(DataGenUtils.rand(r, 1, 1028));	
			JunoRequest item1 = new JunoRequest(key[i], payload[i], (long)0, (long)0, JunoRequest.OperationType.Destroy);
			list.add(item1);
		}

		try {
			LOGGER.debug("Destroy " + numKeys + " keys using batch destroy");
			Iterable <JunoResponse> resp; 
                        if (syncFlag.reactSync.ordinal() == flag) {     //sync react call
                                resp = junoActClient.doBatch(list).toIterable();
                        } else {        //async react call
                                resp = BatchReactSubscriber.async_dobatch(junoActClient, list);
                        }
			int i = 0;
			for (JunoResponse response: resp) {
				String mkey = new String(response.getKey());
				System.out.println("vera mkey is " + mkey + "key 4 is " + (new String(key[4])));
				if ( mkey.equals(new String(key[4]))) {
					AssertJUnit.assertEquals (OperationStatus.IllegalArgument, response.getStatus());
				} else {
					AssertJUnit.assertEquals (OperationStatus.Success, response.getStatus());
					i++;
				}
			}
			AssertJUnit.assertEquals(i, numKeys-1);
		} catch (JunoException mex) {
			AssertJUnit.assertTrue ("Exception should not thrown for long key happened for one item", false);
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
				if (response.getStatus() == OperationStatus.NoKey) {
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
	 * Perform a batch Update on non existing keys 
	 * Verify batch update returns correct status for non-key
	 * @throws JunoException
	 */
	@Test
	public void testBatchDestoryKeyNotExists() throws JunoException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		int numKeys = 5;
		byte[][] key = new byte[numKeys][];
		LOGGER.debug("Create request items and create keys");
		Random r = new Random();
		List<JunoRequest> list = new ArrayList<>();
		for (int i = 0; i < numKeys; i ++) {
			key[i] = DataGenUtils.createKey(DataGenUtils.rand(r, 1, 128)).getBytes();
			JunoRequest item = new JunoRequest(key[i], null, (long)0, (long)0, JunoRequest.OperationType.Destroy);
			list.add(item);
		}

		try {
			LOGGER.debug("Destroy non-existent keys");
			Iterable<JunoResponse> batchResp = null;
                        if (syncFlag.reactSync.ordinal() == flag) {     //sync react call
                                batchResp = junoActClient.doBatch(list).toIterable();
                        } else {        //async react call
                                batchResp = BatchReactSubscriber.async_dobatch(junoActClient, list);
                        }
			for (JunoResponse mResponse: batchResp) {	
				AssertJUnit.assertEquals ( OperationStatus.Success, mResponse.getStatus());	   		    
			}
			LOGGER.info("0");
			LOGGER.info("Completed");
		} catch (JunoException mex) {
			AssertJUnit.assertTrue(false);
		}	
	}

	/**
	 * Create batch keys with no Item in the JunoRequest list
	 * Verify appropriate JunoException is thrown
	 * @throws JunoException //TODO: same question as testBatchGetZeroItem
	 */
	//@Test
	public void testBatchDestroyZeroItem() throws JunoException{
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		
		LOGGER.info("CorrID : ",Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));	
		  
		LOGGER.debug("Send 0 item to Batch Destroy");
		List<JunoRequest> list = new ArrayList<>();
		LOGGER.debug("\n===Batch Destroy is sent ");
		try {
                        if (syncFlag.reactSync.ordinal() == flag) {     //sync react call
                                junoActClient.doBatch(list).toIterable();
                        } else {        //async react call
                                BatchReactSubscriber.async_dobatch(junoActClient, list);
                        }
			AssertJUnit.assertTrue ("Exception is not thrown for no key in Juno Request", false);
		} catch (IllegalArgumentException mex) {
			LOGGER.debug("Exception occurs: " + mex.getMessage());
			AssertJUnit.assertTrue(mex.getMessage().contains("Empty juno request list"));
			LOGGER.info("Exception", mex.getMessage());
			LOGGER.info("2");			
			LOGGER.info("Completed");
		}
	}
}
