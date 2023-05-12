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
import com.paypal.juno.client.io.JunoRequest;
import com.paypal.juno.client.io.JunoResponse;
import com.paypal.juno.client.io.OperationStatus;
import com.paypal.juno.exception.JunoException;
import com.paypal.qa.juno.DataGenUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import javax.inject.Inject;
import javax.inject.Named;
import org.apache.commons.lang.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.AssertJUnit;import com.paypal.juno.client.JunoAsyncClient;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * These test cases to test JunoClientUSFFactory
 */

@ContextConfiguration("classpath:spring-config.xml")
@Component
public class JunoUSFFactoryClientTest extends AbstractTestNGSpringContextTests {

    @Inject
    @Named("junoClient5")
    JunoClient junoClient;
        
    @Inject
    @Named("junoClient6")
    JunoClient junoClient6;

    @Inject
    @Named("junoRClient1")
    JunoReactClient junoRctClient;

    @Inject
    @Named("junoRClient2")
    JunoReactClient junoRTimeoutClient;
    
    @Inject
    JunoClient junoSyncClient;

    @Inject
    JunoAsyncClient junoAsyncClient;

    @Inject
    JunoReactClient junoReactClient;
    
    private Logger LOGGER;

	@BeforeClass
	public void setup() throws JunoException, IOException
	{
		LOGGER = LoggerFactory.getLogger(JunoUSFFactoryClientTest.class);
		LOGGER.debug("Read syncFlag test to findout what needs to be run");
		// Do nothing
	}
    
    /**
     * This is a helper function to do create/read/update/delete keys
     * @param records: to use for payload size
     * @param loops: Number of keys to be created
     * @throws JunoException
     * @throws IOException
     */
    //@Test
	private void testLoadAll(int records, int loops ) throws JunoException,  IOException {		
		long totalCreateTime = 0L;
		String[] key = new String[loops];
		for (int iter = 0; iter < loops; ++iter){
			key[iter] = DataGenUtils.createKey(iter+1);
		}
		StopWatch clock = new StopWatch();
		for (int iter = 0; iter < loops; ++iter) {
			clock.start();
			byte[] payload = DataGenUtils.genBytes(1024 * records);
			byte[] payload1 = DataGenUtils.genBytes(1024 * records);
			junoReactClient.create(key[iter].getBytes(), payload, 100L).block();
			junoClient.update(key[iter].getBytes(), payload1, 100L);
			JunoResponse junoResponse = junoClient.get(key[iter].getBytes());
			AssertJUnit.assertEquals (OperationStatus.Success,junoResponse.getStatus());
			AssertJUnit.assertEquals(key[iter], new String(junoResponse.key()));
			//System.out.println ("Version: " +  junoResponse.getVersion() );					
			AssertJUnit.assertEquals(new String(payload1), new String(junoResponse.getValue()));
			junoClient.delete(key[iter].getBytes());
			clock.stop();
			totalCreateTime += clock.getTime();
			clock.reset();
		}
		//System.out.println("Time per all(create+update+get+delete): " + totalCreateTime / loops + " ms/all");
		//System.out.println("Total time for " + loops + " loops for all "+ records + "KB of data: " + totalCreateTime / 1000 + " seconds");
	}
    
    /**
     * This test case calls testLoadAll() to create/update/read/delete 100 records
     * @throws JunoException
     * @throws IOException
     */
    @Test
	public void testLoadAllForOneHundredRecords() throws JunoException,  IOException {
    		//System.out.println( "\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
    	
		try{
			testLoadAll(200, 10);
		}catch(IllegalArgumentException iaex){
			//System.out.println ("Exception occur: " + iaex.getMessage());
			AssertJUnit.assertTrue(iaex.getMessage().contains("Invalid payload size. current payload size=1024000, max payload size=204800"));
		}catch(Exception e){
			//System.out.println("Exception occured: "+e.getMessage());
		}
	}

    /**
     * Create and read the same key back
     */
    @Test
    public void testCreateKey() {
    	//System.out.println( "\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
        String key = UUID.randomUUID().toString();

        String data = "data to store in juno on " + new Date(System.currentTimeMillis()).toString();
        byte[] bytes = data.getBytes();

        JunoResponse response = junoClient.create(key.getBytes(), bytes);
        // Check for the record
        response = junoClient.get(key.getBytes());
        String dataOut = new String(response.getValue());
        //System.out.println ("Data: " + dataOut);
        assert (data.equals(dataOut));

        JunoResponse rresponse = junoRctClient.create(key.getBytes(), bytes).block();
        rresponse = junoRctClient.get(key.getBytes()).block();
        String rdataOut = new String(rresponse.getValue());
        //System.out.println ("Data from reactClient is : " + rdataOut);
        assert (data.equals(rdataOut));

        LOGGER.info("SUCCESS");
        LOGGER.info("Completed");
    }

	/**
	 * Test with mixed Object types.
	 */
	@Test
	public void mixedObjectTest() {
		//System.out.println( "\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		String key = UUID.randomUUID().toString();

		String data = "data to store in juno by Sync client " + new Date(System.currentTimeMillis()).toString();
		byte[] bytes = data.getBytes();

		// Create the Key using Juno Sync Client
		JunoResponse response = junoSyncClient.create(key.getBytes(), bytes);
		AssertJUnit.assertEquals(response.getStatus(),OperationStatus.Success);

		// Check for the record and data using Async client
		response = junoAsyncClient.get(key.getBytes()).toBlocking().value();
		AssertJUnit.assertEquals(response.getStatus(),OperationStatus.Success);
		String dataOut = new String(response.getValue());
		assert (data.equals(dataOut));

		// Update it with Reactor client
		String data1 = "data is updated by juno react client " + new Date(System.currentTimeMillis()).toString();
		response = junoReactClient.update(key.getBytes(),data1.getBytes()).block();
		AssertJUnit.assertEquals(response.getStatus(),OperationStatus.Success);

		//Validate it using the Juno Sync client
		response = junoClient.get(key.getBytes());
		AssertJUnit.assertEquals(response.getStatus(),OperationStatus.Success);
		dataOut = new String(response.getValue());
		assert (data1.equals(dataOut));

		//Destroy the Key with Juno Async Client and check it using react Client
		response = junoAsyncClient.delete(key.getBytes()).toBlocking().value();
		AssertJUnit.assertEquals(response.getStatus(),OperationStatus.Success);
		response = junoReactClient.get(key.getBytes()).block();
		AssertJUnit.assertEquals(response.getStatus(),OperationStatus.NoKey);

		LOGGER.info("SUCCESS");
		LOGGER.info("Completed");
	}	/**
	 *
	 */
	@Test
    public void testNoConnectionToServer() {
    	//System.out.println( "\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
        String key = UUID.randomUUID().toString();

        String data = "data to store in juno on " + new Date(System.currentTimeMillis()).toString();
        byte[] bytes = data.getBytes();

        try{
        	junoClient6.create(key.getBytes(), bytes);
        	AssertJUnit.assertFalse("Exception not seen for No connection to server Test", false);
        }catch(JunoException e){
        	AssertJUnit.assertTrue("testNoConnectionToServer failed",e.getMessage().contains("Connection Error"));
        	//System.out.println(" Exception1 is:"+e.getMessage());
        }finally {
			LOGGER.info("SUCCESS");
			LOGGER.info("Completed");
		}

        try{
                junoRTimeoutClient.create(key.getBytes(), bytes);
                AssertJUnit.assertFalse("Exception not seen for No connection to server Test", false);
        }catch(JunoException e){
                AssertJUnit.assertTrue("testNoConnectionToServer failed",e.getMessage().contains("Connection Error"));
                //System.out.println(" Exception1 is:"+e.getMessage());
        }finally {
                        LOGGER.info("SUCCESS");
                        LOGGER.info("Completed");
                }
        
        try{
        	List<JunoRequest> list = new ArrayList<>();
        	JunoRequest item = new JunoRequest(key.getBytes(), bytes, (long)0, 10, System.currentTimeMillis(), JunoRequest.OperationType.Create);
			list.add(item);
			junoClient6.doBatch(list);
			AssertJUnit.assertFalse("Exception not seen for No connection to server Test", false);
        }catch(JunoException e){
			//System.out.println(" Exception2 is:"+e.getMessage());
        	AssertJUnit.assertTrue("testNoConnectionToServer failed",e.getCause().getMessage().contains("Connection Error"));
        }finally {
			LOGGER.info("SUCCESS");
			LOGGER.info("Completed");
		}

        try{
                List<JunoRequest> list = new ArrayList<>();
                JunoRequest item = new JunoRequest(key.getBytes(), bytes, (long)0, 10, System.currentTimeMillis(), JunoRequest.OperationType.Create);
                        list.add(item);
                        junoRTimeoutClient.doBatch(list).toIterable();
                        AssertJUnit.assertFalse("Exception not seen for No connection to server Test", false);
        }catch(JunoException e){
                        //System.out.println(" Exception2 is:"+e.getMessage());
                AssertJUnit.assertTrue("testNoConnectionToServer failed",e.getCause().getMessage().contains("No Connection to server"));
        }finally {
                        LOGGER.info("SUCCESS");
                        LOGGER.info("Completed");
                }
    }
    
    /**
     * This test case tests unicode keys
     * A complete interop test will be running this test case, then run C++ test
     * ./test_client.tst --gtest_filter=TestClientCreate.unicodeKeyFromJava    
     * @throws JunoException
     * @throws IOException
     */
	@Test
	public void testCreateChineseKeyForCpp() throws JunoException, IOException{
		//System.out.println( "\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		byte[] key = "Q:������������A:���. Q:����� A:������������  Q:�".getBytes(); 
		byte[] data = "Q:���������������������������������������������".getBytes("UTF-8");
		//System.out.println ("Destroy the key first");
		junoClient.delete(key);
		junoClient.create(key, data, 6L);		
		JunoResponse junoResponse = junoReactClient.get(key, (long)0).block();
		AssertJUnit.assertEquals (OperationStatus.Success,junoResponse.getStatus());
		//System.out.println ("Version: " + junoResponse.getVersion());
		//System.out.println ("Key: " + junoResponse.key() );
		//System.out.println ("Data: " + new String(junoResponse.getValue()));
		AssertJUnit.assertEquals(1, junoResponse.getVersion());
		AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
		AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));
		
		//System.out.println ("\nUpdate key: ");
		byte[] data1 = "Q:������������������������������������A:���������������������������".getBytes("UTF-8");
		long lifetime1 = 12L;
		JunoResponse junoResponse1 = junoReactClient.update(key, data1, lifetime1).block();
		//System.out.println ("New Data1: " + new String(junoResponse.getValue()));
		//System.out.println ("Version: " + junoResponse1.getVersion());
		AssertJUnit.assertEquals(key, junoResponse1.key());	
		junoResponse1 = junoClient.get(key);
		AssertJUnit.assertEquals(data1.length, junoResponse1.getValue().length);
		AssertJUnit.assertEquals(new String(data1), new String(junoResponse1.getValue()));
		
		//System.out.println ("\nConditional Update: ");
		byte[] data2 = "Q:������������������������������������A:���������������������������123".getBytes("UTF-8");
		JunoResponse junoResponse2 = junoClient.compareAndSet(junoResponse1.getRecordContext(), data2, (long)6L);
		//System.out.println ("Version: " + junoResponse2.getVersion());
		AssertJUnit.assertEquals(key, junoResponse2.key());		
		junoResponse2 = junoClient.get(key);
		AssertJUnit.assertEquals(data2.length, junoResponse2.getValue().length);
		AssertJUnit.assertEquals(new String(data2), new String(junoResponse2.getValue()));	
	}
}
