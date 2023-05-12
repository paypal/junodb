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
package com.paypal.juno.client.impl;

import com.paypal.juno.client.JunoClient;
import com.paypal.juno.client.JunoClientFactory;
import com.paypal.juno.client.io.JunoRequest;
import com.paypal.juno.client.io.JunoResponse;
import com.paypal.juno.client.io.OperationStatus;
import com.paypal.juno.conf.JunoPropertiesProvider;
import com.paypal.juno.exception.JunoException;
import com.paypal.juno.mock.MockJunoServer;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.testng.AssertJUnit;

import static org.junit.Assert.assertEquals;

public class JunoConnectionTest {

	private static MockJunoServer mjs;
	private JunoPropertiesProvider jpp;
	 
	 /**
	  * Initialize tests
	  */
	@Before
	public void initialize() throws Exception {
		URL url = JunoClientImpl.class.getClassLoader().getResource("juno.properties");
		jpp = new JunoPropertiesProvider(url);
	}
		
	@After
	public void tearDown() {
		mjs.stop();
		mjs.stopMockServer();
	}
	 
	 @Test
	 public void ConnectionTerminationTest() {
		//Make the socket timeout very less so that the mockserver will 
		//close the connection as soon as it is made
		mjs = new MockJunoServer(1);
		mjs.start();
		
		JunoClient client = JunoClientFactory.newJunoClient(jpp);
		
		try{
			client.create("insertTest1".getBytes(), "Antony".getBytes(),40);
			 AssertJUnit.assertTrue ("Exception not seen for No connection", false);
		}catch(JunoException e){
			//System.out.println("Exception:"+e.getMessage());
			assertEquals(e.getMessage(), "Response Timed out");
		}
	 }
	 
	 @Test
	 public void ConnectionTerminationBatchTest() {
		//Make the socket timeout very less so that the mockserver will 
		//close the connection as soon as it is made
		mjs = new MockJunoServer(1);
		mjs.start();
		
		
		JunoClient client = JunoClientFactory.newJunoClient(jpp);
		
		try{
			List<JunoRequest> jReqList = new ArrayList<JunoRequest>();
			jReqList.add(new JunoRequest("insert1".getBytes(),"Test1".getBytes(),0,5,JunoRequest.OperationType.Create));
			jReqList.add(new JunoRequest("insert2".getBytes(),"Test2".getBytes(),0,5,JunoRequest.OperationType.Create));
			jReqList.add(new JunoRequest("insert3".getBytes(),"Test3".getBytes(),0,5,JunoRequest.OperationType.Create));
			
			Iterable<JunoResponse> jResp = client.doBatch(jReqList);
			for(JunoResponse jr : jResp){
				assertEquals(OperationStatus.ResponseTimeout,jr.getStatus());
			}
			Thread.sleep(1000);
			//assertEquals(resp.getStatus(),);
		}catch(JunoException e){
			//System.out.println("==============================Exception:"+e.getMessage());
			assertEquals(e.getMessage(), "Response Timed out");
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	 }
}