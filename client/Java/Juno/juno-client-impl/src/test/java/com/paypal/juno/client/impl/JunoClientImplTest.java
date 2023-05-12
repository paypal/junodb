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
import com.paypal.juno.client.JunoClientConfigHolder;
import com.paypal.juno.client.JunoClientFactory;
import com.paypal.juno.client.io.JunoRequest;
import com.paypal.juno.client.io.JunoResponse;
import com.paypal.juno.client.io.OperationStatus;
import com.paypal.juno.client.io.RecordContext;
import com.paypal.juno.conf.JunoProperties;
import com.paypal.juno.conf.JunoPropertiesProvider;
import com.paypal.juno.exception.JunoException;
import com.paypal.juno.mock.MockJunoServer;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.*;

@PowerMockIgnore("javax.management.*")
@RunWith(PowerMockRunner.class)
public class JunoClientImplTest {

	private  JunoClientConfigHolder clientCfgHldr;
	private JunoPropertiesProvider jpp;
	private static MockJunoServer mjs;

	/**
	 * Initialize tests
	 */
	@BeforeClass
	public static void init() throws Exception {
	}
	
	@AfterClass
	public static void close() throws Exception {
		Thread.sleep(2000);
	}
	
	
	@Before
	public void initialize() throws Exception {
		URL url = JunoClientImpl.class.getClassLoader().getResource("juno.properties");
		jpp = new JunoPropertiesProvider(url);
		clientCfgHldr = new JunoClientConfigHolder(jpp);
		//Start the Mock server
		mjs = new MockJunoServer(15000);
		mjs.start();
	}
	
	@After
	public void tearDown() {
		clientCfgHldr = null;
		mjs.stop();
		mjs.stopMockServer();
	}

  @Test
  public void insertTest() {

	  URL url = JunoClientImpl.class.getClassLoader().getResource("juno.properties");
	  JunoClient client = JunoClientFactory.newJunoClient(url);
	  
	  JunoResponse resp = client.create("insertTest".getBytes(), "Antony".getBytes());
	  assertEquals(OperationStatus.Success, resp.getStatus());
	  try{
		  resp = client.create("insertTest".getBytes(), "Antony".getBytes());
		  assertEquals(OperationStatus.UniqueKeyViolation, resp.getStatus());
	  }catch(JunoException e){
		  assertTrue("Exception is seen for duplicate key", false);
	  }
  }
  
  @Test
  public void compressTest() throws IOException {
		
	  URL url = JunoClientImpl.class.getClassLoader().getResource("juno.properties");
	  Properties pConfig = new Properties();
	  pConfig.load(url.openStream());
	  pConfig.setProperty(JunoProperties.USE_PAYLOADCOMPRESSION, "true");
	  JunoClient client1 = JunoClientFactory.newJunoClient(new JunoPropertiesProvider(pConfig));
	  
	  pConfig.setProperty(JunoProperties.USE_PAYLOADCOMPRESSION, "false");
	  JunoClient client2 = JunoClientFactory.newJunoClient(new JunoPropertiesProvider(pConfig));
	  
	  
	  byte []  value =  new String("Couchbase stores data as key value pairs where the value is a JSON document and the key is an identifier for retrieving that document. By default cbexport will only export the value portion of the document. If you wish to include the key in the exported document then this option should be specified. The value passed to this option should be the field name that the key is stored under.Skips the SSL verification phase. Specifying this flag will allow a connection using SSL encryption, but will not verify the identity of the server you connect to. You are vulnerable to a man-in-the-middle attack if you use this flag. Either this flag or the --cacert flag must be specified when using an SSL encrypted connection Specifies a CA certificate that will be used to verify the identity of the server being connecting to. Either this flag or the --no-ssl-verify flag must be specified when using an SSL encrypted connection. Specifies the number of concurrent clients to use when exporting data. Fewer clients means exports will take longer, but there will be less cluster resources used to complete the export. More clients means faster exports, but at the cost of more cluster resource usage. This parameter defaults to 1 if it is not specified and it is recommended that this parameter is not set to be higher than the number of CPUs on the machine where the export is taking place. Exports JSON data from Couchbase. The cbexport-json command supports exporting JSON docments to a file with a document on each line or a file that contain a JSON list where each element is a document. The file format to export to can be specified with the --format flag. See the DATASET FORMATS section below for more details on the supported file formats.").getBytes();
	  JunoResponse resp = client1.create("compressTest1".getBytes(), value); //Create with compression enabled
	  assertEquals(OperationStatus.Success, resp.getStatus());
	  
	  resp = client2.get("compressTest1".getBytes()); // Read with compression disabled
	  assertEquals(OperationStatus.Success, resp.getStatus());
//	  //System.out.println("Create:"+String.format("%x", new BigInteger(1,value)));
//	  //System.out.println("Get:"+String.format("%x", new BigInteger(1,resp.getValue())));
	  assertTrue(Arrays.equals(value, resp.getValue()));
	  
	  resp = client2.create("compressTest2".getBytes(),value); // Create with compression disabled
	  assertEquals(OperationStatus.Success, resp.getStatus());
	  
	  resp = client1.get("compressTest2".getBytes()); //Read with compression enabled
	  assertEquals(OperationStatus.Success, resp.getStatus());
	  assertTrue(Arrays.equals(value, resp.getValue()));
	  
	  //System.out.println("Test with uncompressable data.");
	  //Compression on uncompressable data - Check the CAL log for result
	  byte [] value1 = DataGenUtils.genBytes(1025);
	  resp = client1.create("compressTest3".getBytes(), value1);
	  assertEquals(OperationStatus.Success, resp.getStatus());
  }
  
  @Test
  public void insertTest_withTTL() {

	  JunoClient client = JunoClientFactory.newJunoClient(jpp);
	  JunoResponse resp = client.create("insertTest".getBytes(), "Antony".getBytes(),20);
	  assertEquals(OperationStatus.Success,resp.getStatus());
	  try{
	  resp = client.create("insertTest".getBytes(), "Antony".getBytes(),30);
	  assertEquals(OperationStatus.UniqueKeyViolation,resp.getStatus());
	  }catch(JunoException e){
		  assertTrue("Exception is seen for duplicate key", false);
	  }
  }
  
  @Test
  public void GetTest() {
	  
	  JunoClient client = new JunoClientImpl(clientCfgHldr,null);
	  client.create("GetTest".getBytes(), "Antony".getBytes(),20);
	  
	  JunoResponse resp = client.get("GetTest".getBytes());
	  assertEquals(OperationStatus.Success,resp.getStatus());
	  try{
		  resp = client.get("GetTest1".getBytes());
		  assertEquals(OperationStatus.NoKey,resp.getStatus());
	  }catch(JunoException e){
		  assertTrue("Exception is seen for no key", false);
	  }
  }
  
  @Test
  public void GetTest_withTTL() {
	  
	  JunoClient client = new JunoClientImpl(clientCfgHldr,null);
	  client.create("GetTest_withTTL".getBytes(), "Antony".getBytes(),20);
	  
	  JunoResponse resp = client.get("GetTest_withTTL".getBytes(),20);
	  assertEquals(OperationStatus.Success,resp.getStatus());
	  try{
		  resp = client.get("GetTest_withTTL1".getBytes(),10);
		  assertEquals(OperationStatus.NoKey,resp.getStatus());
	  }catch(JunoException e){
		  
		  assertTrue("Exception is seen for no key", false);
	  }
  }
  
  @Test
  public void UpdateTest() {
	  
	  JunoClient client = new JunoClientImpl(clientCfgHldr,null);
	  client.create("UpdateTest".getBytes(), "Antony".getBytes(),20);
	  
	  JunoResponse resp = client.update("UpdateTest".getBytes(),"Updated".getBytes());

	  assertEquals(OperationStatus.Success,resp.getStatus());
	  assertEquals("Updated",(new String(resp.getValue())));
	  assertEquals(2,resp.getVersion());
	  
	  //Try to updated a record which is not present in DB
	  try{
		  resp = client.update("UpdateTest1".getBytes(),"Updated".getBytes());
		  assertEquals(OperationStatus.NoKey,resp.getStatus());
	  }catch(JunoException e){
		  assertTrue("Exception is seen for no key", false);
	  }
  }
  
  @Test
  public void CompateAndSetTest() {
	  
	  JunoClient client = new JunoClientImpl(clientCfgHldr,null);
	  JunoResponse resp = client.create("CompateAndSetTest".getBytes(), "Antony".getBytes(),20);
	  
	  resp = client.compareAndSet(resp.getRecordContext(),"Updated".getBytes(),20);

	  assertEquals(OperationStatus.Success,resp.getStatus());
	  assertEquals("Updated",(new String(resp.getValue())));
	  assertEquals(2,resp.getVersion());
	  
	  RecordContext rctx = new RecordContext(resp.key(),1,resp.getCreationTime(),resp.getTtl());
	  
	  //Try to updated a record with a lower version
	  try{
		  resp = client.compareAndSet(rctx,"Updated".getBytes(),20);
		  assertEquals(OperationStatus.ConditionViolation,resp.getStatus());
		  assertFalse(resp.getRecordContext().equals(rctx));
		  assertFalse(resp.getRecordContext().hashCode() == rctx.hashCode());
		  RecordContext rc = new RecordContext(resp.getRecordContext().getKey(),resp.getRecordContext().getVersion(),resp.getRecordContext().getCreationTime(),resp.getRecordContext().getTtl());
		  assertTrue(rc.equals(resp.getRecordContext()));
	  }catch(JunoException e){
		  assertTrue("Exception is seen for no key", false);
	  }
  }
  
  @Test
  public void UpdateTest_withTTL() {
	  
	  JunoClient client = new JunoClientImpl(clientCfgHldr,null);
	  client.create("UpdateTest_withTTL".getBytes(), "Antony".getBytes(),20);
	  
	  JunoResponse resp = client.update("UpdateTest_withTTL".getBytes(),"Updated".getBytes(),20);

	  assertEquals(OperationStatus.Success,resp.getStatus());
	  assertEquals("Updated",(new String(resp.getValue())));
	  assertEquals(2,resp.getVersion());
  }
  
  @Test
  public void UpsertTest() {
	  
	  JunoClient client = new JunoClientImpl(clientCfgHldr,null);
	  client.create("UpsertTest".getBytes(), "Antony".getBytes(),20);
	  
	  JunoResponse resp = client.set("UpsertTest".getBytes(),"Updated".getBytes());

	  assertEquals(OperationStatus.Success,resp.getStatus());
	  assertEquals("Updated",(new String(resp.getValue())));
	  assertEquals(2,resp.getVersion());
	  
	  resp = client.set("UpsertTest1".getBytes(),"Created".getBytes());
	  
	  assertEquals(OperationStatus.Success,resp.getStatus());
	  assertEquals("Created",(new String(resp.getValue())));
	  assertEquals(1,resp.getVersion());

  }
  
  @Test
  public void UpsertTest_withTTL() {
	  
	  JunoClient client = new JunoClientImpl(clientCfgHldr,null);
	  client.create("UpsertTest_withTTL".getBytes(), "Antony".getBytes(),20);
	  
	  JunoResponse resp = client.set("UpsertTest_withTTL".getBytes(),"Updated".getBytes(),20);

	  assertEquals(OperationStatus.Success,resp.getStatus());
	  assertEquals("Updated",(new String(resp.getValue())));
	  assertEquals(2,resp.getVersion());
	  
	  JunoResponse resp1 = client.set("UpsertTest_withTTL1".getBytes(),"Created".getBytes(),20);
	  
	  assertFalse(resp.equals(resp1));
	  assertFalse(resp.hashCode() == resp1.hashCode());
	  assertFalse(resp.toString().equals(resp1.toString()));
	  assertEquals(OperationStatus.Success,resp1.getStatus());
	  assertEquals("Created",(new String(resp1.getValue())));
	  assertEquals(1,resp1.getVersion());

  }
  
  @Test
  public void DestroyTest() {
	  
	  JunoClient client = new JunoClientImpl(clientCfgHldr,null);
	  client.create("DestroyTest".getBytes(), "Antony".getBytes(),20);
	  
	  JunoResponse resp = client.delete("DestroyTest".getBytes());
	  assertEquals(OperationStatus.Success,resp.getStatus());
	  
	  // try to destroy a record which is not present in DB.
	  resp = client.delete("DestroyTest1".getBytes());
	  assertEquals(OperationStatus.Success,resp.getStatus());

  }

  @Test
  public void BatchNegativeTest() {
	
	try{
		JunoClient client = new JunoClientImpl(clientCfgHldr,null);
		List<JunoRequest> jReqList = new ArrayList<JunoRequest>();
		jReqList.add(new JunoRequest("insert1".getBytes(),"Test1".getBytes(),0,0,JunoRequest.OperationType.Create));
		jReqList.add(new JunoRequest("insert2".getBytes(),"Test2".getBytes(),0,5,JunoRequest.OperationType.Create));
		jReqList.add(new JunoRequest("insert3".getBytes(),"Test3".getBytes(),0,5,JunoRequest.OperationType.Create));
		
		Iterable<JunoResponse> jResp = client.doBatch(jReqList);
		for(JunoResponse jr : jResp){
			if((new String(jr.getKey())).equals("insert1")){
				assertEquals(OperationStatus.IllegalArgument,jr.getStatus());
			}else{
				assertEquals(OperationStatus.Success,jr.getStatus());
			}
		}
	}catch(Exception e){
		//e.printStackTrace();
		//assertEquals(e.getMessage(),"Illegal argument");
		//assertEquals(e.getCause().getMessage(),"The Document's TTL cannot be 0 or negative.");
		
	}
  }
  
  @Test
  public void BatchPositiveTest() {
	  
	  JunoClient client = new JunoClientImpl(clientCfgHldr,null);
	  
		List<JunoRequest> jReqList = new ArrayList<JunoRequest>();
		jReqList.add(new JunoRequest("insert1".getBytes(),"Test1".getBytes(),0,5,JunoRequest.OperationType.Create));
		jReqList.add(new JunoRequest("insert2".getBytes(),"Test2".getBytes(),0,5,JunoRequest.OperationType.Create));
		jReqList.add(new JunoRequest("insert3".getBytes(),"Test3".getBytes(),0,5,JunoRequest.OperationType.Create));
		
		JunoRequest jreq1 = jReqList.get(0);
		Iterable<JunoResponse> jResp = client.doBatch(jReqList);
		for(JunoResponse jr : jResp){
			assertEquals(OperationStatus.Success,jr.getStatus());
		}
		
		jReqList.clear();
		jReqList.add(new JunoRequest("insert1".getBytes(),"".getBytes(),0,5,JunoRequest.OperationType.Get));
		jReqList.add(new JunoRequest("insert2".getBytes(),"Test4".getBytes(),0,5,JunoRequest.OperationType.Update));
		jReqList.add(new JunoRequest("insert3".getBytes(),"Test5".getBytes(),0,5,JunoRequest.OperationType.Update));
		
		JunoRequest jreq2 = jReqList.get(0);
		assertFalse(jreq1.equals(jreq2));
		assertFalse(jreq1.hashCode() == jreq2.hashCode());
		assertFalse(jreq1.toString().equals(jreq2.toString()));
		Iterable<JunoResponse> resp = client.doBatch(jReqList);
		for(JunoResponse response : resp){
			assertEquals(OperationStatus.Success,response.getStatus());
		}
		
		jReqList.remove(2);
		jReqList.add(new JunoRequest("insert3".getBytes(),"Test6".getBytes(),0,5,JunoRequest.OperationType.Set));
		jReqList.add(new JunoRequest("insert4".getBytes(),"Test7".getBytes(),0,5,JunoRequest.OperationType.Set));
		jReqList.add(new JunoRequest("insert5".getBytes(),"Test8".getBytes(),0,5,JunoRequest.OperationType.Set));
		jReqList.add(new JunoRequest("insert6".getBytes(),"Test9".getBytes(),0,5,JunoRequest.OperationType.Set));
		
		resp = client.doBatch(jReqList);
		for(JunoResponse response : resp){
			assertEquals(OperationStatus.Success,response.getStatus());
		}
			
		List<JunoRequest> jReq = new ArrayList<JunoRequest>();
		jReq.add(new JunoRequest("insert1".getBytes(),null,0,0,JunoRequest.OperationType.Destroy));
		jReq.add(new JunoRequest("insert2".getBytes(),null,0,0,JunoRequest.OperationType.Destroy));
		
		resp = client.doBatch(jReq);
		for(JunoResponse response : resp){
			assertEquals(OperationStatus.Success,response.getStatus());
		}
		
		jReqList.clear();
		jReqList.add(new JunoRequest("insert1".getBytes(),null,0,0,JunoRequest.OperationType.Get));
		jReqList.add(new JunoRequest("insert2".getBytes(),null,0,0,JunoRequest.OperationType.Get));
		jReqList.add(new JunoRequest("insert3".getBytes(),null,0,0,JunoRequest.OperationType.Get));
		
		resp = client.doBatch(jReqList);
		for(JunoResponse response : resp){
			if(Arrays.equals(response.getKey(),"insert1".getBytes()) || Arrays.equals(response.getKey(),"insert2".getBytes())){
				assertEquals(OperationStatus.NoKey,response.getStatus());
			}else{
				assertEquals(OperationStatus.Success,response.getStatus());
			}
		}
  }
  
	 @Test
	 public void ConnectionRecycleTest() {
		
		JunoClient client = JunoClientFactory.newJunoClient(jpp);
		JunoResponse resp = client.create("insertTest1".getBytes(), "Antony".getBytes(),40);
		assertEquals(resp.getStatus(), OperationStatus.Success);
		
		//Sleep engough for connection to recycle
		try {
			Thread.sleep(15000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		resp = client.get("insertTest1".getBytes());
		assertEquals(resp.getStatus(), OperationStatus.Success);
	 }
}