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

import com.paypal.juno.client.JunoAsyncClient;
import com.paypal.juno.client.JunoClient;
import com.paypal.juno.client.JunoClientConfigHolder;
import com.paypal.juno.client.JunoClientFactory;
import com.paypal.juno.client.io.JunoRequest;
import com.paypal.juno.client.io.JunoResponse;
import com.paypal.juno.client.io.OperationStatus;
import com.paypal.juno.client.io.RecordContext;
import com.paypal.juno.conf.JunoPropertiesProvider;
import com.paypal.juno.exception.JunoException;
import com.paypal.juno.io.protocol.JunoMessage.OperationType;
import com.paypal.juno.io.protocol.JunoMessage;
import com.paypal.juno.io.protocol.OperationMessage;
import com.paypal.juno.mock.MockJunoServer;
import com.paypal.juno.mock.MockJunoUnresponsiveServer;
import com.paypal.juno.transport.socket.SocketConfigHolder;
import com.paypal.juno.util.JunoClientUtil;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.AssertJUnit;

import rx.Single;
import rx.SingleSubscriber;

import static org.junit.Assert.*;

@RunWith(PowerMockRunner.class)
public class JunoAsyncClientImplTest {

	private  JunoClientConfigHolder clientCfgHldr;
	private JunoAsyncClient client;
	private Method methodValidate;
	private Method methodCreateOpMsg;
	private Method methodDecodeOpMsg;
	private JunoPropertiesProvider jpp;
	private SocketConfigHolder socCfg;
	private JunoResponse jres;
	private static MockJunoServer mjs;
	private static MockJunoUnresponsiveServer mjus;
	public String longKey = new String("InsertNegativeInsertNegativeInsertNegativeInsertNegativeInsertNegativeInsertNegativeInsertNegativeInsertNegativeInsertNegativeInsertNegativeInsertNegativeInsertNegativeInsertNegativeInsertNegativeInsertNegativeInsertNegativeInsertNegativeInsertNegativeInsertNegativeInsertNegativeInsertNegative");

	private static Logger LOGGER;
	/**
	 * Initialize tests
	 */
	@Before
	public void initialize() throws Exception {
		LOGGER = LoggerFactory.getLogger(JunoAsyncClientImplTest.class);
		URL url = JunoClientImpl.class.getClassLoader().getResource("juno.properties");
		jpp = new JunoPropertiesProvider(url);
		clientCfgHldr = new JunoClientConfigHolder(jpp);
		socCfg = new SocketConfigHolder(clientCfgHldr);
		//client = PowerMockito.spy(new JunoAsyncClientImpl(clientCfgHldr));
		methodValidate = JunoClientUtil.class.getDeclaredMethod("validateInput", JunoRequest.class,OperationType.class, JunoClientConfigHolder.class);
		methodValidate.setAccessible(true);
		//Start the Mock server
		mjs = new MockJunoServer(15000);
		mjus = new MockJunoUnresponsiveServer(15000, true);
		mjs.start();
		mjus.start();
		client = JunoClientFactory.newJunoAsyncClient(url);
	}
	
	@After
	public void tearDown() {
		clientCfgHldr = null;
		if(mjs != null) {
			mjs.stop();
			mjs.stopMockServer();
		}
		if(mjus != null) {
			mjus.stop();
			mjus.stopMockServer();
		}
	}

	public static String generateRandomChars(String candidateChars, int length) {
		StringBuilder sb = new StringBuilder();
		Random random = new Random();
		for (int i = 0; i < length; i++) {
			sb.append(candidateChars.charAt(random.nextInt(candidateChars
					.length())));
		}
		return sb.toString();
	}

  @Test
  public void insertTest() {
	  jres = client.create("insertTest".getBytes(), "Antony".getBytes()).toBlocking().value();
	  assertEquals(OperationStatus.Success,jres.getStatus());
	  try{
		  jres = client.create("insertTest".getBytes(), "Antony".getBytes()).toBlocking().value();
		  assertEquals(OperationStatus.UniqueKeyViolation,jres.getStatus());
	  }catch(JunoException e){
		  AssertJUnit.assertTrue ("Exception for UniqueKeyViolation", false);
	  }
  }
  
  @Test
  public void insertTest_withTTL() {
	  
	  JunoResponse resp = client.create("insertTest".getBytes(), "Antony".getBytes(),20).toBlocking().value();
	  assertEquals(OperationStatus.Success,resp.getStatus());
	  try{
		  resp = client.create("insertTest".getBytes(), "Antony".getBytes(),30).toBlocking().value();
		  assertEquals(OperationStatus.UniqueKeyViolation,resp.getStatus());
	  }catch(JunoException e){
		  AssertJUnit.assertTrue ("Exception for UniqueKeyViolation", false);
	  }
  }
  
  @Test
  public void GetTest() {
	  
	  JunoClient jclient = new JunoClientImpl(clientCfgHldr,null);
	  jclient.create("GetTest".getBytes(), "Antony".getBytes(),20);
	  
	  JunoResponse resp = client.get("GetTest".getBytes()).toBlocking().value();
	  assertEquals(OperationStatus.Success,resp.getStatus());
	  try{
		  resp = client.get("GetTest1".getBytes()).toBlocking().value();
		  assertEquals(OperationStatus.NoKey,resp.getStatus());
	  }catch(JunoException e){
		  AssertJUnit.assertTrue ("Exception for NoKey", false);
	  }
  }

  @Test
  public void OpaqueResMapClearTest(){
	  JunoClient jclient = new JunoClientImpl(clientCfgHldr,null);
	  jclient.create("OpaqueResMapClearTest".getBytes(), "Test".getBytes(),20);

	  URL url = JunoClientImpl.class.getClassLoader().getResource("junoUnresponsive.properties");
	  JunoAsyncClient unresClient = JunoClientFactory.newJunoAsyncClient(url);

	  ConcurrentHashMap<Integer, BlockingQueue<OperationMessage>> trial = null;

	  SingleSubscriber<JunoResponse> getSubscriber = new SingleSubscriber<JunoResponse>() {
		  @Override
		  public void onSuccess(JunoResponse res) {
			  assertEquals(OperationStatus.Success,res.getStatus());
		  }

		  @Override
		  public void onError(Throwable e) {
			  assertEquals(OperationStatus.InternalError.getErrorText(), e.getMessage());
		  }
	  };

	  for(int i =0; i< 100; i++){
		  Single<JunoResponse> response = unresClient.get("OpaqueResMapClearTest".getBytes());
		  response.subscribe(getSubscriber);
	  }
	  Set<Thread> threadSet = Thread.getAllStackTraces().keySet();
	  for(Thread thread : threadSet){
		  if(thread.getName().contains("RxIoScheduler")){
			  thread.interrupt();
		  }
	  }

	  try{
		  Thread.sleep(3000);
		  Field opaqueResMapField = unresClient.getClass().getDeclaredField("opaqueResMap");
		  opaqueResMapField.setAccessible(true);
		  trial = (ConcurrentHashMap) opaqueResMapField.get(unresClient);
		  assertEquals(0, trial.size());
	  }catch(Exception e){
		  AssertJUnit.assertTrue("Exception for getDeclaredField or Sleep", false);
	  }
  }
  
  @Test
  public void GetTest_withTTL() {
	  
	  JunoClient jclient = new JunoClientImpl(clientCfgHldr,null);
	  jclient.create("GetTest_withTTL".getBytes(), "Antony".getBytes(),20);
	  
	  JunoResponse resp = client.get("GetTest_withTTL".getBytes(),20).toBlocking().value();
	  assertEquals(OperationStatus.Success,resp.getStatus());
	  try{
		  resp = client.get("GetTest_withTTL1".getBytes(),10).toBlocking().value();
		  assertEquals(OperationStatus.NoKey,resp.getStatus());
	  }catch(JunoException e){
		  AssertJUnit.assertTrue ("Exception for NoKey", false);
	  }
  }
  
  @Test
  public void UpdateTest() {
	  
	  JunoClient jclient = new JunoClientImpl(clientCfgHldr,null);
	  jclient.create("UpdateTest".getBytes(), "Antony".getBytes(),20);
	  
	  JunoResponse resp = client.update("UpdateTest".getBytes(),"Updated".getBytes()).toBlocking().value();

	  assertEquals(OperationStatus.Success,resp.getStatus());
	  assertEquals("Updated",(new String(resp.getValue())));
	  assertEquals(2,resp.getVersion());
	  
	  //Try to updated a record which is not present in DB
	  try{
		  resp = client.update("UpdateTest1".getBytes(),"Updated".getBytes()).toBlocking().value();
		  assertEquals(OperationStatus.NoKey,resp.getStatus());
	  }catch(JunoException e){
		  AssertJUnit.assertTrue ("Exception for NoKey", false);
	  }
	  
  }
  
  @Test
  public void CompateAndSetTest() {
	  
	  JunoClient jclient = new JunoClientImpl(clientCfgHldr,null);
	  JunoResponse resp=jclient.create("CompateAndSetTest".getBytes(), "Antony".getBytes(),20);

	  //System.out.println("The response version is:"+resp.getRecordContext().getVersion());
	  resp = client.compareAndSet(resp.getRecordContext(),"Updated".getBytes(),20).toBlocking().value();

	  assertEquals(OperationStatus.Success,resp.getStatus());
	  assertEquals("Updated",(new String(resp.getValue())));
	  assertEquals(2,resp.getVersion());
	  
	  RecordContext rctx = new RecordContext(resp.key(),1,resp.getCreationTime(),resp.getTtl());
	  //Try to updated a record with a lower version
	  try{
		  resp = client.compareAndSet(rctx,"Updated".getBytes(),20).toBlocking().value();
		  assertEquals(OperationStatus.ConditionViolation,resp.getStatus());
	  }catch(JunoException e){
		  AssertJUnit.assertTrue ("Exception for VersionMismatch", false);
	  }

  }
  
  @Test
  public void UpdateTest_withTTL() {
	  
	  JunoClient jclient = new JunoClientImpl(clientCfgHldr,null);
	  jclient.create("UpdateTest_withTTL".getBytes(), "Antony".getBytes(),20);
	  
	  JunoResponse resp = client.update("UpdateTest_withTTL".getBytes(),"Updated".getBytes(),20).toBlocking().value();

	  assertEquals(OperationStatus.Success,resp.getStatus());
	  assertEquals("Updated",(new String(resp.getValue())));
	  assertEquals(2,resp.getVersion());

  }
  
  @Test
  public void UpsertTest() {
	  
	  JunoClient jclient = new JunoClientImpl(clientCfgHldr,null);
	  jclient.create("UpsertTest".getBytes(), "Antony".getBytes(),20);
	  
	  JunoResponse resp = client.set("UpsertTest".getBytes(),"Updated".getBytes()).toBlocking().value();

	  assertEquals(OperationStatus.Success,resp.getStatus());
	  assertEquals("Updated",(new String(resp.getValue())));
	  assertEquals(2,resp.getVersion());
	  
	  resp = client.set("UpsertTest1".getBytes(),"Created".getBytes()).toBlocking().value();
	  
	  assertEquals(OperationStatus.Success,resp.getStatus());
	  assertEquals("Created",(new String(resp.getValue())));
	  assertEquals(1,resp.getVersion());
	  
  }
  
  @Test
  public void UpsertTest_withTTL() {
	  
	  JunoClient jclient = new JunoClientImpl(clientCfgHldr,null);
	  jclient.create("UpsertTest_withTTL".getBytes(), "Antony".getBytes(),20);
	  
	  JunoResponse resp = client.set("UpsertTest_withTTL".getBytes(),"Updated".getBytes()).toBlocking().value();

	  assertEquals(OperationStatus.Success,resp.getStatus());
	  assertEquals("Updated",(new String(resp.getValue())));
	  assertEquals(2,resp.getVersion());
	  
	  resp = client.set("UpsertTest_withTTL1".getBytes(),"Created".getBytes()).toBlocking().value();
	  
	  assertEquals(OperationStatus.Success,resp.getStatus());
	  assertEquals("Created",(new String(resp.getValue())));
	  assertEquals(1,resp.getVersion());
	 
  }
  
  @Test
  public void DestroyTest() {
	  
	  JunoClient jclient = new JunoClientImpl(clientCfgHldr,null);
	  jclient.create("DestroyTest".getBytes(), "Antony".getBytes(),20);
	  
	  JunoResponse resp = client.delete("DestroyTest".getBytes()).toBlocking().value();
	  assertEquals(OperationStatus.Success,resp.getStatus());
	  
	  // try to destroy a record which is not present in DB.
	  try{
		  resp = client.delete("DestroyTest1".getBytes()).toBlocking().value();
	  	  assertEquals(OperationStatus.Success,resp.getStatus());
	  }catch(JunoException e){
		  AssertJUnit.assertTrue ("Exception for Destroy", false);
	  }
	  
  }
  
  @Test
  public void CreateNegativeTest() {
	  try{
			 client.create("".getBytes(), "InsertNegativeValue".getBytes()).toBlocking().value();
			 AssertJUnit.assertTrue ("Exception not seen for empty key", false);
		 }catch(JunoException e){
			 assertEquals(OperationStatus.IllegalArgument.getErrorText(),e.getMessage());
			 assertTrue(e.getCause() instanceof IllegalArgumentException);
			 assertEquals(e.getCause().getMessage(),"The Document key must not be null or empty");
		 }
		 
		 try{
			 client.create(longKey.getBytes(),"".getBytes()).toBlocking().value();
			 AssertJUnit.assertTrue ("Exception not seen for long key", false);
		 }catch(JunoException e){
			 assertTrue(e.getCause() instanceof IllegalArgumentException);
			 assertEquals(OperationStatus.IllegalArgument.getErrorText(),e.getMessage());
			 assertEquals(e.getCause().getMessage(),"The Document key must not be larger than 128 bytes");
		 }
		 
		 try{
//			 byte[] array = new byte[804900]; // length is bounded by
			 String candidateChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
			 String payload = generateRandomChars(candidateChars,204900);
			 client.create("Test".getBytes(),payload.getBytes()).toBlocking().value();
			 AssertJUnit.assertTrue ("Exception not seen for large Payload", false);
		 }catch(JunoException e){
			 //System.out.println("Error is:"+e.getCause().getMessage());
			 assertEquals(OperationStatus.IllegalArgument.getErrorText(),e.getMessage());
			 assertTrue(e.getCause() instanceof IllegalArgumentException);
			 assertEquals(e.getCause().getMessage(),"The Document Value must not be larger than 204800 bytes. Current value size=204900");
		 }
		 
		 try{
			 client.create("".getBytes(), "InsertNegativeValue".getBytes(),10).toBlocking().value();
			 AssertJUnit.assertTrue ("Exception not seen for empty key", false);
		 }catch(JunoException e){
			 //System.out.println("Error is:"+e.getCause().getMessage());
			 assertEquals(OperationStatus.IllegalArgument.getErrorText(),e.getMessage());
			 assertTrue(e.getCause() instanceof IllegalArgumentException);
			 assertEquals(e.getCause().getMessage(),"The Document key must not be null or empty");
		 }
		 
		 try{
			 client.create("InsertNegative".getBytes(), "InsertNegativeValue".getBytes(),0).toBlocking().value();
			 AssertJUnit.assertTrue ("Exception not seen for 0 TTL", false);
		 }catch(JunoException e){
			// //System.out.println("Error is:"+e.getCause().getMessage());
			 assertEquals(OperationStatus.IllegalArgument.getErrorText(),e.getMessage());
			 assertTrue(e.getCause() instanceof IllegalArgumentException);
			 assertEquals(e.getCause().getMessage(),"The Document's TTL cannot be 0 or negative.");
		 }
  }

  @Test 
  public void GetNegativeTest(){
		 try{
			 client.get("".getBytes()).toBlocking().value();
			 AssertJUnit.assertTrue ("Exception not seen for empty key", false);
		 }catch(JunoException e) {
			 //System.out.println("get Error is:" + e.getCause().getMessage());
			 assertEquals(OperationStatus.IllegalArgument.getErrorText(), e.getMessage());
			 assertTrue(e.getCause() instanceof IllegalArgumentException);
			 assertEquals(e.getCause().getMessage(), "The Document key must not be null or empty");
		 }
		 
		 try{
			 client.get(longKey.getBytes()).toBlocking().value();
			 AssertJUnit.assertTrue ("Exception not seen for long key", false);
		 }catch(JunoException e){
			 assertTrue(e.getCause() instanceof IllegalArgumentException);
			 assertEquals(OperationStatus.IllegalArgument.getErrorText(),e.getMessage());
			 assertEquals(e.getCause().getMessage(),"The Document key must not be larger than 128 bytes");
		 }

		 try{
			 client.get("".getBytes(),200).toBlocking().value();
			  AssertJUnit.assertTrue ("Exception not seen for empty key", false);
  		}catch(JunoException e) {
			//System.out.println("get Error is:" + e.getCause().getMessage());
			assertEquals(OperationStatus.IllegalArgument.getErrorText(), e.getMessage());
			assertTrue(e.getCause() instanceof IllegalArgumentException);
			assertEquals(e.getCause().getMessage(), "The Document key must not be null or empty");
		}

		 try{
			 client.get("getNegative".getBytes(),900000000).toBlocking().value();
		 }catch(JunoException e){
			 //System.out.println("get Error is:"+e.getCause().getMessage());
			 assertEquals(OperationStatus.IllegalArgument.getErrorText(),e.getMessage());
			 assertTrue(e.getCause() instanceof IllegalArgumentException);
			 assertEquals(e.getCause().getMessage(),"Invalid lifetime. current lifetime=900000000, max configured lifetime=259200");
		 }
  }
  
  @Test
  public void UpdateNegativeTest() {
	  try{
		  client.update("UpdateNegative".getBytes(), "UpdateNegativeValue".getBytes(),-1).toBlocking().value();
		  AssertJUnit.assertTrue ("Exception not seen for negative TTL", false);
	  }catch(JunoException e){
		  //System.out.println("update1 Error is:"+e.getCause().getMessage());
		  assertEquals(OperationStatus.IllegalArgument.getErrorText(),e.getMessage());
		  assertTrue(e.getCause() instanceof IllegalArgumentException);
		  assertEquals(e.getCause().getMessage(),"The Document's TTL cannot be negative. Current lifetime=-1");
	  }

	  try{
		  client.update("".getBytes(), "UpdateNegativeValue".getBytes()).toBlocking().value();
		  AssertJUnit.assertTrue ("Exception not seen for empty key", false);
	  }catch(JunoException e){
		  //System.out.println("update2 Error is:"+e.getCause().getMessage());
		  assertEquals(OperationStatus.IllegalArgument.getErrorText(),e.getMessage());
		  assertTrue(e.getCause() instanceof IllegalArgumentException);
		  assertEquals(e.getCause().getMessage(),"The Document key must not be null or empty");
	  }
	  
		 try{
			 client.update(longKey.getBytes(),"".getBytes()).toBlocking().value();
			 AssertJUnit.assertTrue ("Exception not seen for long key", false);
		 }catch(JunoException e){
			 assertTrue(e.getCause() instanceof IllegalArgumentException);
			 assertEquals(OperationStatus.IllegalArgument.getErrorText(),e.getMessage());
			 assertEquals(e.getCause().getMessage(),"The Document key must not be larger than 128 bytes");
		 }

	  try{
		  client.update("UpdateNegative".getBytes(), new String(new char[204900]).replace('\0', 'a').getBytes()).toBlocking().value();
	  }catch(JunoException e){
		  //System.out.println("update3 Error is:"+e.getCause().getMessage());
		  assertEquals(OperationStatus.IllegalArgument.getErrorText(),e.getMessage());
		  assertTrue(e.getCause() instanceof IllegalArgumentException);
		  assertEquals(e.getCause().getMessage(),"The Document Value must not be larger than 204800 bytes. Current value size=204900");
	  }

	  try{
		  client.update("".getBytes(), "UpdateNegativeValue".getBytes(),10).toBlocking().value();
		  AssertJUnit.assertTrue ("Exception not seen for empty key", false);
	  }catch(JunoException e){
		  //System.out.println("update4 Error is:"+e.getCause().getMessage());
		  assertEquals(OperationStatus.IllegalArgument.getErrorText(),e.getMessage());
		  assertTrue(e.getCause() instanceof IllegalArgumentException);
		  assertEquals(e.getCause().getMessage(),"The Document key must not be null or empty");
	  }

	  try{
		  client.update("UpdateNegative".getBytes(), new String(new char[204900]).replace('\0', 'a').getBytes(),0).toBlocking().value();
	  }catch(JunoException e) {
		  //System.out.println("update5 Error is:" + e.getCause().getMessage());
		  assertEquals(OperationStatus.IllegalArgument.getErrorText(), e.getMessage());
		  assertTrue(e.getCause() instanceof IllegalArgumentException);
		  assertEquals(e.getCause().getMessage(), "The Document Value must not be larger than 204800 bytes. Current value size=204900");
	  }
  }

  @Test
  public void SetNegativeTest() {
	try{
		JunoResponse res = client.set("SetNegative".getBytes(), "".getBytes()).toBlocking().value();
		assertEquals(OperationStatus.Success,res.getStatus());
	}catch(JunoException e){
			AssertJUnit.assertTrue ("Exception seen for empty paylaod"+e, false);
	}

	 try{
		 client.set("".getBytes(), "".getBytes()).toBlocking().value();
		  AssertJUnit.assertTrue ("Exception not seen for empty key", false);
	  }catch(JunoException e){
		 assertEquals(OperationStatus.IllegalArgument.getErrorText(),e.getMessage());
		 assertTrue(e.getCause() instanceof IllegalArgumentException);
		 assertEquals(e.getCause().getMessage(),"The Document key must not be null or empty");
	  }

	 try{
		 client.set(longKey.getBytes(),"".getBytes()).toBlocking().value();
		 AssertJUnit.assertTrue ("Exception not seen for long key", false);
	 }catch(JunoException e){
		 assertTrue(e.getCause() instanceof IllegalArgumentException);
		 assertEquals(OperationStatus.IllegalArgument.getErrorText(),e.getMessage());
		 assertEquals(e.getCause().getMessage(),"The Document key must not be larger than 128 bytes");
	 }
	 
	 try{
		  JunoResponse res = client.set("".getBytes(), "".getBytes(),10).toBlocking().value();
		  assertEquals(OperationStatus.Success,res.getStatus());
	  }catch(JunoException e){
		 assertEquals(OperationStatus.IllegalArgument.getErrorText(),e.getMessage());
		 assertTrue(e.getCause() instanceof IllegalArgumentException);
		 assertEquals(e.getCause().getMessage(),"The Document key must not be null or empty");
	  }
	 
	 try{
		 client.set(longKey.getBytes(),"".getBytes(),10).toBlocking().value();
		 AssertJUnit.assertTrue ("Exception not seen for long key", false);
	 }catch(JunoException e){
		 assertTrue(e.getCause() instanceof IllegalArgumentException);
		 assertEquals(OperationStatus.IllegalArgument.getErrorText(),e.getMessage());
		 assertEquals(e.getCause().getMessage(),"The Document key must not be larger than 128 bytes");
	 }
	 
	  try{
		  client.set("SetNegative".getBytes(), "SetNegativeValue".getBytes(),-1).toBlocking().value();
		}catch(JunoException e) {
			assertEquals(OperationStatus.IllegalArgument.getErrorText(), e.getMessage());
			assertTrue(e.getCause() instanceof IllegalArgumentException);
			assertEquals(e.getCause().getMessage(), "The Document's TTL cannot be negative. Current lifetime=-1");
		}
	}
  
	@Test
	public void CompareAndSetNegativeTest() {
		try{
			RecordContext rctx = new RecordContext("Test".getBytes(),1,100,100);
			JunoResponse res = client.compareAndSet(rctx, "CompareAndSetKeyNotInDb".getBytes(),1200).toBlocking().value();
			assertEquals(OperationStatus.NoKey,res.getStatus());
		}catch(JunoException e){
			AssertJUnit.assertTrue ("Exception for no key", false);
		}
		
		 try{
			 RecordContext rctx = new RecordContext(longKey.getBytes(),1,100,100);
			 client.compareAndSet(rctx, "CompareAndSetKeyNotInDb".getBytes(),1200).toBlocking().value();
			 AssertJUnit.assertTrue ("Exception not seen for Long key", false);
		 }catch(JunoException e){
			 assertTrue(e.getCause() instanceof IllegalArgumentException);
			 assertEquals(OperationStatus.IllegalArgument.getErrorText(),e.getMessage());
			 assertEquals(e.getCause().getMessage(),"The Document key must not be larger than 128 bytes");
		 }

		try{
			client.compareAndSet(null, "CompareAndSetKeyNotInDb".getBytes(),1200).toBlocking().value();
			AssertJUnit.assertTrue ("Exception not seen for null ctx", false);
		}catch(JunoException e){
			//System.out.println("Error :"+e.getCause().getMessage());
			assertEquals(OperationStatus.IllegalArgument.getErrorText(),e.getMessage());
			 assertTrue(e.getCause() instanceof IllegalArgumentException);
			 assertEquals(e.getCause().getMessage(),"Record Context cannot be null");
		}
		
		try{
			RecordContext rctx = new RecordContext("".getBytes(),1,100,100);
			client.compareAndSet(rctx, "CompareAndSetKeyNotInDb".getBytes(),1200).toBlocking().value();
			AssertJUnit.assertTrue ("Exception not seen for empty key", false);
		}catch(JunoException e){
			 assertEquals(OperationStatus.IllegalArgument.getErrorText(),e.getMessage());
			 assertTrue(e.getCause() instanceof IllegalArgumentException);
			 assertEquals(e.getCause().getMessage(),"The Document key must not be null or empty");
		}
		
		try{
			RecordContext rctx = new RecordContext("Test".getBytes(),1,100,100);
			client.compareAndSet(rctx, "CompareAndSetNegativeValue".getBytes(),-1).toBlocking().value();
		}catch(JunoException e){
			assertEquals(OperationStatus.IllegalArgument.getErrorText(),e.getMessage());
			assertTrue(e.getCause() instanceof IllegalArgumentException);
			assertEquals(e.getCause().getMessage(),"The Document's TTL cannot be negative. Current lifetime=-1");
		}

	}

   @Test
   public void DestroyNegativeTest() {
	 try{
		 client.delete("DeleteNegative".getBytes()).toBlocking().value();
	 }catch(JunoException e){
		 AssertJUnit.assertTrue ("Exception for deletion for nokey in db", false);
	 }

	 try{
		 client.delete("".getBytes()).toBlocking().value();
		 AssertJUnit.assertTrue ("Exception not seen for empty key", false);
	 }catch(JunoException e){
		 assertEquals(OperationStatus.IllegalArgument.getErrorText(),e.getMessage());
		 assertTrue(e.getCause() instanceof IllegalArgumentException);
		 assertEquals(e.getCause().getMessage(),"The Document key must not be null or empty");
	 }
	 
	 try{
		 client.delete(longKey.getBytes()).toBlocking().value();
	 }catch(JunoException e){
		 assertTrue(e.getCause() instanceof IllegalArgumentException);
		 assertEquals(OperationStatus.IllegalArgument.getErrorText(),e.getMessage());
		 assertEquals(e.getCause().getMessage(),"The Document key must not be larger than 128 bytes");
	 }
  }
  
 @Test
 public void doBatchNegativeTest(){
	 try{
		 List<JunoRequest> list = null;
		 client.doBatch(list).toBlocking();
		 AssertJUnit.assertTrue ("Exception not seen for null request list", false);
	 }catch(JunoException e){
		 //System.out.println("Error1 :"+e.getCause().getMessage());
		 assertTrue(e.getCause() instanceof IllegalArgumentException);
		 assertEquals(OperationStatus.IllegalArgument.getErrorText(),e.getMessage());
		 assertEquals(e.getCause().getMessage(),"Request argument is null");
	 }
	 
	 try{
		 List<JunoRequest> list = new ArrayList<>();
		 client.doBatch(list).toBlocking();
		 AssertJUnit.assertTrue ("Exception not seen for empty request list", false);
	 }catch(JunoException e){
		 //System.out.println("Error2 :"+e.getCause().getMessage());
		 assertTrue(e.getCause() instanceof IllegalArgumentException);
		 assertEquals(OperationStatus.IllegalArgument.getErrorText(),e.getMessage());
		 assertEquals(e.getCause().getMessage(),"Empty request list supplied");
	 }
	 
	 try{
		 List<JunoRequest> list = new ArrayList<>();
		 JunoRequest item = new JunoRequest(longKey.getBytes(), "Test".getBytes(), (long)0, 180, System.currentTimeMillis(), JunoRequest.OperationType.Create);
		 list.add(item);
		 Iterable<JunoResponse> batchResp = client.doBatch(list).toBlocking().toIterable();
		 for (JunoResponse mResponse: batchResp) {	
			 //System.out.println("Resp :"+mResponse.getStatus().getErrorText());
			 assertEquals(OperationStatus.IllegalArgument,mResponse.getStatus());
		 }
	 }catch(JunoException e){
		 //System.out.println("Exceprtion:"+e.getMessage());
		 assertEquals(OperationStatus.InternalError.getErrorText(),e.getMessage());
	 }
	 
	 try{
		 byte[][] key = new byte[5000][];
		 byte[][] payload = new byte[5000][];
		 List<JunoRequest> list = new ArrayList<>();
		 for (int i = 0; i < 5000; i ++) {
			 key[i] = ("Test"+i).getBytes();
			 payload[i] = "test_payload".getBytes();
			 JunoRequest item = new JunoRequest(key[i], payload[i], (long)0, 180, System.currentTimeMillis(), JunoRequest.OperationType.Create);
			 list.add(item);
		 }
		 //System.out.println("Calling do batch");
		 client.doBatch(list).toBlocking().toIterable();
		 //AssertJUnit.assertTrue ("Exception not seen for empty request list", false);
	 }catch(JunoException e){
		 //System.out.println("Error4 :"+e.getCause().getMessage());
		 assertTrue(e.getCause() instanceof IllegalArgumentException);
		 assertEquals(OperationStatus.IllegalArgument.getErrorText(),e.getMessage());
		 assertEquals(e.getCause().getMessage(),"Empty request list supplied");
	 }
 }
   
  @Test
  public void createAndDecodeOperationMessage() {
	  
	  try{
		  String key = "TestKey";
		  String value = "TestValue";
		  long version = 0;
		  long timeToLiveSec = 200;
		  JunoMessage msg = new JunoMessage(key.getBytes(),value.getBytes(),version,0,timeToLiveSec,OperationType.Create);
		  OperationMessage opMsg = (OperationMessage) methodCreateOpMsg.invoke(client,msg,InetAddress.getLocalHost(),8080);
		  assertNotNull(opMsg);
		  JunoMessage junoMsg = (JunoMessage) methodDecodeOpMsg.invoke(client,opMsg,key.getBytes());
		  assertNotNull(junoMsg);
	  }catch(InvocationTargetException e){
		  e.printStackTrace();
		  assertNotNull(e);
	  }catch(Exception e){
		  
	  }
	  
	  try{
		  String key = "TestKey";
		  String value = "TestValue";
		  long version = 1;
		  long timeToLiveSec = 200;
		  JunoMessage msg = new JunoMessage(key.getBytes(),value.getBytes(),version,0,timeToLiveSec,OperationType.Update);
		  OperationMessage opMsg = (OperationMessage) methodCreateOpMsg.invoke(client,msg,InetAddress.getLocalHost(),8080);
		  assertNotNull(opMsg);
		  JunoMessage junoMsg = (JunoMessage) methodDecodeOpMsg.invoke(client,opMsg,key.getBytes());
		  assertNotNull(junoMsg);
	  }catch(InvocationTargetException e){
		  e.printStackTrace();
		  assertNotNull(e);
	  }catch(Exception e){
		  
	  }
  }


  @Test
  public void validateInputPositiveInsert(){
	  try{
		  // Happy Path validation -- Insert
		  try{
			  String key = "TestKey";
			  String value = "TestValue";
			  long version = 0;
			  long timeToLiveSec = 200;
			  JunoRequest req = new JunoRequest(key.getBytes(),value.getBytes(),version,timeToLiveSec,JunoRequest.OperationType.Create);
			  JunoMessage msg = (JunoMessage) methodValidate.invoke(client,req,OperationType.Create,clientCfgHldr);
			  assertNotNull(msg);	
		  }catch(JunoException e){
			  //We shouldn't be here
			  assertNotNull(e);
		  }
	  }catch(Exception e){
	  }
  }
  
  @Test
  public void validateInputPayload(){
	  try{
		  // Positive case - Empty Payload
		  try{
			  String key = "TestKey";
			  String value = "";
			  long version = 1;
			  long timeToLiveSec = 200;
			  JunoRequest req = new JunoRequest(key.getBytes(),value.getBytes(),version,timeToLiveSec,JunoRequest.OperationType.Create);
			  JunoMessage msg = (JunoMessage) methodValidate.invoke(client,req,OperationType.Create,clientCfgHldr);
			  assertNotNull(msg);	
		  }catch(Exception e){
			  //We shouldn't be here
			  assertFalse(true);
		  }

		  // Negative case - null Payload
		  try{
			  String key = "TestKey";
			  long version = 1;
			  long timeToLiveSec = 200;
			  JunoRequest req = new JunoRequest(key.getBytes(),null,version,timeToLiveSec,JunoRequest.OperationType.Create);
			  JunoMessage msg = (JunoMessage) methodValidate.invoke(client,req,OperationType.Create,clientCfgHldr);
			  assertNotNull(msg);
		  }catch(Exception e){
			  //We shouldn't be here
			  assertFalse(true);
		  }
		  
		  // Negative case - Max Payload
		  try{
			  String key = "TestKey";
			  String candidateChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
			  String payload = generateRandomChars(candidateChars,204801);
			  long version = 1;
			  long timeToLiveSec = 200;
			  JunoRequest req = new JunoRequest(key.getBytes(),payload.getBytes(),version,timeToLiveSec,JunoRequest.OperationType.Create);
			  JunoMessage msg = (JunoMessage) methodValidate.invoke(client,req,OperationType.Create,clientCfgHldr);
			  assertNull(msg);	
		  }catch(Exception e){
			  //We should be here
			  assertTrue(e.getCause() instanceof IllegalArgumentException);
			  assertEquals(e.getCause().getMessage(),"The Document Value must not be larger than 204800 bytes. Current value size=204801");
		  }
	  }catch(Exception e){
		  //We shouldn't be here
		  assertFalse(true);
	  }
  }
  
  @Test
  public void validateInputEmptyPayload(){
	  try{
		  //Negative case - Empty Payload for CheckAndSet
		  try{
			  String key = "TestKey";
			  String value = "";
			  long version = 1;
			  long timeToLiveSec = 200;
			  JunoRequest req = new JunoRequest(key.getBytes(),value.getBytes(),version,timeToLiveSec,JunoRequest.OperationType.Set);
			  JunoMessage msg = (JunoMessage) methodValidate.invoke(client,req,OperationType.Update,clientCfgHldr);
			  assertNotNull(msg);	
		  }catch(InvocationTargetException e){
			  assertTrue(e.getCause() instanceof IllegalArgumentException);
			  assertEquals(e.getCause().getMessage(),"The Document Value must not be empty.");
		  }
		  
		  //Negative case - Empty Payload for Update
		  try{
			  String key = "TestKey";
			  String value = "";
			  long version = 1;
			  long timeToLiveSec = 200;
			  JunoRequest req = new JunoRequest(key.getBytes(),value.getBytes(),version,timeToLiveSec,JunoRequest.OperationType.Update);
			  JunoMessage msg = (JunoMessage) methodValidate.invoke(client,req,OperationType.Update,clientCfgHldr);
			  assertNotNull(msg);	
		  }catch(InvocationTargetException e){
			  assertTrue(e.getCause() instanceof IllegalArgumentException);
			  assertEquals(e.getCause().getMessage(),"The Document Value must not be empty.");
		  }
		  
		  //Negative case - Empty Payload for upsert
		  try{
			  String key = "TestKey";
			  String value = "";
			  long version = 1;
			  long timeToLiveSec = 200;
			  JunoRequest req = new JunoRequest(key.getBytes(),value.getBytes(),version,timeToLiveSec,JunoRequest.OperationType.Set);
			  JunoMessage msg = (JunoMessage) methodValidate.invoke(client,req,OperationType.Set,clientCfgHldr);
			  assertNotNull(msg);	
		  }catch(InvocationTargetException e){
			  assertTrue(e.getCause() instanceof IllegalArgumentException);
			  assertEquals(e.getCause().getMessage(),"The Document Value must not be empty.");
		  }
		  
		  //Negative case - Empty Payload for CompareAndSet
		  try{
			  String key = "TestKey";
			  String value = "";
			  long version = 1;
			  long timeToLiveSec = 200;
			  JunoRequest req = new JunoRequest(key.getBytes(),value.getBytes(),version,timeToLiveSec,JunoRequest.OperationType.Update);
			  JunoMessage msg = (JunoMessage) methodValidate.invoke(client,req,OperationType.CompareAndSet,clientCfgHldr);
			  assertNotNull(msg);	
		  }catch(InvocationTargetException e){
			  assertTrue(e.getCause() instanceof IllegalArgumentException);
			  assertEquals(e.getCause().getMessage(),"The Document Value must not be empty.");
		  }
		  
	  }catch(Exception e){
		  //System.out.println("Error invoking validateInput : validateInputEmptyPayload");
	  }
  }
  
  @Test
  public void validateInputEmptyKey(){
	  try{
		  // Negative case - Empty key
		  try{
			  String key = "";
			  String value = "TestValue";
			  long version = 1;
			  long timeToLiveSec = 200;
			  JunoRequest req = new JunoRequest(key.getBytes(),value.getBytes(),version,timeToLiveSec,JunoRequest.OperationType.Create);
			  JunoMessage msg = (JunoMessage) methodValidate.invoke(client,req,OperationType.Create,clientCfgHldr);
			  assertNotNull(msg);	
		  }catch(Exception e){
			  assertTrue(e.getCause() instanceof IllegalArgumentException);
			  assertEquals(e.getCause().getMessage(),"The Document key must not be null or empty");
		  }
	  }catch(Exception e){
	  }
  }
  
  @Test
  public void validateInputMaxKeySize(){
	  try{
		  // Negative case - Max key Size
		  try{
			  String key = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
			  String value = "TestValue";
			  long version = 1;
			  long timeToLiveSec = 200;
			  JunoRequest req = new JunoRequest(key.getBytes(),value.getBytes(),version,timeToLiveSec,JunoRequest.OperationType.Create);
			  JunoMessage msg = (JunoMessage) methodValidate.invoke(client,req,OperationType.Create,clientCfgHldr);
			  assertNotNull(msg);	
		  }catch(Exception e){
			  assertTrue(e.getCause() instanceof IllegalArgumentException);
			  assertEquals(e.getCause().getMessage(),"The Document key must not be larger than 128 bytes");
		  }
	  }catch(Exception e){
		  //System.out.println("Error invoking validateInput : validateInputMaxKeySize");
	  }
  }
  
  @Test
  public void validateInputMaxTTL(){
	  try{
		  //Negative case - Max TTL
		  try{
			  String key = "TestKey";
			  String value = "TestValue";
			  long version = 1;
			  long timeToLiveSec = 259201;
			  JunoRequest req = new JunoRequest(key.getBytes(),value.getBytes(),version,timeToLiveSec,JunoRequest.OperationType.Create);
			  JunoMessage msg = (JunoMessage) methodValidate.invoke(client,req,OperationType.Create,clientCfgHldr);
			  assertNotNull(msg);	
		  }catch(InvocationTargetException e){
			  assertTrue(e.getCause() instanceof IllegalArgumentException);
			  assertEquals(e.getCause().getMessage(),"Invalid lifetime. current lifetime=259201, max configured lifetime=259200");
		  }
	  }catch(Exception e){
		  //System.out.println("Error invoking validateInput : validateInputMaxTTL");
	  }
  }
  
  @Test
  public void validateInput0TTLL(){
	  try{
		  //Negative case - 0 TTL
		  try{
			  String key = "TestKey";
			  String value = "TestValue";
			  long version = 1;
			  long timeToLiveSec = 0;
			  JunoRequest req = new JunoRequest(key.getBytes(),value.getBytes(),version,timeToLiveSec,JunoRequest.OperationType.Create);
			  JunoMessage msg = (JunoMessage) methodValidate.invoke(client,req,OperationType.Create,clientCfgHldr);
			  assertNotNull(msg);	
		  }catch(InvocationTargetException e){
			  assertTrue(e.getCause() instanceof IllegalArgumentException);
			  assertEquals(e.getCause().getMessage(),"The Document's TTL cannot be 0 or negative.");
		  }
	  }catch(Exception e){
		  //System.out.println("Error invoking validateInput : validateInput0TTLL");
	  }
  }
  
  @Test
  public void validateInputNegativeTTL(){
	  try{
		  //Negative case - Negative TTL
		  try{
			  String key = "TestKey";
			  String value = "TestValue";
			  long version = 1;
			  long timeToLiveSec = -1;
			  JunoRequest req = new JunoRequest(key.getBytes(),value.getBytes(),version,timeToLiveSec,JunoRequest.OperationType.Create);
			  JunoMessage msg = (JunoMessage) methodValidate.invoke(client,req,OperationType.Create, clientCfgHldr);
			  assertNotNull(msg);	
		  }catch(InvocationTargetException e){
			  assertTrue(e.getCause() instanceof IllegalArgumentException);
			  assertEquals(e.getCause().getMessage(),"The Document's TTL cannot be negative. Current lifetime=-1");
		  }
	  }catch(Exception e){
		  //System.out.println("Error invoking validateInput : validateInputNegativeTTL");
	  }
  }
  
  @Test
  public void validateInputNegativeVersion(){
	  try{
		  //Negative case - negative version #
		  try{
			  String key = "TestKey";
			  String value = "TestValue";
			  long version = -1;
			  long timeToLiveSec = 200;
			  JunoRequest req = new JunoRequest(key.getBytes(),value.getBytes(),version,timeToLiveSec,JunoRequest.OperationType.Update);
			  JunoMessage msg = (JunoMessage) methodValidate.invoke(client,req,OperationType.CompareAndSet,clientCfgHldr);
			  //assertNotNull(msg);	
			  assertFalse(true); // We should not be here
		  }catch(InvocationTargetException e){
			  assertTrue(e.getCause() instanceof IllegalArgumentException);
			  assertEquals(e.getCause().getMessage(),"The Document version cannot be less than 1. Current version=-1");
		  }
	  }catch(Exception e){
		  //System.out.println("Error invoking validateInput : validateInputNegativeVersion");
	  }
  }
 
}
