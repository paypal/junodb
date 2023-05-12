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
package com.paypal.juno.client;

import javax.inject.Inject;
import javax.inject.Named;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.springframework.stereotype.Component;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.AbstractJUnit4SpringContextTests;
import static org.junit.Assert.*;


/**
 * Test multiple JunoClient implementations being instanatiated
 * into the Spring Bean Context.  The config xml references a Commons
 * Configuration implemetnation that references a properties
 * file with more than 1 set of Juno client configuration
 * properties.
 */

//@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:spring-config-multi-client.xml")
//@ContextConfiguration(locations= {"/spring-config-multi-client.xml"})
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@Component
public class JunoMultiClientTest extends AbstractJUnit4SpringContextTests{

    @Inject
    JunoClient junoClnt;

	@Inject
	JunoClient junoClnt1;

    @Inject
    @Named("junoClient2")
    JunoClient junoClnt2;

    @Inject
    @Named("junoClient3")
    JunoAsyncClient junoAsyncClient3;

	@Inject
	JunoAsyncClient junoAsyncClient;


    @BeforeClass
	public static void initialize() throws Exception {
    	System.setProperty("keymaker.test.appname","Juno-UnitTest");
    	//DO nothing
	}

	@AfterClass
	public static void tearDown() {
		 Thread.currentThread().getThreadGroup().interrupt();
	}

    @Test
    public void testJunoSyncClientNotNull() {
    	assertNotNull(junoClnt);
		assertNotNull(junoClnt1);
    	assertNotNull(junoClnt2);
    	try{
    		junoClnt2.create("Joseph".getBytes(), "Antony".getBytes());
    	}catch(Exception e){
    		//System.out.println("Exception123 :"+e.getMessage());
    		assertTrue("testJunoSyncClientNotNull test failed",e.getMessage().contains("Connection Error"));
    	}
    }

    @Test
    public void testJunoAsyncClientNotNull() {
    	assertNotNull(junoAsyncClient3);
		assertNotNull(junoAsyncClient);
    	try{
			junoAsyncClient3.create("Joseph".getBytes(), "Antony".getBytes()).toBlocking().value();
    	}catch(Exception e){
    		//System.out.println("Exception :"+e.getMessage());
    		assertTrue("testJunoAsyncClientNotNull test failed",e.getMessage().contains("Connection Error"));
    	}
    }

    @Test
	public void testConstructorTest(){
		JunoClient junoClnt = ConstuctorTest.getObject1();
		assertNotNull(junoClnt);
		JunoClient junoClnt1 = ConstuctorTest.getObject2();
		assertNotNull(junoClnt1);
	}

	@Component
	public static class ConstuctorTest {
		private static JunoClient junoClnt;
		private static JunoClient junoClnt1;
		public ConstuctorTest(@Named("junoClient2")JunoClient junoCl,@Named("junoClient4")JunoClient junoCl1){
			junoClnt = junoCl;
			junoClnt1 = junoCl1;
		}

		public static JunoClient getObject1(){
			return junoClnt;
		}

		public static JunoClient getObject2(){
			return junoClnt1;
		}
	}
}
