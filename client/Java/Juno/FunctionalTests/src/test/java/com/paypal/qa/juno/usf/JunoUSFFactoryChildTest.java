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

import com.paypal.juno.client.JunoReactClient;
import com.paypal.juno.client.io.JunoResponse;
import com.paypal.juno.client.io.OperationStatus;
import com.paypal.juno.exception.JunoException;
import java.io.IOException;
import java.util.Date;
import java.util.UUID;
import org.slf4j.Logger;import com.paypal.juno.client.JunoClient;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * This Class is to test the field and constructor Injection that happens in the base class
 */
@ContextConfiguration("classpath:spring-config.xml")
public class JunoUSFFactoryChildTest extends AbstractTestNGSpringContextTests {

    private JunoClient junoClientF;
    private JunoClient junoClientC;
    private JunoReactClient junoReactClientF;
    private JunoReactClient junoReactClientC;
    private Logger LOGGER;

    @BeforeClass
    public void setup() throws JunoException, IOException
    {
        LOGGER = LoggerFactory.getLogger(JunoUSFFactoryClientTest.class);
        LOGGER.debug("Read syncFlag test to find out what needs to be run");
        ApplicationContext context = new ClassPathXmlApplicationContext("spring-config.xml");
        JunoUSFFactoryChild child =  (JunoUSFFactoryChild) context.getBean("JunoUSFChild");
	    JunoUSFFactoryRChild rchild =  (JunoUSFFactoryRChild) context.getBean("JunoUSFRChild");
        junoClientF = child.getFIJunoClient();
        junoClientC = child.getCIJunoClient();
        junoReactClientF = rchild.getFIJunoReactClient();
        junoReactClientC = rchild.getCIJunoReactClient();
    }

    /**
     * Test the Field injected JunoClient object
     */
    @Test
    public void testCreateWithFieldInjectedClient() {
        System.out.println( "\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
        String key = UUID.randomUUID().toString();

        String data = "data to store in juno on " + new Date(System.currentTimeMillis()).toString();
        byte[] payload = data.getBytes();

        JunoResponse response = junoClientF.create(key.getBytes(), payload);
        AssertJUnit.assertEquals(OperationStatus.Success, response.status());
        // Check for the record
        response = junoClientF.get(key.getBytes());
        AssertJUnit.assertEquals(OperationStatus.Success, response.status());
        AssertJUnit.assertEquals(payload, response.getValue());

	JunoResponse rresponse = junoReactClientF.create(key.getBytes(), payload).block();
	AssertJUnit.assertEquals(OperationStatus.UniqueKeyViolation, rresponse.status());

	rresponse = junoReactClientF.get(key.getBytes()).block();
        AssertJUnit.assertEquals(OperationStatus.Success, rresponse.status());
        AssertJUnit.assertEquals(payload, rresponse.getValue());

        LOGGER.info("SUCCESS");
        LOGGER.info("Completed");
    }

    /**
     * Test the Constructor injected JunoClient object
     */
    @Test
    public void testCreateWithConstructorInjectedClient() {
        System.out.println( "\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
        String key = UUID.randomUUID().toString();

        String data = "data to store in juno on " + new Date(System.currentTimeMillis()).toString();
        byte[] payload = data.getBytes();

        JunoResponse response = junoClientC.create(key.getBytes(), payload);
        AssertJUnit.assertEquals(OperationStatus.Success, response.status());
        // Check for the record
        response = junoClientC.get(key.getBytes());
        AssertJUnit.assertEquals(OperationStatus.Success, response.status());
        AssertJUnit.assertEquals(payload, response.getValue());

        JunoResponse rresponse = junoReactClientC.create(key.getBytes(), payload).block();
        AssertJUnit.assertEquals(OperationStatus.UniqueKeyViolation, rresponse.status());

        rresponse = junoReactClientC.get(key.getBytes()).block();
        AssertJUnit.assertEquals(OperationStatus.Success, rresponse.status());
        AssertJUnit.assertEquals(payload, rresponse.getValue());

        LOGGER.info("SUCCESS");
        LOGGER.info("Completed");
    }

    /**
     * Test Create with Constructor injected object and Read with Field
     * injected object.
     */
    @Test
    public void testCreateWithCIOAndReadWithFIO() {
        System.out.println( "\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
        String key = UUID.randomUUID().toString();

        String data = "data to store in juno on " + new Date(System.currentTimeMillis()).toString();
        byte[] payload = data.getBytes();

        JunoResponse response = junoReactClientC.create(key.getBytes(), payload).block();
	AssertJUnit.assertEquals(OperationStatus.Success, response.status());
        response = junoClientC.get(key.getBytes());
        AssertJUnit.assertEquals(OperationStatus.Success, response.status());

        JunoResponse rresponse = junoClientC.create(key.getBytes(), payload);
	AssertJUnit.assertEquals(OperationStatus.UniqueKeyViolation, rresponse.status());
        rresponse = junoReactClientC.get(key.getBytes()).block();
        AssertJUnit.assertEquals(OperationStatus.Success, rresponse.status());

        response = junoClientF.get(key.getBytes());
        AssertJUnit.assertEquals(OperationStatus.Success, response.status());
        AssertJUnit.assertEquals(payload, response.getValue());

        rresponse = junoReactClientF.get(key.getBytes()).block();
        AssertJUnit.assertEquals(OperationStatus.Success, rresponse.status());
        AssertJUnit.assertEquals(payload, rresponse.getValue());

        LOGGER.info("SUCCESS");
        LOGGER.info("Completed");
    }
}
