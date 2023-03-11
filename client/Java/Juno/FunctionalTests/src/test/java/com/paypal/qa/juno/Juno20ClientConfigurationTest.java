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
import com.paypal.juno.client.io.JunoResponse;
import com.paypal.juno.client.io.OperationStatus;
import com.paypal.juno.conf.JunoProperties;
import com.paypal.juno.conf.JunoPropertiesProvider;
import com.paypal.juno.exception.JunoException;
import com.paypal.juno.util.SSLUtil;
import java.io.*;
import java.net.URL;
import java.util.Properties;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class Juno20ClientConfigurationTest {	
	private Properties pConfig;
	private JunoClient junoClient5;
	private Logger LOGGER;

	@BeforeClass
	public void setupBC() throws  IOException{
		LOGGER = LoggerFactory.getLogger(BatchDestroyTest.class);
	}
	@BeforeMethod
	public void setup() throws IOException, InterruptedException {
		URL url = Juno20ClientConfigurationTest.class.getResource("/com/paypal/juno/Juno.properties");
		LOGGER = LoggerFactory.getLogger(Juno20ClientConfigurationTest.class);
		pConfig = new Properties();
		pConfig.load(url.openStream());
		pConfig.setProperty(JunoProperties.RECORD_NAMESPACE, "NS4");
		try {
			junoClient5 = new JunoTestClientImpl(new JunoPropertiesProvider(pConfig), SSLUtil.getSSLContext(), 0);
		} catch (Exception e ){
			LOGGER.debug(e.getMessage());
		}
		Thread.sleep(3000);
	}

	@Test
	public void testCreateWithKey() throws JunoException, IOException{
		JunoPropertiesProvider prop = new JunoPropertiesProvider(pConfig);
		LOGGER.info("\n***TEST CASE: " + new Object(){}.getClass().getEnclosingMethod().getName());
		try {
			LOGGER.info("CorrID : " + Integer.toHexString((new Random()).nextInt(0x10000000) + 3846));
			byte[] data = DataGenUtils.genBytes(10);
			byte[] key = DataGenUtils.genBytes(64);
			JunoResponse mResponse = junoClient5.create(key, data);	
			AssertJUnit.assertEquals (OperationStatus.Success, mResponse.getStatus());
			AssertJUnit.assertTrue(mResponse.getTtl() <= prop.getDefaultLifetime() && mResponse.getTtl() >= prop.getDefaultLifetime()-3);
		}catch(Exception e){
			LOGGER.debug(e.getMessage());
		}
	}
	
	@Test
	public void validateEmptyAPP_NAMETest() throws IOException{

		pConfig.setProperty(JunoProperties.APP_NAME, "");
		pConfig.setProperty(JunoProperties.RECORD_NAMESPACE, "NS4");
		try {
			junoClient5 = JunoClientFactory.newJunoClient(new JunoPropertiesProvider(pConfig), SSLUtil.getSSLContext());
			AssertJUnit.assertTrue(false);    	
		}catch(Exception mex){
			LOGGER.debug(mex.getMessage());
			AssertJUnit.assertTrue(mex.getMessage().contains("Juno configuration value for property, " + JunoProperties.APP_NAME + " cannot be null or empty"));
		}

	}
	
	@Test
	public void validateAPP_NAMEMinSizeTest() throws IOException {
		
		pConfig.setProperty(JunoProperties.APP_NAME, DataGenUtils.genAlphaNumString(0));
		try {
    		junoClient5 = JunoClientFactory.newJunoClient(new JunoPropertiesProvider(pConfig), SSLUtil.getSSLContext());
    		AssertJUnit.assertTrue(false); 
		}catch(Exception mex){
			LOGGER.debug(mex.getMessage());
			AssertJUnit.assertTrue(mex.getMessage().contains("Juno configuration value for property, " + JunoProperties.APP_NAME  + " cannot be null or empty"));
		} 
	}

	@Test
	public void validateAPP_NAMEMaxSizeTest() throws IOException {
		
		pConfig.setProperty(JunoProperties.APP_NAME, DataGenUtils.genAlphaNumString(32));
		try {
    		junoClient5 = JunoClientFactory.newJunoClient(new JunoPropertiesProvider(pConfig), SSLUtil.getSSLContext());
		}catch(Exception mex){
			mex.getMessage();
			Assert.fail("If i reach here");
		} 
	}
		
	@Test
	public void validateAPP_NAMEMoreThanMaxSizeTest() throws IOException {
		
		pConfig.setProperty(JunoProperties.APP_NAME, DataGenUtils.genAlphaNumString(33));
		try {
    		junoClient5 = JunoClientFactory.newJunoClient(new JunoPropertiesProvider(pConfig), SSLUtil.getSSLContext());
    		AssertJUnit.assertTrue(false); //if no exception is given
		}catch(Exception mex){
			LOGGER.debug(mex.getMessage());
			AssertJUnit.assertTrue(mex.getMessage().contains("Application Name length exceeds MAX LENGTH of 32 bytes"));
		} 
	}
	
	@Test
	public void validateEmptyNameSpaceTest() throws IOException{
		
		pConfig.setProperty(JunoProperties.RECORD_NAMESPACE , "");
		try {
    		junoClient5 = JunoClientFactory.newJunoClient(new JunoPropertiesProvider(pConfig), SSLUtil.getSSLContext());
    		AssertJUnit.assertTrue(false); //if no exception is given
		}catch(Exception mex){
			LOGGER.debug(mex.getMessage());
			AssertJUnit.assertTrue(mex.getMessage().contains("Juno configuration value for property, " + JunoProperties.RECORD_NAMESPACE  + " cannot be null or empty"));
		} 
	}
	
	@Test
	public void validateNameSpaceMinSizeTest() throws IOException{
		pConfig.setProperty(JunoProperties.RECORD_NAMESPACE , DataGenUtils.genAlphaNumString(0));
		try {
    		junoClient5 = JunoClientFactory.newJunoClient(new JunoPropertiesProvider(pConfig), SSLUtil.getSSLContext());
    		AssertJUnit.assertTrue(false); //if no exception is given
		}catch(Exception mex){
			LOGGER.debug(mex.getMessage());
			AssertJUnit.assertTrue(mex.getMessage().contains("Juno configuration value for property, " + JunoProperties.RECORD_NAMESPACE  + " cannot be null or empty"));
		} 
	}
	
	@Test
	public void validateNameSpaceMaxSizeTest() throws IOException{
		pConfig.setProperty(JunoProperties.RECORD_NAMESPACE , DataGenUtils.genAlphaNumString(64));
		try {
    		junoClient5 = JunoClientFactory.newJunoClient(new JunoPropertiesProvider(pConfig), SSLUtil.getSSLContext());
		}catch(Exception mex){
		Assert.fail("I should not be here");
		} 
	}
			
	
	@Test
	public void validateNameSpaceMoreThanMaxSizeTest() throws IOException{
		pConfig.setProperty(JunoProperties.RECORD_NAMESPACE , DataGenUtils.genAlphaNumString(65));
		try {
    		junoClient5 = JunoClientFactory.newJunoClient(new JunoPropertiesProvider(pConfig), SSLUtil.getSSLContext());
    		AssertJUnit.assertTrue(false); //if no exception is given
		}catch(Exception mex){
			LOGGER.debug(mex.getMessage());
			AssertJUnit.assertTrue(mex.getMessage().contains("Namespace length exceeds MAX LENGTH of 64 bytes"));
		} 
	}
		
		
	@Test
	public void validateEmptyPrimaryIPTest() throws IOException{
		pConfig.setProperty(JunoProperties.HOST , "");
		pConfig.setProperty(JunoProperties.PORT , "");
		try {
    		junoClient5 = JunoClientFactory.newJunoClient(new JunoPropertiesProvider(pConfig), SSLUtil.getSSLContext());
    		junoClient5.create("emptyPrimaryIP".getBytes(), DataGenUtils.genBytes(10));
    		AssertJUnit.assertTrue(false);
		}catch(Exception mex){
			LOGGER.debug(mex.getMessage());
			AssertJUnit.assertTrue(mex.getMessage().contains("Integer property not valid - Value ="));
		} 
	}
	
	@Test
	public void validateServerPortTest() throws IOException{
		pConfig.setProperty(JunoProperties.PORT , "0");
		try {
    		junoClient5 = JunoClientFactory.newJunoClient(new JunoPropertiesProvider(pConfig), SSLUtil.getSSLContext());
    		junoClient5.create("invalidPort".getBytes(), DataGenUtils.genBytes(10));
    		AssertJUnit.assertTrue(false);
		}catch(Exception mex){
			//System.out.println("The exception is:"+mex.getMessage());
			LOGGER.debug(mex.getMessage());
			AssertJUnit.assertTrue(mex.getMessage().contains("Invalid Juno server port"));
		} 
	}
		
	@Test
	public void validateEmptyDefaultLifeTimeTest() throws IOException{
		pConfig.setProperty(JunoProperties.DEFAULT_LIFETIME , "");
		try {
    		junoClient5 = JunoClientFactory.newJunoClient(new JunoPropertiesProvider(pConfig), SSLUtil.getSSLContext());
    		junoClient5.create("emptyDefaultLifeTime".getBytes(), DataGenUtils.genBytes(10));
    		AssertJUnit.assertTrue(false); //if no exception is given
		}catch(Exception mex){			
			LOGGER.debug(mex.getMessage());
			//assertTrue(mex.getMessage().contains("Invalid property value for the key, default_lifetime"));
			AssertJUnit.assertTrue(mex.getMessage().contains("Long property not valid - Value"));
		} 
	}

	@Test
	public void validateEmptyConnectionTimeOutTest(){
		pConfig.setProperty(JunoProperties.CONNECTION_TIMEOUT , "");
		try {
    		junoClient5 = JunoClientFactory.newJunoClient(new JunoPropertiesProvider(pConfig), SSLUtil.getSSLContext());
    		junoClient5.create("EmptyConnectionTimeOut".getBytes(), DataGenUtils.genBytes(10));
    		AssertJUnit.assertTrue(false); //if no exception is given
		}catch(Exception mex){
			LOGGER.debug(mex.getMessage());
			AssertJUnit.assertTrue(mex.getMessage().contains("Integer property not valid - Value"));
		} 
	}	
	
	@Test
	public void validateMaxConnectionTimeOutTest(){
		pConfig.setProperty(JunoProperties.CONNECTION_TIMEOUT , "6000");
		try {
    		junoClient5 = JunoClientFactory.newJunoClient(new JunoPropertiesProvider(pConfig), SSLUtil.getSSLContext());
    		junoClient5.create("maxConnectionTimeOut".getBytes(), DataGenUtils.genBytes(10));
    		AssertJUnit.assertTrue(false); //if no exception is given
		}catch(Exception mex){
			LOGGER.debug(mex.getMessage());
			AssertJUnit.assertTrue(mex.getMessage().contains("juno.connection.timeout_msec cannot be greater than"));
		} 
	}
	
	@Test
	public void validateRetryTest(){
		pConfig.setProperty(JunoProperties.ENABLE_RETRY ,"true" );
		pConfig.setProperty(JunoProperties.RESPONSE_TIMEOUT,"1");
		try {
    		junoClient5 = JunoClientFactory.newJunoClient(new JunoPropertiesProvider(pConfig), SSLUtil.getSSLContext());
    		junoClient5.create("maxConnectionTimeOut".getBytes(), DataGenUtils.genBytes(10));

		}catch(Exception mex){
			LOGGER.debug(mex.getMessage());
			AssertJUnit.assertTrue(mex.getMessage().contains("Response Timed out"));
		} 
	}
	
	@Test
	public void validateNoAPP_NAMEWhenNamePresent() throws FileNotFoundException, IOException{
		
		pConfig.setProperty(JunoProperties.APP_NAME, "");
		pConfig.setProperty(JunoProperties.RECORD_NAMESPACE, "NS4");
		String key = "validateNoAPP_NAMEExist";
		JunoResponse junoResponse = junoClient5.delete(key.getBytes());
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		byte[] data = DataGenUtils.genBytes(10);
		junoResponse = junoClient5.create(key.getBytes(), data);
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		junoResponse = junoClient5.get(key.getBytes());
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		AssertJUnit.assertEquals(key, new String(junoResponse.key()));
		AssertJUnit.assertTrue(1 == junoResponse.getVersion());
		AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
		AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));
		junoResponse = junoClient5.delete(key.getBytes());
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		pConfig.clear();

	}
	

	
	@Test
	public void validateValidAPP_NAMENoNamePresent() throws FileNotFoundException, IOException{
		pConfig.setProperty(JunoProperties.APP_NAME, "validateNoAPP_NAMEExist");
		pConfig.setProperty(JunoProperties.RECORD_NAMESPACE, "NS4");
		
    	String key = "validateNoAPP_NAMEExist";
    	JunoResponse junoResponse = junoClient5.delete(key.getBytes());
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		byte[] data = DataGenUtils.genBytes(10);
		junoResponse = junoClient5.create(key.getBytes(), data);
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		junoResponse = junoClient5.get(key.getBytes());
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		
		AssertJUnit.assertEquals(key, new String(junoResponse.key()));
		AssertJUnit.assertTrue(1 == junoResponse.getVersion());
		AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
		AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));
		junoResponse = junoClient5.delete(key.getBytes());
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		pConfig.clear();
	}

	@Test
	public void validateNameLengthMoreThanAPP_NAMELengthTest() throws FileNotFoundException, IOException{
		pConfig.setProperty(JunoProperties.APP_NAME, "validateNameLengthMoreThanAPP_NAMELengthTest");
		pConfig.setProperty(JunoProperties.RECORD_NAMESPACE, "NS4");
		
    	String key = "validateNameLengthMoreThanAPP_NAMELengthTest";
       	JunoResponse junoResponse = junoClient5.delete(key.getBytes());
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		byte[] data = DataGenUtils.genBytes(10);
		junoResponse = junoClient5.create(key.getBytes(), data);
		AssertJUnit.assertEquals(OperationStatus.Success,junoResponse.getStatus());
		
		junoResponse = junoClient5.get(key.getBytes());
		AssertJUnit.assertEquals(OperationStatus.Success, junoResponse.getStatus());
		AssertJUnit.assertEquals(key, new String(junoResponse.key()));
		AssertJUnit.assertTrue(1 == junoResponse.getVersion());
		AssertJUnit.assertEquals(data.length, junoResponse.getValue().length);
		AssertJUnit.assertEquals(new String(data), new String(junoResponse.getValue()));
		pConfig.clear();
				
	}
	
	@Test
	public void validateNoAPP_NAMENoNamePresent() throws FileNotFoundException, IOException{
						
		pConfig.setProperty(JunoProperties.APP_NAME, "");
		pConfig.setProperty(JunoProperties.RECORD_NAMESPACE, "NS4");
		try{
			junoClient5 = JunoClientFactory.newJunoClient(new JunoPropertiesProvider(pConfig), SSLUtil.getSSLContext());
			AssertJUnit.assertTrue(false); //if no exception is given
		}catch(Exception mex){
			AssertJUnit.assertTrue(mex.getMessage().contains("Juno configuration value for property, " + JunoProperties.APP_NAME + " cannot be null or empty"));
		}
		pConfig.clear();
				
	}
}
