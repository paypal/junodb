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
package com.juno.samples.impl;

import com.juno.samples.api.JunoClientSampleResource;
import com.paypal.juno.client.JunoAsyncClient;
import com.paypal.juno.client.JunoClient;
import com.paypal.juno.client.JunoClientFactory;
import com.paypal.juno.client.JunoReactClient;
import com.paypal.juno.client.io.JunoResponse;
import com.paypal.juno.client.io.OperationStatus;
import com.paypal.juno.client.io.RecordContext;
import com.paypal.juno.conf.JunoProperties;
import com.paypal.juno.conf.JunoPropertiesProvider;
import com.paypal.juno.exception.JunoException;
import com.paypal.juno.util.SSLUtil;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.signalfx.SignalFxMeterRegistry;
import java.io.UnsupportedEncodingException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.StreamSupport;
import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.autoconfigure.metrics.export.signalfx.SignalFxProperties;
import org.springframework.boot.actuate.autoconfigure.metrics.export.signalfx.SignalFxPropertiesConfigAdapter;
import org.springframework.boot.env.OriginTrackedMapPropertySource;
import org.springframework.core.env.*;
import org.springframework.core.env.Environment;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.context.annotation.RequestScope;
import reactor.core.publisher.Mono;
import rx.Single;
import rx.SingleSubscriber;

/**
 * This resource shows how to use JAX-RS injection and how to use
 * a spring bean as a JAX-RS resource class.
 *
 * Notice that the scope for this bean is request which means that a new
 * instance of this class will be created per request.
 *
 */

@Component
@RequestScope
public class JunoClientSampleResourceImpl implements JunoClientSampleResource {
	@Autowired
	private Environment env;
	private Logger LOGGER = LoggerFactory.getLogger(JunoClientSampleResourceImpl.class);
	@Inject
	public JunoClient junoClient;
	@Inject
	@Named("junoClient2")
	public JunoClient junoClientMeowMrowHalu;
	public JunoAsyncClient junoAsyncClient;
	public JunoReactClient junoReactClient;
	private Properties pConfig;
	SignalFxMeterRegistry sfxRegistry;
	private MutablePropertySources propSrcs;
	public JunoClientSampleResourceImpl() {
		LOGGER.info("JunoClientSampleResourceImpl");
		String uriLink = ""; // Set your Observability Collector Here
		SignalFxProperties properties = new SignalFxProperties();
		properties.setStep(Duration.ofSeconds(60));
		properties.setUri(uriLink);
		properties.setEnabled(true);
		properties.setConnectTimeout(Duration.ofSeconds(15));
		properties.setReadTimeout(Duration.ofSeconds(10));
		properties.setBatchSize(10000);
		properties.setAccessToken("");

		SignalFxPropertiesConfigAdapter config = new SignalFxPropertiesConfigAdapter(properties);
		sfxRegistry = new SignalFxMeterRegistry(config, Clock.SYSTEM);
		Metrics.globalRegistry.add(sfxRegistry);
	}

	@PostConstruct
	public void init() {
		try {
			propSrcs = ((AbstractEnvironment) env).getPropertySources();
			pConfig = new Properties();
			StreamSupport.stream(propSrcs.spliterator(), false)
					.filter(ps -> ps instanceof OriginTrackedMapPropertySource)
					.map(ps -> ((EnumerablePropertySource) ps).getPropertyNames())
					.flatMap(Arrays::<String>stream)
					.forEach(propName -> pConfig.setProperty(propName, env.getProperty(propName)));

			if(pConfig.size() == 0) throw new Exception();

			junoAsyncClient = JunoClientFactory.newJunoAsyncClient(new JunoPropertiesProvider(pConfig), SSLUtil.getSSLContext());
			junoReactClient = JunoClientFactory.newJunoReactClient(new JunoPropertiesProvider(pConfig));
		} catch (Exception e ){
			LOGGER.debug("Exception Occurred " + e.getMessage() + pConfig.toString());
		}
		LOGGER.info(propSrcs.toString() + "\n" + pConfig.toString());
		LOGGER.info(junoClientMeowMrowHalu.getProperties().toString());
	}

	@Override
	public ResponseEntity<String> sayHello() throws UnsupportedEncodingException {
		return ResponseEntity.ok("Hello, World!");
	}

	ResponseEntity<String> formSuccessResponse(JunoResponse resp){
		return ResponseEntity.ok(formSuccesResString(resp));
	}

	String formSuccesResString(JunoResponse resp){
		String doc = "";
		try {
			doc = "{\"status\" : \""+resp.getStatus().getErrorText()+"\", \"creationTime\" : \""+resp.getCreationTime()+"\", \"remainingTTL\" : \""+resp.getTtl()+"\", \"version\" : \""+resp.getVersion()+"\" , \"value\" : \""+new String(resp.getValue(),"UTF-8")+"\"}";
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return doc;
	}

	ResponseEntity<String> formFailureResponse(String msg){
		String doc = "{\"status\":\""+msg+"\"}";
		return ResponseEntity.badRequest().body(doc);
	}

	@Override
	public ResponseEntity<String> recordCreate(String key, String value) throws JunoException {
		LOGGER.info("Key -> " + key);
		LOGGER.info("value -> " + value);
		try{
			JunoResponse junoResponse = junoClient.create(key.getBytes("UTF-8"), value.getBytes("UTF-8"));
			LOGGER.info("SUCCESS");
			return formSuccessResponse(junoResponse);
		}catch(JunoException e){
			LOGGER.info("EXCEPTION");
			return formFailureResponse(e.getMessage());
		} catch (UnsupportedEncodingException e) {
			LOGGER.info("EXCEPTION");
			return formFailureResponse(OperationStatus.UnknownError.getErrorText());
		}finally {
			LOGGER.info("Completed");
		}
	}

	@Override
	public ResponseEntity<String> recordCreate(String key, String value, Long ttl) throws JunoException {
		LOGGER.info("Key -> " + key);
		LOGGER.info("value -> " + value);
		LOGGER.info("ttl -> " + ttl);
		try{
			JunoResponse junoResponse = junoClient.create(key.getBytes("UTF-8"), value.getBytes("UTF-8"), ttl);
			LOGGER.info("SUCCESS");
			return formSuccessResponse(junoResponse);
		}catch(JunoException e){
			LOGGER.info("EXCEPTION");
			return formFailureResponse(e.getMessage());
		} catch (UnsupportedEncodingException e) {
			LOGGER.info("EXCEPTION");
			return formFailureResponse(OperationStatus.UnknownError.getErrorText());
		}finally {
			LOGGER.info("Completed");
		}
	}

	@Override
	public ResponseEntity<String> recordGet(String key) throws JunoException {
		LOGGER.info("Key -> " + key);
		try{
			JunoResponse junoResponse = junoClient.get(key.getBytes("UTF-8"));
			String test = JunoProperties.APP_NAME;
			LOGGER.info("SUCCESS");
			return formSuccessResponse(junoResponse);
		}catch(Exception e){
			LOGGER.info("EXCEPTION : " + e.getMessage());
			return formFailureResponse(e.getMessage());
		}finally {
			LOGGER.info("Completed");
		}
	}

	@Override
	public ResponseEntity<String> recordGet(String key, Long ttl) throws JunoException {
		LOGGER.info("Key -> " + key);
		LOGGER.info("ttl -> " + ttl);
		try{
			JunoResponse junoResponse = junoClient.get(key.getBytes("UTF-8"), ttl);
			LOGGER.info("SUCCESS");
			return formSuccessResponse(junoResponse);
		}catch(JunoException e){
			LOGGER.info("EXCEPTION");
			return formFailureResponse(e.getMessage());
		} catch (UnsupportedEncodingException e) {
			LOGGER.info("EXCEPTION");
			return formFailureResponse(OperationStatus.UnknownError.getErrorText());
		}finally {
			LOGGER.info("Completed");
		}
	}

	@Override
	public ResponseEntity<String> recordUpdate(String key, String value) throws JunoException {
		LOGGER.info("Key -> " + key);
		LOGGER.info("Value ->" + value);
		try{
			JunoResponse junoResponse = junoClient.update(key.getBytes("UTF-8"), value.getBytes("UTF-8"));
			LOGGER.info("SUCCESS");
			return formSuccessResponse(junoResponse);
		}catch(JunoException e){
			LOGGER.info("EXCEPTION");
			return formFailureResponse(e.getMessage());
		} catch (UnsupportedEncodingException e) {
			LOGGER.info("EXCEPTION");
			return formFailureResponse(OperationStatus.UnknownError.getErrorText());
		}finally {
			LOGGER.info("Completed");
		}
	}

	@Override
	public ResponseEntity<String> recordUpdate(String key, String value, Long ttl) throws JunoException {
		LOGGER.info("Key -> " + key);
		LOGGER.info("Value ->" + value);
		LOGGER.info("ttl -> " + ttl);
		try{
			JunoResponse junoResponse = junoClient.update(key.getBytes("UTF-8"), value.getBytes("UTF-8"), ttl);
			LOGGER.info("SUCCESS");
			return formSuccessResponse(junoResponse);
		}catch(JunoException e){
			LOGGER.info("EXCEPTION");
			return formFailureResponse(e.getMessage());
		} catch (UnsupportedEncodingException e) {
			LOGGER.info("EXCEPTION");
			return formFailureResponse(OperationStatus.UnknownError.getErrorText());
		}finally {
			LOGGER.info("Completed");
		}
	}

	@Override
	public ResponseEntity<String> recordSet(String key, String value) throws JunoException {
		LOGGER.info("Key -> " + key);
		LOGGER.info("Value ->" + value);
		try{
			JunoResponse junoResponse = junoClient.set(key.getBytes("UTF-8"), value.getBytes("UTF-8"));
			LOGGER.info("SUCCESS");
			return formSuccessResponse(junoResponse);
		}catch(JunoException e){
			LOGGER.info("EXCEPTION");
			return formFailureResponse(e.getMessage());
		} catch (UnsupportedEncodingException e) {
			LOGGER.info("EXCEPTION");
			return formFailureResponse(OperationStatus.UnknownError.getErrorText());
		}finally {
			LOGGER.info("Completed");
		}
	}

	@Override
	public ResponseEntity<String> recordSet(String key, String value, Long ttl) throws JunoException {
		LOGGER.info("Key -> " + key);
		LOGGER.info("Value ->" + value);
		LOGGER.info("ttl -> " + ttl);
		try{
			JunoResponse junoResponse = junoClient.set(key.getBytes("UTF-8"), value.getBytes("UTF-8"), ttl);
			LOGGER.info("SUCCESS");
			return formSuccessResponse(junoResponse);
		}catch(JunoException e){
			LOGGER.info("EXCEPTION");
			return formFailureResponse(e.getMessage());
		} catch (UnsupportedEncodingException e) {
			LOGGER.info("EXCEPTION");
			return formFailureResponse(OperationStatus.UnknownError.getErrorText());
		}finally {
			LOGGER.info("Completed");
		}
	}

	@Override
	public ResponseEntity<String> recordCompareAndSet(String key, String value) throws JunoException {
		LOGGER.info("Key -> " + key);
		LOGGER.info("Value ->" + value);
		try{
			JunoResponse junoResponse = junoClient.get(key.getBytes("UTF-8"));
			if(junoResponse.getStatus() == OperationStatus.Success){
				junoResponse = junoClient.compareAndSet(junoResponse.getRecordContext(),value.getBytes("UTF-8"),0);

				if(junoResponse.getStatus() == OperationStatus.Success){
					LOGGER.info("SUCCESS");
				}else{
					LOGGER.info("Failed to update key Test1");
					if(junoResponse.getStatus() == OperationStatus.NoKey){
						LOGGER.info("Key not in DB. Some one might have deleted the record.");
					}else if(junoResponse.getStatus() == OperationStatus.ConditionViolation){
						LOGGER.info("Some one might have updated the record before we did.");
					}else if(junoResponse.getStatus() == OperationStatus.RecordLocked){
						LOGGER.info("Some other request has locked the record for update.");
					}else {
						LOGGER.info("Log response.getStatus().getErrorText()");
					}
				}
			} else {
				LOGGER.info("Failed get key Test1");
				if(junoResponse.getStatus() == OperationStatus.NoKey){
					LOGGER.info("Key not in DB");
				}else {
					LOGGER.info(junoResponse.getStatus().getErrorText());
				}
			}

			return formSuccessResponse(junoResponse);
		}catch(JunoException e){
			LOGGER.info("EXCEPTION");
			return formFailureResponse(e.getMessage());
		} catch (UnsupportedEncodingException e) {
			LOGGER.info("EXCEPTION");
			return formFailureResponse(OperationStatus.UnknownError.getErrorText());
		} finally {
			LOGGER.info("Completed");
		}
	}


	@Override
	public ResponseEntity<String> recordCompareAndSetTTL(String key, String value, Long ttl) throws JunoException {
		LOGGER.info("Key -> " + key);
		LOGGER.info("Value ->" + value);
		LOGGER.info("ttl -> " + ttl);
		try{
			JunoResponse junoResponse = junoClient.get(key.getBytes("UTF-8")); //500 is user supplied TTL
			if(junoResponse.getStatus() == OperationStatus.Success){
				junoResponse = junoClient.compareAndSet(junoResponse.getRecordContext(),value.getBytes("UTF-8"), ttl);

				if(junoResponse.getStatus() == OperationStatus.Success){
					LOGGER.info("SUCCESS");
				}else{
					LOGGER.info("Failed to update key Test1");
					if(junoResponse.getStatus() == OperationStatus.NoKey){
						LOGGER.info("Key not in DB. Some one might have deleted the record.");
					}else if(junoResponse.getStatus() == OperationStatus.ConditionViolation){
						LOGGER.info("Some one might have updated the record before we did.");
					}else if(junoResponse.getStatus() == OperationStatus.RecordLocked){
						LOGGER.info("Some other request has locked the record for update.");
					}else {
						LOGGER.info("Log response.getStatus().getErrorText()");
					}
				}
			} else {
				LOGGER.info("Failed get key Test1");
				if(junoResponse.getStatus() == OperationStatus.NoKey){
					LOGGER.info("Key not in DB");
				}else {
					LOGGER.info(junoResponse.getStatus().getErrorText());
				}
			}

			return formSuccessResponse(junoResponse);
		}catch(JunoException e){
			LOGGER.info("EXCEPTION");
			return formFailureResponse(e.getMessage());
		} catch (UnsupportedEncodingException e) {
			LOGGER.info("EXCEPTION");
			return formFailureResponse(OperationStatus.UnknownError.getErrorText());
		} finally {
			LOGGER.info("Completed");
		}
	}

	@Override
	public ResponseEntity<String> recordDelete(String key) throws JunoException {
		LOGGER.info("Key -> " + key);
		try{
			JunoResponse junoResponse = junoClient.delete(key.getBytes("UTF-8"));
			LOGGER.info("SUCCESS");
			return formSuccessResponse(junoResponse);
		}catch(JunoException e){
			LOGGER.info("EXCEPTION");
			return formFailureResponse(e.getMessage());
		} catch (UnsupportedEncodingException e) {
			LOGGER.info("EXCEPTION");
			return formFailureResponse(OperationStatus.UnknownError.getErrorText());
		}finally {
			LOGGER.info("Completed");
		}
	}

	@Override
	public ResponseEntity<String> reactCreate(String key, String value) throws JunoException {
		LOGGER.info("Key -> " + key);
		LOGGER.info("Value ->" + value);
		try{
			AtomicReference<String> response = new AtomicReference<String>();
			Mono<JunoResponse> monoRes = junoReactClient.create(key.getBytes("UTF-8"), value.getBytes("UTF-8"));
			monoRes.subscribe(res ->
					{
						response.set(formSuccesResString(res));
						LOGGER.info("SUCCESS");
						LOGGER.info("Completed");
					}, error ->
					{
						// Failure in processing the request. Do not SLEEP or BLOCK in this method.
						// The cause for failure can be obtained via error.getMessage()
						LOGGER.info("EXCEPTION");
						LOGGER.info("Completed");
						response.set("{\"status\":\"" + error.getMessage() + "\"}");
					}
			);

			for (int i=0; i < 5; i++){
				if(response.get() != null)
					break;
				Thread.sleep(1000);
			}

			if(response.get() != null) {
				return ResponseEntity.ok(response.get());
			}else{
				return formFailureResponse("Operation Timed out2");
			}
		}catch(JunoException | UnsupportedEncodingException | InterruptedException e){
			LOGGER.info("EXCEPTION");
			LOGGER.info("Completed");
			return formFailureResponse(e.getMessage());
		}
	}

	@Override
	public ResponseEntity<String> reactCreate(String key, String value, Long ttl) throws JunoException {
		LOGGER.info("Key -> " + key);
		LOGGER.info("Value ->" + value);
		LOGGER.info("ttl -> " + ttl);
		try{
			AtomicReference<String> response = new AtomicReference<String>();
			Mono<JunoResponse> monoRes = junoReactClient.create(key.getBytes("UTF-8"), value.getBytes("UTF-8"), ttl);
			monoRes.subscribe(res ->
					{
						response.set(formSuccesResString(res));
						LOGGER.info("SUCCESS");
						LOGGER.info("Completed");
					}, error ->
					{
						// Failure in processing the request. Do not SLEEP or BLOCK in this method.
						// The cause for failure can be obtained via error.getMessage()
						LOGGER.info("EXCEPTION");
						LOGGER.info("Completed");
						response.set("{\"status\":\"" + error.getMessage() + "\"}");
					}
			);

			for (int i=0; i < 5; i++){
				if(response.get() != null)
					break;
				Thread.sleep(1000);
			}

			if(response.get() != null) {
				return ResponseEntity.ok(response.get());
			}else{
				return formFailureResponse("Operation Timed out2");
			}
		}catch(JunoException | UnsupportedEncodingException | InterruptedException e){
			LOGGER.info("EXCEPTION");
			LOGGER.info("Completed");
			return formFailureResponse(e.getMessage());
		}
	}


	@Override
	public ResponseEntity<String> reactGet(String key) throws JunoException {
		LOGGER.info("Key -> " + key);
		try{
			AtomicReference<String> response = new AtomicReference<String>();
			Mono<JunoResponse> monoRes = junoReactClient.get(key.getBytes("UTF-8"));
			monoRes.subscribe(res ->
					{
						response.set(formSuccesResString(res));
						LOGGER.info("SUCCESS");
						LOGGER.info("Completed");
					}, error ->
					{
						// Failure in processing the request. Do not SLEEP or BLOCK in this method.
						// The cause for failure can be obtained via error.getMessage()
						LOGGER.info("EXCEPTION");
						LOGGER.info("Completed");
						response.set("{\"status\":\"" + error.getMessage() + "\"}");
					}
			);

			for (int i=0; i < 5; i++){
				if(response.get() != null)
					break;
				Thread.sleep(1000);
			}

			if(response.get() != null) {
				return ResponseEntity.ok(response.get());
			}else{
				return formFailureResponse("Operation Timed out2");
			}
		}catch(JunoException | UnsupportedEncodingException | InterruptedException e){
			LOGGER.info("EXCEPTION");
			LOGGER.info("Completed");
			return formFailureResponse(e.getMessage());
		}
	}

	@Override
	public ResponseEntity<String> reactGet(String key, Long ttl) throws JunoException {
		LOGGER.info("Key -> " + key);
		LOGGER.info("ttl -> " + ttl);
		try{
			AtomicReference<String> response = new AtomicReference<String>();
			Mono<JunoResponse> monoRes = junoReactClient.get(key.getBytes("UTF-8"), ttl);
			monoRes.subscribe(res ->
					{
						response.set(formSuccesResString(res));
						LOGGER.info("SUCCESS");
						LOGGER.info("Completed");
					}, error ->
					{
						// Failure in processing the request. Do not SLEEP or BLOCK in this method.
						// The cause for failure can be obtained via error.getMessage()
						LOGGER.info("EXCEPTION");
						LOGGER.info("Completed");
						response.set("{\"status\":\"" + error.getMessage() + "\"}");
					}
			);

			for (int i=0; i < 5; i++){
				if(response.get() != null)
					break;
				Thread.sleep(1000);
			}

			if(response.get() != null) {
				return ResponseEntity.ok(response.get());
			}else{
				return formFailureResponse("Operation Timed out2");
			}
		}catch(JunoException | UnsupportedEncodingException | InterruptedException e){
			LOGGER.info("EXCEPTION");
			LOGGER.info("Completed");
			return formFailureResponse(e.getMessage());
		}
	}

	@Override
	public ResponseEntity<String> reactUpdate(String key, String value) throws JunoException {
		LOGGER.info("Key -> " + key);
		LOGGER.info("Value ->" + value);
		try{
			AtomicReference<String> response = new AtomicReference<String>();
			Mono<JunoResponse> monoRes = junoReactClient.update(key.getBytes("UTF-8"), value.getBytes("UTF-8"));
			monoRes.subscribe(res ->
					{
						response.set(formSuccesResString(res));
						LOGGER.info("SUCCESS");
						LOGGER.info("Completed");
					}, error ->
					{
						// Failure in processing the request. Do not SLEEP or BLOCK in this method.
						// The cause for failure can be obtained via error.getMessage()
						LOGGER.info("EXCEPTION");
						LOGGER.info("Completed");
						response.set("{\"status\":\"" + error.getMessage() + "\"}");
					}
			);

			for (int i=0; i < 5; i++){
				if(response.get() != null)
					break;
				Thread.sleep(1000);
			}

			if(response.get() != null) {
				return ResponseEntity.ok(response.get());
			}else{
				return formFailureResponse("Operation Timed out2");
			}
		}catch(JunoException | UnsupportedEncodingException | InterruptedException e){
			LOGGER.info("EXCEPTION");
			LOGGER.info("Completed");
			return formFailureResponse(e.getMessage());
		}
	}

	@Override
	public ResponseEntity<String> reactUpdate(String key, String value, Long ttl) throws JunoException {
		LOGGER.info("Key -> " + key);
		LOGGER.info("Value ->" + value);
		LOGGER.info("ttl -> " + ttl);
		try{
			AtomicReference<String> response = new AtomicReference<String>();
			Mono<JunoResponse> monoRes = junoReactClient.update(key.getBytes("UTF-8"), value.getBytes("UTF-8"), ttl);
			monoRes.subscribe(res ->
					{
						response.set(formSuccesResString(res));
						LOGGER.info("SUCCESS");
						LOGGER.info("Completed");
					}, error ->
					{
						// Failure in processing the request. Do not SLEEP or BLOCK in this method.
						// The cause for failure can be obtained via error.getMessage()
						LOGGER.info("EXCEPTION");
						LOGGER.info("Completed");
						response.set("{\"status\":\"" + error.getMessage() + "\"}");
					}
			);

			for (int i=0; i < 5; i++){
				if(response.get() != null)
					break;
				Thread.sleep(1000);
			}

			if(response.get() != null) {
				return ResponseEntity.ok(response.get());
			}else{
				return formFailureResponse("Operation Timed out2");
			}
		}catch(JunoException | UnsupportedEncodingException | InterruptedException e){
			LOGGER.info("EXCEPTION");
			LOGGER.info("Completed");
			return formFailureResponse(e.getMessage());
		}
	}

	@Override
	public ResponseEntity<String> reactSet(String key, String value) throws JunoException {
		LOGGER.info("Key -> " + key);
		LOGGER.info("Value ->" + value);
		try{
			AtomicReference<String> response = new AtomicReference<String>();
			Mono<JunoResponse> monoRes = junoReactClient.set(key.getBytes("UTF-8"), value.getBytes("UTF-8"));
			monoRes.subscribe(res ->
					{
						response.set(formSuccesResString(res));
						LOGGER.info("SUCCESS");
						LOGGER.info("Completed");
					}, error ->
					{
						// Failure in processing the request. Do not SLEEP or BLOCK in this method.
						// The cause for failure can be obtained via error.getMessage()
						LOGGER.info("EXCEPTION");
						LOGGER.info("Completed");
						response.set("{\"status\":\"" + error.getMessage() + "\"}");
					}
			);

			for (int i=0; i < 5; i++){
				if(response.get() != null)
					break;
				Thread.sleep(1000);
			}

			if(response.get() != null) {
				return ResponseEntity.ok(response.get());
			}else{
				return formFailureResponse("Operation Timed out2");
			}
		}catch(JunoException | UnsupportedEncodingException | InterruptedException e){
			LOGGER.info("EXCEPTION");
			LOGGER.info("Completed");
			return formFailureResponse(e.getMessage());
		}
	}


	@Override
	public ResponseEntity<String> reactSet(String key, String value, Long ttl) throws JunoException {
		LOGGER.info("Key -> " + key);
		LOGGER.info("Value ->" + value);
		LOGGER.info("ttl -> " + ttl);
		try{
			AtomicReference<String> response = new AtomicReference<String>();
			Mono<JunoResponse> monoRes = junoReactClient.set(key.getBytes("UTF-8"), value.getBytes("UTF-8"), ttl);
			monoRes.subscribe(res ->
					{
						response.set(formSuccesResString(res));
						LOGGER.info("SUCCESS");
						LOGGER.info("Completed");
					}, error ->
					{
						// Failure in processing the request. Do not SLEEP or BLOCK in this method.
						// The cause for failure can be obtained via error.getMessage()
						LOGGER.info("EXCEPTION");
						LOGGER.info("Completed");
						response.set("{\"status\":\"" + error.getMessage() + "\"}");
					}
			);

			for (int i=0; i < 5; i++){
				if(response.get() != null)
					break;
				Thread.sleep(1000);
			}

			if(response.get() != null) {
				return ResponseEntity.ok(response.get());
			}else{
				return formFailureResponse("Operation Timed out2");
			}
		}catch(JunoException | UnsupportedEncodingException | InterruptedException e){
			LOGGER.info("EXCEPTION");
			LOGGER.info("Completed");
			return formFailureResponse(e.getMessage());
		}
	}

	@Override
	public ResponseEntity<String> reactCompareAndSet(String key, String value) throws JunoException{
		LOGGER.info("Key -> " + key);
		LOGGER.info("Value ->" + value);
		try{
			AtomicReference<String> response = new AtomicReference<>();
			JunoResponse monoRes = junoClient.get(key.getBytes("UTF-8"));
			RecordContext recCtx = monoRes.getRecordContext();

			Mono<JunoResponse> compareAndSetResponse = junoReactClient.compareAndSet(recCtx,value.getBytes("UTF-8"), 0);
			compareAndSetResponse.subscribe(res ->
					{
						response.set(formSuccesResString(res));
						LOGGER.info("SUCCESS");
						LOGGER.info("Completed");
					}, error ->
					{
						// Failure in processing the request. Do not SLEEP or BLOCK in this method.
						// The cause for failure can be obtained via error.getMessage()
						LOGGER.info("EXCEPTION");
						LOGGER.info("Completed");
						response.set("{\"status\":\"" + error.getMessage() + "\"}");
					}
			);

			for (int i=0; i < 5; i++){
				if(response.get() != null)
					break;
				Thread.sleep(1000);
			}

			if(response.get() != null) {
				return ResponseEntity.ok(response.get());
			}else{
				return formFailureResponse("Operation Timed out2");
			}
		}catch(JunoException | UnsupportedEncodingException | InterruptedException e){
			LOGGER.info("EXCEPTION");
			LOGGER.info("Completed");
			return formFailureResponse(e.getMessage());
		}
	}


	@Override
	public ResponseEntity<String> reactCompareAndSetTTL(String key, String value, Long ttl) throws JunoException{
		LOGGER.info("Key -> " + key);
		LOGGER.info("Value ->" + value);
		LOGGER.info("ttl ->" + ttl);
		try{
			AtomicReference<String> response = new AtomicReference<>();
			JunoResponse monoRes = junoClient.get(key.getBytes("UTF-8"));
			RecordContext recCtx = monoRes.getRecordContext();

			Mono<JunoResponse> compareAndSetResponse = junoReactClient.compareAndSet(recCtx,value.getBytes("UTF-8"), ttl);
			compareAndSetResponse.subscribe(res ->
					{
						response.set(formSuccesResString(res));
						LOGGER.info("SUCCESS");
						LOGGER.info("Completed");
					}, error ->
					{
						// Failure in processing the request. Do not SLEEP or BLOCK in this method.
						// The cause for failure can be obtained via error.getMessage()
						LOGGER.info("EXCEPTION");
						LOGGER.info("Completed");
						response.set("{\"status\":\"" + error.getMessage() + "\"}");
					}
			);

			for (int i=0; i < 5; i++){
				if(response.get() != null)
					break;
				Thread.sleep(1000);
			}

			if(response.get() != null) {
				return ResponseEntity.ok(response.get());
			}else{
				return formFailureResponse("Operation Timed out2");
			}
		}catch(JunoException | UnsupportedEncodingException | InterruptedException e){
			LOGGER.info("EXCEPTION");
			LOGGER.info("Completed");
			return formFailureResponse(e.getMessage());
		}
	}


	@Override
	public ResponseEntity<String> reactDelete(String key) throws JunoException {
		LOGGER.info("Key -> " + key);
		try{
			AtomicReference<String> response = new AtomicReference<String>();
			Mono<JunoResponse> monoRes = junoReactClient.delete(key.getBytes("UTF-8"));
			monoRes.subscribe(res ->
					{
						response.set(formSuccesResString(res));
						LOGGER.info("SUCCESS");
						LOGGER.info("Completed");
					}, error ->
					{
						// Failure in processing the request. Do not SLEEP or BLOCK in this method.
						// The cause for failure can be obtained via error.getMessage()
						LOGGER.info("EXCEPTION");
						LOGGER.info("Completed");
						response.set("{\"status\":\"" + error.getMessage() + "\"}");
					}
			);

			for (int i=0; i < 5; i++){
				if(response.get() != null)
					break;
				Thread.sleep(1000);
			}

			if(response.get() != null) {
				return ResponseEntity.ok(response.get());
			}else{
				return formFailureResponse("Operation Timed out2");
			}
		}catch(JunoException | UnsupportedEncodingException | InterruptedException e){
			LOGGER.info("EXCEPTION");
			LOGGER.info("Completed");
			return formFailureResponse(e.getMessage());
		}
	}

	private SingleSubscriber<JunoResponse> createSubscriber(AtomicReference<String> response){
		SingleSubscriber<JunoResponse> subscriber = new SingleSubscriber<JunoResponse>() {
			@Override
			public void onError(Throwable e) {
				LOGGER.info("EXCEPTION");
				LOGGER.info("Completed");
				response.set("{\"status\":\"" + e.getMessage() + "\"}");
				LOGGER.error(e.getMessage());
			}

			@Override
			public void onSuccess(JunoResponse res) {
				response.set(formSuccesResString(res));
				if(res.getStatus() == OperationStatus.Success){
					LOGGER.info(String.valueOf(res.getStatus()));
					LOGGER.info("Completed");
				} else {
					if(res.getStatus() == OperationStatus.UniqueKeyViolation){
						LOGGER.warn("Key Already exists in DB");
					} else if(res.getStatus() == OperationStatus.TTLExtendFailure) {
						LOGGER.warn("Failure to Extend the TTL of the record.");
					} else{
						LOGGER.warn(res.getStatus().getErrorText());
					}
				}
			}
		};

		return subscriber;
	}

	@Override
	public ResponseEntity<String> asyncCreate(String key, String value) throws JunoException {
		LOGGER.info("Key -> " + key);
		LOGGER.info("Value ->" + value);

		try{
			AtomicReference<String> response = new AtomicReference<String>();
			Single<JunoResponse> createResponse = junoAsyncClient.create(key.getBytes("UTF-8"),value.getBytes("UTF-8"));
			createResponse.subscribe(createSubscriber(response));

			for (int i=0; i < 5; i++){
				if(response.get() != null)
					break;
				Thread.sleep(1000);
			}

			if(response.get() != null) {
				return ResponseEntity.ok(response.get());
			}else{
				return formFailureResponse("Operation Timed out2");
			}
		}catch(JunoException | UnsupportedEncodingException | InterruptedException e){
			LOGGER.info("EXCEPTION");
			LOGGER.info("Completed");
			return formFailureResponse(e.getMessage());
		}
	}

	@Override
	public ResponseEntity<String> asyncCreate(String key, String value, Long ttl) throws JunoException {
		LOGGER.info("Key -> " + key);
		LOGGER.info("Value ->" + value);
		LOGGER.info("ttl -> " + ttl);

		try{
			AtomicReference<String> response = new AtomicReference<String>();
			Single<JunoResponse> createResponse = junoAsyncClient.create(key.getBytes("UTF-8"),value.getBytes("UTF-8"), ttl);
			createResponse.subscribe(createSubscriber(response));

			for (int i=0; i < 5; i++){
				if(response.get() != null)
					break;
				Thread.sleep(1000);
			}

			if(response.get() != null) {
				return ResponseEntity.ok(response.get());
			}else{
				return formFailureResponse("Operation Timed out2");
			}
		}catch(JunoException | UnsupportedEncodingException | InterruptedException e){
			LOGGER.info("EXCEPTION");
			LOGGER.info("Completed");
			return formFailureResponse(e.getMessage());
		}
	}


	@Override
	public ResponseEntity<String> asyncGet(String key) throws JunoException {
		LOGGER.info("Key -> " + key);
		try{
			AtomicReference<String> response = new AtomicReference<String>();
			Single<JunoResponse> getResponse = junoAsyncClient.get(key.getBytes("UTF-8"));
			getResponse.subscribe(createSubscriber(response));

			for (int i=0; i < 5; i++){
				if(response.get() != null)
					break;
				Thread.sleep(1000);
			}

			if(response.get() != null) {
				return ResponseEntity.ok(response.get());
			}else{
				return formFailureResponse("Operation Timed out2");
			}
		}catch(JunoException | UnsupportedEncodingException | InterruptedException e){
			LOGGER.info("EXCEPTION");
			LOGGER.info("Completed");
			return formFailureResponse(e.getMessage());
		}
	}

	@Override
	public ResponseEntity<String> asyncGet(String key, Long ttl) throws JunoException {
		LOGGER.info("Key -> " + key);
		LOGGER.info("ttl -> " + ttl);
		try{
			AtomicReference<String> response = new AtomicReference<String>();
			Single<JunoResponse> getResponse = junoAsyncClient.get(key.getBytes("UTF-8"), ttl);
			getResponse.subscribe(createSubscriber(response));

			for (int i=0; i < 5; i++){
				if(response.get() != null)
					break;
				Thread.sleep(1000);
			}

			if(response.get() != null) {
				return ResponseEntity.ok(response.get());
			}else{
				return formFailureResponse("Operation Timed out2");
			}
		}catch(JunoException| UnsupportedEncodingException | InterruptedException e){
			LOGGER.info("EXCEPTION");
			LOGGER.info("Completed");
			return formFailureResponse(e.getMessage());
		}
	}

	@Override
	public ResponseEntity<String> asyncUpdate(String key, String value) throws JunoException {
		LOGGER.info("Key -> " + key);
		LOGGER.info("Value ->" + value);
		try{
			AtomicReference<String> response = new AtomicReference<String>();
			Single<JunoResponse> updateResponse = junoAsyncClient.update(key.getBytes("UTF-8"),value.getBytes("UTF-8"));
			updateResponse.subscribe(createSubscriber(response));

			for (int i=0; i < 5; i++){
				if(response.get() != null)
					break;
				Thread.sleep(1000);
			}

			if(response.get() != null) {
				return ResponseEntity.ok(response.get());
			}else{
				return formFailureResponse("Operation Timed out2");
			}
		}catch(JunoException | UnsupportedEncodingException | InterruptedException e){
			LOGGER.info("EXCEPTION");
			LOGGER.info("Completed");
			return formFailureResponse(e.getMessage());
		}
	}

	@Override
	public ResponseEntity<String> asyncUpdate(String key, String value, Long ttl) throws JunoException {
		LOGGER.info("Key -> " + key);
		LOGGER.info("Value ->" + value);
		LOGGER.info("ttl -> " + ttl);
		try{
			AtomicReference<String> response = new AtomicReference<String>();
			Single<JunoResponse> updateResponse = junoAsyncClient.update(key.getBytes("UTF-8"),value.getBytes("UTF-8"), ttl);
			updateResponse.subscribe(createSubscriber(response));

			for (int i=0; i < 5; i++){
				if(response.get() != null)
					break;
				Thread.sleep(1000);
			}

			if(response.get() != null) {
				return ResponseEntity.ok(response.get());
			}else{
				return formFailureResponse("Operation Timed out2");
			}
		}catch(JunoException | UnsupportedEncodingException | InterruptedException e){
			LOGGER.info("EXCEPTION");
			LOGGER.info("Completed");
			return formFailureResponse(e.getMessage());
		}
	}

	@Override
	public ResponseEntity<String> asyncSet(String key, String value) throws JunoException {
		LOGGER.info("Key -> " + key);
		LOGGER.info("Value ->" + value);
		try{
			AtomicReference<String> response = new AtomicReference<String>();
			Single<JunoResponse> setResponse = junoAsyncClient.set(key.getBytes("UTF-8"),value.getBytes("UTF-8"));
			setResponse.subscribe(createSubscriber(response));

			for (int i=0; i < 5; i++){
				if(response.get() != null)
					break;
				Thread.sleep(1000);
			}

			if(response.get() != null) {
				return ResponseEntity.ok(response.get());
			}else{
				return formFailureResponse("Operation Timed out2");
			}
		}catch(JunoException | UnsupportedEncodingException | InterruptedException e){
			LOGGER.info("EXCEPTION");
			LOGGER.info("Completed");
			return formFailureResponse(e.getMessage());
		}
	}


	@Override
	public ResponseEntity<String> asyncSet(String key, String value, Long ttl) throws JunoException {
		LOGGER.info("Key -> " + key);
		LOGGER.info("Value ->" + value);
		LOGGER.info("ttl -> " + ttl);
		try{
			AtomicReference<String> response = new AtomicReference<String>();
			Single<JunoResponse> setResponse = junoAsyncClient.set(key.getBytes("UTF-8"),value.getBytes("UTF-8"), ttl);
			setResponse.subscribe(createSubscriber(response));

			for (int i=0; i < 5; i++){
				if(response.get() != null)
					break;
				Thread.sleep(1000);
			}

			if(response.get() != null) {
				return ResponseEntity.ok(response.get());
			}else{
				return formFailureResponse("Operation Timed out2");
			}
		}catch(JunoException | UnsupportedEncodingException | InterruptedException e){
			LOGGER.info("EXCEPTION");
			LOGGER.info("Completed");
			return formFailureResponse(e.getMessage());
		}
	}

	@Override
	public ResponseEntity<String> asyncCompareAndSet(String key, String value) throws JunoException{
		LOGGER.info("Key -> " + key);
		LOGGER.info("Value ->" + value);
		try{
			AtomicReference<String> response = new AtomicReference<String>();
			JunoResponse monoRes = junoClient.get(key.getBytes("UTF-8"));
			RecordContext recCtx = monoRes.getRecordContext();
			Single<JunoResponse> compareAndSetResponse = junoAsyncClient.compareAndSet(recCtx, value.getBytes("UTF-8"), 0);
			compareAndSetResponse.subscribe(createSubscriber(response));

			for (int i=0; i < 5; i++){
				if(response.get() != null)
					break;
				Thread.sleep(1000);
			}

			if(response.get() != null) {
				return ResponseEntity.ok(response.get());
			}else{
				return formFailureResponse("Operation Timed out2");
			}
		}catch(JunoException | UnsupportedEncodingException | InterruptedException e){
			LOGGER.info("EXCEPTION");
			LOGGER.info("Completed");
			return formFailureResponse(e.getMessage());
		}
	}


	@Override
	public ResponseEntity<String> asyncCompareAndSetTTL(String key, String value, Long ttl) throws JunoException{
		LOGGER.info("Key -> " + key);
		LOGGER.info("Value ->" + value);
		LOGGER.info("ttl -> " + ttl);
		try{
			AtomicReference<String> response = new AtomicReference<String>();
			JunoResponse monoRes = junoClient.get(key.getBytes("UTF-8"));
			RecordContext recCtx = monoRes.getRecordContext();
			Single<JunoResponse> compareAndSetResponse = junoAsyncClient.compareAndSet(recCtx, value.getBytes("UTF-8"), 0);
			compareAndSetResponse.subscribe(createSubscriber(response));

			for (int i=0; i < 5; i++){
				if(response.get() != null)
					break;
				Thread.sleep(1000);
			}

			if(response.get() != null) {
				return ResponseEntity.ok(response.get());
			}else{
				return formFailureResponse("Operation Timed out2");
			}
		}catch(JunoException | UnsupportedEncodingException | InterruptedException e){
			LOGGER.info("EXCEPTION");
			LOGGER.info("Completed");
			return formFailureResponse(e.getMessage());
		}
	}


	@Override
	public ResponseEntity<String> asyncDelete(String key) throws JunoException {
		LOGGER.info("Key -> " + key);
		try{
			AtomicReference<String> response = new AtomicReference<String>();
			Single<JunoResponse> deleteResponse = junoAsyncClient.delete(key.getBytes("UTF-8"));
			deleteResponse.subscribe(createSubscriber(response));

			for (int i=0; i < 5; i++){
				if(response.get() != null)
					break;
				Thread.sleep(1000);
			}

			if(response.get() != null) {
				return ResponseEntity.ok(response.get());
			}else{
				return formFailureResponse("Operation Timed out2");
			}
		}catch(JunoException | UnsupportedEncodingException | InterruptedException e){
			LOGGER.info("EXCEPTION");
			LOGGER.info("Completed");
			return formFailureResponse(e.getMessage());
		}
	}
}