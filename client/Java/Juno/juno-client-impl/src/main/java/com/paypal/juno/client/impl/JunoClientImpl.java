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
import com.paypal.juno.client.io.JunoRequest;
import com.paypal.juno.client.io.JunoResponse;
import com.paypal.juno.client.io.RecordContext;
import com.paypal.juno.exception.JunoException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.net.ssl.SSLContext;

class JunoClientImpl implements JunoClient{
	
	JunoReactClientImpl reactClient;
	
	/**
	 * Creates JunoReactClient object which will be used for the
	 * rest of the methods to do the blocking call on their respective 
	 * async operations
	 * @param config
	 * @pram ctx - SSL context
	 */
	public JunoClientImpl(JunoClientConfigHolder config,SSLContext ctx){
		reactClient = new JunoReactClientImpl(config,ctx,false);
	}
	
	/**
	 * Insert a record into Juno DB with default TTL
	 * @param key - Key of the record to be Inserted
	 * @param value - Record Value
	 * @return JunoResponse - Juno Response object which contains the status of the operation
	 * @throws JunoException - Throws Exception if any exception while processing the request
	 */
	public JunoResponse create(byte[] key, byte[] value) throws JunoException {
		return reactClient.create(key,value).block();
	}

	/**
	 * Insert a record into Juno DB with user supplied TTL
	 * @param key - Key of the record to be Inserted
	 * @param value - Record Value
	 * @param timeToLiveSec - Time to Live for the record
	 * @return JunoResponse - Juno Response object which contains the status of the operation
	 * @throws JunoException - Throws Exception if any exception while processing the request
	 */
	public JunoResponse create(byte[] key, byte[] value, long timeToLiveSec) throws JunoException {
		return reactClient.create(key, value, timeToLiveSec).block();
	}

	/**
	 * Get a record from Juno DB
	 * @param key - Key of the record to be retrieved
	 * @return JunoResponse - Juno Response object which contains the status of the operation, 
	 * 						  version of the record and value of the record.
	 * @throws JunoException - Throws Exception if any issue while processing the request
	 */
	public JunoResponse get(byte[] key) throws JunoException {
		return reactClient.get(key).block();
	}

	/**
	 * Get a record from Juno DB and Extend the TTL
	 * @param key - Key of the record to be retrieved
	 * @param timeToLiveSec - Time to Live for the record
	 * @return JunoResponse - Juno Response object which contains the status of the operation, 
	 * 						  version of the record and value of the record.
	 * @throws JunoException - Throws Exception if any exception while processing the request
	 */
	public JunoResponse get(byte[] key, long timeToLiveSec) throws JunoException {
		return reactClient.get(key, timeToLiveSec).block();
	}

	/**
	 * Update a record in Juno DB
	 * @param key - Key of the record to be Updated
	 * @param value - Record Value
	 * @return JunoResponse - Juno Response object which contains the status of the operation
	 * @throws JunoException - Throws Exception if any issue while processing the request
	 */
	public JunoResponse update(byte[] key, byte[] value) throws JunoException {
		return reactClient.update(key, value).block();
	}

	/**
	 * Update a record in Juno DB and Extend its TTL
	 * @param key - Key of the record to be Updated
	 * @param value - Record Value
	 * @param timeToLiveSec - Time to Live for the record
	 * @return JunoResponse - Juno Response object which contains the status of the operation.
	 * @throws JunoException - Throws Exception if any exception while processing the request
	 */
	public JunoResponse update(byte[] key, byte[] value, long timeToLiveSec) throws JunoException {
		return reactClient.update(key, value, timeToLiveSec).block();
	}

	/**
	 * Update the record if present in Juno DB else create that record with the default TTL in the configuration
	 * @param key - Key of the record to be Upserted
	 * @param value - Record Value
	 * @return JunoResponse - Juno Response object which contains the status of the operation
	 * @throws JunoException - Throws Exception if any exception while processing the request
	 */
	public JunoResponse set(byte[] key, byte[] value) throws JunoException {
		return reactClient.set(key, value).block();
	}

	/**
	 * Update the record if present in Juno DB and extend its TTL else create that record with the supplied TTL.
	 * @param key - Key of the record to be Upserted
	 * @param value - Record Value
	 * @param timeToLiveSec - Time to Live for the record
	 * @return JunoResponse - Juno Response object which contains the status of the operation
	 * @throws JunoException - Throws Exception if any exception while processing the request
	 */
	public JunoResponse set(byte[] key, byte[] value, long timeToLiveSec) throws JunoException {
		return reactClient.set(key, value, timeToLiveSec).block();
	}


	/**
	 * Perform batch operations
	 * @param request - List of request to be processed
	 * @return List of JunoResponses - Juno Response object which contains the status of the operation
	 * @throws JunoException - Throws Exception if any exception while processing the request
	 */
	public Iterable<JunoResponse> doBatch(Iterable<JunoRequest> request) throws JunoException {
		Iterable<JunoResponse> jRes = reactClient.doBatch(request).toIterable();
		List<JunoResponse> respList = new ArrayList<>();
		for(JunoResponse resp : jRes){
			respList.add(resp);
		}
		return respList;
	}

	/**
	 * Delete the record from Juno DB
	 * @param key - Record Key to be deleted
	 * @return JunoResponse - Juno Response object which contains the status of the operation
	 * @throws JunoException - Throws Exception if any exception while processing the request
	 */
	public JunoResponse delete(byte[] key) throws JunoException {
		return reactClient.delete(key).block();
	}

	/**
	 * Compare the version of the record in Juno DB and update it only if the supplied version
	 * is greater than or equal to the existing version in Juno DB
	 * @param jcx - Record context from a previous Get operation
	 * @param value - Record Value
	 * @param timeToLiveSec - Time to Live for the record. If set to 0 then the TTL is not extended.
	 * @return JunoResponse - Juno Response object which contains the status of the operation
	 * @throws JunoException - Throws Exception if any exception while processing the request
	 */
	public JunoResponse compareAndSet(RecordContext jcx, byte[] value, long timeToLiveSec) throws JunoException {
		return reactClient.compareAndSet(jcx, value, timeToLiveSec).block();
	}

	/**
	 * Return the configured Juno properties for this current instance in a MAP
	 */
	public Map<String, String> getProperties() {
		return reactClient.getProperties();
	}
}