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

import com.paypal.juno.client.io.JunoRequest;
import com.paypal.juno.client.io.JunoResponse;
import com.paypal.juno.client.io.RecordContext;
import com.paypal.juno.exception.JunoException;
import java.util.Map;
import rx.Observable;
import rx.Single;

public interface JunoAsyncClient {
	/**
	 * Insert a record into Juno DB with default TTL
	 * @param key - Key of the record to be Inserted
	 * @param value - Record Value
	 * @return Single<JunoResponse> - Emits a single response or Error with processing the request
	 * @throws JunoException - Throws Exception if any exception while processing the request
	 */
	public Single<JunoResponse> create(byte[] key, byte[] value) throws JunoException;

	/**
	 * Insert a record into Juno DB with user supplied TTL
	 * @param key - Key of the record to be Inserted
	 * @param value - Record Value
	 * @param timeToLiveSec - Time to Live for the record
	 * @return Single<JunoResponse> - Emits a single response or Error with processing the request
	 * @throws JunoException - Throws Exception if any exception while processing the request
	 */
	public Single<JunoResponse> create(byte[] key, byte[] value, long timeToLiveSec) throws JunoException;
	
	/**
	 * Get a record from Juno DB
	 * @param key - Key of the record to be retrieved
	 * @return Single<JunoResponse> - Emits a single response with the record value or Error
	 * @throws JunoException - Throws Exception if any issue while processing the request
	 */
	public Single<JunoResponse> get(byte[] key) throws JunoException;

	/**
	 * Get a record from Juno DB and Extend the TTL
	 * @param key - Key of the record to be retrieved
	 * @param timeToLiveSec - Time to Live for the record
	 * @return Single<JunoResponse> - Emits a single response or Error with processing the request
	 * @throws JunoException - Throws Exception if any exception while processing the request
	 */
	public Single<JunoResponse> get(byte[] key, long timeToLiveSec) throws JunoException;

	/**
	 * Update a record in Juno DB
	 * @param key - Key of the record to be Updated
	 * @param value - Record Value
	 * @return Single<JunoResponse> - Emits a single response or Error with processing the request
	 * @throws JunoException - Throws Exception if any issue while processing the request
	 */
	public Single<JunoResponse> update(byte[] key, byte[] value) throws JunoException;

	/**
	 * Update a record in Juno DB and Extend its TTL
	 * @param key - Key of the record to be Updated
	 * @param value - Record Value
	 * @param timeToLiveSec - Time to Live for the record
	 * @return Single<JunoResponse> - Emits a single response or Error with processing the request
	 * @throws JunoException - Throws Exception if any exception while processing the request
	 */
	public Single<JunoResponse> update(byte[] key, byte[] value, long timeToLiveSec) throws JunoException;
	
	/**
	 * Update the record if present in Juno DB else create that record with the default TTL in the configuration
	 * @param key - Key of the record to be Upserted
	 * @param value - Record Value
	 * @return Single<JunoResponse> - Emits a single response or Error with processing the request
	 * @throws JunoException - Throws Exception if any exception while processing the request
	 */
	public Single<JunoResponse> set(byte[] key, byte[] value) throws JunoException;
	
	/**
	 * Update the record if present in Juno DB and extend its TTL else create that record with the supplied TTL.
	 * @param key - Key of the record to be Upserted
	 * @param value - Record Value
	 * @param timeToLiveSec - Time to Live for the record
	 * @return Single<JunoResponse> - Emits a single response or Error with processing the request
	 * @throws JunoException - Throws Exception if any exception while processing the request
	 */
	public Single<JunoResponse> set(byte[] key, byte[] value, long timeToLiveSec) throws JunoException;
	
	/**
	 * Delete the record from Juno DB
	 * @param key - Record Key to be deleted
	 * @return Single<JunoResponse> - Emits a single response or Error with processing the request
	 * @throws JunoException - Throws Exception if any exception while processing the request
	 */
	public Single<JunoResponse> delete(byte[] key) throws JunoException;
	
	/**
	 * Compare the version of the record in Juno DB and update it only if the supplied version
	 * is greater than or equal to the existing version in Juno DB
	 * @param jcx - Record context from a previous Get operation
	 * @param value - Record Value
	 * @param timeToLiveSec - Time to Live for the record. If set to 0 then the TTL is not extended.
	 * @return Single<JunoResponse> - Emits a single response or Error with processing the request
	 * @throws JunoException - Throws Exception if any exception while processing the request
	 */
	public Single<JunoResponse> compareAndSet(RecordContext jcx, byte[] value, long timeToLiveSec) throws JunoException;
	
	/**
	 * Perform batch operation on list of requests
	 * @param request - List of requests with necessary data for that operation
	 * @return List of responses for the requests
	 * @throws JunoException - Throws Exception if any issue while processing the requests
	 */
	public Observable<JunoResponse> doBatch(Iterable<JunoRequest> request) throws JunoException;

	/**
	 * return the properties of the current bean in a MAP
	 * The map consists of property name and its value. Property name can be found
	 * in com.paypal.juno.conf.JunoProperties
	 */
	public Map<String, String> getProperties();
}
