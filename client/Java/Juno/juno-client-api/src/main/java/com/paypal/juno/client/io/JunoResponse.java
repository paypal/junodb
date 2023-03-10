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
package com.paypal.juno.client.io;

import java.io.Serializable;

public class JunoResponse extends AbstractResponse implements Serializable {

	private static final long serialVersionUID = 1L;
	private byte[] value;
	private final RecordContext rcx;

	public JunoResponse(byte[] key, byte[] value, long version, long ttl, long createTime, OperationStatus status) {
		super(key, status);
		this.value = value;
		rcx = new RecordContext(key,version,createTime,ttl);
	}
	
	/**
	 * @return Juno record context
	 */
	public RecordContext getRecordContext() {
		return rcx;
	}

	/**
	 * @return the key
	 */
	public byte[] getKey() {
		return rcx.getKey();
	}
	/**
	 * @return the value
	 */
	public byte[] getValue() {
		return value;
	}

	/**
	 * @return the version
	 */
	public long getVersion() {
		return rcx.getVersion();
	}

	/**
	 * @return the Remaining time to live
	 */
	public long getTtl() {
		return rcx.getTtl();
	}
	
	/**
	 * @return the operation Status
	 */
	public OperationStatus getStatus(){
		return super.status();
	}

	public long getCreationTime() {
		return rcx.getCreationTime();
	}
}