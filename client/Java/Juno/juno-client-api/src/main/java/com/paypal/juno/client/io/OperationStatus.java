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

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public enum OperationStatus {

    // Client side Response codes, Error Text status success(true) and failure(false)

	Success(0,"No error",true),
	NoKey(1,"Key not found",true),
	BadParam(2,"Bad parameter",false),
	UniqueKeyViolation(3,"Duplicate key",true),
	RecordLocked(4,"Record Locked",true),
	IllegalArgument(5,"Illegal argument",false),
	ConditionViolation(6,"Condition in the request violated",true),
	InternalError(7,"Internal error",false),
	QueueFull(8,"Outbound client queue full",false),
	NoStorage(9,"No storage server running",false),
	TTLExtendFailure(10,"Failure to extend TTL on get",true),
	ResponseTimeout(11,"Response Timed out",false),
	ConnectionError(12,"Connection Error",false),
	UnknownError(13,"Unknown Error",false);

	
	private final int code;
	private final String errorText;
	private final boolean txnOk;

	private static final Map<Integer, OperationStatus> lookup = new HashMap<Integer, OperationStatus>();

	static {
		for (OperationStatus s : EnumSet
				.allOf(OperationStatus.class))
			lookup.put(s.getCode(), s);
	}

	/**
	 * Constructor
	 * 
	 * @param code
	 * @param errorText
	 */
	OperationStatus(int code, String errorText,boolean txnOk) {
		this.code = code;
		this.errorText = errorText;
		this.txnOk = txnOk;
	}

	public int getCode() {
		return this.code;
	}

	public String getErrorText() {
		return this.errorText;
	}

	public static OperationStatus get(int code) {
		return lookup.get(code);
	}
	
	public boolean isTxnOk(){
		return txnOk;
	}
	
}

