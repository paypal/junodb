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

import com.paypal.juno.client.io.OperationStatus;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public enum ServerOperationStatus {

	    // Response codes that match the JUNO Server side protocol
		Success(0, "no error", OperationStatus.Success),
		BadMsg(1, "bad message",OperationStatus.InternalError),
		NoKey(3, "key not found",OperationStatus.NoKey),
		DupKey(4, "dup key",OperationStatus.UniqueKeyViolation),
		BadParam(7, "bad parameter",OperationStatus.BadParam),
		RecordLocked(8, "record locked",OperationStatus.RecordLocked),
		NoStorageServer(12, "no active storage server",OperationStatus.NoStorage),
		ServerBusy(14, "Server busy",OperationStatus.InternalError),
	    VersionConflict(19, "version conflict",OperationStatus.ConditionViolation),
	    OpStatusSSReadTTLExtendErr(23,"Error extending TTL by SS",OperationStatus.InternalError),
	    CommitFailure(25, "Commit Failure",OperationStatus.InternalError),
	    InconsistentState(26,"Inconsistent State",OperationStatus.Success),
	    Internal(255,"Internal error",OperationStatus.InternalError),
		
		//Client specific errors 
		QueueFull(256,"Outbound client queue full",OperationStatus.QueueFull),
		ConnectionError(257,"Connection error",OperationStatus.ConnectionError),
		ResponseTimedout(258,"Response timed out",OperationStatus.ResponseTimeout);
	
		private final int code;
		private final String errorText;
		private OperationStatus resStatus;

		private static final Map<Integer, ServerOperationStatus> lookup = new HashMap<Integer, ServerOperationStatus>();

		static {
			for (ServerOperationStatus s : EnumSet
					.allOf(ServerOperationStatus.class))
				lookup.put(s.getCode(), s);
		}

		/**
		 * Constructor
		 * 
		 * @param code
		 * @param errorText
		 */
		ServerOperationStatus(int code, String errorText, OperationStatus rs) {
			this.code = code;
			this.errorText = errorText;
			this.resStatus = rs;
		}

		public int getCode() {
			return this.code;
		}

		public String getErrorText() {
			return this.errorText;
		}

		public static ServerOperationStatus get(int code) {
			if(lookup.get(code) == null)
				return Internal;
			else
				return lookup.get(code);
		}
		
		public OperationStatus getOperationStatus() {
			return resStatus;
		}
}
