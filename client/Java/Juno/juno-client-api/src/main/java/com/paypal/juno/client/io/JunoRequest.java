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
import java.util.Arrays;

public class JunoRequest extends AbstractRequest implements Serializable {

	private static final long serialVersionUID = 1L;
	private Long timeToLiveSec;
	private byte[] value;
	private long version;
	private long creationTime;
	private OperationType type;

	public enum OperationType {
		Create(1,"CREATE"),
		Get(2,"GET"),
		Update(3,"UPDATE"),
		Set(4,"SET"),
		Destroy(5,"DESTROY");
		
		private final int code;
		private final String opType;
		
		/**
		 * Constructor
		 * 
		 * @param code
		 * @param opText
		 */
		OperationType(int code, String opText) {
			this.code = code;
			this.opType = opText;
		}

		public int getCode() {
			return code;
		}

		public String getOpType() {
			return opType;
		}

	};

	public JunoRequest(byte[] key, byte[] value, long version, long timeToLiveSec,long creationTime,OperationType type) {
		super(key);
		this.value = value;
		this.timeToLiveSec = timeToLiveSec;
		this.version = version; 
		this.creationTime = creationTime;
		this.type = type;
	}
	
	public JunoRequest(byte[] key, byte[] value, long version, long timeToLiveSec, OperationType type) {
		super(key);
		this.value = value;
		this.timeToLiveSec = timeToLiveSec;
		this.version = version; 
		this.type = type;
	}

	public JunoRequest(byte[] key, byte[] value, long version, OperationType type) {
		super(key);
		this.value = value;
		this.timeToLiveSec = null;
		this.version = version;
		this.type = type;
	}
	public JunoRequest(byte[] key,long version, long timeToLiveSec, OperationType type) {
		super(key);
		this.timeToLiveSec = timeToLiveSec;
		this.version = version;
		this.type = type;
	}

	public Long getTimeToLiveSec() {
		return timeToLiveSec;
	}
	
	public byte[] getValue() {
		return value;
	}

	public long getVersion() {
		return version;
	}
	
	public long getCreationTime() {
		return creationTime;
	}
	
	public OperationType getType() {
		return type;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + (int) (timeToLiveSec ^ (timeToLiveSec >>> 32));
		result = prime * result + Arrays.hashCode(value);
		return result;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		JunoRequest other = (JunoRequest) obj;
		if (timeToLiveSec != other.timeToLiveSec)
			return false;
		if (!Arrays.equals(value, other.value))
			return false;
		if(version != other.version)
			return false;
		return true;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "JunoRequest [timeToLive=" + timeToLiveSec + ", value=" + Arrays.toString(value) + ", key()="
				+ Arrays.toString(key()) + ", version=" + version + ", toString()=" + super.toString() + ", getClass()=" + getClass() + "]";
	}
}