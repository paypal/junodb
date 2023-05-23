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
package com.paypal.juno.io.protocol;

import com.paypal.juno.client.ServerOperationStatus;
import com.paypal.juno.io.protocol.PayloadOperationMessage.CompressionType;
import java.util.UUID;

public class JunoMessage {

	private byte[] key;
	private byte[] value;
	private long version;
	private long expiry;
	private long timeToLiveSec;
	private String nameSpace;
	private String applicationName;
	private OperationType opType;
	private ServerOperationStatus status;
	private long creationTime;
	private long reqStartTime;
	private long reqHandlingTime; 
	private long messageSize;
	private UUID reqId;
	private boolean isPayloadCompressed;
	private int compressionAchieved;
	private CompressionType compressionType;
	
	public enum OperationType {
		Nop(0,"NOP"),
		Create(1,"CREATE"),
		Get(2,"GET"),
		Update(3,"UPDATE"),
		Set(4,"SET"),
		CompareAndSet(5,"COMPAREANDSET"),
		Destroy(6,"DESTROY");
		
		private final int code;
		private final String opType;
		
		
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
	
	public JunoMessage(){
		
	}
	
	public JunoMessage(byte[] key, byte[] value, long version,long expiry,long ttl,OperationType opType){
		this.key = key;
		this.value = value;
		this.version = version;
		this.expiry = expiry;
		this.timeToLiveSec = ttl;
		this.opType = opType;
	}
	
	public byte[] getValue() {
		return value;
	}
	public long getVersion() {
		return version;
	}
	public void setVersion(long version) {
		this.version = version;
	}
	public long getExpiry() {
		return expiry;
	}
	public void setExpiry(long expiry) {
		this.expiry = expiry;
	}
	public long getTimeToLiveSec() {
		return timeToLiveSec;
	}
	public void setTimeToLiveSec(long timeToLiveSec) {
		this.timeToLiveSec = timeToLiveSec;
	}
	public void setValue(byte[] value) {
		this.value = value;
	}

	public byte[] getKey() {
		return key;
	}

	public void setKey(byte[] key) {
		this.key = key;
	}

	public OperationType getOpType() {
		return opType;
	}

	public void setOpType(OperationType opType) {
		this.opType = opType;
	}

	public ServerOperationStatus getStatus() {
		return status;
	}

	public void setStatus(ServerOperationStatus status) {
		this.status = status;
	}
	public String getNameSpace() {
		return nameSpace;
	}

	public void setNameSpace(String nameSpace) {
		this.nameSpace = nameSpace;
	}

	public String getApplicationName() {
		return applicationName;
	}

	public void setApplicationName(String applicationName) {
		this.applicationName = applicationName;
	}

	public long getCreationTime() {
		return creationTime;
	}

	public void setCreationTime(long creationTime) {
		this.creationTime = creationTime;
	}

	public long getReqStartTime() {
		return reqStartTime;
	}

	public void setReqStartTime(long reqStartTime) {
		this.reqStartTime = reqStartTime;
	}
	
	public void setReqHandlingTime(long rht) {
		this.reqHandlingTime = rht;
	}
	
	public long getReqHandlingTime() {
		return reqHandlingTime;
	}

	public long getMessageSize() {
		return messageSize;
	}

	public void setMessageSize(long messageSize) {
		this.messageSize = messageSize;
	}

	public UUID getReqId() {
		return reqId;
	}

	public void setReqId(UUID reqId) {
		this.reqId = reqId;
	}
	
	public boolean isPayloadCompressed() {
		return isPayloadCompressed;
	}

	public void setPayloadCompressed(boolean isPayloadCompressed) {
		this.isPayloadCompressed = isPayloadCompressed;
	}

	public int getCompressionAchieved() {
		return compressionAchieved;
	}

	public void setCompressionAchieved(int compressionAchieved) {
		this.compressionAchieved = compressionAchieved;
	}

	public CompressionType getCompressionType() {
		return compressionType;
	}

	public void setCompressionType(CompressionType compressionType) {
		this.compressionType = compressionType;
	}
}
