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

import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;

// This class represents the MetaData Component
public class MetaMessageTagAndType {
	public enum FieldType {
		Dummy(0x0),
		TimeToLive (0x1),
		Version(0x2),
		CreationTime(0x3),
		ExpirationTime(0x4),
		RequestID(0x5),
		SourceInfo(0x6),
		LastModificationTime(0x7),
		OriginatorReqID(0x8),
		CorrelationID(0x9),
		RequestHandlingTime(0xa);
		FieldType(int type) {
		}
	};

	final private FieldType fieldType;
	final private byte fieldSize;
	final private boolean isVariable;
	
	public boolean isVariable() {
		return isVariable;
	}

	public MetaMessageTagAndType(byte tagAndSizeType) {
		byte tmp = (byte)(0xFF & tagAndSizeType);
		// Check if the Field Tag is not in our enum then mark it Dummy to skip it.
		if((tmp & 0x1F)  > 0xa){
			fieldType = FieldType.values()[0];
		}else{
			//System.out.println("The tmp is:"+((tmp & 0x1F)));
			fieldType = FieldType.values()[(tmp & 0x1F)];
		}
		
		if ((tmp >> 5) == 0) {
			this.isVariable = true;
			fieldSize = 0;
		}
		else {
			fieldSize = (byte) (tmp >> 5);
			this.isVariable = false;
		}
	}

	public FieldType getFieldType() {
		return fieldType;
	}
	
	public byte getFieldSize() {
		byte rt = (byte) (1 << (1 + fieldSize));
		return rt;
	}
	
	public byte getValue() {
		byte value = (byte) (fieldType.ordinal());
		if (!this.isVariable) {
			value |= fieldSize << 5;
		}
		return value;
	}
	
	// Will be overridden by the sub class
	public void writeValue(ByteBuffer out) {
		//byte value = (byte) ((fieldType.ordinal() + 1) | ((fieldSize) << 5));
		//out.put(value);
	}
	
	public void writeBuf(ByteBuf out) {
		throw new RuntimeException("writeBuf not implemented in sub class.");
	}
	
	// Will be overridden by the sub class
	public int getBufferLength() {
		return  0;
	}
}