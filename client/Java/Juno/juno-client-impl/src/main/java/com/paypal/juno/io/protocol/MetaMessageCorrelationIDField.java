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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * 
 *
 *  Correlation ID field structure.
 	Correlation ID field
    Tag     : 0x09
    SizeType: 0x0
    
    Tag: 0x09
	+----+-------------------------------------------
	|  0 | field size (including padding)
	+----+-------------------------------------------
	|  1 | octet sequence length
	+----+-------------------------------------------
	|    | octet sequence, padding to 4-byte aligned
	+----+-------------------------------------------
 */
public class MetaMessageCorrelationIDField extends MetaMessageTagAndType {
    private static final Logger logger = LoggerFactory.getLogger(MetaMessageSourceField.class);

	private byte componentSize;
	private byte correlationIdLength;
	private byte [] correlationId; // 4 bytes padding

	public void setCorrelationId(byte[] correlationId) {
		this.correlationId = correlationId;
		this.correlationIdLength = (byte)correlationId.length;
		componentSize = (byte) (1 + 1 + correlationId.length); // 1 byte is for total size and 1 byte for correlationId length
		//Add padding for size if the total size if not a multiple of 4
		int offset = componentSize % 4;
		if (offset != 0) {
			componentSize += 4 - offset;
		}
	}
	
	public int getBufferLength() {
		return componentSize;
	}
	
	public byte[] getCorrelationId() {
		return correlationId;
	}

	public static Logger getLogger() {
		return logger;
	}

	public MetaMessageCorrelationIDField(byte tagAndSizeType) {
		super(tagAndSizeType);
	}
	
	public void writeBuf(ByteBuf out) {
		int indexStart = out.writerIndex();
		out.writeByte(componentSize);
		out.writeByte(correlationIdLength);
		if (correlationId != null) {
			out.writeBytes(correlationId);
		}
		
		//Add padding if needed
		OperationMessage.writeBufPadding(indexStart, out, 4);
	}
}
