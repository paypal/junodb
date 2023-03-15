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

public class MetaMessageFixedField extends MetaMessageTagAndType {
	private long content;
	private byte [] variableContent;
	
	public long getContent() {
		return content;
	}

	public void setContent(long content) {
		this.content = content;
	}
	
	public byte[] getVariableContent() {
		return variableContent;
	}

	public void setVariableContent(byte[] variableContent) {
		this.variableContent = variableContent;
	}

	public MetaMessageFixedField(byte tagAndSizeType) {
		super(tagAndSizeType);
	}
	
	public MetaMessageFixedField readBuf(ByteBuf in) {
		int size = this.getFieldSize();
		if (size == 4) {
			this.content = in.readUnsignedInt();   // long
		}
		else {
			variableContent = new byte [size];
			in.readBytes(variableContent);
		}
		
		return this;
	}
	
	public void writeBuf(ByteBuf out) {
		int size = this.getFieldSize();
		if (size == 4) {
			int value = (int) (0xFFFFFFFF & content);
			out.writeInt(value);
		}
		else {
			out.writeBytes(variableContent);
		}
	}

	public int getLength() {
		if (variableContent != null) {
			return variableContent.length;
		}
		else {
			return 4;
		}
	}
}