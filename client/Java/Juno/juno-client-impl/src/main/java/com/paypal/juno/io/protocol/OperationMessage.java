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

import com.paypal.juno.exception.JunoException;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OperationMessage {
	private static final Logger logger = LoggerFactory.getLogger(OperationMessage.class);

	// Payload Tag/ID: 0x01
	// Meta Tag/ID: 0x02
	public static enum Type {
		Payload(0x1),
		Meta(0x02);
		int type;
		Type(int type) {
			this.type = type;
		}
		public int getValue(){
			return type;
		}
	}

	private MessageHeader header = null;
	private PayloadOperationMessage  payloadComponent;
	private MetaOperationMessage metaComponent;
	private String serverIp;
	
	public MessageHeader getHeader() {
		return header;
	}
	public void setHeader(MessageHeader header) {
		this.header = header;
	}
	
	public int getLength() {
		return (int)getHeader().getMessageSize();
	}
	
	public PayloadOperationMessage getPayloadComponent() {
		return payloadComponent;
	}
	
	public void setPayloadComponent(PayloadOperationMessage payloadComponent) {
		this.payloadComponent = payloadComponent;
	}
	
	public MetaOperationMessage getMetaComponent() {
		return metaComponent;
	}
	
	public void setMetaComponent(MetaOperationMessage metaComponent) {
		this.metaComponent = metaComponent;
	}
	
	public String getServerIp() {
		return serverIp;
	}
	
	public void setServerIp(String serverIp) {
		this.serverIp = serverIp;
	}

	// Read the message body which may have an set of components. There are currently
	// two types of components one is Payload and other is Metadata. The 1 byte tag 
	// indicates the type of component.
	//	** Component **
	//	 
	//	+-----------------------+-------------------------+-----------------+----------------+--------------+
	//	| 4-byte component size | 1 byte component Tag/ID | component header| component body | padding to 8 |
	//	+-----------------------+-------------------------+-----------------+----------------+--------------+
	public OperationMessage readBuf(ByteBuf in) {
		if (logger.isDebugEnabled()) {
			logger.debug("Index position: " + in.readerIndex());
		}
		int start = in.readerIndex();
		if (this.header == null) {
			this.header = new MessageHeader();
			header.readBuf(in);
		}
		long total = start + header.getMessageSize() - MessageHeader.size();
		int index = in.readerIndex();
		while (total - in.readerIndex() > 0) {
			long componentSize = in.readUnsignedInt();     // long
			byte tag = in.readByte();
			switch (OperationMessage.Type.values()[(short)(tag & 0xff) - 1]) {
			case Payload:
				this.payloadComponent = new PayloadOperationMessage(componentSize, tag);
				payloadComponent.readBuf(in);
				break;
			case Meta:
				metaComponent = new MetaOperationMessage(componentSize, tag);
				metaComponent.readBuf(in);
				break;
			default:
				throw new JunoException("Invalid type");
			}
			if (logger.isDebugEnabled()) {
				logger.debug("Reader Index: " + in.readerIndex() + "; header length: " + MessageHeader.size());
			}
			readBufPadding(index, in, 8);
			index = in.readerIndex();
		}
		return this;
	}
	
	public void writeBuf(ByteBuf out) {
		int size = 0;
		//Check if the meta component is not null
		if (metaComponent != null) {
			size += metaComponent.getBufferLength();;
		}
		//Check if the payload component is not null
		if (payloadComponent != null) {
			size += payloadComponent.getBufferLength();
		}
		
		int offset = size % 8;
		if (offset != 0) {
			size += (8 - offset);
		}
		size += MessageHeader.size();
		header.setMessageSize(size);
		// Header
		header.writeBuf(out);
		//Add meta Component
		int index = out.writerIndex();
		if(metaComponent != null){
			metaComponent.writeBuf(out);
			writeBufPadding(index, out, 8);
		}
		//Add Payload Component
		index = out.writerIndex();
		if (payloadComponent != null) {
			payloadComponent.writeBuf(out);
			writeBufPadding(index, out, 8);
		}
	}
	
	static public void readBufPadding(int start, ByteBuf in, int padding) {
		int endIndex = in.readerIndex();
		int offset = (endIndex - start) % padding;
		if (offset != 0) {
			ByteBuf buf = in.readBytes(padding - offset);
			buf.release();
		}
	}
	
	static public void writeBufPadding(int start, ByteBuf out, int padding) {
		int endIndex = out.writerIndex();
		int offset = (endIndex - start) % padding;
		if (offset != 0) {
			out.writeZero(padding - offset);
		}
	}
}
