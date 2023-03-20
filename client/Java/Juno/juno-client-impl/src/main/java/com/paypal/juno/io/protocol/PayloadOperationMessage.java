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
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Payload (or KeyValue) Component is a type in which we will have the Namespace, key and value fileds.
 */
public class PayloadOperationMessage {
	private static final Logger logger = LoggerFactory.getLogger(PayloadOperationMessage.class);

	private long componentSize;
	private final byte tag;

	private long valueLength;
	private byte nameSpaceLength;
	private int keyLength;
	private byte[] namespace;
	private byte[] key;
	private byte[] value;
	private boolean isPloadCompressed;
	private CompressionType compType;

	public enum CompressionType {
		None("None"),
		Snappy("Snappy");
		// More algorithm to be added in future
		private final String cmpType;
		
		private static final Map<String, CompressionType> lookup = new HashMap<String, CompressionType>();

		static {
			for (CompressionType s : EnumSet
					.allOf(CompressionType.class))
				lookup.put(s.getCompressionType(), s);
		}
		
		CompressionType(String cmpText) {
			this.cmpType = cmpText;
		}

		public String getCompressionType() {
			return cmpType;
		}
		
		public static CompressionType getCompressionType(String ctype){
			return lookup.get(ctype);
		}
	};

	public long getValueLength() {
		return valueLength;
	}

	public void setValueLength(long valueLength) {
		this.valueLength = valueLength;
	}

	public byte getNameSpaceLength() {
		return nameSpaceLength;
	}

	public void setNameSpaceLength(byte nameSpaceLength) {
		this.nameSpaceLength = nameSpaceLength;
	}

	public int getKeyLength() {
		return keyLength;
	}

	public void setKeyLength(int keyLength) {
		this.keyLength = keyLength;
	}

	public byte[] getNamespace() {
		return namespace;
	}

	public void setNamespace(byte[] namespace) {
		this.namespace = namespace;
		this.nameSpaceLength = (byte) this.namespace.length;
		getBufferLength(); // Calculate the buffer length immediatly
	}

	public byte[] getKey() {
		return key;
	}

	public void setKey(byte[] key) {
		this.key = key;
		this.keyLength = this.key.length;
	}

	public byte[] getValue() {
		return value;
	}

	public void setValue(byte[] value) {
		this.value = value;
		this.valueLength = this.value == null ? 0 : this.value.length;
	}

	public CompressionType getCompressedType() {
		return this.compType;
	}

	public void setCompressionType(CompressionType compressionType) {
		this.compType = compressionType;
		if(compressionType == CompressionType.None){
			this.isPloadCompressed = false;
		}else{
			this.isPloadCompressed = true;
		}
	}
	
	public PayloadOperationMessage(long componentSize, byte tag) {
		this.componentSize = componentSize;
		this.tag = tag;
		this.isPloadCompressed = false;
		this.compType = CompressionType.None;
	}
	
	public int getBufferLength() {
		// componentSize(4) + tag(1) + this.nameSpaceLength(1) +
		// key(2) + value(4)
		long valueFieldLen = 0;
		// TO DO. Optimize this section when adding more compression types
		if(this.valueLength != 0){
			valueFieldLen = this.valueLength + 1; // 1 byte for payload type
			if(this.isPloadCompressed){ // compression enabled
				valueFieldLen += 1; // 1 byte for size of compression type
				valueFieldLen += this.compType.getCompressionType().length();
			}
		}
		
		int size = (int) (12 + this.namespace.length + this.key.length + valueFieldLen);
		int offset = size % 8;
		if (offset != 0) {
			size += 8 - offset;
		}
		this.componentSize = size;
		return size;
	}
	
	//	** Payload (or KeyValue) Component **
	//	 
	//	A 12-byte header followed by name, key and value
	//		Tag/ID: 0x01 
	//	* Header *
	//	 
	//	      |0|1|2|3|4|5|6|7|0|1|2|3|4|5|6|7|0|1|2|3|4|5|6|7|0|1|2|3|4|5|6|7|
	//	      |              0|              1|              2|              3|
	//	------+---------------+---------------+---------------+---------------+
	//	    0 | Size                                                          |
	//	------+---------------+---------------+-------------------------------+
	//	    4 | Tag/ID (0x01) | namespace len | key length                    |
	//	------+---------------+---------------+-------------------------------+
	//	    8 | payload length                                                |
	//	------+---------------------------------------------------------------+
	//	 
	//	 (
	//	  The max namespace length: 255
	//	  payload length = 0 if len(payload data) = 0, otherwise,
	//	  payload length = 1 + len(payload data) = len(payload field)
	//	 )
	//	 
	//	 
	//	* Body *
	//	+---------+-----+---------------+-------------------------+
	//	|namespace| key | payload field | Padding to align 8-byte |
	//	+---------+-----+---------------+-------------------------+
	//	 
	//	* Payload field*
	//	+---------------------+--------------+
	//	| 1 byte payload type | Payload data |
	//	+---------------------+--------------+
	//	 
	//	* Payload Type
	//	0: payload data is the actual value passed from client user
	//	1: payload data is encrypted by Juno client library, details not specified
	//	2: payload data is encrypted by Juno proxy with AES-GCM. encryption key length is 256 bits
	//	3: Payload data is compressed by Juno Client library.
	//	 
	//	* Payload data
	//	for payload type 2
	//	+--------------------------------+----------------+----------------+
	//	| 4 bytes encryption key version | 12 bytes nonce | encrypted data | 
	//	+--------------------------------+----------------+----------------+
	//
	//	for payload type 3
	//	+-------------------------------------+--------------------+------------+
	//	| 1 byte size of compression type | compression type | compressed data  |
	//	+-------------------------------------+--------------------+------------+
	//
	//	* compression types
	//  1) snappy-v1 (default algorithm)
	//	2) zlib-1.2.3

	public PayloadOperationMessage readBuf(ByteBuf in) {
		this.nameSpaceLength = in.readByte();		 // Name space length
		this.keyLength = in.readUnsignedShort();     // Key length
		long valueFieldLen = in.readUnsignedInt();   // Payload legth = 1(payload type) + len(payload data) = len(payload field) or Payload legth = 0 if no payload

		this.namespace = new byte[(short)(this.nameSpaceLength & 0xff)];
		
		//Start reading the body
		in.readBytes(this.namespace);
		this.key = new byte[this.keyLength];
		in.readBytes(this.key);
		if (valueFieldLen > 0) {
			this.valueLength = valueFieldLen - 1;			//read Payload type
			int payloadType = in.readByte();
			if(payloadType == 3){   // check for payload compression
				int compTypeSize = in.readByte(); // read size of compressipn type
				this.valueLength--;
				byte [] compType = new byte[compTypeSize];
				in.readBytes(compType); 	//read compression type
				this.valueLength -= compTypeSize; // This is actual compressed payload size 
				setCompressionType(CompressionType.getCompressionType(new String(compType)));
			}
			this.value = new byte[(int) this.valueLength];
			in.readBytes(this.value);
		} else {
			this.value = null;
			this.valueLength = 0;
		}
		if (logger.isDebugEnabled()) {
			logger.debug("Key: " + new String(key));
			logger.debug("namespace: " + new String(namespace));
		}
		return this;
	}

	// Here we construct the full component for the payload.
	public void writeBuf(ByteBuf out) {
		out.writeInt((int) this.componentSize);
		out.writeByte(this.tag);
		out.writeByte(this.nameSpaceLength);
		out.writeShort((short)this.keyLength);

		int payloadLen = 0;
		if(this.valueLength != 0){
			payloadLen = (int)this.valueLength+1;
			if(this.isPloadCompressed){ // compression enabled
					payloadLen += 1; // 1 byte for compression type size
					payloadLen += this.compType.getCompressionType().length();
			}
		}
		out.writeInt(payloadLen);
		out.writeBytes(this.namespace);
		out.writeBytes(this.key);
		if (logger.isDebugEnabled()) {
			logger.debug("Key: " + new String(key));
			logger.debug("namespace: " + new String(namespace));
		}
		if (payloadLen != 0) {
			if(isPloadCompressed){ // if compression is enabled
				out.writeByte(3);
				out.writeByte(compType.getCompressionType().length());
				out.writeBytes(this.compType.getCompressionType().getBytes());
			}else{
				out.writeZero(1);
			}
			out.writeBytes(this.value);
		}
	}
}
