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
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;


/**
** MetaData Component **
A variable length header followed by a set of meta data fields
        Tag/ID: 0x02
* Header *
    | 0| 1| 2| 3| 4| 5| 6| 7|
  0 | size                  | 4 bytes
----+-----------------------+---------
  4 | Tag/ID (0x02)         | 1 byte
----+-----------------------+---------
  5 | Number of fields      | 1 byte
----+--------------+--------+---------
  6 | Field tag    |SizeType| 1 byte
----+--------------+--------+---------
    | ...                   |
----+-----------------------+---------
    | padding to 4          |
----+-----------------------+---------
(Don't think we need a header size. )
SizeType:
  0             variable length field, for that case,
                the first 1 byte of the field MUST be
                the size of the field(padding to 4 byte).
                The max is 255.
  n             Fixed length: 2 ^ (n+1)  bytes
* Body *
----+-----------------------+---------
    | Field data            | defined by Field tag
----+-----------------------+---------
    | ...                   |
----+-----------------------+---------
    | padding to 8          |
----+-----------------------+---------
* Predefined Field Types *
TimeToLive Field
        Tag             : 0x01
        SizeType: 0x01
Version Field
        Tag             : 0x02
        SizeType: 0x01
Creation Time Field
        Tag             : 0x03
        SizeType: 0x01
Expiration Time Field
        Tag             : 0x04
        SizeType: 0x01
RequestID/UUID Field
        Tag             : 0x05
        SizeType: 0x03
Source Info Field
        Tag             : 0x06
        SizeType: 0
        | 0| 1| 2| 3| 4| 5| 6| 7| 0| 1| 2| 3| 4| 5| 6| 7| 0| 1| 2| 3| 4| 5| 6| 7| 0| 1| 2| 3| 4| 5| 6| 7|
        |                      0|                      1|                      2|                      3|
        +-----------+-----------+--------------------+--+-----------------------+-----------------------+
        | size (include padding)| app name length    | T| Port                                          |
        +-----------------------+--------------------+--+-----------------------------------------------+
        | IPv4 address if T is 0 or IPv6 address if T is 1                                              |
        +-----------------------------------------------------------------------------------------------+
        | application name, padding to 4-bytes aligned                                                  |
        +-----------------------------------------------------------------------------------------------+
*/
public class MetaOperationMessage {
	public long getComponentSize() {
		return componentSize;
	}

	// Not for serialize/deserialize
	long version;
	long ttl;
	long creationTime;
	long expirationTime;
	long requestHandlingTime;  // millisecond duration on JunoServ
	byte[] requestId;
	UUID requestUuid;
	long componentSize;
	final byte tag;
	
	// serialize/deserialize
	final private List<MetaMessageTagAndType> fieldList = new ArrayList<>();
	
	public List<MetaMessageTagAndType> getFieldList() {
		return fieldList;
	}

	public MetaOperationMessage(long componentSize, byte tag) {
		this.componentSize = componentSize;
		this.tag = tag;
	}
	
	public long getVersion() {
		return version;
	}

	public long getTtl() {
		return ttl;
	}

	public long getCreationTime() {
		return creationTime;
	}

	public long getExpirationTime() {
		return expirationTime;
	}
	
	public long getRequestHandlingTime() {
		return requestHandlingTime;
	}

	public byte[] getRequestId() {
		return requestId;
	}

	public UUID getRequestUuid(){
		return requestUuid;
	}
	
	public String getRequestIdString() {
		if (requestUuid == null) {
			return new String("not_set");
		}
		return requestUuid.toString();
	}
	
	public void setRequestId(byte [] reqId) {
		requestId = reqId;
		if(requestUuid == null){
			ByteBuffer buf = ByteBuffer.wrap(requestId);
			UUID uuid = new UUID(buf.getLong(0), buf.getLong(8));
			requestUuid = uuid;
		}
	}
	
	public void setRequestUuid(UUID id) {
		requestUuid = id;
		if(requestId == null){
			ByteBuffer buf = ByteBuffer.wrap(new byte[16]);
			buf.putLong(id.getMostSignificantBits());
			buf.putLong(id.getLeastSignificantBits());
			requestId = buf.array();
		}
	}
	
	public String getCorrelationIDString() {
		byte[] id = null;
		for (int i = 0; i < fieldList.size(); i++) {
			
			MetaMessageTagAndType item = fieldList.get(i);
			if (item instanceof MetaMessageCorrelationIDField) {
				id = ((MetaMessageCorrelationIDField)item).getCorrelationId();
				break;
			}
		}
		
		if (id == null) {
			return new String("not_set");
		}
		
		return new String(id);
	}
	
	public void addSourceField(byte[] ip4, int port, byte[] appName) {
		MetaMessageSourceField source = new MetaMessageSourceField((byte) (MetaMessageTagAndType.FieldType.SourceInfo.ordinal()));
		source.setIp4(ip4);
		source.setPort(port);
		source.setAppName(appName);
		
		fieldList.add(source);
	}
	
	public MetaMessageSourceField getSourceField() {
		for (int i = 0; i < fieldList.size(); i++) {
			
			MetaMessageTagAndType item = fieldList.get(i);
			if (item instanceof MetaMessageSourceField) {
				return (MetaMessageSourceField)item;
			}
		}
		
		return null;
	}
	
	public int getBufferLength() {
		// Header
		// componentSize(4) + tag(1) + Number of fields(1) + number Of fields bytes + padding 4
		int size = 6 + this.fieldList.size();
		int offset = size % 4;
		if (offset != 0) {
			size += 4 - offset;
		}
		
		// Data
		for (int i = 0; i < this.fieldList.size(); i ++) {
			MetaMessageTagAndType tagAndSizeType = this.fieldList.get(i);
			if (!tagAndSizeType.isVariable()) {
				size += tagAndSizeType.getFieldSize();
			}
			else {
				size += (tagAndSizeType).getBufferLength();
			}
		}
		offset = size % 8;
		if (offset != 0) {
			size += 8 - offset;
		}
		this.componentSize = size;
		return size;
	}
	
	public MetaOperationMessage readBuf(ByteBuf in) {
		int indexStart = in.readerIndex();
		short fields = in.readUnsignedByte();
		byte [] tagAndSizeTypes = new byte[fields];
		in.readBytes(tagAndSizeTypes);
		// Header is done
		// escape padding here, padding to 4 bytes.
		OperationMessage.readBufPadding(indexStart - 4 - 1 , in, 4);

		for (int i = 0; i < fields; i ++) {			
			MetaMessageTagAndType tagAndSizeType = new MetaMessageTagAndType(tagAndSizeTypes[i]);
			MetaMessageTagAndType.FieldType type = tagAndSizeType.getFieldType();
			switch(type) {
			case CreationTime:
			case ExpirationTime:
			case RequestHandlingTime:
			case RequestID:
			case TimeToLive:
			case Version: {
					MetaMessageFixedField field = new MetaMessageFixedField(tagAndSizeTypes[i]);
					field.readBuf(in);
					fieldList.add(field);
					if(type.equals(MetaMessageTagAndType.FieldType.CreationTime)){
						creationTime=field.getContent();
					}else if(type.equals(MetaMessageTagAndType.FieldType.ExpirationTime)){
						expirationTime=field.getContent();
					}else if(type.equals(MetaMessageTagAndType.FieldType.RequestHandlingTime)) {
						requestHandlingTime=field.getContent();
					}else if(type.equals(MetaMessageTagAndType.FieldType.RequestID)){
						setRequestId(field.getVariableContent());
					}else if(type.equals(MetaMessageTagAndType.FieldType.TimeToLive)){
						ttl=field.getContent();
					}else if(type.equals(MetaMessageTagAndType.FieldType.Version)){
						version=field.getContent();
					}
					break;
				}
			case SourceInfo: {
					MetaMessageSourceField field = new MetaMessageSourceField(tagAndSizeTypes[i]);
					field.readBuf(in);
					fieldList.add(field);
					break;				
				}
//			case CorrelationID: {
//					MetaMessageCorrelationIDField field = new MetaMessageCorrelationIDField(tagAndSizeTypes[i]);
//					field.readBuf(in);
//					fieldList.add(field);
//					break;				
//				}
			default:
				//Here we just need to skip the bytes for unknown Field type
				if(tagAndSizeType.isVariable()){
					short size = in.readUnsignedByte(); // The size of the variable length tag is found at the first byte of the Tag body
					ByteBuf buf = in.readBytes(size - 1); //Since the size if inclusive of itself so (size - 1).
					buf.release();
				}else{
					int len = tagAndSizeType.getFieldSize();
					ByteBuf buf = in.readBytes(len);
					buf.release();
				}
				break;
			}
		}
		return this;
	}
	
	public void writeBuf(ByteBuf out) {
		//Populate the Header
		int indexStart = out.writerIndex();
		out.writeInt((int) this.componentSize);
		out.writeByte(this.tag);
		out.writeByte((byte)this.fieldList.size());
		//Write the field Tag and Size Type fields
		for (int i = 0; i < this.fieldList.size(); i ++) {
			byte value = this.fieldList.get(i).getValue();
			out.writeByte(value);
		}
		
		// Add padding if necessary
		OperationMessage.writeBufPadding(indexStart, out, 4);
		
		// Write the actual Meta data (Body)
		for (int i = 0; i < this.fieldList.size(); i ++) {
			MetaMessageTagAndType field = this.fieldList.get(i);
			field.writeBuf(out);
		}
	}
}
