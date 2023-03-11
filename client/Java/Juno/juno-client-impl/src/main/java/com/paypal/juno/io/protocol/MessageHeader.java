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

//
// Message header
// It comprises of the Protocol Header and Operational messager header


public class MessageHeader {
	private static final Logger logger = LoggerFactory.getLogger(PayloadOperationMessage.class);
	private static final int MESSAGE_HEADER_MAGIC = 0x5050;
	private static final short PROTOCOL_VERSION = 1;

	/**
	 * MESSAGE_HEADER_MAGIC
	 * 
	 * Unsigned16
	 */
	private int magic = 0;
	/**
	 * version of entire serialized message
	 * 
	 * Unsigned8
	 */
	private short version = 0;

	/**
	 * msgType of serialized message
	 * 
	 * Unsigned8
	 */
	private short msgType = 0;

	/**
	 * msgType of serialized message
	 * 
	 * Unsigned8
	 */
	private short messageRQ = 0;
	
	/**
	 * Size of the operationMsg
	 * 
	 * Unsigned32
	 */
	private int messageSize = 0;
	/**
	 * Opaque of the message
	 * 
	 * Unsigned32
	 */
	private int opaque = 0;
	/**
	 * Opcode of the message
	 * 
	 * opcode list:
	 *   0x00    Nop
	 *   0x01    Create
	 *   0x02    Get
	 *   0x03    Update
	 *   0x04    Set
	 *   0x05    Destroy
	 *   0x81    PrepareCreate
	 *   0x82    Read
	 *   0x83    PrepareUpdate
	 *   0x84    PrepareSet
	 *   0x85    Delete
	 *   0xC1    Commit
	 *   0xC2    Abort (Rollback)
	 *   0xC3    Repair
	 *   0xFE    MockSetParam
	 *   oxFF    MockReSet
	 * Unsigned8
	 */
	private short opcode;		
	/**
	 * flags of the message
	 * 
	 * Value - 1 if it is for replication
	 * Unsigned8
	 */
	private short flags;
	
	/**
	 * vbucket id
	 * 
	 * Unsigned16
	 */
	private int vbucket;

	/**
	 * Opaque of the message
	 * 
	 * Unsigned8
	 */
	private short status;
	
	public MessageHeader() {
		this.magic = MESSAGE_HEADER_MAGIC;
		this.version = PROTOCOL_VERSION;
		//bit 0-5
		//Message Type
		//0: Operational Message
		//1: Admin Message
		//2: Cluster Control Message
		this.msgType = (short)MessageType.OperationalMessage.ordinal();
		// bit 6-7	
		//RQ flag
		//0: response
		//1: two way request
		//3: one way request
		this.messageRQ = (short)MessageRQ.TwoWayRequest.ordinal();
		this.flags = 0;
	}

	public short getMagic() {
		return (short)magic;
	}

	public int getMessageSize() {
		return messageSize;
	}

	public void setMessageSize(int messageSize) {
		this.messageSize = messageSize;
	}

	/**
	 * @param version
	 *            the version to set
	 */
	public void setVersion(short version) {
		this.version = version;
	}

	/**
	 * @param msgType
	 *            the msgType to set
	 */
	public void setMsgType(short msgType) {
		this.msgType = msgType;
	}

	/**
	 * @param messageRQ
	 *            the messageRQ to set
	 */
	public void setMessageRQ(short messageRQ) {
		this.messageRQ = messageRQ;
	}

	/**
	 * @return the opaque
	 */
	public int getOpaque() {
		return opaque;
	}

	/**
	 * @param opaque
	 *            the opaque to set
	 */
	public void setOpaque(int opaque) {
		this.opaque = opaque;
	}
	
	/**
	 * @return the opcode
	 */
	public short getOpcode() {
		return opcode;
	}

	/**
	 * @param opcode the opcode to set
	 */
	public void setOpcode(short opcode) {
		this.opcode = opcode;
	}

	/**
	 * @param flags the flags to set
	 */
	public void setFlags(short flags) {
		this.flags = flags;
	}

	/**
	 * @return the status
	 */
	public short getStatus() {
		return status;
	}

	/**
	 * @param status the status to set
	 */
	public void setStatus(short status) {
		this.status = status;
	}

	/**
	 * @param magic the magic to set
	 */
	public void setMagic(short magic) {
		this.magic = magic;
	}

	public static int size() {
		return 16;
	}

	static public enum MessageOpcode {
		Nop(0x0),
		Create(0x1),
		Get(0x2),
		Update(0x3),
		Set(0x4),
		Destroy(0x5),
		PrepareCreate(0x81),
		Read(0x82),
		PrepareUpdate(0x83),
		PrepareSet(0x84),
		PrepareDelete(0x85),
		Delete(0x86),
		Commit(0xC1),
		Abort(0xC2),
		Repair(0xC3),
	    MarkDelete(0xC4),
	    Clone(0xE1),
		MockSetParam(0xFE),
		MockReSet(0xFF);
		
		MessageOpcode(int type) {
		}
	}
	
	public static enum MessageRQ {
		Response(0), 
		TwoWayRequest(1), 
		OneWayRequest(2);

		MessageRQ(int type) {
		}
	}

	public static enum MessageType {
		OperationalMessage(0), 
		AdminMessage(1), 
		CluisterControlMessage(2);

		MessageType(int type) {
		}
	}

	// Here we are creating the Protocol header and Opertional message header (request to proxy)
	// Protocol Header - 12 bytes
	//----------------
	//    	| 0| 1| 2| 3| 4| 5| 6| 7| 0| 1| 2| 3| 4| 5| 6| 7| 0| 1| 2| 3| 4| 5| 6| 7| 0| 1| 2| 3| 4| 5| 6| 7|
	// byte |                      0|                      1|                      2|                      3|
	//------+-----------------------+-----------------------+-----------------------+-----------------------+
	//  0 	| magic                                         | version               | message type flag     |
	//    	|                                               |                       +-----------------+-----+
	//    	|                                               |                       | type            | RQ  |
	//------+-----------------------------------------------+-----------------------+-----------------+-----+
	//  4 	| message size                                                                                  |
	//------+-----------------------------------------------------------------------------------------------+
	//  8 	| opaque                                                                                        |
	//------+-----------------------------------------------------------------------------------------------+
	//
	// Operational Message Header  - 4 bytes
	//---------------------------
	// operational request header
	//    	|0|1|2|3|4|5|6|7|0|1|2|3|4|5|6|7|0|1|2|3|4|5|6|7|0|1|2|3|4|5|6|7|
	// byte |              0|              1|              2|              3|
	//------+---------------+---------------+---------------+---------------+
	//  0  	| opcode        |flag           | shard Id or vbucket           |
	//     	|               +-+-------------+                               |
	//     	|               |R|             |                               |
	//------+---------------+-+-------------+-------------------------------+
	//
	// shared ID or vbucket does not have any significance for this client. Only proxy 
	// will set this field when sending request to SS.
	//
	public void writeBuf(ByteBuf out) {
		out.writeShort((short) getMagic());
		out.writeByte((byte) version);
		byte tmp = (byte) msgType;
		tmp |= (messageRQ << 6);
		out.writeByte(tmp);
		out.writeInt(messageSize);
		out.writeInt(opaque);
		out.writeByte((byte) opcode);
		if (logger.isDebugEnabled()) {
			logger.debug("Operation: " + opcode);
			//logger.debug("namespace: " + new String(namespace));
		}
		out.writeByte((byte) flags);
		if(messageRQ == MessageRQ.Response.ordinal()){
			out.writeShort(status);
		} else {
			out.writeShort((short) (vbucket & 0xFFFF));		
		}
	}
	
	// Here we are parsing in the Protocol header and Opertional message header(response from proxy)
	// Protocol Header 12 bytes
	//----------------
	//    	| 0| 1| 2| 3| 4| 5| 6| 7| 0| 1| 2| 3| 4| 5| 6| 7| 0| 1| 2| 3| 4| 5| 6| 7| 0| 1| 2| 3| 4| 5| 6| 7|
	// byte |                      0|                      1|                      2|                      3|
	//------+-----------------------+-----------------------+-----------------------+-----------------------+
	//  0 	| magic                                         | version               | message type flag     |
	//    	|                                               |                       +-----------------+-----+
	//    	|                                               |                       | type            | RQ  |
	//------+-----------------------------------------------+-----------------------+-----------------+-----+
	//  4 	| message size                                                                                  |
	//------+-----------------------------------------------------------------------------------------------+
	//  8 	| opaque                                                                                        |
	//------+-----------------------------------------------------------------------------------------------+
	//
	// Operational Message Header  4 bytes
	//---------------------------
	//
	// operational response header
	//      |0|1|2|3|4|5|6|7|0|1|2|3|4|5|6|7|0|1|2|3|4|5|6|7|0|1|2|3|4|5|6|7|
	// byte |              0|              1|              2|              3|
	//------+---------------+---------------+---------------+---------------+
	//  0  	| opcode        |flag           | reserved      | status        |
	//     	|               +-+-------------+               |               |
	//     	|               |R|             |               |               |
	//------+---------------+-+-------------+---------------+---------------+
	
	public MessageHeader readBuf(ByteBuf in) throws JunoException {
		
		this.magic = in.readShort();      // int
		//System.out.println("Magic is:"+this.magic);
		Assert.isTrue("Magic check ", MESSAGE_HEADER_MAGIC == this.magic);
		this.version = in.readUnsignedByte();     // short
		short tmp = in.readUnsignedByte();        // short
		this.msgType = (short)(tmp & 0x3f);
		this.messageRQ = (short)(tmp >> 6);
		this.messageSize = in.readInt();  // int
		//System.out.println("message size is:"+this.messageSize);
		this.opaque = in.readInt();       			// int
		this.opcode = in.readUnsignedByte();      // short
		this.flags = in.readUnsignedByte();       // short
		in.readByte();
		this.status = in.readUnsignedByte();      // short
		return this;
		}
}