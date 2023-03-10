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
package com.paypal.juno.mock;

import com.paypal.juno.io.protocol.MessageHeader.MessageRQ;
import com.paypal.juno.io.protocol.MessageHeader;
import com.paypal.juno.io.protocol.MetaMessageFixedField;
import com.paypal.juno.io.protocol.MetaMessageTagAndType.FieldType;
import com.paypal.juno.io.protocol.MetaMessageTagAndType;
import com.paypal.juno.io.protocol.MetaOperationMessage;
import com.paypal.juno.io.protocol.OperationMessage;
import com.paypal.juno.io.protocol.PayloadOperationMessage.CompressionType;
import com.paypal.juno.io.protocol.PayloadOperationMessage;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Act like a Mayfly20 server
 * 
 * This server is implemented just well enough to pass our unit tests, and should not be used
 * to forsake integration testing.
 * 
 * @author dufox
 *
 */
public class MockJunoServer extends Thread {
	
	private static final short DATATAG_MAGIC = 0x5050;
	
	static private Map<String, Map<String, MockJunoRecord>> myMap = new HashMap<String, Map<String, MockJunoRecord>>();
	//final private AtomicLong fakeTimeStamp = new AtomicLong();
	
	//static ServerSocket variable
    private static ServerSocket server;
    //socket server port on which it will listen
    private static int port = 8090;
    private int socketTimeout;
    
	public MockJunoServer(int sockTimeout) {
		//create the socket server object
        try {
			server = new ServerSocket(port);
			socketTimeout = sockTimeout;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
		
	public void stopMockServer(){
		try {
			myMap.clear();
			server.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void run(){
		while(true){
            //creating socket and waiting for client connection
            try {
				Socket socket = server.accept();
				processRequest processor = new processRequest(socket,socketTimeout);
				processor.start();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public class processRequest extends Thread{
		
		Socket socket;
		int socketSoTimeout;
		public processRequest(Socket socket,int sockTimeout){
			this.socket = socket;
			socketSoTimeout = sockTimeout;
		}
		@Override
	    public void run() {
				try{
				while(socket.isConnected()){
					ByteBuf header = Unpooled.buffer(16);
					ByteBuffer buff = ByteBuffer.allocate(16);
					if(socketSoTimeout > 0)
						socket.setSoTimeout(socketSoTimeout);
					// Read Header first
					int bytesRead = socket.getInputStream().read(buff.array());
					if(bytesRead != 16){
						throw new Exception("Header not found");
					}
					header.writeBytes(buff);
					//Read the Body
					MessageHeader msgHdr = new MessageHeader();
			        msgHdr.readBuf(header);
			        
			        ByteBuf body = Unpooled.buffer(msgHdr.getMessageSize() - 16);
			        buff = ByteBuffer.allocate(msgHdr.getMessageSize() - 16);
			        bytesRead = socket.getInputStream().read(buff.array());
					if(bytesRead != (msgHdr.getMessageSize() - 16)){
						throw new Exception("body not found");
					}
					body.writeBytes(buff);
					ByteBuffer resp = readAndProcessMessage(msgHdr,body);
			        
					socket.getOutputStream().write(resp.array());
				}
			}
			catch(Exception e){
				e.printStackTrace();
			}
		}
		/**
		 * Handle the client request and return a response
		 * 
		 * @param bais the input stream containing the serialized the message from the client
		 * @return An input stream containing the serialized response for the client.
		 * @throws Exception
		 */
		public ByteBuffer readAndProcessMessage(MessageHeader msgHdr,ByteBuf in) throws Exception {
			String appName = null;
	        OperationMessage opMsg = new OperationMessage();
	        opMsg.setHeader(msgHdr);
	        opMsg.readBuf(in);
	   	 	/* opcode list:
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
	   		 */
			switch (msgHdr.getOpcode()) {
			case 0: // - No-op
				break;
			case 1: // - Create
				return handleCreate(opMsg);
			case 2: // - Get
				return handleGet(opMsg);
			case 3: // - Update
				return handleUpdate(opMsg);
			case 4: // - Set
				return handleSet(opMsg);
			case 5: // - Destroy
				return handleDestory(opMsg);
			default:
				break;
			}
			
			return null;
		}
		
		/**
		 * Check to see if the key already exists.
		 * 
		 * @param namespace outer key
		 * @param key inner key
		 * @return true if we have both keys in our map, false otherwise
		 */
		private boolean keyExists(String namespace, String key) {
			if (myMap.containsKey(namespace)) {
				Map<String, MockJunoRecord> innerMap = myMap.get(namespace);
				if (innerMap.containsKey(key)) {
					return true;
				}
			}
			return false;
		}
		
		/**
		 * Return our mock record based on the namespace and key.
		 * 
		 * @param namespace outer key
		 * @param key inner key
		 * @return the mock record if we have it, otherwise null.
		 */
		private MockJunoRecord getRecord(String namespace, String key) {
			MockJunoRecord mmr = null;
			if (keyExists(namespace, key)) {
				Map<String, MockJunoRecord> innerMap = myMap.get(namespace);
				mmr = innerMap.get(key);
			}
			return mmr;
		}
		
		/**
		 * Store the record based on namespace and key.
		 * 
		 * @param namespace outer key
		 * @param key inner key
		 * @param mmr the record to store
		 */
		private void addRecord(String namespace, String key, MockJunoRecord mmr) {
			if (!myMap.containsKey(namespace)) {
				myMap.put(namespace, new HashMap<String, MockJunoRecord>());
			}
			
			myMap.get(namespace).put(key, mmr);
		}
		
		/**
		 * Remove the record with the given namespace and key. Removing the same
		 * namespace and key twice is safe to do, but the second one has no effect.  
		 * @param namespace outer key
		 * @param key inner key
		 */
		private void removeRecord(String namespace, String key) {
			if (keyExists(namespace, key)) {
				Map<String, MockJunoRecord> innerMap = myMap.get(namespace);
				innerMap.remove(key);
			}
		}
		
		/**
		 * Serialize a response to send back to the client
		 * 
		 * @param mmheader the header for the message
		 * @param appName the appname of the client
		 * @param mom the operation message
		 * @return The input stream containing the serialized response.
		 * @throws java.io.IOException
		 */
		private ByteBuffer serializeResponse(OperationMessage opMsg) throws IOException {
			ByteBuf buff = Unpooled.buffer(opMsg.getHeader().getMessageSize());
			opMsg.writeBuf(buff);
			ByteBuffer bbuff = buff.nioBuffer();
			return bbuff;
		}
	
		/**
		 * Given a mock mayfly record build an operation message for the response
		 * @param mmr
		 * @param status
		 * @return
		 */
		private OperationMessage buildMockOperationMessage(MockJunoRecord mmr, int status) {
			OperationMessage opMsg = new OperationMessage();
			MetaOperationMessage metaComponents = new MetaOperationMessage(0L, (byte) 0x2);
			opMsg.setMetaComponent(metaComponents);
			List<MetaMessageTagAndType> list = opMsg.getMetaComponent().getFieldList();
			//Check for version and add
			int version = (int) mmr.version.get();
			if (version != 0) {
				MetaMessageFixedField field = new MetaMessageFixedField((byte) ((FieldType.Version.ordinal()) | (1 << 5)));
				field.setContent(version);
				list.add(field);
			}
			
			// Check for Creation time and add
			long createTime = mmr.createTime;
			if (createTime != 0) {
				MetaMessageFixedField field = new MetaMessageFixedField((byte) ((FieldType.CreationTime.ordinal()) | (1 << 5)));
				field.setContent(createTime);
				list.add(field);
			}
			
			MetaMessageFixedField field = new MetaMessageFixedField((byte) ((FieldType.RequestHandlingTime.ordinal()) | (1 << 5)));
			field.setContent(126);
			list.add(field);
			
			field = new MetaMessageFixedField((byte) ((FieldType.RequestID.ordinal()) | (3 << 5)));
			field.setVariableContent(mmr.reqId.array());
			list.add(field);
			
			MessageHeader msgHdr = new MessageHeader();
			msgHdr.setStatus((short)status);
			msgHdr.setVersion((short)1);
			msgHdr.setMagic(DATATAG_MAGIC);
			msgHdr.setMessageRQ((short)MessageRQ.Response.ordinal());
			msgHdr.setOpaque(mmr.opaque);
			
			PayloadOperationMessage plOpMsg = new PayloadOperationMessage(0L,(byte)0x1);
			plOpMsg.setKey(mmr.key.getBytes());
			plOpMsg.setValue(mmr.opaque_data);
			plOpMsg.setNamespace(mmr.namespace.getBytes());
			plOpMsg.setKeyLength(mmr.key.length());
			plOpMsg.setCompressionType(CompressionType.getCompressionType(mmr.compType));
			if(mmr.opaque_data != null)
				plOpMsg.setValueLength(mmr.opaque_data.length);
			
			opMsg.setPayloadComponent(plOpMsg);
			msgHdr.setMessageSize(plOpMsg.getBufferLength()+opMsg.getMetaComponent().getBufferLength()+16);
			opMsg.setHeader(msgHdr);
			return opMsg;
		}
	
		/**
		 * Take the message from the client and handle creating a record
		 *
		 * @param opMsg
		 * @return
		 * @throws java.io.IOException
		 */
		private ByteBuffer handleCreate(OperationMessage msg) throws IOException {
			OperationMessage op = null;
			if (!keyExists(new String(msg.getPayloadComponent().getNamespace()), new String(msg.getPayloadComponent().getKey()))) {
				MockJunoRecord mmr = new MockJunoRecord();
				mmr.namespace = new String(msg.getPayloadComponent().getNamespace());
				mmr.key = new String(msg.getPayloadComponent().getKey());
	
				if( msg.getPayloadComponent().getValue() != null ) {
					mmr.compType = msg.getPayloadComponent().getCompressedType().toString();
					mmr.opaque_data = msg.getPayloadComponent().getValue().clone();
				}
				mmr.version.incrementAndGet();
				//mmr.createTime = fakeTimeStamp.incrementAndGet();
				mmr.createTime = msg.getMetaComponent().getCreationTime();
				mmr.lifetime = msg.getMetaComponent().getTtl();
				mmr.opaque = msg.getHeader().getOpaque();
				if(msg.getMetaComponent().getRequestId() != null){
					mmr.reqId.put(msg.getMetaComponent().getRequestId());
				}else{
					//System.out.println("req ID is null.....");
				}
				
				op = buildMockOperationMessage(mmr, 0);
				addRecord(mmr.namespace, mmr.key, mmr);
	
			} else {
				op = new OperationMessage();
				MessageHeader msgHdr = new MessageHeader();
				msgHdr.setStatus((short)4); // Set Duplicate
				msgHdr.setMagic(DATATAG_MAGIC);
				//System.out.println("Message header size :"+MessageHeader.size());
				msgHdr.setMessageRQ((short)MessageRQ.Response.ordinal());
				msgHdr.setOpaque(msg.getHeader().getOpaque());
				op.setHeader(msgHdr);
				
				//Set the req_id in response
				MetaOperationMessage metaComponents = new MetaOperationMessage(0L,(byte)OperationMessage.Type.Meta.getValue());
				op.setMetaComponent(metaComponents);
				List<MetaMessageTagAndType> list = op.getMetaComponent().getFieldList();
				MetaMessageFixedField field = new MetaMessageFixedField((byte) ((FieldType.RequestID.ordinal()) | (3 << 5)));
				field.setVariableContent(msg.getMetaComponent().getRequestId());
				list.add(field);
				
				msgHdr.setMessageSize(MessageHeader.size()+metaComponents.getBufferLength());
			}
			return serializeResponse(op);
		}
	
		/**
		 * Given a message from a client handle getting a mayfly record.
		 *
		 * @param opMsg
		 * @return
		 * @throws java.io.IOException
		 */
		private ByteBuffer handleGet(OperationMessage opMsg) throws IOException {
			OperationMessage op = null;
	
			MockJunoRecord mmr = getRecord(new String(opMsg.getPayloadComponent().getNamespace()),new String(opMsg.getPayloadComponent().getKey()));
			if (mmr != null) {
				mmr.opaque = opMsg.getHeader().getOpaque();
				mmr.reqId.clear();
				mmr.reqId.put(opMsg.getMetaComponent().getRequestId());
				//System.out.println("Req ID in mock:"+Hex.encodeHexString(opMsg.getMetaComponent().getRequestId()));
				op = buildMockOperationMessage(mmr, 0);
			} else {
				mmr = new MockJunoRecord();
				mmr.key = new String(opMsg.getPayloadComponent().getKey());
				mmr.namespace = new String(opMsg.getPayloadComponent().getNamespace());
				mmr.createTime = opMsg.getMetaComponent().getCreationTime();
				mmr.lifetime = opMsg.getMetaComponent().getTtl();
				mmr.opaque = opMsg.getHeader().getOpaque(); 
				mmr.opaque_data = new byte[0];
				if(opMsg.getMetaComponent().getRequestId() != null){
					mmr.reqId.put(opMsg.getMetaComponent().getRequestId());
				}else{
					//System.out.println("req ID is null.....");
				}
				op = buildMockOperationMessage(mmr, 3);
			}
	
			return serializeResponse(op);
		}
	
		/**
		 * Destroy a mayfly record
		 *
		 * @param opMsg
		 * @return
		 * @throws java.io.IOException
		 */
		private ByteBuffer handleDestory(OperationMessage opMsg) throws IOException {
			MockJunoRecord mmr = getRecord(new String(opMsg.getPayloadComponent().getNamespace()),new String(opMsg.getPayloadComponent().getKey()));
			removeRecord(new String(opMsg.getPayloadComponent().getNamespace()),new String(opMsg.getPayloadComponent().getKey()));
			OperationMessage op = null;
			if (mmr != null) {
				mmr.opaque = opMsg.getHeader().getOpaque();
				mmr.reqId.clear();
				mmr.reqId.put(opMsg.getMetaComponent().getRequestId());
				op = buildMockOperationMessage(mmr, 0);
			} else {
				op = new OperationMessage();
				MessageHeader msgHdr = new MessageHeader();
				msgHdr.setStatus((short)0); //Set Success
				msgHdr.setMagic(DATATAG_MAGIC);
				msgHdr.setMessageRQ((short)MessageRQ.Response.ordinal());
				msgHdr.setOpaque(opMsg.getHeader().getOpaque());
				op.setHeader(msgHdr);
				
				//Set the req_id in response
				MetaOperationMessage metaComponents = new MetaOperationMessage(0L,(byte)OperationMessage.Type.Meta.getValue());
				op.setMetaComponent(metaComponents);
				List<MetaMessageTagAndType> list = op.getMetaComponent().getFieldList();
				MetaMessageFixedField field = new MetaMessageFixedField((byte) ((FieldType.RequestID.ordinal()) | (3 << 5)));
				field.setVariableContent(opMsg.getMetaComponent().getRequestId());
				list.add(field);
				
				msgHdr.setMessageSize(MessageHeader.size()+metaComponents.getBufferLength());
			}
			return serializeResponse(op);
		}
	
		/**
		 * Update a mayfly record.
		 *
		 * @param opMsg
		 * @return
		 * @throws java.io.IOException
		 */
		private ByteBuffer handleUpdate(OperationMessage opMsg) throws IOException {
			MockJunoRecord mmr = getRecord(new String(opMsg.getPayloadComponent().getNamespace()),new String(opMsg.getPayloadComponent().getKey()));
			OperationMessage op = null;
			if (mmr != null) {
				mmr.opaque = opMsg.getHeader().getOpaque();
				mmr.reqId.clear();
				mmr.reqId.put(opMsg.getMetaComponent().getRequestId());
				if (mmr.version.get() == opMsg.getMetaComponent().getVersion() || opMsg.getMetaComponent().getVersion() == 0) {
					mmr.version.incrementAndGet();
					mmr.opaque_data = opMsg.getPayloadComponent().getValue().clone();
					op = buildMockOperationMessage(mmr, 0);
				} else {
					op = buildMockOperationMessage(mmr, 19); // version too old
				}
			}else{
				op = new OperationMessage();
				MessageHeader msgHdr = new MessageHeader();
				msgHdr.setStatus((short)3); //Set Success
				msgHdr.setMagic(DATATAG_MAGIC);
				msgHdr.setMessageRQ((short)MessageRQ.Response.ordinal());
				msgHdr.setOpaque(opMsg.getHeader().getOpaque());
				op.setHeader(msgHdr);
				
				//Set the req_id in response
				MetaOperationMessage metaComponents = new MetaOperationMessage(0L,(byte)OperationMessage.Type.Meta.getValue());
				op.setMetaComponent(metaComponents);
				List<MetaMessageTagAndType> list = op.getMetaComponent().getFieldList();
				MetaMessageFixedField field = new MetaMessageFixedField((byte) ((FieldType.RequestID.ordinal()) | (3 << 5)));
				field.setVariableContent(opMsg.getMetaComponent().getRequestId());
				list.add(field);
				
				msgHdr.setMessageSize(MessageHeader.size()+metaComponents.getBufferLength());
			}
			return serializeResponse(op);
		}
		
		/**
		 * Set a mayfly record.
		 *
		 * @param opMsg
		 * @return
		 * @throws java.io.IOException
		 */
		private ByteBuffer handleSet(OperationMessage opMsg) throws IOException {
			MockJunoRecord mmr = getRecord(new String(opMsg.getPayloadComponent().getNamespace()),new String(opMsg.getPayloadComponent().getKey()));
			OperationMessage op = null;
			if (mmr != null) {
				mmr.opaque = opMsg.getHeader().getOpaque();
				mmr.reqId.clear();
				mmr.reqId.put(opMsg.getMetaComponent().getRequestId());
				mmr.version.incrementAndGet();
				mmr.opaque_data = opMsg.getPayloadComponent().getValue().clone();
				op = buildMockOperationMessage(mmr, 0);
				return serializeResponse(op);
			}else{
				return handleCreate(opMsg);
			}
		}
	}
}