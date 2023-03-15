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
package com.paypal.juno.net;

import com.paypal.juno.client.ServerOperationStatus;
import com.paypal.juno.io.protocol.MessageHeader.MessageOpcode;
import com.paypal.juno.io.protocol.MessageHeader.MessageRQ;
import com.paypal.juno.io.protocol.MessageHeader.MessageType;
import com.paypal.juno.io.protocol.MessageHeader;
import com.paypal.juno.io.protocol.MetaMessageSourceField;
import com.paypal.juno.io.protocol.MetaOperationMessage;
import com.paypal.juno.io.protocol.OperationMessage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.net.InetAddress;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * 
 * To send Nop to proxy servers and get server ip.
 * 
 */

class PingMessage extends OperationMessage {

	private static final Logger LOGGER = LoggerFactory.getLogger(PingMessage.class);
	private static byte[] localAddress = null;
	
	PingMessage(String appName, int opaque) {	
		MessageHeader header = new MessageHeader();
		
		header.setMsgType((byte)(MessageType.OperationalMessage.ordinal()));
		header.setFlags((byte) 0); // This field is not significant for client.
		header.setMessageRQ((byte)(MessageRQ.TwoWayRequest.ordinal())); 
		header.setOpcode((short)(MessageOpcode.Nop.ordinal()));
		header.setOpaque(opaque);
		header.setStatus((byte)(ServerOperationStatus.BadMsg.getCode()));
		setHeader(header);
		
		MetaOperationMessage mo = new MetaOperationMessage(0L,(byte)OperationMessage.Type.Meta.getValue());
		if (appName == null) {
			appName = new String("JunoInternal");
		}
		mo.addSourceField(getLocalAddress(), 0, appName.getBytes());
		
		setMetaComponent(mo);
	}
	
	ByteBuf pack() {
		ByteBuf out = Unpooled.buffer(getLength());
		writeBuf(out);
		return out;
	}
	
	static byte[] getLocalAddress() {
		if (localAddress != null) {
			return localAddress;
		}
		
		try {
			localAddress = InetAddress.getLocalHost().getAddress();
		} catch(Exception e){
			localAddress = InetAddress.getLoopbackAddress().getAddress();
		}
		return localAddress;
	}
	
	static boolean isPingResp(OperationMessage op, BaseProcessor processor) {
		
		if (processor.useLTM()) {
			return false;
		}
		
		MessageHeader header = op.getHeader();
		if (header == null || (header.getOpcode() != (short)(MessageOpcode.Nop.ordinal()))) {
			return false;  // not a Nop response
		}
	
		MetaOperationMessage mo = op.getMetaComponent();
		if (mo == null) {
			return false;
		}
		
		MetaMessageSourceField source = mo.getSourceField();		
		if (source == null || source.getAppName() == null) {
			return false;
		}
		
		String str = new String(source.getAppName());
		if (!str.equals("JunoInternal")) {
				return false;
		}
	
		// Extract ip from ping response.
		byte[] w = source.getIp4();		
		if (w == null || w.length < 4) {
			LOGGER.warn("Ping resp ip=null");
			processor.setPingIp("");
			return true;
		}
	
		if (w[0] == 127) {
			LOGGER.warn("Ping resp ip=127.*.*.*");
			processor.setPingIp("");		
			return true;
		}
		 
		if (Arrays.equals(PingMessage.getLocalAddress(), w)) {
			LOGGER.debug("Ping resp ip same as local addr");
			processor.setPingIp("");
			return true;
		}
	
		str = source.getIp4String(w);
		
		// Pass ip to sending thread
		LOGGER.debug("Ping resp ip="+str);
		processor.setPingIp(str);				
		return true;										
	}
}

