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

import com.paypal.juno.io.protocol.MessageHeader;
import com.paypal.juno.io.protocol.OperationMessage;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import java.net.InetSocketAddress;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * To convert message buffer to OperationMessage.
 * 
 */

public class MessageDecoder extends ByteToMessageDecoder {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(MessageDecoder.class);
	
	private boolean parseHeader = true;
	private MessageHeader header = null;
	private int bodySize = 0;
	
	@Override
	protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
		
		if (parseHeader) {
			int len = 16;
			if (in.readableBytes() < len) {
				return;  
			}
			header = new MessageHeader();
			header.readBuf(in);	
			bodySize = header.getMessageSize() - len;
			parseHeader = false;
		}

		if (in.readableBytes() < bodySize) {
			return;
		}
			
		OperationMessage opMsg = new OperationMessage();
		opMsg.setHeader(header);
		header = null;  // release reference
			
		opMsg.readBuf(in);
		String serverIp = ((InetSocketAddress)ctx.channel().remoteAddress()).getAddress().getHostAddress();
		opMsg.setServerIp(serverIp);
		out.add(opMsg);
		
		parseHeader = true;
		bodySize = 0;
	}
}
