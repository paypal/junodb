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

import com.paypal.juno.io.protocol.OperationMessage;
import com.paypal.juno.util.JunoMetrics;
import com.paypal.juno.util.JunoStatusCode;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * To process response from proxy servers.  This object is linked to channel pipeline.
 * 
 */

public class ClientHandler extends ChannelInboundHandlerAdapter {

	private static final Logger LOGGER = LoggerFactory.getLogger(ClientHandler.class);
	private IOProcessor processor;

//	@Autowired
//	InstanceLocation instanceLocation;
//	private final String INSTANCE_GEO_PP_US = "PP_US";
		
	ClientHandler(IOProcessor p) {
		processor = p;
	}
		
	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

		processor.incrementRecvCount();
		if (!(msg instanceof OperationMessage)) {
			LOGGER.error("Invalid OperationMessage from downstream.");
			ReferenceCountUtil.release(msg);
			return;
		}
		
		if (!PingMessage.isPingResp((OperationMessage)msg, processor)) {
			processor.putResponse((OperationMessage)msg);
		}
		
		ReferenceCountUtil.release(msg);
		if (processor.onEvent(TestEvent.READ_FAIL)) {
			exceptionCaught(ctx, new RuntimeException("Test Read Fail"));
		}
	}
	
	@Override
	public void channelInactive(ChannelHandlerContext ctx) {
		processor.validateMsgCount();
	}
	    
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
		
		if (cause instanceof InterruptedException) {
			ctx.close();
			return;
		}
		final Map<String,CharSequence> trans = new HashMap<String, CharSequence>();
		LOGGER.error(cause.toString() + " " + cause.getMessage());
		trans.put("name", "JUNO_RECEIVE");
		trans.put("server", processor.getServerAddr());
		trans.put("error", cause.toString());
		trans.put("status", JunoStatusCode.ERROR.toString());
		LOGGER.error("ClientHandler Error : {}", trans);
		ctx.close();
		JunoMetrics.recordErrorCount("JUNO_RECEIVE",processor.getRemoteIpAddr(),cause.getClass().getName());
	}
}
