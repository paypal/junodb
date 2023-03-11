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
package com.paypal.juno.protocol;

import com.paypal.juno.io.protocol.MessageHeader.MessageOpcode;
import com.paypal.juno.io.protocol.MessageHeader.MessageRQ;
import com.paypal.juno.io.protocol.MessageHeader;
import com.paypal.juno.io.protocol.MetaMessageFixedField;
import com.paypal.juno.io.protocol.MetaMessageTagAndType.FieldType;
import com.paypal.juno.io.protocol.MetaMessageTagAndType;
import com.paypal.juno.io.protocol.MetaOperationMessage;
import com.paypal.juno.io.protocol.OperationMessage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class MetaComponentTest {

	@Before
	public void initialize() throws Exception {
	}
	
	@After
	public void tearDown() {
	}
	
	@Test
	public void requestHandlingTimeTest() {
		OperationMessage opMsg = new OperationMessage();
		MetaOperationMessage mo = new MetaOperationMessage(0L, (byte) 0x2);
		opMsg.setMetaComponent(mo);
		
		List<MetaMessageTagAndType> list = opMsg.getMetaComponent().getFieldList();
		MetaMessageFixedField field = new MetaMessageFixedField((byte) ((FieldType.RequestHandlingTime.ordinal()) | (1 << 5)));
		long rht = 321;
		field.setContent(rht);
		list.add(field);
		
		field = new MetaMessageFixedField((byte) ((FieldType.Version.ordinal()) | (1 << 5)));
		long version = 8;
		field.setContent(version);
		list.add(field);
		
		field = new MetaMessageFixedField((byte) ((FieldType.CreationTime.ordinal()) | (1 << 5)));
		field.setContent(101);
		list.add(field);
		
		MessageHeader header = new MessageHeader();
		header.setOpcode((short)MessageOpcode.Set.ordinal());
		header.setVersion((short)1);
		header.setMessageRQ((short)MessageRQ.Response.ordinal()); 
		
		int len = opMsg.getMetaComponent().getBufferLength()+16;
		header.setMessageSize(len);
		opMsg.setHeader(header);

		ByteBuf msg = Unpooled.buffer(len);
		opMsg.writeBuf(msg);
		
		OperationMessage resp = new OperationMessage();
		resp.readBuf(msg);
		
		assertEquals(resp.getMetaComponent().getRequestHandlingTime(), rht);
		assertEquals(resp.getMetaComponent().getVersion(), version);
	}
}
