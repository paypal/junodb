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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetaMessageSourceField extends MetaMessageTagAndType {
    private static final Logger logger = LoggerFactory.getLogger(MetaMessageSourceField.class);

	private byte componentSize;
	private byte appNameLength; // include one bit for ip type
	private boolean isIP6;
	private int port; // 2 bytes
	private byte [] ip4; // 4 bytes
	private byte [] ip6;
	private byte [] appName; // 4 bytes padding

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public byte[] getIp4() {
		return ip4;
	}

	public void setIp4(byte[] ip4) {
		this.ip4 = ip4;
	}

	public static String getIp4String(byte[] w) {
		if (w == null || w.length < 4) {
			return new String("");
		}
		return (int)(w[0] & 0xff) + "." + (int)(w[1] & 0xff) + "." + 
			 	(int)(w[2] & 0xff) + "." + (int)(w[3] & 0xff);
	}
	
	public byte[] getAppName() {
		return appName;
	}

	public int getBufferLength() {
		componentSize = (byte) (1 + 1 + 2 + (this.isIP6 ? 16 : 4) + ((appName == null) ? 0 : appName.length));
		int offset = componentSize % 4;
		if (offset != 0) {
			componentSize += 4 - offset;
		}
		return componentSize;
	}
	
	public void setAppName(byte[] appName) {
		this.appName = appName;
		appNameLength = (byte)appName.length;
	}

	public MetaMessageSourceField(byte tagAndSizeType) {
		super(tagAndSizeType);
	}

	public MetaMessageSourceField readBuf(ByteBuf in) {
		int index = in.readerIndex();
		componentSize = in.readByte();
		appNameLength = in.readByte();
		this.isIP6 = (appNameLength & 0x80) == 0x80;
		appNameLength = (byte) (appNameLength & 0x7F);
		port = in.readUnsignedShort();       // int
		if (this.isIP6) {
			ip6 = new byte[16];
			in.readBytes(ip6);
		}
		else {
			ip4 = new byte[4];
			in.readBytes(ip4);
		}
		appName = new byte[appNameLength];
		in.readBytes(appName);
		int tail = index + ((int)(componentSize & 0xff) - in.readerIndex());
		//Skip the padding if any
		if (tail > 0) {
			ByteBuf buf = in.readBytes(tail);
			buf.release();
		}
			
		return this;
	}

	public void writeBuf(ByteBuf out) {
		int indexStart = out.writerIndex();
		out.writeByte(componentSize);
		out.writeByte((byte)(appNameLength | (this.isIP6 ? 0x80 : 0)));
		out.writeShort((short)port);
		if (this.isIP6) {
			Assert.isTrue("IP6",ip6 != null);
			out.writeBytes(ip6);
		}
		else {
			out.writeBytes(ip4);
		}
		if (appName != null) {
			out.writeBytes(appName);
			if (logger.isDebugEnabled()) {
				logger.debug("Application Name: " + new String(appName));
			}
		}
		
		// Add padding if needed
		OperationMessage.writeBufPadding(indexStart, out, 4);
	}
}
