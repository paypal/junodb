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
package com.paypal.juno.transport.socket;

import com.paypal.juno.client.JunoClientConfigHolder;
import com.paypal.juno.transport.TransportConfigHolder;
import java.net.InetSocketAddress;
import javax.net.ssl.SSLContext;

public class SocketConfigHolder implements TransportConfigHolder{

	InetSocketAddress inetAddress;
	String host;
	int port;
	boolean useSSL;
	int connectTimeout;
	int connectionLifeTime;
	int connectionPoolSize;
	int responseTimeout;
	boolean bypassLTM;
	String ns;
	boolean reconnectOnFail;
	SSLContext ctx;

	public SocketConfigHolder(JunoClientConfigHolder config){
		inetAddress = config.getServer();
		connectTimeout = config.getConnectionTimeoutMsecs();
		connectionLifeTime = config.getConnectionLifeTime();
		useSSL = config.getUseSSL();
		port = config.getPort();
		host = config.getHost();
		connectionPoolSize = config.getConnectionPoolSize();
		responseTimeout = config.getResponseTimeout();
		bypassLTM = config.getByPassLTM();
		ns = config.getRecordNamespace();
		reconnectOnFail = config.getReconnectOnFail();
	}
	
	public SocketConfigHolder() {
	}
	
	public int getPort() {
		return port;
	}
	
	public String getHost() {
		return host;
	}

	public void setPort(int port) {
		this.port = port;
	}
	
	public int getConnectTimeout() {
		return connectTimeout;
	}

	public InetSocketAddress getInetAddress() {
		return inetAddress;
	}
	
	public int getConnectionLifeTime() {
		return connectionLifeTime;
	}

	public boolean useSSL() {
		return useSSL;
	}
	
	public int getConnectionPoolSize() {
		return connectionPoolSize;
	}

	public int getResponseTimeout() {
		return responseTimeout;
	}
	
	public SSLContext getCtx() {
		return ctx;
	}

	public void setCtx(SSLContext ctx) {
		this.ctx = ctx;
	}
	
	public boolean getBypassLTM() {
		return bypassLTM;
	}

	public boolean getReconnectOnFail(){ return reconnectOnFail; }
	
	public String getRecordNamespace() {
		return ns;
	}
	
	public boolean isTestMode() {
		return false;
	}

	public String getJunoPool() {
		//Check if the hosts name stats with junoserv. If yes then return junoserv-<pool> otherwise
		//just return the port number
		if(host.startsWith("junoserv-")){
			String junoPool[] = host.split("-",2);
			return junoPool[0];
		}else{
			// For all other endpoints that does not have junoserv as the endpoint prefix use host:port
			return new String(host + ":" + port);
		}
	}
}

