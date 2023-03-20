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
//if I have comments here, does it break the system?
package com.paypal.juno.conf;

public final class JunoPropertyDefaultValue {
	// Default
	static public final int responseTimeoutMS = 200;
	static public final int connectionTimeoutMS = 200;
	static public final int connectionPoolSize = 1;
	static public final int connectionLifetimeMS = 30000;
	static public final long defaultLifetimeS = 259200;
	
	// Max for all above property
	static public final int maxResponseTimeoutMS = 5000;
	static public final int maxConnectionLifetimeMS = 30000;
	static public final int maxconnectionTimeoutMS = 5000;
	static public final int maxKeySizeB = 128;
	static public final int maxValueSizeB = 204800;
	static public final int maxNamespaceLength = 64;
	static public final int maxConnectionPoolSize = 3;
	static public final long maxLifetimeS = 259200;

    // Required
	static public final String host = "";
	static public final int port = 0;
	static public final String appName = ""; 
	static public final String recordNamespace = "";
	
	//optional
	public static final boolean useSSL = true;
	public static final boolean reconnectOnFail = false;
	public static final boolean usePayloadCompression = false;
	public static final boolean operationRetry = false;
	static public final boolean byPassLTM = true;
}
