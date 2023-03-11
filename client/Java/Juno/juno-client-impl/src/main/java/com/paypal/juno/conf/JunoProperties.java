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
package com.paypal.juno.conf;

public final class JunoProperties {
	// property name definitions
	public static final String RESPONSE_TIMEOUT = "juno.response.timeout_msec";
	public static final String CONNECTION_TIMEOUT = "juno.connection.timeout_msec";
	public static final String DEFAULT_LIFETIME = "juno.default_record_lifetime_sec";
	public static final String CONNECTION_LIFETIME = "juno.connection.recycle_duration_msec";
	public static final String CONNECTION_POOL_SIZE = "juno.connection.pool_size";
	public static final String RECONNECT_ON_FAIL = "juno.connection.reconnect_on_fail";
	public static final String HOST = "juno.server.host";
	public static final String PORT = "juno.server.port";
	public static final String APP_NAME = "juno.application_name";
	public static final String RECORD_NAMESPACE = "juno.record_namespace";
	public static final String USE_SSL = "juno.useSSL";
	public static final String USE_PAYLOADCOMPRESSION = "juno.usePayloadCompression";
	public static final String ENABLE_RETRY = "juno.operation.retry";
	public static final String BYPASS_LTM = "juno.connection.byPassLTM";
	public static final String CONFIG_PREFIX = "prefix";
	
	
	// Max for each property
	public static final String MAX_LIFETIME = "juno.max_record_lifetime_sec";
	public static final String MAX_KEY_SIZE = "juno.max_key_size";
	public static final String MAX_VALUE_SIZE = "juno.max_value_size";
	public static final String MAX_RESPONSE_TIMEOUT = "juno.response.max_timeout_msec";
	public static final String MAX_CONNECTION_TIMEOUT = "juno.connection.max_timeout_msec";
	public static final String MAX_CONNECTION_LIFETIME = "juno.connection.max_recycle_duration_msec";
	public static final String MAX_CONNECTION_POOL_SIZE = "juno.connection.max_pool_size";
	public static final String MAX_NAMESPACE_LENGTH = "juno.max_record_namespace_length";
}
