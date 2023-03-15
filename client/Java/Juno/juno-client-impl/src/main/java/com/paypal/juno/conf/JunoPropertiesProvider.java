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

import com.paypal.juno.util.BasePropertiesProvider;
import java.net.URL;
import java.util.Properties;
import org.apache.commons.configuration.Configuration;

public final class JunoPropertiesProvider extends BasePropertiesProvider {
	
	// Property holders.
    private Integer responseTimeout;
    private Integer connectionTimeout;
    private Integer connectionLifetime;
	private Integer connectionPoolSize;
    private Long defaultLifetime;
    
    private Long maxLifetime;
    private Integer maxKeySize;
    private Integer maxValueSize;
    private Integer maxNameSpaceLength;
	private Integer maxResponseTimeout;
	private Integer maxConnectionTimeout;
	private Integer maxConnectionPoolSize;
	private Integer maxConnectionLifetime;

	private String host;
	private Integer port;
	private String appName;
	private String recordNamespace;
	private Boolean useSSL;
	private Boolean usePayloadCompression;
	private Boolean operationRetry;
	private Boolean byPassLTM;
	private Boolean reconnectOnFail;
	private String configPrefix;
	private Configuration config;

	/**
	 *  Constructs the default properties provider.
	 *  
	 *  @param props Juno poperties.
	 */
	public JunoPropertiesProvider(Properties props) {
		super(props);
		validateAndFillAll();
	}
	
	/**
	 *  Constructs the default properties provider.
	 *  
	 *  @param url location of the Juno poperties file.
	 */
	public JunoPropertiesProvider(URL url) {
		super(url);
		validateAndFillAll();
	}

	/**
	 *  Validate and populate the juno proprties
	 *  
	 */
	private void validateAndFillAll() {
		this.connectionLifetime = getIntProperty(JunoProperties.CONNECTION_LIFETIME,JunoPropertyDefaultValue.connectionLifetimeMS);
		this.connectionPoolSize = getIntProperty(JunoProperties.CONNECTION_POOL_SIZE,JunoPropertyDefaultValue.connectionPoolSize);
		this.recordNamespace = getStringProperty(JunoProperties.RECORD_NAMESPACE, JunoPropertyDefaultValue.recordNamespace);
		this.maxConnectionPoolSize = getIntProperty(JunoProperties.MAX_CONNECTION_POOL_SIZE,JunoPropertyDefaultValue.maxConnectionPoolSize);
		this.maxNameSpaceLength = getIntProperty(JunoProperties.MAX_NAMESPACE_LENGTH,JunoPropertyDefaultValue.maxNamespaceLength);
		this.byPassLTM = getBooleanProperty(JunoProperties.BYPASS_LTM,JunoPropertyDefaultValue.byPassLTM);
		this.configPrefix = getStringProperty(JunoProperties.CONFIG_PREFIX,null);
		this.host = getStringProperty(JunoProperties.HOST, JunoPropertyDefaultValue.host);
		this.reconnectOnFail = getBooleanProperty(JunoProperties.RECONNECT_ON_FAIL,JunoPropertyDefaultValue.reconnectOnFail);
		this.port = getIntProperty(JunoProperties.PORT,JunoPropertyDefaultValue.port);
		this.appName = getStringProperty(JunoProperties.APP_NAME, JunoPropertyDefaultValue.appName);
		this.useSSL = getBooleanProperty(JunoProperties.USE_SSL, JunoPropertyDefaultValue.useSSL);
		this.responseTimeout = getIntProperty(JunoProperties.RESPONSE_TIMEOUT,JunoPropertyDefaultValue.responseTimeoutMS);
		this.connectionTimeout = getIntProperty(JunoProperties.CONNECTION_TIMEOUT,JunoPropertyDefaultValue.connectionTimeoutMS);
		this.defaultLifetime = getLongProperty(JunoProperties.DEFAULT_LIFETIME,JunoPropertyDefaultValue.defaultLifetimeS);
		this.usePayloadCompression = getBooleanProperty(JunoProperties.USE_PAYLOADCOMPRESSION,JunoPropertyDefaultValue.usePayloadCompression);
		this.maxConnectionLifetime = getIntProperty(JunoProperties.MAX_CONNECTION_LIFETIME,JunoPropertyDefaultValue.maxConnectionLifetimeMS);
		this.maxKeySize = getIntProperty(JunoProperties.MAX_KEY_SIZE,JunoPropertyDefaultValue.maxKeySizeB);
		this.maxValueSize = getIntProperty(JunoProperties.MAX_VALUE_SIZE,JunoPropertyDefaultValue.maxValueSizeB);
		this.maxLifetime = getLongProperty(JunoProperties.MAX_LIFETIME,JunoPropertyDefaultValue.maxLifetimeS);
		this.operationRetry = getBooleanProperty(JunoProperties.ENABLE_RETRY,JunoPropertyDefaultValue.operationRetry);
		this.configPrefix = getStringProperty(JunoProperties.CONFIG_PREFIX,"");
	}

    @Override
    public String toString() {
        return "JunoPropertiesProvider{" +
                " connectionTimeoutMS=" + connectionTimeout +
                ", connectionPoolSize=" + connectionPoolSize +
                ", defaultLifetime=" + defaultLifetime +
                ", maxLifetime=" + maxLifetime +
                ", host='" + host + '\'' +
                ", port='" + port + '\'' +
                ", appName='" + appName +
                ", recordNamespace='" + recordNamespace +
                ", useSSL = " + useSSL +
                ", usePayloadCompression =" + usePayloadCompression +
                ", responseTimeout = " + responseTimeout +
                ", maxConnectionPoolSize=" + connectionPoolSize +
                ", maxConnectionLifetime=" + maxConnectionLifetime +
                ", maxKeySize=" + maxKeySize +
                ", maxValueSize=" + maxValueSize +
                ", maxLifetime=" + maxLifetime +
                ", maxNameSpaceLength=" + maxNameSpaceLength +
                ", operationRetry=" + operationRetry +
                ", byPassLTM=" + byPassLTM +
				", reconnectOnFail=" + reconnectOnFail +
                '}';
    }

	public Integer getConnectionTimeout() {
		return connectionTimeout;
	}

	public void setConnectionTimeout(Integer connectionTimeout) { this.connectionTimeout = connectionTimeout; }

	public Integer getConnectionPoolSize() {
		return connectionPoolSize;
	}

	public Long getDefaultLifetime() {
		return defaultLifetime;
	}

	public void setDefaultLifetime(Long defaultLifetime) {
		this.defaultLifetime = defaultLifetime;
	}

	public String getHost() {
		return host;
	}
	
	public Integer getPort() { return port; }

	public String getAppName() {
		return appName;
	}

	public void setAppName(String appName) { this.appName = appName; }
	
	public Integer getConnectionLifetime() {
	      return connectionLifetime;
	}
	
	public Integer getMaxKeySize() {
		return maxKeySize;
	}

	public void setMaxKeySize(Integer maxKeySize) {
		this.maxKeySize = maxKeySize;
	}
	
	public String getRecordNamespace() {
		return recordNamespace;
	}

	public Boolean useSSL(){
		return useSSL;
	}

	public Integer getResponseTimeout() {
		return responseTimeout;
	}

	public void setResponseTimeout(Integer responseTimeout) { this.responseTimeout = responseTimeout; }
	
    public Long getMaxLifetime() {
		return maxLifetime;
	}

	public void setMaxLifetime(Long maxLifetime) { this.maxLifetime = maxLifetime; }

	public Integer getMaxValueSize() {
		return maxValueSize;
	}

	public void setMaxValueSize(Integer maxValueSize) {
		this.maxValueSize = maxValueSize;
	}

	public Integer getMaxConnectionPoolSize() {
		return maxConnectionPoolSize;
	}
	
    public Integer getMaxNameSpaceLength() {
		return maxNameSpaceLength;
	}

	public void setMaxNameSpaceLength(Integer maxNameSpaceLength) {
		this.maxNameSpaceLength = maxNameSpaceLength;
	}

	public Integer getMaxConnectionLifetime() {
		return maxConnectionLifetime;
	}
	
	public Boolean isUsePayloadCompression() {
		return usePayloadCompression;
	}

	public void SetUsePayloadCompression(Boolean usePayloadCompression) { this.usePayloadCompression = usePayloadCompression; }
	
	public Boolean getOperationRetry(){
		return operationRetry;
	}

	public void setOperationRetry(Boolean operationRetry){
		this.operationRetry = operationRetry;
	}
	
	public Boolean getByPassLTM(){
		return byPassLTM;
	}

	public Boolean getReconnectOnFail(){ return reconnectOnFail; }

	public String getConfigPrefix() { return configPrefix; }

	public void setConfig(Configuration config) { this.config = config; }

	public Configuration getConfig() { return this.config; }
}
