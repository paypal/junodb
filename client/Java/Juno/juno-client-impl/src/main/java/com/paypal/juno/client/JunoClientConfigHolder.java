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
package com.paypal.juno.client;

import com.paypal.juno.conf.JunoProperties;
import com.paypal.juno.conf.JunoPropertiesProvider;
import com.paypal.juno.conf.JunoPropertyDefaultValue;
import com.paypal.juno.exception.JunoClientConfigException;
import com.paypal.juno.util.JunoClientUtil;
import com.paypal.juno.util.JunoConstants;
import com.paypal.juno.util.JunoMetrics;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JunoClientConfigHolder {
	private static final Logger LOGGER = LoggerFactory.getLogger(JunoClientConfigHolder.class);
	protected final JunoPropertiesProvider junoProp;
	private final InetSocketAddress serversAddress;
	private final Configuration config;

//	@Autowired
//	InstanceLocation instanceLocation;
//	private final String INSTANCE_GEO_PP_US = "PP_US";

	public JunoClientConfigHolder(JunoPropertiesProvider junoProp) {
		JunoClientUtil.throwIfNull(junoProp, "config");
		try {
			this.junoProp = junoProp;
			this.config = junoProp.getConfig();
			validateAll();
			serversAddress = getServerAddress(this.junoProp);
		} catch (Exception ce) {
			throw new JunoClientConfigException(ce);
		}
	}

	public String printProperties(){
		return junoProp.toString();
	}

	public Map<String,String> getProperties(){
		Map<String,String> prop= new HashMap<>();
		prop.put(JunoProperties.RESPONSE_TIMEOUT,String.valueOf(this.getResponseTimeout()));
		prop.put(JunoProperties.CONNECTION_TIMEOUT,String.valueOf(this.getConnectionTimeoutMsecs()));
		prop.put(JunoProperties.CONNECTION_POOL_SIZE,String.valueOf(this.getConnectionPoolSize()));
		prop.put(JunoProperties.DEFAULT_LIFETIME,String.valueOf(this.getDefaultLifetimeSecs()));
		prop.put(JunoProperties.HOST,this.getHost());
		prop.put(JunoProperties.PORT,String.valueOf(this.getPort()));
		prop.put(JunoProperties.APP_NAME,this.getApplicationName());
		prop.put(JunoProperties.RECORD_NAMESPACE,this.getRecordNamespace());
		prop.put(JunoProperties.USE_PAYLOADCOMPRESSION,String.valueOf(this.getUsePayloadCompression()));
		prop.put(JunoProperties.BYPASS_LTM,String.valueOf(this.getByPassLTM()));
		prop.put(JunoProperties.RECONNECT_ON_FAIL,String.valueOf(this.getReconnectOnFail()));
		prop.put(JunoProperties.ENABLE_RETRY,String.valueOf(this.isRetryEnabled()));
		prop.put(JunoProperties.CONFIG_PREFIX,junoProp.getConfigPrefix());
		return prop;
	}

	protected void validateAll() {
		// Call all gets triggering all validations..
		this.getConnectionTimeoutMsecs();
		this.getApplicationName();
		this.getRecordNamespace();
		this.getDefaultLifetimeSecs();
		this.getConnectionLifeTime();
		this.getConnectionPoolSize();
		this.getHost();
		this.getMaxKeySize();
		this.getResponseTimeout();
		this.getMaxValueSize();
	}

	/**
	 * This config is optional
	 * 
	 * @param junoProp
	 *            Juno configuration
	 * @return List of server in format of: ip:port
	 */
	public static InetSocketAddress getServerAddress(JunoPropertiesProvider junoProp) {
		InetAddress ipAddress;
		InetSocketAddress result = null;
		String host = junoProp.getHost().trim();
		int port = junoProp.getPort();

		
		try {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("Juno servers VIP: "+ host +" and PORT: " + port);
			}
				
			if (host == null || host.isEmpty()) {
				LOGGER.error("Juno configuration value for property, " + JunoProperties.HOST
						+ " the serevr is not configured");
				throw new JunoClientConfigException("Juno server not configured...");
			}
				
			if (port < 1) {
				LOGGER.error("Juno configuration value for property, " + JunoProperties.PORT
						+ " the port cannot be null or less than 1");
				throw new JunoClientConfigException("Invalid Juno server port...");
					
			}
				
			ipAddress = InetAddress.getByName(host);
			result = new InetSocketAddress(ipAddress,port);
				
		} catch (UnknownHostException e) {
			LOGGER.error("Unable to look up the given host " + host);
		}

		return result;
	}

	public String getApplicationName() {
		String appName = this.junoProp.getAppName();
		if (appName == null || appName.length() == 0) {
			throw new JunoClientConfigException("Juno configuration value for property, " + JunoProperties.APP_NAME
					+ " cannot be null or empty");
		}
		if (appName.getBytes().length > JunoConstants.APP_NAME_MAX_LEN) {
			String msg = "Application Name length exceeds MAX LENGTH of " + JunoConstants.APP_NAME_MAX_LEN + " bytes";
			LOGGER.error(msg);
			throw new JunoClientConfigException(msg);
		}
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("JunoClient applicationName: " + appName);
		}
		return appName;
	}

	private Integer getMaxRecordNameSpaceLength(){
		Integer recordNameSpaceLength = validateAndReturnDefaultInt(JunoProperties.MAX_NAMESPACE_LENGTH,
				this.junoProp.getMaxNameSpaceLength(), 0, Integer.MAX_VALUE,JunoPropertyDefaultValue.maxNamespaceLength);
		LOGGER.debug("JunoClient max record namespace lenth: " + recordNameSpaceLength);
		return recordNameSpaceLength;
	}
	
	public String getRecordNamespace() {
		String ns = this.junoProp.getRecordNamespace();
		if (ns == null || ns.length() == 0) {
			throw new JunoClientConfigException("Juno configuration value for property, "
					+ JunoProperties.RECORD_NAMESPACE + " cannot be null or empty");
		}
		if (ns.getBytes().length > getMaxRecordNameSpaceLength()) {
			String msg = "Namespace length exceeds MAX LENGTH of " + getMaxRecordNameSpaceLength() + " bytes";
			LOGGER.error(msg);
			throw new JunoClientConfigException(msg);
		}
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("JunoClient recordNamespace: " + ns);
		}
		return ns;
	}

	private Integer getMaxConnectionLifeTime(){
		Integer maxConnectionLifeTime = validateAndReturnDefaultInt(JunoProperties.MAX_CONNECTION_LIFETIME,
				this.junoProp.getMaxConnectionLifetime(), 6000, Integer.MAX_VALUE,JunoPropertyDefaultValue.maxConnectionLifetimeMS);
		LOGGER.debug("JunoClient max Connection Lifetime (sec): " + maxConnectionLifeTime);
		return maxConnectionLifeTime;
	}
	
	public Integer getConnectionLifeTime() {
		Integer connectionLifeTime = validateAndReturnDefaultInt(JunoProperties.CONNECTION_LIFETIME,
				this.junoProp.getConnectionLifetime(), 5000, getMaxConnectionLifeTime(),JunoPropertyDefaultValue.connectionLifetimeMS);
		LOGGER.debug("JunoClient Connection Lifetime (sec): " + connectionLifeTime);
		return connectionLifeTime;
	}
	
	private Integer getMaxConnectionPoolSize(){
		Integer maxConnectionPoolSize = validateAndReturnDefaultInt(JunoProperties.MAX_CONNECTION_POOL_SIZE,
				this.junoProp.getMaxConnectionPoolSize(), 1, Integer.MAX_VALUE,JunoPropertyDefaultValue.maxConnectionPoolSize);
		LOGGER.debug("JunoClient max Connection pool size: " + maxConnectionPoolSize);
		return maxConnectionPoolSize;
	}
	
	public Integer getConnectionPoolSize() {
		Integer connectionPoolSize = validateAndReturnDefaultInt(JunoProperties.CONNECTION_POOL_SIZE,
				this.junoProp.getConnectionPoolSize(), 1, getMaxConnectionPoolSize(),JunoPropertyDefaultValue.connectionPoolSize);
		LOGGER.debug("JunoClient Connection pool size: " + connectionPoolSize);
		return connectionPoolSize;
	}

	public String getHost(){
		return junoProp.getHost().trim();
	}

	public int getPort(){
		return junoProp.getPort();
	}

	public boolean getUseSSL(){
		return this.junoProp.useSSL();
	}

	public InetSocketAddress getServer() {
		return serversAddress;
	}

	public boolean getByPassLTM(){
		return this.junoProp.getByPassLTM();
	}

	public boolean getReconnectOnFail(){ return this.junoProp.getReconnectOnFail(); }

	//---------------------------------- Validation Methods--------------------------------------------
	
	protected final Integer  validateAndReturnDefaultInt(String key, Integer prop, int min, int max, Integer defaultVal) {
		if (prop == null) {
			return defaultVal;
		}
		if (prop < min) {
			throw new JunoClientConfigException(
					"Juno configuration value for property " + key + " cannot be less than " + min);
		}
		if (prop > max) {
			throw new JunoClientConfigException(
					"Juno configuration value for property " + key + " cannot be greater than " + max);
		}
		return prop;
	}

	protected final Long validateAndReturnDefaultLong(String key, Long prop, long min, long max, Long defaultVal) {
		if (prop == null) {
			return defaultVal;
		}
		if (prop < min) {
			throw new JunoClientConfigException(
					"Juno configuration value for property " + key + " cannot be less than " + min);
		}

		if (prop > max) {
			throw new JunoClientConfigException(
					"Juno configuration value for property " + key + " cannot be greater than " + max);
		}
		
		return prop;
	}

	//---------------------------------- Dynamic Config--------------------------------------------

	protected final Integer processIntProperty(String property, Integer currentValue, int min,int max, Integer defaultValue){
		String propertyWithPrefix = junoProp.getConfigPrefix()!=""?junoProp.getConfigPrefix()+"."+property:property;
		Integer rcsValue = getRemoteConfigInt(propertyWithPrefix,currentValue);
		Integer intProperty = currentValue;

		if(rcsValue != null && !rcsValue.equals(currentValue)){
			try {
				intProperty = validateAndReturnDefaultInt(propertyWithPrefix, rcsValue, min, max, defaultValue);
			}catch(Exception e){
				JunoMetrics.recordEventCount("JUNO_CONFIG",property,JunoMetrics.EXCEPTION);
				LOGGER.warn("Exception in Dynamic config. "+e.getMessage());
			}
		} else {

			intProperty = validateAndReturnDefaultInt(propertyWithPrefix, currentValue, min, max,defaultValue);
		}
		return intProperty;
	}

	protected final Long processLongProperty(String property, Long currentValue, long min,long max, Long defaultValue){
		String propertyWithPrefix = junoProp.getConfigPrefix()!=""?junoProp.getConfigPrefix()+"."+property:property;
		Long rcsValue = getRemoteConfigLong(propertyWithPrefix,currentValue);
		Long longProperty = currentValue;
		//If value is updated in RCS then it will not be same as currentValue
		if(rcsValue != null && !rcsValue.equals(currentValue)){
			try {
				longProperty = validateAndReturnDefaultLong(propertyWithPrefix, rcsValue, min, max, defaultValue);
			}catch(Exception e){
				JunoMetrics.recordEventCount("JUNO_CONFIG",property,JunoMetrics.EXCEPTION);
				LOGGER.warn("Exception in Dynamic config. "+e.getMessage());
			}
		} else {

			longProperty = validateAndReturnDefaultLong(propertyWithPrefix, currentValue, min, max,defaultValue);
		}
		return longProperty;
	}

	protected final Boolean processBoolProperty(String property, Boolean currentValue){
		String propertyWithPrefix = junoProp.getConfigPrefix()!=""?junoProp.getConfigPrefix()+"."+property:property;
		Boolean rcsValue = getRemoteConfigBool(propertyWithPrefix,currentValue);
		Boolean boolProperty = currentValue;

		if(rcsValue != null && !rcsValue.equals(currentValue)){
			boolProperty = rcsValue;
		}
		return boolProperty;
	}

	private Integer getRemoteConfigInt(String propertyWithPrefix, Integer currentValue){
		if(junoProp.getConfig() != null && junoProp.getConfig().containsKey(propertyWithPrefix)){
			try {
				Integer intProp = junoProp.getConfig().getInt(propertyWithPrefix);
				return intProp;
			}catch (Exception e){
				LOGGER.error("Error in Config ("+propertyWithPrefix+"). Exception :"+e.getMessage());
			}
		}
		return currentValue;
	}

	private Boolean getRemoteConfigBool(String propertyWithPrefix, Boolean currentValue){
		if(junoProp.getConfig() != null && junoProp.getConfig().containsKey(propertyWithPrefix)){
			try {
				Boolean boolProp = junoProp.getConfig().getBoolean(propertyWithPrefix);
				return boolProp;
			}catch (Exception e){
				LOGGER.error("Error in Config ("+propertyWithPrefix+"). Exception :"+e.getMessage());
			}
		}
		return currentValue;
	}

	private Long getRemoteConfigLong(String propertyWithPrefix, Long currentValue){
		if(junoProp.getConfig() != null && junoProp.getConfig().containsKey(propertyWithPrefix)){
			try {
				Long longProp = junoProp.getConfig().getLong(propertyWithPrefix);
				return longProp;
			}catch (Exception e){
				LOGGER.error("Error in Config ("+propertyWithPrefix+"). Exception :"+e.getMessage());
			}
		}
		return currentValue;
	}

	// ------------------------------- Following Parameters can change Values dynamically--------------------
	//Dynamic Config update enabled
	public Boolean getUsePayloadCompression(){
		Boolean usePayloadCompression = processBoolProperty(JunoProperties.USE_PAYLOADCOMPRESSION,junoProp.isUsePayloadCompression());
		if(usePayloadCompression !=null && !usePayloadCompression.equals(junoProp.isUsePayloadCompression())){
			junoProp.SetUsePayloadCompression(usePayloadCompression);
			LOGGER.info("JunoClient Payload compression Dynamic updated value: " + usePayloadCompression);

			LOGGER.debug("JUNO","JUNO_CONFIG", "JunoClient Payload compression Dynamic updated value: " + usePayloadCompression);

			JunoMetrics.recordEventCount("JUNO_CONFIG",JunoProperties.USE_PAYLOADCOMPRESSION,JunoMetrics.SUCCESS);
		}
		LOGGER.debug("JunoClient use payload compression: " + usePayloadCompression);
		return usePayloadCompression;
	}

	//Dynamic Config update enabled
	public Integer getConnectionTimeoutMsecs() {
		Integer connTimeout = processIntProperty(JunoProperties.CONNECTION_TIMEOUT,junoProp.getConnectionTimeout(),1,JunoPropertyDefaultValue.maxconnectionTimeoutMS,JunoPropertyDefaultValue.connectionTimeoutMS);
		if(connTimeout != null && !connTimeout.equals(junoProp.getConnectionTimeout())){
			junoProp.setConnectionTimeout(connTimeout);
			LOGGER.info("JunoClient Connection Timeout Dynamic updated value: "+connTimeout);

			LOGGER.debug("JUNO","JUNO_CONFIG", "JunoClient Connection Timeout Dynamic updated value: "+connTimeout);

			JunoMetrics.recordEventCount("JUNO_CONFIG",JunoProperties.CONNECTION_TIMEOUT,JunoMetrics.SUCCESS);
		}
		LOGGER.debug("JunoClient connection timeout (msecs): " + connTimeout);
		return connTimeout;
	}

	//Dynamic Config update enabled
	public Long getMaxLifetimeSecs(){
		Long maxLifeTime = processLongProperty(JunoProperties.MAX_LIFETIME,junoProp.getMaxLifetime(),1,Long.MAX_VALUE,JunoPropertyDefaultValue.maxLifetimeS);
		if(maxLifeTime != null && !maxLifeTime.equals(junoProp.getMaxLifetime())){
			junoProp.setMaxLifetime(maxLifeTime);
			LOGGER.info("JunoClient Max TTL Dynamic updated value: "+maxLifeTime);

			LOGGER.debug("JUNO","JUNO_CONFIG","JunoClient Max TTL Dynamic updated value: "+maxLifeTime);

			JunoMetrics.recordEventCount("JUNO_CONFIG",JunoProperties.MAX_LIFETIME,JunoMetrics.SUCCESS);
		}
		LOGGER.debug("JunoClient max lifetime (sec): " + maxLifeTime);
		return maxLifeTime;
	}

	//Dynamic Config update enabled
	public Long getDefaultLifetimeSecs() {
		Long lifetime = processLongProperty(JunoProperties.DEFAULT_LIFETIME,junoProp.getDefaultLifetime(),1,getMaxLifetimeSecs(),JunoPropertyDefaultValue.defaultLifetimeS);
		if(lifetime != null && !lifetime.equals(junoProp.getDefaultLifetime())){
			junoProp.setDefaultLifetime(lifetime);
			LOGGER.info("JunoClient TTL Dynamic updated value: "+lifetime);
			JunoMetrics.recordEventCount("JUNO_CONFIG",JunoProperties.DEFAULT_LIFETIME,JunoMetrics.SUCCESS);
		}
		LOGGER.debug("JunoClient default lifetime (sec): " + lifetime);
		return lifetime;
	}

	//Dynamic Config update enabled
	public boolean isRetryEnabled(){
		Boolean enableRetry = processBoolProperty(JunoProperties.ENABLE_RETRY,junoProp.getOperationRetry());
		if(enableRetry != null && !enableRetry.equals(junoProp.getOperationRetry())){
			junoProp.setOperationRetry(enableRetry);
			LOGGER.info("JunoClient Operation retry Dynamic updated value: "+enableRetry);
			JunoMetrics.recordEventCount("JUNO_CONFIG",JunoProperties.ENABLE_RETRY,JunoMetrics.SUCCESS);
		}
		LOGGER.debug("JunoClient Operation retry: " + enableRetry);
		return enableRetry;
	}

	//Dynamic Config update enabled
	public Integer getMaxValueSize(){
		Integer maxValueSize = processIntProperty(JunoProperties.MAX_VALUE_SIZE,junoProp.getMaxValueSize(),1,Integer.MAX_VALUE,JunoPropertyDefaultValue.maxValueSizeB);
		if(maxValueSize != null && !maxValueSize.equals(junoProp.getMaxValueSize())){
			junoProp.setMaxValueSize(maxValueSize);
			LOGGER.info("JunoClient max Value size Dynamic updated value: "+maxValueSize);
			JunoMetrics.recordEventCount("JUNO_CONFIG",JunoProperties.MAX_VALUE_SIZE,JunoMetrics.SUCCESS);
		}
		LOGGER.debug("JunoClient max Value size: " + maxValueSize);
		return maxValueSize;
	}

	//Dynamic Config update enabled
	public Integer getMaxKeySize() {
		Integer maxKeySize = processIntProperty(JunoProperties.MAX_KEY_SIZE,junoProp.getMaxKeySize(),1,Integer.MAX_VALUE,JunoPropertyDefaultValue.maxKeySizeB);
		if(maxKeySize != null && !maxKeySize.equals(junoProp.getMaxKeySize())){
			junoProp.setMaxKeySize(maxKeySize);
			LOGGER.info("JunoClient Key size Dynamic updated value: "+maxKeySize);
			JunoMetrics.recordEventCount("JUNO_CONFIG",JunoProperties.MAX_KEY_SIZE,JunoMetrics.SUCCESS);
		}
		LOGGER.debug("JunoClient Key size: " + maxKeySize);
		return maxKeySize;
	}

	//Dynamic Config update enabled
	public Integer getResponseTimeout() {
		Integer responseTimeout = processIntProperty(JunoProperties.RESPONSE_TIMEOUT,junoProp.getResponseTimeout(),1,JunoPropertyDefaultValue.maxResponseTimeoutMS,JunoPropertyDefaultValue.responseTimeoutMS);
		if(responseTimeout != null && !responseTimeout.equals(junoProp.getResponseTimeout())){
			junoProp.setResponseTimeout(responseTimeout);
			LOGGER.info("JunoClient Response timeout Dynamic updated value: "+responseTimeout);
			JunoMetrics.recordEventCount("JUNO_CONFIG",JunoProperties.RESPONSE_TIMEOUT,JunoMetrics.SUCCESS);
		}
		LOGGER.debug("JunoClient Response timeout (msec): " + responseTimeout);
		return responseTimeout;
	}
}
