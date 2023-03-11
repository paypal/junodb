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
package com.paypal.juno.util;

import com.paypal.juno.exception.JunoClientConfigException;
import java.io.IOException;
import java.net.URL;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BasePropertiesProvider {
	private static final Logger logger = LoggerFactory.getLogger(BasePropertiesProvider.class);

	protected final Properties config;
	
	protected BasePropertiesProvider(URL url) {
		JunoClientUtil.throwIfNull(url, "URL");
		this.config = new Properties();
		try {
			this.config.load(url.openStream());
		} catch (IOException e) {
			logger.error(
					"Unable to read the config properties file", e);
			throw new JunoClientConfigException(
					"Unable to read the config properties file", e);
		}
	}
	
	/**
	 *  Constructs the default properties provider.
	 *  
	 *  @param config properties
	 */
	protected BasePropertiesProvider(Properties config) {
		this.config = config;
	}
	
	protected final Integer getIntProperty(final String key, int defaultValue) {
		String sval = config.getProperty(key);
		int ival = defaultValue;
		if (sval != null) {
			try {
				ival = Integer.parseInt(sval.trim());
			} 
			catch(Exception e) {
				throw new JunoClientConfigException("Integer property not valid - Value = " + sval, e);
			}
		}
		return ival;
	}

	protected final Integer getIntegerProperty(final String key) {
		String sval = config.getProperty(key);
		Integer ival = null;
		if (sval != null && !sval.trim().equalsIgnoreCase("null")) {
			try {
				ival = Integer.parseInt(sval.trim());
			}
			catch(Exception e) {
				throw new JunoClientConfigException("Integer property not valid - Value = " + sval, e);
			}
		}
		return ival;
	}

	protected final Long getLongProperty(final String key, long defaultValue) {
		String sval = config.getProperty(key);
		long ival = defaultValue;
		if (sval != null) {
			try {
				ival = Long.parseLong(sval.trim());
			}
			catch(Exception e) {
				throw new JunoClientConfigException("Long property not valid - Value = " + sval, e);
			}
		}
		return ival;
	}

	protected final Long getLongProperty(final String key) {
		String sval = config.getProperty(key);
		Long ival = null;
		if (sval != null) {
			try {
				ival = Long.parseLong(sval.trim());
			}
			catch(Exception e) {
				throw new JunoClientConfigException("Long property not valid - Value = " + sval, e);
			}
		}
		return ival;
	}

	protected final Boolean getBooleanProperty(final String key, boolean defaultValue) {
		String sval = config.getProperty(key);
		if (sval == null) {
			return defaultValue;
		}
		try {
			return Boolean.valueOf(sval);
		}
		catch(Exception e) {
			throw new JunoClientConfigException("Boolean property not valid - Value = " + sval, e);
		}
	}

	protected final Boolean getBooleanProperty(final String key) {
		String sval = config.getProperty(key);
		if (sval == null) {
			return null;
		}
		try {
			return Boolean.valueOf(sval);
		}
		catch(Exception e) {
			throw new JunoClientConfigException("Boolean property not valid - Value = " + sval, e);
		}
	}

	protected final String getStringProperty(String key, String defaultValue) {
		String sval = config.getProperty(key);
		return (sval != null ? sval : defaultValue);
	}
}
