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

import com.paypal.juno.client.impl.JunoClientFactoryInternal;
import com.paypal.juno.conf.JunoPropertiesProvider;
import java.net.URL;
import javax.net.ssl.SSLContext;

/**
 * Factory for the various implementations of Juno client operations.
 * This is a final class with all static methods.
 * 
 * <p>
 * Every Juno client user is expected to instantiate the Juno Client 
 * instances using this factory class.
 * <p>
 * Before invoking any methods on the JunoClient instances returned by
 * the methods of this factory class, it is the responsibility of the user to
 * initialize CAL. 
 * The JunoClient implementations make use of CAL for logging.
 *
 */
public final class JunoClientFactory {
	private static final JunoClientFactoryInternal factory = new JunoClientFactoryInternal();

	private JunoClientFactory() {
		//Cannot be instantiated outside this class.
	}

	/**
	 * Instantiates a JunoClient implementation using the Juno config 
	 * properties from the given URL.
	 * <p>
	 * Ensure that uKernel CAL Client is initialized.
	 *  
	 * @param url URL corresponding to the Juno config properties file. 
	 * Cannot be null.
	 * 
	 * @return JunoClient instance initialized with the properties from the 
	 * given URL.
	 */
	public static JunoClient newJunoClient(URL url) {
		return factory.newJunoClient(url);
	}

	/**
	 * Instantiates a JunoClient implementation using the given Juno
	 * property provider.
	 * 
	 * @param junoProps Juno configuration properties.
	 * Cannot be null.
	 * 
	 * @return JunoClient instance initialized with the properties from the given
	 * Juno configuration properties.
	 */
	public static JunoClient newJunoClient(JunoPropertiesProvider junoProps) {
		return factory.newJunoClient(junoProps);
	}
	
	/**
	 * Instantiates a JunoClient implementation using the given Juno
	 * property provider and client supplied SSLContext.
	 * 
	 * @param junoProps Juno configuration properties.Cannot be null.
	 * @param sslCtx - Client supplied SSL context
	 * 
	 * @return JunoClient instance initialized with the properties from the given
	 * Juno configuration properties.
	 */
	public static JunoClient newJunoClient(JunoPropertiesProvider junoProps,SSLContext sslCtx) {
		return factory.newJunoClient(junoProps,sslCtx);
	}
	
	/**
	 * Instantiates a JunoAsyncClient implementation using the Juno config 
	 * properties from the given URL.
	 * <p>
	 * Ensure that uKernel CAL Client is initialized.
	 *  
	 * @param url URL corresponding to the Juno config properties file. 
	 * Cannot be null. 
	 *  
	 * @return JunoAsyncClient instance initialized with the properties from the
	 * given URL. This is not threadsafe.
	 */
	public static JunoAsyncClient newJunoAsyncClient(URL url) {
		return factory.newJunoAsyncClient(url);
	}

	/**
	 * Instantiates a JunoAsyncClient implementation using the given Juno 
	 * property provider.
	 * 
	 * @param junoProps Juno configuration properties.
	 * Cannot be null.
	 * 
	 * @return JunoAsyncClient instance initialized with the properties from the given
	 * Juno configuration properties.
	 */
	public static JunoAsyncClient newJunoAsyncClient(JunoPropertiesProvider junoProps) {
		return factory.newJunoAsyncClient(junoProps);
	}
	
	/**
	 * Instantiates a JunoAsyncClient implementation using the given Juno 
	 * property provider and client supplied SSLContext.
	 * 
	 * @param junoProps Juno configuration properties.Cannot be null.
	 * @param sslCtx - Client supplied SSL context
	 * 
	 * @return JunoAsyncClient instance initialized with the properties from the given
	 * Juno configuration properties.
	 */
	public static JunoAsyncClient newJunoAsyncClient(JunoPropertiesProvider junoProps,SSLContext sslCtx) {
		return factory.newJunoAsyncClient(junoProps,sslCtx);
	}

	/**
	 * Instantiates a newJunoReactClient implementation using the Juno config
	 * properties from the given URL.
	 *
	 * @param url URL corresponding to the Juno config properties file.
	 * Cannot be null.
	 *
	 * @return JunoReactClient instance initialized with the properties from the
	 * given URL. This is not threadsafe.
	 */
	public static JunoReactClient newJunoReactClient(URL url) {
		return factory.newJunoReactClient(url);
	}

	/**
	 * Instantiates a JunoReactClient implementation using the given Juno
	 * property provider.
	 *
	 * @param junoProps Juno configuration properties.
	 * Cannot be null.
	 *
	 * @return JunoReactClient instance initialized with the properties from the given
	 * Juno configuration properties.
	 */
	public static JunoReactClient newJunoReactClient(JunoPropertiesProvider junoProps) {
		return factory.newJunoReactClient(junoProps);
	}

	/**
	 * Instantiates a JunoReactClient implementation using the given Juno
	 * property provider and client supplied SSLContext.
	 *
	 * @param junoProps Juno configuration properties.Cannot be null.
	 * @param sslCtx - Client supplied SSL context
	 *
	 * @return JunoReactClient instance initialized with the properties from the given
	 * Juno configuration properties.
	 */
	public static JunoReactClient newJunoReactClient(JunoPropertiesProvider junoProps,SSLContext sslCtx) {
		return factory.newJunoReactClient(junoProps,sslCtx);
	}
}