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
package com.paypal.juno.client.impl;

import com.paypal.juno.client.JunoAsyncClient;
import com.paypal.juno.client.JunoClient;
import com.paypal.juno.client.JunoClientConfigHolder;
import com.paypal.juno.client.JunoClientFactory;
import com.paypal.juno.client.JunoReactClient;
import com.paypal.juno.conf.JunoPropertiesProvider;
import com.paypal.juno.util.JunoClientUtil;
import java.net.URL;
import javax.net.ssl.SSLContext;
/**
 * This is the internal factory class for the JunoClient and is a public
 * class as an implementation side-effect.  
 * <p>
 * This class should never be used directly by the Client.
 * 
 * @see JunoClientFactory
 */
public final class JunoClientFactoryInternal {

		/**
		 * Creates an new Juno client object with Juno properties file and with
		 * JunoPropertiesProvider object.
		 * 
		 * @param url URL of the Juno client properties file.
		 * @return new Juno client object
		 */
		public final JunoClient newJunoClient(URL url) {
			JunoClientUtil.throwIfNull(url, "URL");
			final JunoPropertiesProvider junoProps = new JunoPropertiesProvider(url);
			return newJunoClient(junoProps);
		}
		
		/**
		 * Creates an new Juno client object with Juno properties file and with
		 * JunoPropertiesProvider object.
		 * 
		 * @param junoProps - Juno client properties.
		 * @return new Juno client object
		 */
		public final JunoClient newJunoClient(JunoPropertiesProvider junoProps) {
			JunoClientUtil.throwIfNull(junoProps, "Juno Properties");
			final JunoClientConfigHolder cfgHldr = new JunoClientConfigHolder(junoProps);
			return new JunoClientImpl(cfgHldr,null);
		}
		
		/**
		 * Creates an new Juno client object with Juno properties and client supplied SSLContext.
		 * 
		 * @param  junoProps - Juno client properties.
		 * @param  ctx - Client supplied SSL Context.
		 * @return new Juno client object
		 */
		public final JunoClient newJunoClient(JunoPropertiesProvider junoProps,SSLContext ctx) {
			JunoClientUtil.throwIfNull(junoProps, "Juno Properties");
			final JunoClientConfigHolder cfgHldr = new JunoClientConfigHolder(junoProps);
			return new JunoClientImpl(cfgHldr,ctx);
		}

		/**
		 * Creates an new Juno client object with Juno properties file.
		 *
		 * @param url URL of the Juno client properties file.
		 * @return new Juno async client object
		 */
		public final JunoAsyncClient newJunoAsyncClient(URL url) {
			JunoClientUtil.throwIfNull(url, "URL");
			final JunoPropertiesProvider junoProps = new JunoPropertiesProvider(url);
			return newJunoAsyncClient(junoProps);
		}

		/**
		 * Creates an new Juno Async client object with J properties.
		 * 
		 * @param  junoProps - Juno client properties.
		 * @return new Juno async client object
		 */
		public  final JunoAsyncClient newJunoAsyncClient(JunoPropertiesProvider junoProps) {
			JunoClientUtil.throwIfNull(junoProps, "Juno Properties");
			final JunoClientConfigHolder cfgHldr = new JunoClientConfigHolder(junoProps);
			return new JunoAsyncClientImpl(cfgHldr,null,true);
		}
		
		/**
		 * Creates an new Juno Async client object with Juno properties and client supplied SSLContext.
		 * 
		 * @param  junoProps - Juno client properties.
		 * @param  ctx - Client supplied SSL Context.
		 * @return new Juno async client object
		 */
		public  final JunoAsyncClient newJunoAsyncClient(JunoPropertiesProvider junoProps,SSLContext ctx) {
			JunoClientUtil.throwIfNull(junoProps, "Juno Properties");
			final JunoClientConfigHolder cfgHldr = new JunoClientConfigHolder(junoProps);
			return new JunoAsyncClientImpl(cfgHldr,ctx,true);
		}

		/**
		 * Creates an new Juno react client object with Juno properties file.
		 *
		 * @param url URL of the Juno client properties file.
		 * @return new Juno reactor client object
		 */
		public final JunoReactClient newJunoReactClient(URL url) {
			JunoClientUtil.throwIfNull(url, "URL");
			final JunoPropertiesProvider junoProps = new JunoPropertiesProvider(url);
			return newJunoReactClient(junoProps);
		}

		/**
		 * Creates an new Juno react client object with Juno properties.
		 *
		 * @param  junoProps - Juno client properties.
		 * @return new Juno reactor client object
		 */
		public  final JunoReactClient newJunoReactClient(JunoPropertiesProvider junoProps) {
			JunoClientUtil.throwIfNull(junoProps, "Juno Properties");
			final JunoClientConfigHolder cfgHldr = new JunoClientConfigHolder(junoProps);
			return new JunoReactClientImpl(cfgHldr,null,true);
		}

		/**
		 * Creates an new Juno react client object with Juno properties and client supplied SSLContext.
		 *
		 * @param  junoProps - Juno client properties.
		 * @param  ctx - Client supplied SSL Context.
		 * @return new Juno async client object
		 */
		public  final JunoReactClient newJunoReactClient(JunoPropertiesProvider junoProps, SSLContext ctx) {
			JunoClientUtil.throwIfNull(junoProps, "Juno Properties");
			final JunoClientConfigHolder cfgHldr = new JunoClientConfigHolder(junoProps);
			return new JunoReactClientImpl(cfgHldr,ctx,true);
		}
}
