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
package com.paypal.qa.juno;

import com.paypal.juno.client.JunoAsyncClient;
import com.paypal.juno.client.JunoClient;
import com.paypal.juno.client.JunoClientFactory;
import com.paypal.juno.client.JunoReactClient;
import com.paypal.juno.client.io.JunoRequest;
import com.paypal.juno.client.io.JunoResponse;
import com.paypal.juno.client.io.RecordContext;
import com.paypal.juno.conf.JunoPropertiesProvider;
import com.paypal.juno.exception.JunoException;
import java.util.Map;
import javax.net.ssl.SSLContext;

public final class JunoTestClientImpl implements JunoClient {
	
	private enum syncFlag  {sync, rxAsync, ReactAsync};
	private JunoClient junoSyncClient;
	private JunoAsyncClient junoAsyncClient;
	private JunoReactClient junoReactClient; 
	
	public JunoTestClientImpl(JunoPropertiesProvider props,SSLContext ctx, int flag) {
		if (syncFlag.sync.ordinal() == flag) {
			junoSyncClient = JunoClientFactory.newJunoClient(props,ctx);
		} else if (syncFlag.rxAsync.ordinal() == flag) {
			junoAsyncClient = JunoClientFactory.newJunoAsyncClient(props,ctx);
		} else if (syncFlag.ReactAsync.ordinal() == flag) {
			junoReactClient = JunoClientFactory.newJunoReactClient(props,ctx);	
		}

	}
				
	@Override
	public JunoResponse create(byte[] key, byte[] serializedContent) throws JunoException {
		return create(key, serializedContent, 0);
	}

	public JunoResponse create(byte[] key, byte[] serializedContent, int flag) throws JunoException {
		if (syncFlag.sync.ordinal() == flag) {
			return junoSyncClient.create(key, serializedContent);
		} else {
			return junoReactClient.create(key, serializedContent).block();
		}
	}

        @Override
        public JunoResponse create(byte[] key, byte[] serializedContent, long lifetime) throws JunoException {
		return create(key, serializedContent, lifetime, 0);
        }

        public JunoResponse create(byte[] key, byte[] serializedContent, long lifetime, int flag) throws JunoException {
                if (syncFlag.sync.ordinal() == flag) {
                        return junoSyncClient.create(key, serializedContent, lifetime);
                } else {
                        return junoReactClient.create(key,serializedContent,lifetime).block();
                }
        }

	public Iterable<JunoResponse> batchInsert(Iterable<JunoRequest> request) throws JunoException {
		return null;
	};

        @Override
        public JunoResponse get(byte[] key) throws JunoException {
		return get(key, 0);
 	}

	public JunoResponse get(byte[] key, int flag) throws JunoException {
		if (syncFlag.sync.ordinal() == flag) {
			return junoSyncClient.get(key);
		} else {
			return junoReactClient.get(key).block();
		}
	}

        @Override
        public JunoResponse get(byte[] key, long newLifetime) throws JunoException {
		return get(key, newLifetime, 0);
        }

        public JunoResponse get(byte[] key, long newLifetime, int flag) throws JunoException {
                if (syncFlag.sync.ordinal() == flag) {
                        return junoSyncClient.get(key, newLifetime);
                } else {
                        return junoReactClient.get(key,newLifetime).block();
                }
        }

	public Iterable<JunoResponse> batchGet(Iterable<JunoRequest> request) throws JunoException {
		return null;
	}

        @Override
        public JunoResponse update(byte[] key, byte[] value) throws JunoException {
		return update(key, value, 0);
        }

	public JunoResponse update(byte[] key, byte[] value, int flag) throws JunoException {
		if (syncFlag.sync.ordinal() == flag) {
			return junoSyncClient.update(key,value);
		} else {
			return junoReactClient.update(key, value).block();
		}
	}

        @Override
        public JunoResponse update(byte[] key, byte[] value, long newLifetime) throws JunoException {
		return update(key, value, newLifetime, 0);
        }

	public JunoResponse update(byte[] key, byte[] value, long newLifetime, int flag) throws JunoException {
		if (syncFlag.sync.ordinal() == flag) {
			return junoSyncClient.update(key,value,newLifetime);
		} else { 
			return junoReactClient.update(key,value,newLifetime).block();
		}
	}
	
	public Iterable<JunoResponse> batchUpdate(Iterable<JunoRequest> request) throws JunoException {
		return null;
	};

        @Override
        public JunoResponse set(byte[] key, byte[] serializedContent) throws JunoException {
		return set(key, serializedContent, 0);
        }
	
	public JunoResponse set(byte[] key, byte[] serializedContent, int flag) throws JunoException {
		if (syncFlag.sync.ordinal() == flag) {
			return junoSyncClient.set(key, serializedContent);
		} else {
			return junoReactClient.set(key,serializedContent).block();
		}
	}

        @Override
        public JunoResponse set(byte[] key, byte[] serializedContent, long lifetime) throws JunoException {
		return set(key, serializedContent, lifetime, 0);               
        }

	public JunoResponse set(byte[] key, byte[] serializedContent, long lifetime, int flag) throws JunoException {
		if (syncFlag.sync.ordinal() == flag) {
			return junoSyncClient.set(key, serializedContent, lifetime);
		} else { 
			return junoReactClient.set(key,serializedContent,lifetime).block();
		}
	}
	
	public Iterable<JunoResponse> batchUpsert(Iterable<JunoRequest> request) throws JunoException{
		return null;
	};

        @Override
        public JunoResponse delete(byte[] key) throws JunoException {
		return delete(key, 0);
        }

	public JunoResponse delete(byte[] key, int flag) throws JunoException {
		if (syncFlag.sync.ordinal() == flag) {
			return junoSyncClient.delete(key);
		} else {
			return junoReactClient.delete(key).block();
		}
	}
	
	public Iterable<JunoResponse> batchDelete(Iterable<byte[]> keys) throws JunoException {
		return null;
	};

        @Override
        public JunoResponse compareAndSet(RecordContext rcx, byte[] serializedContent, long lifetime) throws JunoException {
		return compareAndSet(rcx,serializedContent,lifetime, 0);
        }
	
	public JunoResponse compareAndSet(RecordContext rcx, byte[] serializedContent, long lifetime, int flag) throws JunoException {
		if (syncFlag.sync.ordinal() == flag) {
			return junoSyncClient.compareAndSet(rcx,serializedContent,lifetime);
		} else {
			return junoReactClient.compareAndSet(rcx,serializedContent,lifetime).block();
		}
	}

        @Override
        public Iterable<JunoResponse> doBatch(Iterable<JunoRequest> request) throws JunoException {
		return null;
        }

	@Override
	public Map<String, String> getProperties() {
		return null;
	}
}
