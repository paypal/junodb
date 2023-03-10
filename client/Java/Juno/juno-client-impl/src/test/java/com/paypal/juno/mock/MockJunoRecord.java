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
package com.paypal.juno.mock;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Just a simple class to hold the mayfly records
 * @author dufox
 *
 */
final class MockJunoRecord {
	public String namespace;
	public byte[] opaque_data;
	public long lifetime;
	public String key;
	public AtomicLong version = new AtomicLong(0);
	public long createTime;
	public int opaque;
	public ByteBuffer reqId = ByteBuffer.allocate(16);
	public String compType;
}