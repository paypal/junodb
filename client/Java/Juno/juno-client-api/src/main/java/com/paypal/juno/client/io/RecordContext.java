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
package com.paypal.juno.client.io;

import java.util.Arrays;
import java.util.Objects;

/**
 * This final class encapsulates the meta data extracted from the returned Juno 
 * server response. The JunoContext is not directly an abstraction in the 
 * Juno wire protocol.
 * 
 * <p>
 * JunoContext is used to correlate the GET call and the UPDATE call 
 * (from the same User/Client) to the Juno server in multi-user systems to 
 * preserve integrity of changes of a single user. Optimistic locking is used 
 * to detect collision of {@code <key, value>} pair while your program was trying to 
 * cas particular copy of record.
 * 
 * <p>
 * This is an immutable class.
 * For more information on Java Juno Client usage. 
 *
 */
public final class RecordContext {
	
	private final byte [] key;
	private final Long version;
	private final Long creationTime;
	private final Long timeToLiveSec;
	/**
	 * Constructs a new MayflyContext using the mandatory fields.
	 * 
	 * 
	 * @param key Key part of the unique key for Juno server storage. Cannot 
	 * be null.
	 * 
	 * @param version Version of the record in Juno server storage. Can be a zero
     *                if be returned for a miss/key not found.
     *                
     * @param creationTime Creation time part of the unique key for Juno server 
	 * storage. Cannot be null.
	 * 
	 * @param ttl Time to live part of the unique key for Juno server
	 * 
	 * @throws IllegalArgumentException If Namespace is null/empty or Key is 
	 * null/empty or Version is not positive.
	 */
	public RecordContext(byte[] key, long version,long creationTime,long ttl) {
//		ExceptionUtils.throwIfNull(creationTime, "CreationTime");
//		ExceptionUtils.throwIfNull(ttl,"TTL");
//		ExceptionUtils.throwIfNull(key, "Key");
//		ExceptionUtils.throwIfNull(version, "Version");

		this.timeToLiveSec = ttl;
		this.key = key;
		this.creationTime = creationTime;
		this.version = version;
	}



	/**
	 * Returns the Key of the record which is being queried.
	 * @return String value of the key.
	 */
	public final byte [] getKey() {
		return key;
	}

	/**
	 * Returns the version of the record in the Juno storage. 
	 * @return version of the record in the Juno storage.
	 */
	public final long getVersion() {
		return version;
	}
	
	/**
	 * Returns the version of the record in the Juno storage. 
	 * @return version of the record in the Juno storage.
	 */
	public Long getCreationTime() {
		return creationTime;
	}
	
	/**
	 * Returns the TimeToLive that is a part of the internal Key used in Juno 
	 * storage.
	 * @return long value for the TTL 
	 */
	public final long getTtl() {
		return timeToLiveSec;
	}

	@Override
	public int hashCode() {
		return Objects.hash(key,version,creationTime,timeToLiveSec);
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		RecordContext other = (RecordContext) obj;
		if (!Arrays.equals(key, other.key)) //key cannot be null
			return false;
		if (!creationTime.equals(other.creationTime)) //creationTime cannot be null
			return false;
		if (!version.equals(other.version)) //ver cannot be null
			return false;
		if (!timeToLiveSec.equals(other.timeToLiveSec)) //ttl cannot be null
			return false;
		return true;
	}
}