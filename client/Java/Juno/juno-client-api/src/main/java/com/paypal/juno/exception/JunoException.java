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
package com.paypal.juno.exception;

/**
 * The base exception type thrown by the JunoClient. Clients can access a
 * {@link ThrowableWrapper} that will conveniently allow {@link JunoException}
 * s to be wrapped around other {@link Throwable}s.
 * 
 */
public class JunoException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8434646335093981801L;

	/**
	 * {@link OperationStatus}, for use by error mapper.
	 */
	
	/**
	 * Construct a JunoException.
	 */
	public JunoException() {
		super();
	}

	/**
	 * Construct an JunoException given a message.
	 * 
	 * @param message
	 *            the error text
	 */
	public JunoException(String message) {
		// Guard against null messages, since we have seen those occur
		super(message);
	}

	/**
	 * Construct an JunoException given a message and a cause.
	 * 
	 * @param message
	 *            the error text
	 * @param cause
	 *            the {@link Throwable} cause
	 */
	public JunoException(String message, Throwable cause) {
		// Guard against null messages, since we have seen those occur
		super(message);
		initCause(cause);
	}

	/**
	 * Construct an JunoException given a cause.
	 * 
	 * @param cause
	 *            the {@link Throwable} cause
	 */
	public JunoException(Throwable cause) {
		super(cause.getMessage());
		initCause(cause);
	}
}
