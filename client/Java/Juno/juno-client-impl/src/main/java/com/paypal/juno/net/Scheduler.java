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
package com.paypal.juno.net;

import com.paypal.juno.transport.socket.SocketConfigHolder;
import com.paypal.juno.util.JunoLogLevel;
import com.paypal.juno.util.JunoStatusCode;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * To synchronize recycle of connections.  One scheduler per worker pool.
 * 
 */

class ConnectLock {
	ReentrantLock lock;
	AtomicLong owner;
	AtomicBoolean connected;
	
	ConnectLock() {
		lock = new ReentrantLock();
		owner = new AtomicLong(1);
		connected = new AtomicBoolean();
	}
}

class Scheduler {

	private static final Logger LOGGER = LoggerFactory.getLogger(Scheduler.class);
	
	private static final SecureRandom ran = new SecureRandom();
	private static final long shift = 5000;  // milliseconds
	private long connectionLifeTime;
	private WorkerPool pool;
	private SocketConfigHolder config;
	
	private ConnectLock[] locks;
	private BlockingQueue<Object> waitQueue;
	private volatile boolean ready = false;
	private final int LOG_CYCLE = 20;
	
	Scheduler(int lifeTime, int poolSize, WorkerPool pool, SocketConfigHolder config) {
		connectionLifeTime = lifeTime;
		this.pool = pool;
		this.config = config;
		
		locks = new ConnectLock[poolSize];	
		for (int i = 0; i < poolSize; i++) {
			locks[i] = new ConnectLock();
		}
		
		waitQueue = new ArrayBlockingQueue<Object>(poolSize*2);
	}
	
	synchronized private int lockAny() throws InterruptedException {
		int k = -1;
		for (int i = 0; i < locks.length; i++) {
			if (!locks[i].lock.isLocked()) {
				k = i;
				break;
			}
		}
		
		if (WorkerPool.isQuit()) {
			throw new InterruptedException("Interrupted");
		}
		
		pool.addWorker();
		if (k >= 0) {
			locks[k].lock.lockInterruptibly();
		}
		
		return k;
	}
	
	/* 
	 * Called before connect.
	 */
	int acquireConnectLock(int index) throws InterruptedException {
		
		if (index >= 0 && locks[index].lock.isHeldByCurrentThread()) {
			return index;
		}
		
		while (true) {
			
			int k = lockAny();
			if (k >= 0) {
				return k;
			}
			
			while (true) { // Wait for lock release.
				Object x = waitQueue.poll(2, TimeUnit.SECONDS);
				if (WorkerPool.isQuit()) {
					throw new InterruptedException("Interrupted");
				}
				if (x != null) {
					break;
				}
			}
		}
	}
	
	JunoLogLevel getConnectLogLevel(int lockIndex) {
		JunoLogLevel level = JunoLogLevel.OFF;
		if (lockIndex < 0) {
			return level;
		}

		long id = locks[lockIndex].owner.get();
		if (id < LOG_CYCLE/2 || (id % LOG_CYCLE) == 0) {
			level = LOGGER.isDebugEnabled()?JunoLogLevel.INFO:JunoLogLevel.OFF;
		}

		return level;
	}

	JunoLogLevel getDisconnectLogLevel(long ownerId) {
		if (ownerId < LOG_CYCLE/2 || (ownerId % LOG_CYCLE) == 0) {
			return LOGGER.isDebugEnabled()?JunoLogLevel.INFO:JunoLogLevel.OFF;
		}

		return JunoLogLevel.OFF;
	}

	/*
	 * Set and return the owner of the connection after connection has been created.
	 */
	long setConnectOwner(int index) {
		setConnected(index);
		return locks[index].owner.incrementAndGet();
	}

	/*
	 * Release the lock for a standby worker to connect.
	 * Check if the connection owner has changed.
	 */
	boolean connectOwnerExpired(int index, long ownerId) {
		if (index < 0) {
			return true;
		}
		if (locks[index].lock.isHeldByCurrentThread()) {
			locks[index].lock.unlock();
			waitQueue.offer(new Object());
			return false;
		}

		// Check if the owner has changed.
		// This happens when another worker has acquired the lock and started a new connection.
		return (locks[index].owner.get() != ownerId);
	}

	/*
	 * Select a slot for next disconnect time.
	 */
	long selectTimeSlot() {

		long x = System.currentTimeMillis() + connectionLifeTime;
		if (connectionLifeTime >= 2 * shift) {
			x -= (long)(shift * ran.nextFloat());
		}

		return x;
	}

	private void setConnected(int index) {
		if (index < 0 || index >= locks.length) {
			return;
		}

		locks[index].connected.set(true);
	}

	void setDisconnected(int index, long ownerId) {
		if (index < 0 || index >= locks.length) {
			return;
		}

		if (ownerId != locks[index].owner.get()) {
			return;
		}

		locks[index].connected.set(false);
	}

	boolean isConnected() {

		for (int i = 0; i < locks.length; i++) {
			if (locks[i].connected.get()) {
				return true;
			}
		}
		return false;
	}

	boolean isIndexedChannelConnected(int index) {
		if (index < 0 || index >= locks.length) {
			return false;
		}
		return locks[index].connected.get();
	}

	boolean waitForReady(int connectionTimeout) {
		final int waitTime = 50;
		int count = 3 * connectionTimeout / waitTime;

		for (int i = 0; i < count; i++) {
			if (isConnected()) {
				LOGGER.info("Worker pool ready.");
				return true;
			}

			try {
				Thread.sleep(waitTime);
			} catch (Exception e) {
			}
		}

		return false;
	}

	boolean isTestMode() {
		return config.isTestMode();
	}

	boolean onEvent(TestEvent event) {

		if (!isTestMode()) {
			return false;
		}

		int val = event.maskedValue();
		if (val == 0) {
			return false;
		}

		LOGGER.warn("On test event: "+ event);
		final Map<String,CharSequence> trans = new HashMap<String, CharSequence>();
		trans.put("name","JUNO_TEST");
		trans.put("event", Long.toString(val));
		trans.put("status", JunoStatusCode.WARNING.toString());
		LOGGER.warn(JunoStatusCode.WARNING + " {} ", trans);
		return true;
	}
	
	void onException(TestEvent event) throws Exception {
		
		if (!onEvent(event)) {
			return;
		}
			
		event.triggerException();
	}
}
