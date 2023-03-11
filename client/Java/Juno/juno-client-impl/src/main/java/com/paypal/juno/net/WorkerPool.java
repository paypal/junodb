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
import com.paypal.juno.util.SSLUtil;
import java.io.*;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.SSLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * IO Worker pool.
 * 
 */

class WorkerThreadFactory implements ThreadFactory {
	
	private static final AtomicInteger poolId = new AtomicInteger();
	private final AtomicInteger nextId = new AtomicInteger();
	private final String prefix;
	private final boolean daemon;
	
	WorkerThreadFactory(String poolName, boolean daemon) {
		this.prefix = poolName + '-' + poolId.incrementAndGet() + '-';
		this.daemon = daemon;
	}
	
	@Override
	public Thread newThread(Runnable r) {
		
		Thread t = new Thread(r, prefix + nextId.incrementAndGet());
		t.setDaemon(daemon);
	        
		return t;
	}
}

class WorkerPool {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(WorkerPool.class);
	
	private static volatile boolean quit = false;
	
	private int maxWorkers;
	private BlockingQueue<Runnable> taskQueue;
	
	private ThreadPoolExecutor pool = null;
	private SocketConfigHolder config;
	private RequestQueue requestQueue;
	private Scheduler scheduler;
	private SSLContext ctx;
	
	WorkerPool(SocketConfigHolder cfg, RequestQueue queue) {
		maxWorkers = 2 * cfg.getConnectionPoolSize();
		taskQueue = new ArrayBlockingQueue<Runnable>(maxWorkers);
		scheduler = new Scheduler(cfg.getConnectionLifeTime(), cfg.getConnectionPoolSize(), this, cfg);
		
		init(cfg, queue);	
		scheduler.waitForReady(cfg.getConnectTimeout());
	}
	
	synchronized void init(SocketConfigHolder cfg, RequestQueue q) {
		if (pool != null) {
			return;
		}
		
		config = cfg;
		requestQueue = q;
		if (config.useSSL()) {
			ctx = config.getCtx();
			if (ctx != null) {
				LOGGER.info("Use client ssl context.");
			} else{
				loadSslContext();
			}
		}
			
		quit = false;
		pool = new ThreadPoolExecutor(maxWorkers, maxWorkers, 0, TimeUnit.SECONDS, taskQueue,
				new WorkerThreadFactory("JunoWorker", true));
		
		// Add all workers.
		for (int i = 0; i < maxWorkers; i++) {
			addWorker();
		}
	}
	
	synchronized void shutdown() {
		if (pool == null) {
			return;
		}
		
		LOGGER.info("Shutdown IO.");
		quit = true;
		pool.shutdownNow();
		
		try {
			pool.awaitTermination(5, TimeUnit.SECONDS);
		} catch (InterruptedException e){
			LOGGER.debug("Worker got interrupted.");
			Thread.currentThread().interrupt();
		}
		pool = null;
	}

	private void loadSslContext() {
		ctx = SSLUtil.getSSLContext();

		if (ctx == null) {
			throw new RuntimeException("unable to get ssl context.");
		}
	}

	public String[] getResourceListing(URL url) {
		URLClassLoader classLoader = new URLClassLoader(new URL[] { url });
		InputStream inputStream = classLoader.getResourceAsStream("secrets");
		BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
		return br.lines().toArray(String[]::new);
	}

	void addWorker() {
		
		if (pool.getQueue().remainingCapacity() <= 0) {
			return;
		}
		
		try {
			pool.submit(new IOProcessor(config, requestQueue, scheduler, ctx, requestQueue.getOpaqueResMap()));
		} catch (RejectedExecutionException e) {
			// No more wokers needed.
		}
	}
	
	boolean isConnected() {
		return scheduler.isConnected();
	}
	
	static boolean isQuit() {
		return quit;
	}

	public SocketConfigHolder getConfig(){
		return config;
	}
}
