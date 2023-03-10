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

import com.paypal.juno.io.protocol.MetaOperationMessage;
import com.paypal.juno.io.protocol.OperationMessage;
import com.paypal.juno.transport.socket.SocketConfigHolder;
import com.paypal.juno.util.JunoMetrics;
import com.paypal.juno.util.JunoStatusCode;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * To pass messages to IO worker threads.
 * 
 * Example: requestQueue = RequestQueue.getInstance(config);
 *          boolean ok = requestQueue.enqueue(req);
 *          
 */

class QueueEntry {
	ByteBuf msg;
	String reqId;
	long enqueueTime;
	
	QueueEntry(ByteBuf req, String id) {
		msg = req;
		reqId = id;
		enqueueTime = System.currentTimeMillis();
	}
}

public class RequestQueue {

	private static final Logger LOGGER = LoggerFactory.getLogger(RequestQueue.class);
	private static final int MAX_RECYCLE_ATTEMPT = 2;
	private static final int INITIAL_VALUE_RECYCLE_ATTEMPT = 0;
	private static final long RECYCLE_CONNECT_TIMEOUT = 180000; // 3 minutes
	private static final double INITIAL_AVERAGE_VALUE = 0.0;
	private static Map<InetSocketAddress, RequestQueue> reqMap = new Hashtable<InetSocketAddress, RequestQueue>();
	private final ConcurrentHashMap<Integer,BlockingQueue<OperationMessage>> opaqueRespQueueMap = new ConcurrentHashMap<Integer,BlockingQueue<OperationMessage>>();
	private final BlockingQueue<QueueEntry> queue =  new ArrayBlockingQueue<QueueEntry>(1000);
	private WorkerPool workerPool;
	private long responseTimeout;
	private Timer timer;
	private AtomicInteger failedAttempts = new AtomicInteger();
	private AtomicInteger successfulAttempts = new AtomicInteger();
	private double average;
	private static final double FAILURE_THRESHOLD = 0.3;
	private static final int INTERVAL = 1000;
	private static final int SAFETY_BUFFER = 30;
	private final PropertyChangeSupport changes = new PropertyChangeSupport(this);
	private int recycleAttempt = INITIAL_VALUE_RECYCLE_ATTEMPT;
	private long nextRecycleAttemptDue = System.currentTimeMillis();
//	@Autowired
//	InstanceLocation instanceLocation;
//	private final String INSTANCE_GEO_PP_US = "PP_US";
	
	/*
	 * Get a request queue based on proxy port.
	 */
	synchronized public static RequestQueue getInstance(SocketConfigHolder cfg) {
		
		InetSocketAddress inetAddress = InetSocketAddress.createUnresolved(cfg.getHost(), cfg.getPort());
		RequestQueue q = reqMap.computeIfAbsent(inetAddress, k -> new RequestQueue(cfg));
		return q;
	}
	
	public ConcurrentHashMap<Integer, BlockingQueue<OperationMessage>> getOpaqueResMap() {
		return opaqueRespQueueMap;
	}
	
	/*
	 * Shutdown all worker pools.
	 */
	synchronized public static void shutdown() {
		if (reqMap == null) {
			return;
		}
		
		for (Map.Entry<InetSocketAddress, RequestQueue> entry : reqMap.entrySet()) {
			entry.getValue().shutdownWorkerPool();
		}
		
		reqMap = null;
 	}
	
	/*
	 * Serialize an OperationMessage, and put it into the queue.
	 */
	public boolean enqueue(OperationMessage req) {
		
		ByteBuf out = Unpooled.buffer(req.getLength());
		req.writeBuf(out);
        		
		MetaOperationMessage mo = req.getMetaComponent();
		String requestId = "not_set";
		if (mo != null) {
			requestId = mo.getRequestIdString();
		}
		return enqueue(new QueueEntry(out, requestId));	
	}
	
	public boolean isConnected() {
		return workerPool.isConnected();
	}
	
	static void clear() {
		if (reqMap == null) {
			return;
		}
		
		reqMap.clear();
	}
	
	private RequestQueue(SocketConfigHolder cfg) {
		workerPool = new WorkerPool(cfg, this);
		responseTimeout = cfg.getResponseTimeout();
		if(cfg.getReconnectOnFail()){
			timer = new Timer();
			TimerTask task = new TimerTask() {
				@Override
				public void run() {
					checkForUnresponsiveConnection(successfulAttempts.getAndSet(0), failedAttempts.getAndSet(0));
				}
			};
			timer.schedule(task, INTERVAL, INTERVAL);
		}
	}

	private void checkForUnresponsiveConnection(int success, int failed){
		int totalAttempts = success + failed + SAFETY_BUFFER;
		double alpha = 0.1;
		double currentValue = ((double)failed/totalAttempts);
		double ema = alpha * currentValue + (1 - alpha) * average;
		average = Double.isNaN(ema) || Double.isInfinite(ema) ? average : ema;
		if(reconnectOnFailEnabled() && average >= FAILURE_THRESHOLD){
			recycleAttempt++;
			nextRecycleAttemptDue = System.currentTimeMillis();
			changes.firePropertyChange("recycleNow", -1, 0);
		}
	}

	private boolean reconnectOnFailEnabled() {
		if(System.currentTimeMillis() < nextRecycleAttemptDue) {
			return false;
		}

		if(recycleAttempt >= MAX_RECYCLE_ATTEMPT){
			changes.firePropertyChange("RECYCLE_CONNECT_TIMEOUT", -1, 1);
			nextRecycleAttemptDue += RECYCLE_CONNECT_TIMEOUT;
			recycleAttempt = INITIAL_VALUE_RECYCLE_ATTEMPT;
			return false;
		}

		return true;
	}

	private void shutdownWorkerPool() {
		workerPool.shutdown();
	}
	
	/*
	 * For IO workers to dequeue a request message.
	 */
	QueueEntry dequeue() throws InterruptedException {	
		QueueEntry entry = queue.poll(1, TimeUnit.SECONDS); 
		if (entry == null) {
			return null;
		}
		
		long due = entry.enqueueTime + responseTimeout;
		if (System.currentTimeMillis() > due) {
			LOGGER.error(
			"Expired requests got discarded. "+"req_id="+entry.reqId+
			" enqueue_time="+entry.enqueueTime+" resp_timeout="+responseTimeout);
			final Map<String,CharSequence> trans = new HashMap<String, CharSequence>();
			trans.put("name","JUNO_REQUEST_EXPIRED");
			trans.put("req_id", entry.reqId);
			trans.put("enqueue_time", Long.toString(entry.enqueueTime));
			trans.put("resp_timeout", Long.toString(responseTimeout));
			trans.put("status", JunoStatusCode.WARNING.toString());
			LOGGER.warn(JunoStatusCode.WARNING + " {} ", trans);
			JunoMetrics.recordErrorCount("JUNO_REQUEST_EXPIRED",workerPool.getConfig().getHost()+":"+workerPool.getConfig().getPort(),JunoMetrics.ERROR);
			return null;
		}
		return entry; 
	}
	
	boolean enqueue(QueueEntry msg) {
		try {
			boolean ok = queue.offer(msg);
			if (ok) {
				return true;
			}

			LOGGER.error("Outbound queue is full.");
			return false;
			
		} catch (Exception e) {
			LOGGER.error("Adding message to request queue:"+e.toString());
			return false;
		}	
	}
	
	int size() {
		return queue.size();
	}

	public void addPropertyChangeListener(PropertyChangeListener l) {
		changes.addPropertyChangeListener(l);
	}

	public void incrementFailedAttempts() { failedAttempts.incrementAndGet(); }

	public void incrementSuccessfulAttempts() { successfulAttempts.incrementAndGet(); }

	public void resetValues() {
		average = INITIAL_AVERAGE_VALUE;
	}

	public double getAverage(){
		return average;
	}
}
