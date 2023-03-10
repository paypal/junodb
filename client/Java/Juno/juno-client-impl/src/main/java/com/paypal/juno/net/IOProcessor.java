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
import com.paypal.juno.util.JunoLogLevel;
import com.paypal.juno.util.JunoMetrics;
import com.paypal.juno.util.JunoStatusCode;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.JdkSslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.Future;
import java.net.InetAddress;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * To process IO messages sent to and received from proxy servers.
 * 
 *
 */

class IOProcessor extends BaseProcessor implements Runnable {

	private static final Logger LOGGER = LoggerFactory.getLogger(IOProcessor.class);
	
	private static AtomicInteger counter = new AtomicInteger();
	private static final int INITIAL_BYPASSLTM_RETRY_INTERVAL = 337500;
	private int bypassLTMRetryInterval = INITIAL_BYPASSLTM_RETRY_INTERVAL;
	private static final int MAX_BYPASSLTM_RETRY_INTERVAL = 86400000;
	private int id;
	private SocketConfigHolder config;
	private RequestQueue requestQueue;
	private Scheduler scheduler;

	private Bootstrap bootstrap;
	private Channel ch;
	private int handshakeFail = 0;
	private String serverAddr = "";
	
	private boolean recycleStarted = false;
	// stats per connect session.
	private int sendCount = 0;
	private int failCount = 0;
	private AtomicInteger recvCount = new AtomicInteger();
	
	private final int INIT_WAIT_TIME = 200;
	private final int MAX_WAIT_TIME = 60000;

	private int reconnectWaitTime = INIT_WAIT_TIME;
	private long nextReconnectDue;
	private long nextByPassLTMCheckTime = System.currentTimeMillis();
	private final SecureRandom ran = new SecureRandom();
	private ConcurrentHashMap<Integer,BlockingQueue<OperationMessage>> opaqueRespQueueMap;
	private SSLContext ctx;
	private String remoteConfigAddr;
	private String remoteIpAddr;
	
	private int lockIndex = -1;
	private long ownerId = -1;
	private boolean reconnectNow = false;
	
	enum Status {
		CONNECT_FAIL,
		WAIT_FOR_MESSAGE,
		SENT_DONE
	}
	
	IOProcessor(SocketConfigHolder config, RequestQueue queue, Scheduler scheduler, SSLContext ctx,
			ConcurrentHashMap<Integer,BlockingQueue<OperationMessage>> opaqueRespQueueMap) {
		this.id = counter.getAndIncrement();
		this.config = config;
		this.requestQueue = queue;
		this.nextReconnectDue = Long.MAX_VALUE;
		this.scheduler = scheduler;
		this.opaqueRespQueueMap = opaqueRespQueueMap;
		this.ctx = ctx;
		this.remoteConfigAddr = getHost()+":"+getPort();
		this.remoteIpAddr = this.remoteConfigAddr;
		requestQueue.addPropertyChangeListener(evt -> {
			if(evt.getPropertyName().equals("recycleNow")) {
				this.reconnectNow = true;
			}
		});
	}
	
	private String getHost() {
		return config.getHost();
	}
	
	private int getPort() {
		return config.getPort();
	}
	
	private int getConnectTimeout(){
		return config.getConnectTimeout();
	}
	
	boolean useLTM() {
		if (getHost().equals("127.0.0.1")) {
			return true;
		}
		return !config.getBypassLTM();
	}

	private boolean isBypassLTMDisabled() {
		long currentTime= System.currentTimeMillis();
		if (currentTime > nextByPassLTMCheckTime && bypassLTMRetryInterval < MAX_BYPASSLTM_RETRY_INTERVAL) {
			return false;
		}
		return true;
	}
	
	void putResponse(OperationMessage opMsg) {
		int opaq = opMsg.getHeader().getOpaque();

		BlockingQueue<OperationMessage> respQueue = opaqueRespQueueMap.get(opaq);
		if (respQueue == null) {
			LOGGER.debug("The response queue for opaque="+opaq+" no longer exists.  Probably response timed out.");
			final Map<String,CharSequence> trans = new HashMap<String, CharSequence>();
			trans.put("name", "JUNO_LATE_RESPONSE");
			trans.put("server", serverAddr);
			trans.put("req_id", opMsg.getMetaComponent().getRequestIdString());
			trans.put("opaque", String.valueOf(opaq));
			trans.put("ns", config.getRecordNamespace());
			trans.put("w", ""+config.getConnectionPoolSize());
			trans.put("rht", ""+opMsg.getMetaComponent().getRequestHandlingTime());
			trans.put("status", JunoStatusCode.ERROR.toString());
			LOGGER.error("Error {} ", trans);
			JunoMetrics.recordErrorCount("JUNO_LATE_RESPONSE",remoteIpAddr,JunoMetrics.ERROR);
			return;
		}
		
		try {
			scheduler.onException(TestEvent.EXCEPTION_3);
			boolean ok = respQueue.offer(opMsg);
			if (!ok) {
				LOGGER.error("Response queue is full.");
					final Map<String,CharSequence> trans = new HashMap<String, CharSequence>();
					trans.put("name", "JUNO_RESPONSE_QUEUE_FULL");
					trans.put("server", serverAddr);
					trans.put("req_id", opMsg.getMetaComponent().getRequestIdString());
					trans.put("status",JunoStatusCode.ERROR.toString());
					LOGGER.error(" Error : {}", trans);
				JunoMetrics.recordErrorCount("JUNO_RESPONSE_QUEUE_FULL",remoteIpAddr,JunoMetrics.ERROR);
			}
		} catch (Exception e) {
			LOGGER.error("Adding response to response queue: "+e.getMessage());
			final Map<String,CharSequence> trans = new HashMap<String, CharSequence>();
			trans.put("name", "JUNO_RETURN_RESPONSE");
			trans.put("server", serverAddr);
			trans.put("req_id", opMsg.getMetaComponent().getRequestIdString());
			trans.put("error", e.toString());
			trans.put("status",JunoStatusCode.ERROR.toString());
			LOGGER.error(" Error : {}", trans);
			JunoMetrics.recordErrorCount("JUNO_RETURN_RESPONSE",remoteIpAddr,e.getClass().getName());
		}
	}
	
	boolean onEvent(TestEvent event) {
		return scheduler.onEvent(event);
	}
	
	void incrementRecvCount() {
		recvCount.incrementAndGet();
	}

	// This method return the actual Juno box IP when Bypass LTM feature is enabled, else it returns VIP IP:Port
	String getServerAddr() {
		return serverAddr;
	}

	// This method returns the VIP IP:Port
	String getRemoteIpAddr(){
		return remoteIpAddr;
	}

	String getRaddr(Channel chan) {
		String remote = chan.remoteAddress().toString();
		int off = remote.indexOf("/") + 1;
		return remote.substring(off);
	}
	
	void validateMsgCount() {
		int numRecv = recvCount.get();
		if (sendCount <= numRecv && !scheduler.onEvent(TestEvent.MISSING_RESPONSE)) {
			recycleStarted = false;
			return;
		}
		
		String text = "send_count="+sendCount+" fail_count="+failCount+" recv_count="+numRecv+" connection_lost="+!recycleStarted;
		
		LOGGER.error("Missing response: "+text);
		final Map<String,CharSequence> trans = new HashMap<String, CharSequence>();
		trans.put("name", "JUNO_MISSING_RESPONSE");
		trans.put("server", serverAddr);
		trans.put("send_count", Long.toString(sendCount));
		trans.put("fail_count", Long.toString(failCount));
		trans.put("recv_count", Long.toString(numRecv));
		trans.put("connection_lost", Boolean.toString(!recycleStarted));
		trans.put("status",JunoStatusCode.ERROR.toString());
		LOGGER.error(" Error : {}", trans);
		JunoMetrics.recordErrorCount("JUNO_MISSING_RESPONSE",remoteIpAddr,(!recycleStarted)?"connection_recycle":"connection_lost");
		
		recycleStarted = false;
	}
	
	private JdkSslContext getSSLContext() {
		JdkSslContext nettyCtx = new JdkSslContext(ctx, true, ClientAuth.NONE); 
		return nettyCtx;
	}
	
	private void disconnect(Channel other) throws InterruptedException {
		if (!isOpened()) {
			return;
		}
		
		recycleStarted = true;
		Channel chan = this.ch;
		if (other != null) {
			recycleStarted = false;
			chan = other;
		}
		
		String raddr = getRaddr(chan); 
		
		chan.close().awaitUninterruptibly();
		JunoLogLevel level = scheduler.getDisconnectLogLevel(ownerId);
		if(level != JunoLogLevel.OFF) {
			LOGGER.info(String.valueOf(scheduler.getDisconnectLogLevel(ownerId)), "Closed connection to " + raddr);
		}
		final Map<String,CharSequence> trans = new HashMap<String, CharSequence>();
		trans.put("name", remoteIpAddr);
		trans.put("framework", "juno");
		if (!remoteIpAddr.equals(raddr)) {
			trans.put("usePingIP", "true");
		}
		trans.put("raddr", raddr);
		trans.put("id", getConnectID());
		trans.put("status",JunoStatusCode.SUCCESS.toString());
		LOGGER.info(JunoStatusCode.SUCCESS + " {} ", trans);
		
		if (WorkerPool.isQuit()) {
			throw new InterruptedException("Interrupted");
		}

		reconnectNow = false;
		requestQueue.resetValues();
	}
	
	private boolean isOpened() {
		return (ch != null && ch.isActive());
	}
	
	private boolean isSSL() {
		return (ch.pipeline().get(SslHandler.class) != null);
	}
	
	private void setConnectOwner() {
		ownerId = scheduler.setConnectOwner(lockIndex);
	}
	
	private String getConnectID() {
		return Long.toString(lockIndex) + "_" + Long.toString(ownerId & 0x1);
	}
	
	private void afterConnect() {

		reconnectWaitTime = INIT_WAIT_TIME;	
		setNextReconnectDue();
		
		sendCount = 0;
		failCount = 0;
		recvCount.set(0);
		recycleStarted = false;
	}
	
	private boolean connect() throws InterruptedException {
		
		if (isOpened() && handshakeFail <= 0) {
			return true;  // already connected and no handshake failure
		}
		
		lockIndex = scheduler.acquireConnectLock(lockIndex);
		JunoLogLevel level = scheduler.getConnectLogLevel(lockIndex);
		
		// Check if any connection is ready for this channel.
		boolean notConnected = !scheduler.isIndexedChannelConnected(lockIndex);
		
		boolean ok = ipConnect(null, level);
		if (!ok) {
			return false;
		}
		
		handshakeFail = 0;
		if (notConnected || useLTM() || isBypassLTMDisabled()) {
			setConnectOwner();
			return true;
		}
		
		// Send a Nop to get server ip.
		PingMessage req = new PingMessage(null, 0);
		ByteBuf out = req.pack();
		clearPingRespQueue();	
		ChannelFuture f = ch.writeAndFlush(out.retain().duplicate()).awaitUninterruptibly();
		out.release();
		
		if (WorkerPool.isQuit()) {
			throw new InterruptedException("Interrupted");
		}
		
		if (!f.isDone() || !f.isSuccess()) { 
			return true; 				
		}
		
		InetAddress ip = getPingIp();
		if (ip == null) {
			return true;
		}
		
		Channel old = this.ch;
		this.ch = null;
		
		ok = ipConnect(ip, level);
		if (ok) {
			LOGGER.debug("connected via ping ip="+ip.toString());
			disconnect(old);
			old = null;
			bypassLTMRetryInterval = INITIAL_BYPASSLTM_RETRY_INTERVAL;
			nextByPassLTMCheckTime = System.currentTimeMillis();
		} else {
			this.ch = old;
			nextByPassLTMCheckTime += bypassLTMRetryInterval;

			if(bypassLTMRetryInterval < MAX_BYPASSLTM_RETRY_INTERVAL){
				bypassLTMRetryInterval = bypassLTMRetryInterval * 2;
			}

		}
		
		setConnectOwner();
		
		return true;
	}
	
	// Connect and optionally do ssl handshake.
	private boolean ipConnect(InetAddress ip, JunoLogLevel level) throws InterruptedException {

		final Map<String,CharSequence> trans = new HashMap<String, CharSequence>();
		long startTimeInMs = System.currentTimeMillis();
		boolean handshakeStarted = false;
		
		try {
			if (!isOpened()) {
				
				bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, getConnectTimeout());

				trans.put("framework", "juno");
				int qsize = requestQueue.size();
				trans.put("cfgAddr", remoteConfigAddr);
				trans.put("qsize", Long.toString(qsize));
				
				// DNS lookup
				if (ip == null) {
					long start = System.currentTimeMillis();
					try {
						ip = InetAddress.getByName(getHost());
					}catch(Exception e){
						trans.put("name", getHost()+":"+getPort());
						throw e;
					}
					String remote = ip.toString();
					int off = remote.indexOf("/") + 1;
					remoteIpAddr = remote.substring(off)+":"+getPort();
					
					long duration = System.currentTimeMillis() - start;
				
					if (duration >= 500 || scheduler.onEvent(TestEvent.DNS_DELAY)) { // ms
						final Map<String,CharSequence> transTxn = new HashMap<String, CharSequence>();
						transTxn.put("name", "JUNO_DNS_DELAY");
						transTxn.put("ip", ip.toString());
						transTxn.put("status",JunoStatusCode.WARNING.toString());
						transTxn.put("duration", String.valueOf(duration));
						LOGGER.warn(JunoStatusCode.WARNING + " {} ", transTxn);
						JunoMetrics.recordTimer("JUNO_DNS_DELAY",ip.toString(),JunoMetrics.WARNING,duration);
					}
				} else {
					trans.put("usePingIP", "true");
				}

				trans.put("name", getHost()+":"+getPort());

				this.ch = bootstrap.connect(ip, getPort()).sync().channel();
				
				String local = ch.localAddress().toString();
				int off = local.indexOf("/") + 1;
				trans.put("laddr", local.substring(off));
				
				serverAddr = getRaddr(ch);

				String str = serverAddr + "&w=" + Long.toString(config.getConnectionPoolSize());
				trans.put("raddr", str);
				trans.put("id", getConnectID());

				int numRecv = recvCount.get();
				String qsizeStr = "";
				if (qsize > 0) {
					qsizeStr = " qsize="+Long.toString(qsize);
				}

				org.slf4j.event.Level eventLevel;
				if (sendCount > 0 || failCount > 0) {
					if (sendCount > numRecv || failCount > 0) {
						eventLevel = org.slf4j.event.Level.INFO;
					} else {
						eventLevel = org.slf4j.event.Level.DEBUG;
					}
					LOGGER.atLevel(eventLevel).log("Connected to "+serverAddr+qsizeStr+ " send_count="+sendCount+" fail_count="+failCount+" recv_count="+numRecv);
					trans.put("send_count", Long.toString(sendCount));
					trans.put("fail_count", Long.toString(failCount));
					trans.put("recv_count", Long.toString(numRecv));
				} else {
					if (level != JunoLogLevel.OFF) {
						LOGGER.info("Connected to " + serverAddr + qsizeStr);
					}
				}
				
				if (isSSL()) {
					afterConnect();
				}
			}
					
			if (scheduler.isTestMode()) {
				scheduler.onException(TestEvent.INTERRUPTED);
				scheduler.onException(TestEvent.EXCEPTION);
			}
			
			if (!isSSL()) { // non-ssl				
				trans.put("status",JunoStatusCode.SUCCESS.toString());
				LOGGER.info(JunoStatusCode.SUCCESS + " {} ", trans);
				// Log Metrics timer
				//JunoMetrics.recordTimer("CONNECT",getHost()+":"+getPort(),JunoMetrics.SUCCESS,System.currentTimeMillis() - startTimeInMs);
				JunoMetrics.recordConnectCount(remoteIpAddr,JunoMetrics.SUCCESS,"none");
				
				if (!scheduler.isConnected()) {
					int wait = getConnectTimeout();
					if (wait > 1000) {
						wait = 1000;
					}
					Thread.sleep(wait);
				}
				
				if (!isOpened() || scheduler.onEvent(TestEvent.CONNECTION_LOST)) {
					scheduler.setDisconnected(lockIndex, ownerId);
					LOGGER.warn("Connection closed by server.");
					final Map<String,CharSequence> subTrans = new HashMap<String, CharSequence>();
					subTrans.put("name", "JUNO_CONNECTION_LOST");
					subTrans.put("server", serverAddr);
					subTrans.put("error", "connection_closed_by_server");
					subTrans.put("status",JunoStatusCode.ERROR.toString());
					LOGGER.error(" Error : {}", subTrans);
					JunoMetrics.recordErrorCount("JUNO_CONNECTION_LOST",remoteIpAddr,"connection_closed_by_server");
					return false;
				}
				
				handshakeFail = 0;
				afterConnect();
				
				return true; 
			}
			
			// ssl handshake
			SslHandler sslHandler = this.ch.pipeline().get(SslHandler.class);
			handshakeStarted = true;
			//sslHandler.engine().setEnableSessionCreation(false);
			Instant start = Instant.now();
			Future<Channel> f = sslHandler.handshakeFuture().sync();
			long elapsed = Duration.between(start, Instant.now()).toMillis();
			
			// succeeded.
			handshakeFail = 0;
			if(level != JunoLogLevel.OFF) {
				LOGGER.info("SSL handshake successfully completed.");
			}
			
			SSLSession sess = sslHandler.engine().getSession();
			String cipher = "unknown";
			String ver="unknown";
			if (sess != null) {
				cipher = sess.getCipherSuite();
				ver = sess.getProtocol();
			}
			String sslInfo = elapsed+"&cipher="+cipher+"&ver="+ver;
			trans.put("handshake_ms", sslInfo);

			trans.put("status",JunoStatusCode.SUCCESS.toString());
			LOGGER.info(JunoStatusCode.SUCCESS + " {} ", trans);
			//Log Metrics timer - Its will be logged by Framework
			//JunoMetrics.recordTimer("CONNECT",getHost()+":"+getPort(),JunoMetrics.SUCCESS,System.currentTimeMillis() - startTimeInMs);
			JunoMetrics.recordConnectCount(remoteIpAddr,JunoMetrics.SUCCESS,"none");

			Thread.sleep(1000);
			return true;
			
		} catch (InterruptedException e) {
			trans.put("error", "Interrupted");
			trans.put("status",JunoStatusCode.ERROR.toString());
			LOGGER.error(" Error : {}", trans);
			//Log Metrics timer - Its will be logged by Framework
			//JunoMetrics.recordTimer("CONNECT",getHost()+":"+getPort(),JunoMetrics.ERROR,System.currentTimeMillis() - startTimeInMs);
			JunoMetrics.recordConnectCount(remoteIpAddr,JunoMetrics.ERROR,"InterruptedException");
			throw e;
			
		} catch (Exception e) {
			scheduler.setDisconnected(lockIndex, ownerId);
			
			String err;
			if (handshakeStarted) {
				err = "SSL handshake with " + serverAddr + " failed: " + e.toString();
				handshakeFail++;
				JunoMetrics.recordConnectCount(remoteIpAddr,JunoMetrics.ERROR,"Handshake_failure");
			} else {
				err = "Connect to "+getHost()+":"+getPort() + " failed. Config Timeout :" + getConnectTimeout() + ". " + e.toString();
				JunoMetrics.recordConnectCount(remoteIpAddr,JunoMetrics.ERROR,e.getClass().getName());
			}

			LOGGER.error(err);
			trans.put("error", err);
			trans.put("status",JunoStatusCode.ERROR.toString());
			LOGGER.error(" Error : {}", trans);
			//Log Metrics timer
			//JunoMetrics.recordTimer("CONNECT",getHost()+":"+getPort(),JunoMetrics.ERROR,System.currentTimeMillis() - startTimeInMs);

			if (handshakeStarted && handshakeFail > 2) { 
				// Add backoff time before retry.
				int shift = handshakeFail - 2;
				if (shift > 9) {
					shift = 9;
				}
				int wait = 20 << shift;
				Thread.sleep(wait);
			}
			
			if (isOpened()){
				try {
					ch.close().sync();
				} catch (InterruptedException ex) {
					throw ex;			
				} catch (Exception ex) {
				}
			}
			
			return false;
		}
	}
	
	// Send messages to proxy.
	private Status send() throws InterruptedException {
		
		if (!connect()) {
			return Status.CONNECT_FAIL;
		}
		
		// Fetch a message.
		QueueEntry entry = requestQueue.dequeue();     	
		if (entry == null) {
			return Status.WAIT_FOR_MESSAGE;
		}
		
		ByteBuf msg = entry.msg;
		if (!connect()) {
			boolean ok = requestQueue.enqueue(entry);
			LOGGER.info("requeue="+ok+" server="+remoteConfigAddr);
			return Status.CONNECT_FAIL;
		}
			
		// Flush the message.
		ChannelFuture f = ch.writeAndFlush(msg.retain().duplicate()).awaitUninterruptibly();
		
		if (f.isDone() && f.isSuccess() && !scheduler.onEvent(TestEvent.SEND_FAIL)) { // succeeded
			sendCount++;
			LOGGER.debug("netty send ok.  server="+serverAddr);
		} else { // failed
			failCount++;
			Throwable cause = f.cause();
			if (cause == null) {
				cause = new RuntimeException("netty failure.");	
			}
				
			OperationMessage op = new OperationMessage();
			op.readBuf(msg);
			MetaOperationMessage mo = op.getMetaComponent();
			String requestId = "not_set";
			String corrId = "not_set";
			if (mo != null) {
				requestId = mo.getRequestIdString();
				corrId = mo.getCorrelationIDString();
			}
				
			LOGGER.error("server="+serverAddr+" req_id="+requestId+" corr_id="+corrId+" Send failed:"+cause);

				final Map<String,CharSequence> trans = new HashMap<String, CharSequence>();
				trans.put("name", "JUNO_SEND");
				trans.put("server", serverAddr);
				trans.put("req_id", requestId);
				trans.put("corr_id_", corrId);
				trans.put("error", cause.toString());
				trans.put("status",JunoStatusCode.ERROR.toString());
				LOGGER.error(" Error : {}", trans);

			JunoMetrics.recordErrorCount("JUNO_SEND",remoteIpAddr,cause.getClass().getName());
		}
		
		msg.release();
        
		if (WorkerPool.isQuit()) {
			if (f.isSuccess()) {
				final int DELAY = 2 * config.getResponseTimeout();   // milliseconds
				Thread.sleep(DELAY);
			}
			throw new InterruptedException("Interrupted");
		}
        
		return Status.SENT_DONE;  
	}
	
	private Bootstrap configBootstrap(EventLoopGroup g) {
		bootstrap = new Bootstrap();
		
		bootstrap.group(g).channel(NioSocketChannel.class);		
		bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 
				         config.getConnectTimeout())
		 	.option(ChannelOption.SO_KEEPALIVE, true)
		 	.option(ChannelOption.TCP_NODELAY,  true);
			
		bootstrap.handler(new ChannelInitializer<SocketChannel>() {
      	  
			@Override
			public void initChannel(SocketChannel ch) throws Exception {
            	  
				ChannelPipeline pipeline = ch.pipeline();
				if (config.useSSL()) {
					JdkSslContext sslContext = getSSLContext();
					//System.out.println("SSL session  cache size:"+sslContext.context().getProtocol());
					pipeline.addLast(sslContext.newHandler(ch.alloc()));
				}
                  
				pipeline
					.addLast("decoder",  new MessageDecoder())                     
					.addLast("handler", new ClientHandler(IOProcessor.this));
			}
		});
		
		return bootstrap;
	}
	
	private void setNextReconnectDue() {
		nextReconnectDue = scheduler.selectTimeSlot();
	}
	
	private boolean reconnectDue() {
		return System.currentTimeMillis() > nextReconnectDue;
	}
	
	private boolean recycleNow() throws InterruptedException {
		
		if (!reconnectDue()) {
			return false;
		}
		
		if (!scheduler.connectOwnerExpired(lockIndex, ownerId)){
			return false;  // The other worker has not connected yet.
		}
		
		final int DELAY = (int)(2 * config.getResponseTimeout());   // milliseconds
		Thread.sleep(DELAY);	
		LOGGER.debug("Recycle connection ...");
		return true;
	}
	
	public void run() {
		
		LOGGER.info("Start worker_" + this.id);
		EventLoopGroup clientGroup = new NioEventLoopGroup(2, 
				new DefaultThreadFactory("JunoNioEventLoop", true));
		boolean logOnce = scheduler.isConnected();
				
		try{
			bootstrap = configBootstrap(clientGroup);	
			while (true) {
				
				if (send() == Status.CONNECT_FAIL) {
            		
					// Wait a little before reconnect.
					Thread.sleep(reconnectWaitTime);
					reconnectWaitTime *= 2;
					if (reconnectWaitTime > MAX_WAIT_TIME) {
						reconnectWaitTime = MAX_WAIT_TIME;
					}
					// Add random adjustment.
					reconnectWaitTime *= (1 + 0.3 * ran.nextFloat());
				} else if (!logOnce && lockIndex == 0 && ownerId <= 2) {
					
					String tail = " (tcp)"; 
					if (isSSL()) {
						tail = "";
					}
					LOGGER.info("Connected to "+serverAddr+tail);
					logOnce = true;
				}
				
				// Recycle connection if due
				if (recycleNow() || reconnectNow) {
					disconnect(null);
					if (WorkerPool.isQuit()) {
						return;
					}
				}
				
				if (scheduler.isTestMode()) {
					scheduler.onException(TestEvent.INTERRUPTED_2);
					scheduler.onException(TestEvent.EXCEPTION_2);
				}
			}
		} catch (InterruptedException e) {
			LOGGER.debug("Worker got interrupted.");
			Thread.currentThread().interrupt();
			return;
		} catch (Exception e) {
			LOGGER.error(e.getMessage());

				final Map<String,CharSequence> trans = new HashMap<String, CharSequence>();
				trans.put("name", "JUNO_IO");
				trans.put("server", remoteConfigAddr);
				trans.put("error", e.toString());
				trans.put("status",JunoStatusCode.ERROR.toString());
				LOGGER.error(" Error : {}", trans);

			JunoMetrics.recordErrorCount("JUNO_IO",remoteIpAddr,e.getClass().getName());
		} finally {
			LOGGER.info("Shutdown worker_" + this.id);
			// Release the connect lock if any.
			scheduler.connectOwnerExpired(lockIndex, ownerId);
			clientGroup.shutdownGracefully();
			
			try { // terminate threads
				clientGroup.terminationFuture().sync();
			} catch (Exception e) {
			}
		}
	}	
}
