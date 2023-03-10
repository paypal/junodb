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

import com.paypal.juno.client.JunoClientConfigHolder;
import com.paypal.juno.client.JunoReactClient;
import com.paypal.juno.client.io.JunoRequest;
import com.paypal.juno.client.io.JunoResponse;
import com.paypal.juno.client.io.OperationStatus;
import com.paypal.juno.client.io.RecordContext;
import com.paypal.juno.exception.JunoException;
import com.paypal.juno.io.protocol.JunoMessage;
import com.paypal.juno.io.protocol.OperationMessage;
import com.paypal.juno.net.RequestQueue;
import com.paypal.juno.transport.socket.SocketConfigHolder;
import com.paypal.juno.util.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.SSLContext;
import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class JunoReactClientImpl implements JunoReactClient {
    private final JunoClientConfigHolder configHolder;
    private final static AtomicInteger opaqueGenarator = new AtomicInteger();
    private final RequestQueue reqQueue;
    private final ConcurrentHashMap<Integer, BlockingQueue<OperationMessage>> opaqueResMap;
    ExecutorService executor;
    private boolean isAsync = true;
    private static final int MAX_RETRY=1;
    private static final int MAX_RETRY_INTERVAL=90;
    private static final int MIN_RETRY_INTERVAL=10;

//    @Autowired
//    InstanceLocation instanceLocation;
//    private final String INSTANCE_GEO_PP_US = "PP_US";
    /**
     * The logger. We make this a non-static member in order to prevent this
     * from being synchronized.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(JunoReactClientImpl.class);

    JunoReactClientImpl(JunoClientConfigHolder config, SSLContext ctx, boolean isAsync){
        this(config,ctx);
        this.isAsync = isAsync;
    }

    protected JunoReactClientImpl(JunoClientConfigHolder config,SSLContext ctx){
        this.configHolder = config;
        SocketConfigHolder socCfg = new SocketConfigHolder(configHolder);
        socCfg.setCtx(ctx);
        reqQueue = RequestQueue.getInstance(socCfg);
        opaqueResMap = reqQueue.getOpaqueResMap();
        executor = Executors.newCachedThreadPool();
    }

    /**
     * Insert a record into Juno DB with default TTL
     * @param key - Key of the record to be Inserted
     * @param value - Record Value
     * @return Single<JunoResponse> - Emits a single response or Error with processing the request
     * @throws JunoException - Throws Exception if any exception while processing the request
     */
    public Mono<JunoResponse> create(byte[] key, byte[] value) throws JunoException {
        try{
            final JunoRequest req = new JunoRequest(key,value,0,JunoRequest.OperationType.Create);
            return processSingle(req, JunoMessage.OperationType.Create);
        }catch(JunoException je){
            throw je;
//        }catch (IllegalArgumentException iae) {
//            throw new JunoException(OperationStatus.IllegalArgument.getErrorText(),iae);
        } catch(Exception e){
            throw new JunoException(e.getMessage());
        }
    }

    /**
     * Insert a record into Juno DB with user supplied TTL
     * @param key - Key of the record to be Inserted
     * @param value - Record Value
     * @param timeToLiveSec - Time to Live for the record
     * @return Mono<JunoResponse> - Emits a single response or Error with processing the request
     * @throws JunoException - Throws Exception if any exception while processing the request
     */
    public Mono<JunoResponse> create(byte[] key, byte[] value, long timeToLiveSec) throws JunoException {
        try{
            final JunoRequest req = new JunoRequest(key,value,0,timeToLiveSec,JunoRequest.OperationType.Create);
            return processSingle(req, JunoMessage.OperationType.Create);
        }catch(JunoException je){
            throw je;
//        }catch (IllegalArgumentException iae) {
//            throw new JunoException(OperationStatus.IllegalArgument.getErrorText(),iae);
        }catch(Exception e){
            throw new JunoException(e.getMessage());
        }
    }

    /**
     * Get a record from Juno DB
     * @param key - Key of the record to be retrieved
     * @return Mono<JunoResponse> - Emits a single response with the record value or Error
     * @throws JunoException - Throws Exception if any issue while processing the request
     */
    public Mono<JunoResponse> get(byte[] key) throws JunoException {
        try{
            final JunoRequest req = new JunoRequest(key,0,0,JunoRequest.OperationType.Get);
            return processSingle(req, JunoMessage.OperationType.Get);
        }catch(JunoException je){
            throw je;
//        }catch (IllegalArgumentException iae) {
//            throw new JunoException(OperationStatus.IllegalArgument.getErrorText(),iae);
        }catch(Exception e){
            throw new JunoException(e.getMessage());
        }
    }

    /**
     * Get a record from Juno DB and Extend the TTL
     * @param key - Key of the record to be retrieved
     * @param timeToLiveSec - Time to Live for the record
     * @return Mono<JunoResponse> - Emits a single response or Error with processing the request
     * @throws JunoException - Throws Exception if any exception while processing the request
     */
    public Mono<JunoResponse> get(byte[] key, long timeToLiveSec) throws JunoException {
        try{
            final JunoRequest req = new JunoRequest(key,0,timeToLiveSec,JunoRequest.OperationType.Get);
            return processSingle(req, JunoMessage.OperationType.Get);
        }catch(JunoException je){
            throw je;
//        }catch (IllegalArgumentException iae) {
//            throw new JunoException(OperationStatus.IllegalArgument.getErrorText(),iae);
        }catch(Exception e){
            throw new JunoException(e.getMessage());
        }
    }

    /**
     * Update a record in Juno DB
     * @param key - Key of the record to be Updated
     * @param value - Record Value
     * @return Mono<JunoResponse> - Emits a single response or Error with processing the request
     * @throws JunoException - Throws Exception if any issue while processing the request
     */
    public Mono<JunoResponse> update(byte[] key, byte[] value) throws JunoException {
        try{
            final JunoRequest req = new JunoRequest(key,value,0,0,JunoRequest.OperationType.Update);
            return processSingle(req, JunoMessage.OperationType.Update);
        }catch(JunoException je){
            throw je;
//        }catch (IllegalArgumentException iae) {
//            throw new JunoException(OperationStatus.IllegalArgument.getErrorText(),iae);
        }catch(Exception e){
            throw new JunoException(e.getMessage());
        }
    }

    /**
     * Update a record in Juno DB and Extend its TTL
     * @param key - Key of the record to be Updated
     * @param value - Record Value
     * @param timeToLiveSec - Time to Live for the record
     * @return Mono<JunoResponse> - Emits a single response or Error with processing the request
     * @throws JunoException - Throws Exception if any exception while processing the request
     */
    public Mono<JunoResponse> update(byte[] key, byte[] value, long timeToLiveSec) throws JunoException {
        try{
            final JunoRequest req = new JunoRequest(key,value,0,timeToLiveSec,JunoRequest.OperationType.Update);
            return processSingle(req, JunoMessage.OperationType.Update);
        }catch(JunoException je){
            throw je;
//        }catch (IllegalArgumentException iae) {
//            throw new JunoException(OperationStatus.IllegalArgument.getErrorText(),iae);
        }catch(Exception e){
            throw new JunoException(e.getMessage());
        }
    }

    /**
     * Update the record if present in Juno DB else create that record with the default TTL in the configuration
     * @param key - Key of the record to be Upserted
     * @param value - Record Value
     * @return Mono<JunoResponse> - Emits a single response or Error with processing the request
     * @throws JunoException - Throws Exception if any exception while processing the request
     */
    public Mono<JunoResponse> set(byte[] key, byte[] value) throws JunoException {
        try{
            final JunoRequest req = new JunoRequest(key,value,0,JunoRequest.OperationType.Set);
            return processSingle(req, JunoMessage.OperationType.Set);
        }catch(JunoException je){
            throw je;
//        }catch (IllegalArgumentException iae) {
//            throw new JunoException(OperationStatus.IllegalArgument.getErrorText(),iae);
        }catch(Exception e){
            throw new JunoException(e.getMessage());
        }
    }

    /**
     * Update the record if present in Juno DB and extend its TTL else create that record with the supplied TTL.
     * @param key - Key of the record to be Upserted
     * @param value - Record Value
     * @param timeToLiveSec - Time to Live for the record
     * @return Mono<JunoResponse> - Emits a single response or Error with processing the request
     * @throws JunoException - Throws Exception if any exception while processing the request
     */
    public Mono<JunoResponse> set(byte[] key, byte[] value, long timeToLiveSec) throws JunoException {
        try{
            final JunoRequest req = new JunoRequest(key,value,0,timeToLiveSec,JunoRequest.OperationType.Set);
            return processSingle(req, JunoMessage.OperationType.Set);
        }catch(JunoException je){
            throw je;
//        }catch (IllegalArgumentException iae) {
//            throw new JunoException(OperationStatus.IllegalArgument.getErrorText(),iae);
        }catch(Exception e){
            throw new JunoException(e.getMessage());
        }
    }

    /**
     * Delete the record from Juno DB
     * @param key - Record Key to be deleted
     * @return Mono<JunoResponse> - Emits a single response or Error with processing the request
     * @throws JunoException - Throws Exception if any exception while processing the request
     */
    public Mono<JunoResponse> delete(byte[] key) throws JunoException {
        try{
            final JunoRequest req = new JunoRequest(key,0,0,JunoRequest.OperationType.Destroy);
            return processSingle(req, JunoMessage.OperationType.Destroy);
        }catch(JunoException je){
            throw je;
//        }catch (IllegalArgumentException iae) {
//            throw new JunoException(OperationStatus.IllegalArgument.getErrorText(),iae);
        }catch(Exception e){
            throw new JunoException(e.getMessage());
        }
    }

    /**
     * Return the configured Juno properties for this current instance in a MAP
     */
    public Map<String, String> getProperties() {
        return this.configHolder.getProperties();
    }

    /**
     * Compare the version of the record in Juno DB and update it only if the supplied version
     * is greater than or equal to the existing version in Juno DB
     * @param jcx - Juno record context
     * @param value - Record Value
     * @param timeToLiveSec - Time to Live for the record. If set to 0 then the TTL is not extended.
     * @return Mono<JunoResponse> - Emits a single response or Error with processing the request
     * @throws JunoException - Throws Exception if any exception while processing the request
     */
    public Mono<JunoResponse> compareAndSet(RecordContext jcx, byte[] value, long timeToLiveSec) throws JunoException {
        try{
            if(jcx == null){
                throw new IllegalArgumentException("Record Context cannot be null");
            }
            final JunoRequest req = new JunoRequest(jcx.getKey(),value,jcx.getVersion(),timeToLiveSec,jcx.getCreationTime(),JunoRequest.OperationType.Update);
            return processSingle(req, JunoMessage.OperationType.CompareAndSet);
        }catch(JunoException je){
            throw je;
        }catch (IllegalArgumentException iae) {
            SocketConfigHolder socConfig = new SocketConfigHolder(configHolder);
            JunoMetrics.recordOpCount(socConfig.getJunoPool(), JunoRequest.OperationType.Update.getOpType(), OperationStatus.IllegalArgument.getErrorText(), iae.getMessage());
            throw new JunoException(OperationStatus.IllegalArgument.getErrorText(),iae);
        }catch(Exception e){
            throw new JunoException(e.getMessage());
        }
    }

    /**
     * This is the method for performing a batch of records
     * @param request- List of requests to be processed
     * Juno Request object
     * @return Observable<JunoResponse> - Emits a single response or Error with processing the request
     * @throws JunoException - Throws Exception if any exception while processing the request
     */
    @SuppressWarnings("deprecation")
    @Override
    public Flux<JunoResponse> doBatch(final Iterable<JunoRequest> request) throws JunoException {
        SocketConfigHolder socConfig = new SocketConfigHolder(configHolder);
        //Check for null and empty argument
        if(request == null){
            JunoMetrics.recordOpCount(socConfig.getJunoPool(),"JUNO_BATCH", OperationStatus.IllegalArgument.getErrorText(),"null_request_list");
            throw new JunoException(OperationStatus.IllegalArgument.getErrorText(), new IllegalArgumentException("Request argument is null"));
        }else if(!request.iterator().hasNext()){
                JunoMetrics.recordOpCount(socConfig.getJunoPool(), "JUNO_BATCH", OperationStatus.IllegalArgument.getErrorText(),"empty_request_list");
            throw new JunoException(OperationStatus.IllegalArgument.getErrorText(), new IllegalArgumentException("Empty request list supplied"));
        }
        long opStartTime = System.currentTimeMillis();
        final Map<String,CharSequence> rootTrans = new HashMap<String, CharSequence>();
        rootTrans.put("isAsync", String.valueOf(isAsync));
        rootTrans.put("USE_SSL", configHolder.getUseSSL()? JunoConstants.JUNO_SSL_CLIENT : JunoConstants.JUNO_CLIENT);
        rootTrans.put("OPTYPE", "JUNO_BATCH");
        try{
            Flux<JunoResponse> resp = Flux.<JunoResponse>create(t-> {

                boolean txnFailed = false;
                //This Map has all the requests along with their UUIDs.
                final ConcurrentHashMap<UUID,JunoMessage> reqIdReqMsgMap = JunoClientUtil.bulkValidate(request,t,configHolder,socConfig,isAsync);
                int batchSize = reqIdReqMsgMap.size();
                AtomicInteger reqSent = new AtomicInteger(0); // Number of Requests sent
                //int resCount=0; // Number of Responses received
                int totalReqSent=0;
                int totalResReceived=0;
                Integer batchOpaque=0;

                try{
                    int retryCounter=0;
                    boolean operationRetry = configHolder.isRetryEnabled();
                    rootTrans.put("num_request_for_this_batch",String.valueOf(batchSize));

                    while(retryCounter <= MAX_RETRY){
                        retryCounter++;
                        reqSent.set(0); //Initialize the reqSent count to 0
                        int resCount = 0;   //Initialize the resCount count to 0
                        batchOpaque = opaqueGenarator.incrementAndGet(); // Opaqueue for batch operation

                        // Blocking queue to receive the response
                        BlockingQueue<OperationMessage> respQueue = new ArrayBlockingQueue<OperationMessage>(reqIdReqMsgMap.size());
                        opaqueResMap.put(batchOpaque,respQueue); // Add the blocking queue to the opaqueRespMap

                        if(!reqQueue.isConnected()){
                            throw new JunoException(OperationStatus.ConnectionError.getErrorText());
                        }

                        SendBatch sndBatch = new SendBatch(batchOpaque,reqQueue,reqIdReqMsgMap,reqSent);
                        Future<Integer> reqSender = executor.submit(sndBatch);

                        rootTrans.put("batch_id_"+retryCounter,String.valueOf(batchOpaque));
                        long respTimeout = configHolder.getResponseTimeout();

                        //Receive the Responses for requests sent in request queue
                        while (!reqSender.isDone() || reqSent.get() > resCount ){

                            // wait up to 100ms for the first request to be sent out
                            if(reqSent.get() == 0){
                                long start = System.currentTimeMillis();
                                synchronized (reqSent) {
                                    reqSent.wait(100);
                                }
                                if((System.currentTimeMillis() - start) > 50) {
                                    rootTrans.put("NAME", "JUNO_BATCH_SEND_DELAYED");
                                    rootTrans.put("timeToSendRequest",String.valueOf(System.currentTimeMillis() - start));
                                    LOGGER.warn(JunoStatusCode.WARNING.toString() + " {} ", rootTrans);
                                    JunoMetrics.recordEventCount("JUNO_BATCH_SEND_DELAYED","UNKNOWN",JunoMetrics.WARNING);
                                }
                            }

                            long startTime = System.currentTimeMillis();

                            //wait for response to arrive
                            OperationMessage responseOpeMsg = respQueue.poll(respTimeout, TimeUnit.MILLISECONDS);

                            respTimeout -= (System.currentTimeMillis() - startTime);
                            respTimeout = respTimeout<0?0:respTimeout; //Just in case not to go negative

                            //When response time out, the respQueue will return null
                            if(responseOpeMsg == null){
                                reqQueue.incrementFailedAttempts();
                                break;
                            }

                            //Fetch the original request using the request ID
                            JunoMessage reqMsg = reqIdReqMsgMap.get(responseOpeMsg.getMetaComponent().getRequestUuid());
                            if(reqMsg == null){
                                //This should not be the case. Do nothing just continue
                                continue;
                            }

                            resCount++; // Got a valid response
                            reqQueue.incrementSuccessfulAttempts();
                            //Decode the received operation message and create the JunoResponse
                            JunoMessage respMessage = JunoClientUtil.decodeOperationMessage(responseOpeMsg,reqMsg.getKey(),configHolder);
                            JunoResponse junoResp = new JunoResponse(respMessage.getKey(),respMessage.getValue(),respMessage.getVersion(),
                                    respMessage.getTimeToLiveSec(),respMessage.getCreationTime(),respMessage.getStatus().getOperationStatus());

                            if(respMessage.getStatus().getOperationStatus() != OperationStatus.Success){
                                //Check if retry is configured and the error is a retryable error
                                if(operationRetry && JunoClientUtil.checkForRetry(respMessage.getStatus().getOperationStatus())){
                                    JunoMetrics.recordOpCount(socConfig.getJunoPool(),"B_"+reqMsg.getOpType().getOpType(), respMessage.getStatus().getErrorText());
                                    continue;
                                }
                            }

                            //Remove the request from reqMap once there is a successful response
                            reqIdReqMsgMap.remove(responseOpeMsg.getMetaComponent().getRequestUuid());

                            //Add CAL for the responses.
                            final Map<String,CharSequence> trans = new HashMap<String, CharSequence>();
                            trans.put("isAsync", String.valueOf(isAsync));
                            trans.put("USE_SSL", configHolder.getUseSSL()? JunoConstants.JUNO_SSL_CLIENT : JunoConstants.JUNO_CLIENT);
                            trans.put("B_OPTYPE", "B_"+reqMsg.getOpType().getOpType());
                            trans.put("batch_id",String.valueOf(batchOpaque));
                            trans.put("hex_key", Hex.encodeHexString(reqMsg.getKey()).toUpperCase());
                            trans.put("req_id", responseOpeMsg.getMetaComponent().getRequestIdString());
                            trans.put("req_attempt",String.valueOf(retryCounter));
                            trans.put("ver", reqMsg.getVersion()+"|"+respMessage.getVersion());
                            trans.put("ttl", reqMsg.getTimeToLiveSec()+"|"+respMessage.getTimeToLiveSec());
                            trans.put("msg_size", reqMsg.getMessageSize()+"|"+Long.toString(responseOpeMsg.getHeader().getMessageSize()));
                            if(reqMsg.isPayloadCompressed()){
                                trans.put("comp_%", Integer.toString(reqMsg.getCompressionAchieved()));
                            }
                            long reqValSize = (reqMsg.getValue() != null)? reqMsg.getValue().length:0;
                            trans.put("val_size", reqValSize+"|"+respMessage.getValue().length);
                            trans.put("server", responseOpeMsg.getServerIp()+":"+String.valueOf(socConfig.getPort()));
                            if(respMessage.getReqHandlingTime() > 0){
                            	trans.put("rht", ""+respMessage.getReqHandlingTime()); 
                            }
                            trans.put("status", respMessage.getStatus().getErrorText());

                            long duration = System.currentTimeMillis() - reqMsg.getReqStartTime();
                            //Set CAL ok if the error code is acceptable.
                            if(junoResp.getStatus().isTxnOk()){
                                trans.put("STATUS", JunoStatusCode.SUCCESS.toString());
                                JunoMetrics.recordOpTimer(JunoMetrics.JUNO_LATENCY_METRIC,"B_"+reqMsg.getOpType().getOpType(), socConfig.getJunoPool(), JunoMetrics.SUCCESS, duration);

                            }else{
                                txnFailed = true;
                                trans.put("STATUS", JunoStatusCode.ERROR.toString());
                                JunoMetrics.recordOpTimer(JunoMetrics.JUNO_LATENCY_METRIC,"B_"+reqMsg.getOpType().getOpType(), socConfig.getJunoPool(), JunoMetrics.ERROR, duration);
                            }
                            JunoMetrics.recordOpCount(socConfig.getJunoPool(),"B_"+reqMsg.getOpType().getOpType(),respMessage.getStatus().getErrorText());
                            trans.put("Duration", String.valueOf(duration));
                            LOGGER.info("TxnStatus : {}", trans);
                            t.next(junoResp);
                        }

                        //remove the respQueue for the current batchOpaque. So that retry will use a different opaque.
                        opaqueResMap.remove(batchOpaque);
                        totalReqSent += reqSent.get();
                        totalResReceived += resCount;

                        //If not all the requests are processed
                        if(reqIdReqMsgMap.size() != 0){
                            if(!operationRetry){
                                txnFailed=true;
                                for(Map.Entry<UUID, JunoMessage> entry : reqIdReqMsgMap.entrySet()){
                                    JunoMessage jMsg = entry.getValue();
                                    JunoResponse junoResp = new JunoResponse(jMsg.getKey(),jMsg.getValue(),0,0,0,jMsg.getStatus().getOperationStatus());
                                    final Map<String,CharSequence> trans = new HashMap<String, CharSequence>();
                                    trans.put("isAsync", String.valueOf(isAsync));
                                    trans.put("USE_SSL", configHolder.getUseSSL()? JunoConstants.JUNO_SSL_CLIENT : JunoConstants.JUNO_CLIENT);
                                    trans.put("B_OPTYPE", "B_"+jMsg.getOpType().getOpType());
                                    trans.put("batch_id",String.valueOf(batchOpaque));
                                    trans.put("hex_key", Hex.encodeHexString(jMsg.getKey()));
                                    trans.put("req_id", jMsg.getReqId().toString());
                                    trans.put("req_attempt",String.valueOf(retryCounter));
                                    //trans.put("Operation",jMsg.getOpType().getOpType());
                                    trans.put("msg_size", jMsg.getMessageSize()+"|"+0);
                                    if(jMsg.isPayloadCompressed()){
                                        trans.put("comp_%", Integer.toString(jMsg.getCompressionAchieved()));
                                    }
                                    long reqValSize = (jMsg.getValue() != null)? jMsg.getValue().length:0;
                                    trans.put("val_size", reqValSize+"|"+0);
                                    trans.put("server", socConfig.getJunoPool());
                                    trans.put("status",jMsg.getStatus().getErrorText());
                                    trans.put("Txn_STATUS", JunoStatusCode.ERROR.toString());;
                                    long duration = System.currentTimeMillis() - jMsg.getReqStartTime();
                                    JunoMetrics.recordOpTimer(JunoMetrics.JUNO_LATENCY_METRIC,"B_"+jMsg.getOpType().getOpType(), socConfig.getJunoPool(), JunoMetrics.ERROR,duration);
                                    trans.put("Duration", String.valueOf(duration));
                                    LOGGER.info("TxnStatus : {}", trans);
                                    JunoMetrics.recordOpCount(socConfig.getJunoPool(), "B_"+jMsg.getOpType().getOpType(), junoResp.getStatus().getErrorText());
                                    t.next(junoResp);
                                }
                            }else{
                                operationRetry=false;
                                // Retry intervl will be between 10 - 100 msecs
                                final int randVal = new Random().nextInt(MAX_RETRY_INTERVAL-MIN_RETRY_INTERVAL);
                                Thread.sleep(randVal+MIN_RETRY_INTERVAL);
                                continue;
                            }
                        }
                        break;
                    }
                    rootTrans.put("num_request_sent",String.valueOf(totalReqSent));
                    rootTrans.put("num_response_received",String.valueOf(totalResReceived));
                    rootTrans.put("ROOT_TXN_STATUS", txnFailed?JunoStatusCode.ERROR.toString():JunoStatusCode.SUCCESS.toString());
                    JunoMetrics.recordOpTimer(JunoMetrics.JUNO_LATENCY_METRIC, "JUNO_BATCH", socConfig.getJunoPool(),
                            txnFailed?JunoMetrics.ERROR:JunoMetrics.SUCCESS, System.currentTimeMillis() - opStartTime);
                    LOGGER.info("RootTxnStatus : {}", rootTrans);
                    t.complete();
                }catch(Exception e){
                    rootTrans.put("num_request_sent",String.valueOf(totalReqSent));
                    rootTrans.put("num_response_received",String.valueOf(totalResReceived));
                    rootTrans.put("ROOT_TXN_STATUS", JunoStatusCode.ERROR.toString());
                    JunoMetrics.recordOpTimer(JunoMetrics.JUNO_LATENCY_METRIC, "JUNO_BATCH",socConfig.getJunoPool(),
                            JunoMetrics.ERROR, System.currentTimeMillis() - opStartTime);
                    LOGGER.error("RootTxnStatus : {}", rootTrans);
                    LOGGER.error(e.getMessage() +" ["+ this.configHolder.printProperties()+"]");
                    t.error(new JunoException(OperationStatus.InternalError.getErrorText(),e));
                }finally{
                    opaqueResMap.remove(batchOpaque);
                }
            }).subscribeOn(isAsync? Schedulers.boundedElastic():Schedulers.immediate());
            return resp;
        }catch(Exception e){
            LOGGER.error(e.getMessage() +" ["+ this.configHolder.printProperties()+"]");
            rootTrans.put("exception",e.getMessage());
            rootTrans.put("ROOT_TXN_STATUS", JunoStatusCode.ERROR.toString());
            LOGGER.error("RootTxnStatus : {}", rootTrans);
            JunoMetrics.recordOpTimer(JunoMetrics.JUNO_LATENCY_METRIC, "JUNO_BATCH", socConfig.getJunoPool(),
                    JunoMetrics.ERROR, System.currentTimeMillis() - opStartTime);
            throw new JunoException(e.getMessage());
        }
    }

    /**
     * Process Single requests in async manner.
     * @param req - JunoRequest
     * @param opType - Operation to be performed
     * @return JunoResponse - Juno response object with status.
     */
    private Mono<JunoResponse> processSingle(final JunoRequest req, final JunoMessage.OperationType opType){
        // CAL initialization
        final Map<String,CharSequence> trans = new HashMap<String, CharSequence>();
        trans.put("isAsync", String.valueOf(isAsync));
        trans.put("USE_SSL", configHolder.getUseSSL()? JunoConstants.JUNO_SSL_CLIENT : JunoConstants.JUNO_CLIENT);
        trans.put("OPTYPE", opType.getOpType());
        long opStartTime = System.currentTimeMillis();
        SocketConfigHolder socConfig = new SocketConfigHolder(configHolder);
        try{
            Mono<JunoResponse> resp = Mono.<JunoResponse>create(t-> {
                int opaque = 0;
                try{
                    final JunoMessage reqMsg = JunoClientUtil.validateInput(req, opType, configHolder);  //TODO remove optype
                    trans.put("hex_key", Hex.encodeHexString(reqMsg.getKey()));
                    int retryCounter=0;
                    boolean operationRetry = configHolder.isRetryEnabled();
                    while(retryCounter <= MAX_RETRY){
                        retryCounter++;

                        opaque = opaqueGenarator.incrementAndGet();

                        // Check if connection is available to server
                        if(!reqQueue.isConnected()){
                            throw new JunoException(OperationStatus.ConnectionError.getErrorText());
                        }

                        OperationMessage operationMessage =  JunoClientUtil.createOperationMessage(reqMsg,opaque);
                        BlockingQueue<OperationMessage> respQueue = new ArrayBlockingQueue<OperationMessage>(1);
                        opaqueResMap.put(opaque,respQueue);

                        // Enqueue the message to netty transport
                        if(!reqQueue.enqueue(operationMessage)){
                            opaqueResMap.remove(opaque);
                            throw new JunoException(OperationStatus.QueueFull.getErrorText());
                        }

                        //wait for response to arrive
                        OperationMessage responseOpeMsg = respQueue.poll(configHolder.getResponseTimeout(), TimeUnit.MILLISECONDS);
                        opaqueResMap.remove(opaque);

                        if(responseOpeMsg == null){
                            if(operationRetry){
                                operationRetry=false;
                                // Retry interval will be between 10 - 100 msecs
                                final int randVal = new Random().nextInt(MAX_RETRY_INTERVAL-MIN_RETRY_INTERVAL);
                                Thread.sleep(randVal+MIN_RETRY_INTERVAL);
                                continue;
                            }
                            trans.put("req_id", operationMessage.getMetaComponent().getRequestIdString());
                            trans.put("req_attempt",String.valueOf(retryCounter));
                            trans.put("server", socConfig.getJunoPool());
                            trans.put("failRatioAverage", String.valueOf(reqQueue.getAverage()));
                            reqQueue.incrementFailedAttempts();
                            throw new JunoException(OperationStatus.ResponseTimeout.getErrorText(),new TimeoutException());
                        }
                        reqQueue.incrementSuccessfulAttempts();
                        // Decode the formed operation message and create the JunoResponse
                        JunoMessage respMsg = JunoClientUtil.decodeOperationMessage(responseOpeMsg,reqMsg.getKey(),configHolder);
                        if(respMsg.getStatus().getOperationStatus() != OperationStatus.Success){
                            if(operationRetry && JunoClientUtil.checkForRetry(respMsg.getStatus().getOperationStatus())){
                                JunoMetrics.recordOpCount(socConfig.getJunoPool(), opType.getOpType(), respMsg.getStatus().getErrorText());
                                operationRetry=false;
                                // Retry intervl will be between 10 - 100 msecs
                                final int randVal = new Random().nextInt(MAX_RETRY_INTERVAL-MIN_RETRY_INTERVAL);
                                Thread.sleep(randVal+MIN_RETRY_INTERVAL);
                                continue;
                            }
                        }
                        trans.put("req_id", operationMessage.getMetaComponent().getRequestIdString());
                        trans.put("req_attempt",String.valueOf(retryCounter));

                        JunoResponse junoResp = new JunoResponse(reqMsg.getKey(),respMsg.getValue(),respMsg.getVersion(),
                                respMsg.getTimeToLiveSec(),respMsg.getCreationTime(),respMsg.getStatus().getOperationStatus());

                        //Check if we got the response only for our request by comparing the ID in request and response.
                        if (!Arrays.equals(operationMessage.getMetaComponent().getRequestId(),
                                responseOpeMsg.getMetaComponent().getRequestId())) {
                            trans.put("resp_id", responseOpeMsg.getMetaComponent().getRequestIdString());
                            trans.put("server", socConfig.getJunoPool());
                            throw new JunoException("Response id does not match the request id.");
                        }
                        trans.put("ver", reqMsg.getVersion()+"|"+junoResp.getVersion());
                        trans.put("ttl", reqMsg.getTimeToLiveSec()+"|"+respMsg.getTimeToLiveSec());
                        if(reqMsg.isPayloadCompressed()){
                            trans.put("comp_%", Integer.toString(reqMsg.getCompressionAchieved()));
                        }
                        long reqValSize = (reqMsg.getValue() != null)? reqMsg.getValue().length:0;
                        trans.put("val_size", reqValSize+"|"+respMsg.getValue().length);
                        trans.put("msg_size", reqMsg.getMessageSize()+"|"+Long.toString(responseOpeMsg.getHeader().getMessageSize()));
                        trans.put("server", responseOpeMsg.getServerIp()+":"+String.valueOf(socConfig.getPort()));
                        if(respMsg.getReqHandlingTime() > 0){
                        	trans.put("rht", ""+respMsg.getReqHandlingTime());
                        }
                        
                        //Return only on acceptable errors else throw exception. Set CAL ok if the error code is acceptable.
                        if(junoResp.getStatus() == OperationStatus.Success || junoResp.getStatus() == OperationStatus.ConditionViolation ||
                                junoResp.getStatus() == OperationStatus.NoKey ||	junoResp.getStatus() == OperationStatus.RecordLocked ||
                                junoResp.getStatus() == OperationStatus.UniqueKeyViolation || junoResp.getStatus() == OperationStatus.TTLExtendFailure){
                            trans.put("status",junoResp.getStatus().getErrorText());
                            LOGGER.info(JunoStatusCode.SUCCESS + " SUCCESS {} ", trans);
                            JunoMetrics.recordOpTimer(JunoMetrics.JUNO_LATENCY_METRIC, opType.getOpType(), socConfig.getJunoPool(), JunoMetrics.SUCCESS,System.currentTimeMillis() - opStartTime);
                            JunoMetrics.recordOpCount(socConfig.getJunoPool(), opType.getOpType(), junoResp.getStatus().getErrorText());
                            t.success(junoResp);
                        }else{
                            trans.put("server", socConfig.getJunoPool());
                            throw new JunoException(respMsg.getStatus().getErrorText());
                        }
                        break;
                    }
                } catch(JunoException e){
                    trans.put("server", socConfig.getJunoPool());
                    trans.put("status",e.getMessage());
                    LOGGER.error(JunoStatusCode.ERROR + " ERROR {} ", trans);
                    JunoMetrics.recordOpTimer(JunoMetrics.JUNO_LATENCY_METRIC, opType.getOpType(), socConfig.getJunoPool(), JunoMetrics.ERROR,System.currentTimeMillis() - opStartTime);
                    JunoMetrics.recordOpCount(socConfig.getJunoPool(), opType.getOpType(), e.getMessage());
                    LOGGER.error(e.getMessage() +" ["+ this.configHolder.printProperties()+"]");
                    t.error(e);
                } catch (IllegalArgumentException iae) {
                    trans.put("server", socConfig.getJunoPool());
                    trans.put("details", iae.getMessage());
                    LOGGER.error(JunoStatusCode.ERROR + " ERROR {} ", trans);
                    JunoMetrics.recordOpTimer(JunoMetrics.JUNO_LATENCY_METRIC, opType.getOpType(), socConfig.getJunoPool(), JunoMetrics.ERROR, System.currentTimeMillis() - opStartTime);
                    if(iae.getCause() != null)
                        JunoMetrics.recordOpCount(socConfig.getJunoPool(), opType.getOpType(), OperationStatus.IllegalArgument.getErrorText(), iae.getCause().getMessage());
                    else
                        JunoMetrics.recordOpCount(socConfig.getJunoPool(), opType.getOpType(), OperationStatus.IllegalArgument.getErrorText());
                    LOGGER.error(iae.getMessage() + " [" + this.configHolder.getProperties() + "]");
                    throw new JunoException(OperationStatus.IllegalArgument.getErrorText(), iae);
                } catch(Exception e){
                    trans.put("server", socConfig.getJunoPool());
                    trans.put("status",e.getMessage());
                    LOGGER.error(JunoStatusCode.ERROR + " ERROR {} ", trans);
                    JunoMetrics.recordOpTimer(JunoMetrics.JUNO_LATENCY_METRIC, opType.getOpType(), socConfig.getJunoPool(), JunoMetrics.ERROR,System.currentTimeMillis() - opStartTime);
                    JunoMetrics.recordOpCount(socConfig.getJunoPool(), opType.getOpType(), OperationStatus.InternalError.getErrorText());
                    LOGGER.error(e.getMessage()+" [" + this.configHolder.getProperties()+"]");
                    t.error(new JunoException(OperationStatus.InternalError.getErrorText(),e));
                } finally {
                    opaqueResMap.remove(opaque);
                }
            }).subscribeOn(isAsync? Schedulers.boundedElastic():Schedulers.immediate());
            return resp;
        } catch (Exception e) {
            trans.put("server", socConfig.getJunoPool());
            trans.put("details",e.getMessage());
            LOGGER.error(JunoStatusCode.ERROR + " ERROR {} ", trans);
            JunoMetrics.recordOpTimer(JunoMetrics.JUNO_LATENCY_METRIC, opType.getOpType(),socConfig.getJunoPool(), JunoMetrics.ERROR,System.currentTimeMillis() - opStartTime);
            JunoMetrics.recordOpCount(socConfig.getJunoPool(), opType.getOpType(),OperationStatus.InternalError.getErrorText());
            LOGGER.error(e.getMessage()+ " [" + this.configHolder.getProperties()+"]");
            throw new JunoException(OperationStatus.InternalError.getErrorText(),e);
        }
    }
}
