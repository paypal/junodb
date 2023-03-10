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
package com.paypal.juno.util;

import com.paypal.juno.client.ServerOperationStatus;
import com.paypal.juno.io.protocol.JunoMessage;
import com.paypal.juno.io.protocol.OperationMessage;
import com.paypal.juno.net.RequestQueue;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is dedicated to send the batch request
 * in a dedicated thread.
 */
public class SendBatch implements Callable<Integer> {

    Integer batchOpaque;
    RequestQueue reqQueue;
    ConcurrentHashMap<UUID, JunoMessage> reqIdReqMsgMap;
    final AtomicInteger reqCount;
    private static Logger LOGGER = LoggerFactory.getLogger(SendBatch.class);

    public SendBatch(Integer batchOpaque,RequestQueue reqQueue,ConcurrentHashMap<UUID,JunoMessage> reqIdReqMsgMap,AtomicInteger reqCount){
        this.batchOpaque = batchOpaque;
        this.reqQueue = reqQueue;
        this.reqIdReqMsgMap = reqIdReqMsgMap;
        this.reqCount= reqCount;
    }
    @Override
    public Integer call() throws Exception {
        try{
            //long batchStartTime = System.currentTimeMillis();
            //Set any port number
            for(Map.Entry<UUID,JunoMessage> jMs : reqIdReqMsgMap.entrySet()){
                //jMs.getValue().setReqStartTime(batchStartTime);
                OperationMessage operationMessage =  JunoClientUtil.createOperationMessage(jMs.getValue(),batchOpaque);
                // Enqueue the message to netty transport
                boolean rc = reqQueue.enqueue(operationMessage);
                if(!rc){
                    jMs.getValue().setStatus(ServerOperationStatus.QueueFull);
                }else{
                    reqCount.incrementAndGet();
                }
                if(reqCount.get() == 1){
                    synchronized (reqCount) {
                        reqCount.notifyAll(); // Notify only for the first request alone.
                    }
                }
            }
        }catch(Exception e){
            LOGGER.info("JUNO_BATCH_SEND", JunoStatusCode.ERROR.toString(), "Exception while enqueuing the request");
        }
        return reqCount.get();
    }
}
