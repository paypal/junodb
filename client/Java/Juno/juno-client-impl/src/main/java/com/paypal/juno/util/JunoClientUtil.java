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

import com.paypal.juno.client.JunoClientConfigHolder;
import com.paypal.juno.client.ServerOperationStatus;
import com.paypal.juno.client.io.JunoRequest;
import com.paypal.juno.client.io.JunoResponse;
import com.paypal.juno.client.io.OperationStatus;
import com.paypal.juno.exception.JunoException;
import com.paypal.juno.exception.JunoInputException;
import com.paypal.juno.io.protocol.*;
import com.paypal.juno.transport.socket.SocketConfigHolder;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;
import reactor.core.publisher.FluxSink;
import rx.Subscriber;

public class JunoClientUtil {

    //Juno Input Errors
    private static final String NULL_OR_EMPTY_KEY = "null_or_empty_key";
    private static final String MAX_KEY_SIZE_EXCEEDED = "key_size_exceeded";
    private static final String PAYLOAD_EXCEEDS_MAX_LIMIT = "payload_size_exceeded";
    private static final String ZERO_OR_NEGATIVE_TTL = "invalid_ttl";
    private static final String TTL_EXCEEDS_MAX = "ttl_exceeded_max";
    private static final String ZERO_OR_NEGATIVE_VERSION = "invalid_version";

    /**
     * The logger. We make this a non-static member in order to prevent this
     * from being synchronized.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(JunoClientUtil.class);

    public static void throwIfNull(Object value, String name) {
        notNull(value, name + " must not be null");
    }

    public static void notNull(Object object, String message) {
        if (object == null) {
            throw new IllegalArgumentException(message);
        }
    }

    public static boolean checkForRetry(OperationStatus status){
        if(status == OperationStatus.RecordLocked || status == OperationStatus.TTLExtendFailure ||
                status == OperationStatus.InternalError || status == OperationStatus.NoStorage ){
            return true;
        }else{
            return false;
        }
    }

    /**
     * Validate all the requests in a batch
     * @param req - list of requests in a batch
     * @param t - Subscriber for this batch
     * @return junoMsgMap - Map of Juno request ID and Juno message
     */
    public static ConcurrentHashMap<UUID,JunoMessage> bulkValidate(Iterable<JunoRequest> req, Subscriber<? super JunoResponse> t,
                                                                   JunoClientConfigHolder configHolder, SocketConfigHolder sockConfig,
                                                                   boolean isAsync) {
        Iterator<JunoRequest> reqIter = req.iterator();
        final ConcurrentHashMap<UUID,JunoMessage> junoMsgMap = new ConcurrentHashMap<UUID,JunoMessage>();
        while(reqIter.hasNext()){
            JunoRequest request = reqIter.next();
            try{
                JunoMessage jMsg = validateInput(request, getType(request.getType()),configHolder);
                jMsg.setStatus(ServerOperationStatus.ResponseTimedout); // Set all request status as response timed out.
                jMsg.setReqStartTime(System.currentTimeMillis());
                junoMsgMap.put(jMsg.getReqId(),jMsg);
            }catch(Exception e){
                final Map<String,CharSequence> childTrans = new HashMap<String, CharSequence>();
                if(request.key() !=null && request.key().length != 0) {
                    childTrans.put("hex_key", Hex.encodeHexString(request.key()));
                }
                childTrans.put("exception",e.getMessage());
                LOGGER.error(JunoStatusCode.ERROR + " {} ", childTrans);
                if(e.getCause() != null)
                    JunoMetrics.recordOpCount(sockConfig.getJunoPool(), "B_"+ request.getType().getOpType(), OperationStatus.IllegalArgument.getErrorText(),e.getCause().getMessage());
                else
                    JunoMetrics.recordOpCount(sockConfig.getJunoPool(), "B_"+ request.getType().getOpType(), OperationStatus.IllegalArgument.getErrorText());
                LOGGER.error(e.getMessage());
                JunoResponse resp = new JunoResponse(request.key(),request.getValue(),request.getVersion(),request.getTimeToLiveSec(),request.getCreationTime(),OperationStatus.IllegalArgument);
                t.onNext(resp);
            }
        }
        return junoMsgMap;
    }

    /**
     * Validate all the requests in a batch
     * @param req - list of requests in a batch
     * @param t - Subscriber for this batch
     * @return junoMsgMap - Map of Juno request ID and Juno message
     */
    public static ConcurrentHashMap<UUID,JunoMessage> bulkValidate(Iterable<JunoRequest> req, FluxSink<JunoResponse> t,
                                                                   JunoClientConfigHolder configHolder, SocketConfigHolder sockConfig,
                                                                   boolean isAsync) {
        Iterator<JunoRequest> reqIter = req.iterator();
        final ConcurrentHashMap<UUID,JunoMessage> junoMsgMap = new ConcurrentHashMap<UUID,JunoMessage>();
        while(reqIter.hasNext()){
            JunoRequest request = reqIter.next();
            try{
                JunoMessage jMsg = validateInput(request, getType(request.getType()),configHolder);
                jMsg.setStatus(ServerOperationStatus.ResponseTimedout); // Set all request status as response timed out.
                jMsg.setReqStartTime(System.currentTimeMillis());
                junoMsgMap.put(jMsg.getReqId(),jMsg);
            }catch(Exception e){
                final Map<String,CharSequence> childTrans = new HashMap<String, CharSequence>();
                if(request.key() !=null && request.key().length != 0) {
                    childTrans.put("hex_key", Hex.encodeHexString(request.key()));
                }
                childTrans.put("exception",e.getMessage());
                LOGGER.error(JunoStatusCode.ERROR + " {} ", childTrans);
                if(e.getCause() != null)
                    JunoMetrics.recordOpCount(sockConfig.getJunoPool(), "B_"+ request.getType().getOpType(), OperationStatus.IllegalArgument.getErrorText(),e.getCause().getMessage());
                else
                    JunoMetrics.recordOpCount(sockConfig.getJunoPool(), "B_"+ request.getType().getOpType(), OperationStatus.IllegalArgument.getErrorText());
                LOGGER.error(e.getMessage());
                JunoResponse resp = new JunoResponse(request.key(),request.getValue(),request.getVersion(),request.getTimeToLiveSec(),request.getCreationTime(),OperationStatus.IllegalArgument);
                t.next(resp);
            }
        }
        return junoMsgMap;
    }
    /**
     * Validate the user supplied inputs for limits based on the operation type
     * @param req - Request parameters to be validated
     * @param opr - Type of Operation
     * @return JunoMessage - JunoMessage object formed out of the request.
     */
    public static JunoMessage validateInput(JunoRequest req, JunoMessage.OperationType opr, JunoClientConfigHolder configHolder) throws IllegalArgumentException{

        //Null and empty Key validation is moved from JunoRequest object to here
        if (req.key() == null || req.key().length == 0) {
			throw new IllegalArgumentException("The Document key must not be null or empty",new JunoInputException(NULL_OR_EMPTY_KEY));
		}

        long recordTtl = (long)((req.getTimeToLiveSec() == null) ? configHolder.getDefaultLifetimeSecs() : req.getTimeToLiveSec());
        JunoMessage junoMsg = new JunoMessage(req.key(),req.getValue(),req.getVersion(),0,recordTtl,opr);

        if (req.key().length > configHolder.getMaxKeySize()) {
            throw new IllegalArgumentException("The Document key must not be larger than "+configHolder.getMaxKeySize()+" bytes",
                    new JunoInputException(MAX_KEY_SIZE_EXCEEDED));
        }

        //Validate the Payload. Payload cannot be > 204800 bytes
        if(opr != JunoMessage.OperationType.Get && opr != JunoMessage.OperationType.Destroy){
            byte [] payload = req.getValue();
            if(req.getValue() == null){
                payload = new byte[0];
            }else if(payload != null && payload.length > 1024 && configHolder.getUsePayloadCompression()){
                try {
                    byte [] compressedPayload = Snappy.compress(req.getValue());
                    // Calculate % compression achieved
                    int compPercent = 100 - ((compressedPayload.length * 100)/req.getValue().length);
                    if(compPercent > 0){ // do compression only if its effective
                        payload = compressedPayload;
                        // Currently we have only one compression type
                        junoMsg.setCompressionType(PayloadOperationMessage.CompressionType.Snappy);
                        junoMsg.setPayloadCompressed(true);
                        junoMsg.setCompressionAchieved(compPercent);
                    }
                } catch (IOException e) {
                    // Exception while compressing so continue without compressing
                }
            }
            if(payload.length > configHolder.getMaxValueSize()) {
                String error = "The Document Value must not be larger than 204800 bytes. Current value size=" + payload.length;
                throw new IllegalArgumentException(error, new JunoInputException(PAYLOAD_EXCEEDS_MAX_LIMIT));
            }
            junoMsg.setValue(payload);
        }

        //Validate TTL
        if( recordTtl < 0){
            String error = "The Document's TTL cannot be negative. Current lifetime=" + recordTtl;
            throw new IllegalArgumentException(error,new JunoInputException(ZERO_OR_NEGATIVE_TTL));
        }else if(recordTtl > configHolder.getMaxLifetimeSecs() ){
            String error = "Invalid lifetime. current lifetime=" + recordTtl + ", max configured lifetime=" + configHolder.getMaxLifetimeSecs();
            throw new IllegalArgumentException(error,new JunoInputException(TTL_EXCEEDS_MAX));
        }

        switch(opr){
            case Create:
                if(recordTtl == 0 || recordTtl < 0){
                    String error = "The Document's TTL cannot be 0 or negative.";
                    throw new IllegalArgumentException(error,new JunoInputException(ZERO_OR_NEGATIVE_TTL));
                }
                break;
            case Update:
            case Set:
                break;
            case CompareAndSet:
                if(req.getVersion() < 1){
                    String error = "The Document version cannot be less than 1. Current version="+req.getVersion();
                    throw new IllegalArgumentException(error, new JunoInputException(ZERO_OR_NEGATIVE_VERSION));
                }
                break;
            case Get:
            case Destroy:
                break;
            default:
                break;
        }
        junoMsg.setNameSpace(configHolder.getRecordNamespace()); // Set Name space
        junoMsg.setApplicationName(configHolder.getApplicationName()); // Set Application name
        junoMsg.setReqId(UUID.randomUUID()); // Set the Requests ID here itself
        return junoMsg;
    }

    /**
     * Mapping between the Optype in Request and Optype in JunoMessage
     * @param opType - JunoRequest.OperationType
     * @return JunoMesage.OperationType
     */
    private static JunoMessage.OperationType getType(JunoRequest.OperationType opType){
        switch(opType){
            case Create:
                return JunoMessage.OperationType.Create;
            case Get:
                return JunoMessage.OperationType.Get;
            case Update:
                return JunoMessage.OperationType.Update;
            case Set:
                return JunoMessage.OperationType.Set;
            case Destroy:
                return JunoMessage.OperationType.Destroy;
            default:
                return 	JunoMessage.OperationType.Nop;
        }
    }
    /**
     * This method creates the Juno operation protocol message object
     * @param junoMsg - Juno Message object
     * @param opaque - To identify a request
     * @return OperationMessage - Operation request message
     */
    public static OperationMessage createOperationMessage(JunoMessage junoMsg, Integer opaque) {
        OperationMessage opMsg = new OperationMessage();
        MessageHeader header = new MessageHeader();
        MessageHeader.MessageOpcode code;
        switch (JunoMessage.OperationType.values()[junoMsg.getOpType().ordinal()]) {
            case Create:
                code = MessageHeader.MessageOpcode.Create;
                break;
            case Destroy:
                code = MessageHeader.MessageOpcode.Destroy;
                break;
            case Get:
                code = MessageHeader.MessageOpcode.Get;
                break;
            case Set:
                code = MessageHeader.MessageOpcode.Set;
                break;
            case Update:
            case CompareAndSet:
                code = MessageHeader.MessageOpcode.Update;
                break;
            default:
                throw new JunoException("internal Error, invalid type: " + junoMsg.getOpType().ordinal());
        }
        //int flags = MessageRQ.TwoWayRequest.ordinal();
        header.setMsgType((byte) MessageHeader.MessageType.OperationalMessage.ordinal());
        header.setFlags((byte) 0); // This field is not significant for client.
        header.setMessageRQ((byte) MessageHeader.MessageRQ.TwoWayRequest.ordinal());
        header.setOpcode((short)code.ordinal());
        header.setOpaque(opaque);
        header.setStatus((byte) ServerOperationStatus.BadMsg.getCode());
        opMsg.setHeader(header);

        //************************* Form the Meta Component **************************
        MetaOperationMessage metaComponents = new MetaOperationMessage(0L,(byte)OperationMessage.Type.Meta.getValue());
        opMsg.setMetaComponent(metaComponents);
        List<MetaMessageTagAndType> list = opMsg.getMetaComponent().getFieldList();

        //Check for version and add
        int version = (int)junoMsg.getVersion();
        if (version != 0) {
            MetaMessageFixedField field = new MetaMessageFixedField((byte) ((MetaMessageTagAndType.FieldType.Version.ordinal()) | (1 << 5)));
            field.setContent(version);
            list.add(field);
        }

        // Check for Creation time and add
        if(junoMsg.getOpType() ==  JunoMessage.OperationType.Create || junoMsg.getOpType() == JunoMessage.OperationType.Set){
            long createTime = System.currentTimeMillis() / 1000;
            if (createTime != 0) {
                MetaMessageFixedField field = new MetaMessageFixedField((byte) ((MetaMessageTagAndType.FieldType.CreationTime.ordinal()) | (1 << 5)));
                field.setContent(createTime);
                list.add(field);
            }
        }
        // Check for TTL and add
        long lifetime = junoMsg.getTimeToLiveSec();
        if (lifetime != 0) {
            MetaMessageFixedField field = new MetaMessageFixedField((byte) ((MetaMessageTagAndType.FieldType.TimeToLive.ordinal()) | (1 << 5)));
            field.setContent(lifetime);
            list.add(field);
        }

        //Add CAL Correlation ID
        String corrId = String.valueOf(UUID.randomUUID());
        if(corrId != null){
            //System.out.println("Correlation ID available+++++++++++++++++++++++++++++++++++++++++++");
            MetaMessageCorrelationIDField field = new MetaMessageCorrelationIDField((byte) ((MetaMessageTagAndType.FieldType.CorrelationID.ordinal()) | (0 << 5)));
            field.setCorrelationId(corrId.getBytes());
            list.add(field);
        }

        // Create request ID and add it
        UUID uuid = junoMsg.getReqId();
        ByteBuffer buf = ByteBuffer.wrap(new byte[16]);
        buf.putLong(uuid.getMostSignificantBits());
        buf.putLong(uuid.getLeastSignificantBits());
        MetaMessageFixedField field = new MetaMessageFixedField((byte) ((MetaMessageTagAndType.FieldType.RequestID.ordinal()) | (3 << 5)));
        field.setVariableContent(buf.array());
        list.add(field);
        opMsg.getMetaComponent().setRequestId(buf.array());

        // Add source info
        MetaMessageSourceField meta = new MetaMessageSourceField((byte) (MetaMessageTagAndType.FieldType.SourceInfo.ordinal()));
        if (junoMsg.getApplicationName() != null) {
            meta.setAppName(junoMsg.getApplicationName().getBytes());
        }
        InetAddress localAddress = getLocalIp();
        meta.setIp4(localAddress.getAddress());
        meta.setPort(0); // Set this as 1 as of now.
        list.add(meta);

        //*************************** Form the Payload Component **********************
        PayloadOperationMessage pp = new PayloadOperationMessage(0L, (byte) OperationMessage.Type.Payload.getValue());
        pp.setKey(junoMsg.getKey());
        pp.setKeyLength(junoMsg.getKey().length);
        pp.setNamespace(junoMsg.getNameSpace().getBytes());
        pp.setNameSpaceLength((byte) junoMsg.getNameSpace().getBytes().length);

        byte [] payload = junoMsg.getValue();
        if(junoMsg.isPayloadCompressed()){
            pp.setCompressionType(junoMsg.getCompressionType());
        }
        pp.setValue(payload);
        pp.setValueLength(payload == null ? 0 : payload.length);
        opMsg.setPayloadComponent(pp);
        int len = metaComponents.getBufferLength()+pp.getBufferLength()+16;
        opMsg.getHeader().setMessageSize(len);
        junoMsg.setMessageSize(len);
        return opMsg;
    }

    /**
     * This method decodes the Operation message got from Juno Server over the I/O channel and
     * creates the JunoMessage object.
     * @param opMsg - Operaion message got from Juno Server
     * @param key - Key of record
     * @return JunoMessage - Juno Message object
     */
    public static JunoMessage decodeOperationMessage(OperationMessage opMsg, byte[] key, JunoClientConfigHolder configHolder) {
        JunoMessage message = new JunoMessage();
        //Decode the Meta component
        if(opMsg.getMetaComponent() != null){
            List<MetaMessageTagAndType> list = opMsg.getMetaComponent().getFieldList();
            long createTime = 0;
            byte [] appName = null;
            long lifeTime = 0;
            long version = 0;
            long reqHandlingTime = 0;
            for (int i = 0; i < list.size(); i ++) {
                MetaMessageTagAndType type = list.get(i);
                MetaMessageTagAndType.FieldType fieldType = type.getFieldType();
                MetaMessageFixedField src;
                switch (fieldType) {
                    case CreationTime:
                        src = (MetaMessageFixedField)type;
                        createTime = src.getContent();
                        break;
                    case Dummy:
                        break;
                    case ExpirationTime:
                        break;
                    case RequestID:
                        break;
                    case SourceInfo:
                        MetaMessageSourceField infoSrc = (MetaMessageSourceField)type;
                        appName = infoSrc.getAppName();
                        break;
                    case TimeToLive:
                        src = (MetaMessageFixedField)type;
                        lifeTime = src.getContent();
                        break;
                    case Version:
                        src = (MetaMessageFixedField)type;
                        version = src.getContent();
                        break;
                    case RequestHandlingTime:
                        src = (MetaMessageFixedField)type;
                        reqHandlingTime = src.getContent();
                        break;
                    default:
                        break;

                }
            }
            message.setVersion((short) version);
            message.setTimeToLiveSec((int) lifeTime);
            message.setCreationTime(createTime);
            message.setApplicationName(configHolder.getApplicationName());
            message.setReqHandlingTime(reqHandlingTime);
        }

        //Decode the Header
        int status = (int)opMsg.getHeader().getStatus();
        message.setStatus(ServerOperationStatus.get(status));

        //Decode the Payload Component
        PayloadOperationMessage pp = opMsg.getPayloadComponent();

        message.setValue("".getBytes()); // Set empty payload and later override with actual
        if(pp != null){
            if (pp.getValueLength() != 0) {
                if(pp.getCompressedType() == PayloadOperationMessage.CompressionType.Snappy){
                    try {
                        message.setValue(Snappy.uncompress(pp.getValue()));
                    } catch (IOException e) {
                        // TODO What to do?
                        throw new JunoException("Exception while uncompressing data. "+e.getMessage());
                    }
                }else{
                    message.setValue(pp.getValue());
                }
            }
            message.setNameSpace(new String(pp.getNamespace()));
            if(Arrays.equals(key,pp.getKey())){
                // Log CAL event for mismatch in key. It should not happen.
            }
            message.setKey(pp.getKey());
        }

        // Populate the total message size for this operation
        message.setMessageSize(opMsg.getHeader().getMessageSize());

        return message;
    }

    private static InetAddress getLocalIp(){
        InetAddress localAddress;
        try{
            localAddress = InetAddress.getLocalHost();
        }catch(UnknownHostException e){
            localAddress = InetAddress.getLoopbackAddress();
        }
        return localAddress;
    }
}
