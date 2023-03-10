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

import com.paypal.juno.client.io.JunoResponse;
import com.paypal.juno.client.io.OperationStatus;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class ResponseTest {
    @Test
    public void testJunoRequest1() {
        JunoResponse resp1 = new JunoResponse("key1".getBytes(), "value1".getBytes(), 10, 10, 1649784170, OperationStatus.Success);
        assertEquals(new String(resp1.key()), "key1");
        assertEquals(new String(resp1.getValue()),"value1");
        assertEquals(resp1.getVersion(),10);
        assertEquals(resp1.getTtl(),10);
        assertEquals(resp1.getCreationTime(),1649784170);
        assertEquals(resp1.getStatus(),OperationStatus.Success);
        assertEquals(new String(resp1.getRecordContext().getKey()),"key1");
        assertEquals(resp1.getRecordContext().getTtl(),10);
        assertEquals(resp1.getRecordContext().getVersion(),10);
        assertEquals(resp1.getRecordContext().getCreationTime(),new Long(1649784170));
        assertFalse(resp1.getRecordContext().equals(resp1));
    }
}
