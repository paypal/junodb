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
package com.paypal.qa.juno.usf;

import com.paypal.juno.client.JunoClient;
import javax.inject.Inject;
import javax.inject.Named;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;

abstract public class JunoUSFFactoryBase  extends AbstractTestNGSpringContextTests {

    //Inject Juno object here
    @Inject
    @Named("junoClientAuth")
    JunoClient junoClientF;

    JunoClient junoClientC;

//    @Inject
//    @Named("junoReactClientAuth")
//    JunoReactClient junoReactClientF;
//
//    JunoReactClient junoReactClientC; 

    public JunoUSFFactoryBase(JunoClient junoClient) {
        this.junoClientC = junoClient;
    }

//    public JunoUSFFactoryBase(JunoReactClient junoReactClient) {
//        this.junoReactClientC = junoReactClient;
//    }

    public JunoClient getFieldInjectedJunoClient() {
        return this.junoClientF;
    }

    public JunoClient getConstructorInjectedJunoClient() {
        return this.junoClientC;
    }

//    public JunoReactClient getFieldInjectedJunoReactClient() {
//        return this.junoReactClientF;
//    }
//
//    public JunoReactClient getConstructorInjectedJunoReactClient() {
//        return this.junoReactClientC;
//    }
}
