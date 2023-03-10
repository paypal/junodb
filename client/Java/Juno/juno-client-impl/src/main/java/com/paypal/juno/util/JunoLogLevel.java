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

import ch.qos.logback.classic.Level;

public enum JunoLogLevel {
    DEFAULT(-1, Level.WARN, java.util.logging.Level.WARNING),
    DEBUG(0, Level.DEBUG, java.util.logging.Level.FINE),
    INFO(1, Level.INFO, java.util.logging.Level.INFO),
    WARN(2, Level.WARN, java.util.logging.Level.WARNING),
    ERROR(3, Level.ERROR, java.util.logging.Level.SEVERE),
    FATAL(4, Level.ERROR, JunoLogLevelExtension.FATAL),
    CONFIG(5, Level.INFO, java.util.logging.Level.CONFIG),
    FINE(6, Level.DEBUG, java.util.logging.Level.FINE),
    FINER(7, Level.DEBUG, java.util.logging.Level.FINER),
    FINEST(8, Level.TRACE,java.util.logging.Level.FINEST),
    ALL(9, Level.ALL, java.util.logging.Level.ALL),
    OFF(10, Level.OFF,java.util.logging.Level.OFF);


    private int m_junoLevel;
    private Level m_logbackLevel;
    private java.util.logging.Level m_jdkJunoLogLevel;

    private JunoLogLevel (int junoLevel, Level logbackLevel, java.util.logging.Level jdkJunoLogLevel) {
        m_junoLevel = junoLevel;
        m_logbackLevel = logbackLevel;
        m_jdkJunoLogLevel = jdkJunoLogLevel;
    }

    public Level getLogbackLevel() {
        return m_logbackLevel;
    }

    public int getLogbackLevelValue() {
        return m_logbackLevel.toInt();
    }

    public int getjunoLevelValue() {
        return m_junoLevel;
    }

    public java.util.logging.Level getLevel() {
        return m_jdkJunoLogLevel;
    }

    public int getLevelValue() {
        return m_jdkJunoLogLevel.intValue();
    }

    private static class JunoLogLevelExtension extends java.util.logging.Level {
        public final static java.util.logging.Level FATAL =
                new JunoLogLevel.JunoLogLevelExtension("FATAL", 1100);
        protected JunoLogLevelExtension(String name, int value) {
            super(name, value, null);
        }
        private static final long serialVersionUID = 9149560934874662806L;
    }
}