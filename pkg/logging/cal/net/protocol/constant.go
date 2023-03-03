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
//  Package utility provides the utility interfaces for mux package
//  
package protocol

// CAL message Class field options
const (
	TxnStart  = 't'
	TxnEnd    = 'T'
	AtomicTxn = 'A'
	Event     = 'E'
	Heartbeat = 'H'
)

// maxMsgLen is the longest wire-formatted message length
// of CAL messages. Note that wire formatting adds bytes. This
// is exported only to document that messages will be truncated
// somewhere around a size of 4k.
const MaxMsgLen = 4096

const MaxMsgBufferSize = 10000

// MaxNamespaceLen is the longest allowed string for CAL
// namespace (Msg's Name or Type) fields. If a Name or Type is longer
// than MaxNamespaceLen, it will be truncated at (MaxNamespaceLen-1)
// bytes and have a "+" appended to it.
const MaxNamespaceLen = 127

// CAL logging type
const (
	CalTypeFile   string = "FILE"
	CalTypeSocket string = "SOCKET"
)
