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
  
/*
package proc implements proxy request handling logic.

OpStatus to Client and Proxy (for replication)
  Create
  ======
  * NoError
  * BadMsg
    if fail to decode the request
  * BadParam
  * Internal
    if fail to entrypt payload, fail to set shard ID
  * SSError
    (NoStorageServer instead?)
  * NoStorageServer
  * DupKey
  * RecordLocked
  * Inconsistent
  * CommitFailure

  Update
  ======
  * NoError
  * NoKey
  * Internal
    if fail to encrypt payload
  * VersionConflict (replication and conditional update)
  * RecordLocked
  * SSError
    (NoStorageServer instead?)
  * BadParam
  * CommitFailure
  * Inconsistent
  * NoStorageServer
  * BadMsg

  Set
  ===
  * NoError
  * Internal
  * VersionConflict (replication)
  * RecordLocked
  * SSError
    (NoStorageServer instead?)
  * BadParam
  * CommitFailure
  * Inconsistent
  * NoStorageServer
  * BadMsg

  Destroy
  =======
  * NoError
  * RecordLocked
  * VersionConflict
  * SSError
    (NoStorageServer instead?)
  * BadParam
  * CommitFailure
  * Inconsistent
  * Internal
  * NoStorageServer
  * BadMsg

  Get
  ===
  * NoError
  * NoKey
  * SSReadTTLExtendErr
  * SSError
  * BadParam
  * NoStorageServer
  * BadMsg

OpStatus from Storage Server

  PrepareCreate
  =============
  * NoError
  * AlreadyFulfilled
  * Inserting
    (considered as SUCCESS)
  * RecordLocked
  * DupKey
    (considered as ERROR)
  * SSError
  * BadParam
  * (SSOutofResource)

  PrepareUpdate
  =============
  * NoError
  * AlreadyFulfilled
  * Inserting
  * RecordLocked
  * VersionConflict
  * SSError
  * BadParam
  * (SSOutofResource)

  PrepareSet
  ==========
  * NoError
  * AlreadyFulfilled
  * Inserting
  * RecordLocked
  * SSError
  * VersionConflict
  * BadParam
  * (SSOutofResource)

  PrepareDelete
  =============
  * NoError
  * AlreadyFulfilled
  * RecordLocked
  * SSError
  * NoKey
  * VersionConflict ///?????????????????????????????????????????
  * BadParam

  MarkDelete
  ==========
  DOUBLE CHECK MarkDelete logic regarding locked by other
  * NoError
  * AlreadyFulfilled
  * SSError
  * RecordLocked ?????
  * BadParam (if originatorRequestID missing in the request)

  Read
  ====
  * NoError
  * NoKey
  * SSReadTTLExtendErr
  * KeyMarkedDelete
  * SSError
  * BadParam

  Abort
  =====
  * NoError
  * NoUncommitted

  Delete
  ======
  * NoError
  * RecordLocked
  * SSError
  * NoKey
  * VersionConflict
  * BadParam

  Commit
  -------------------------
  * NoError
  * AlreadyFulfilled
  * NoUncommitted
  * SSError
  * BadParam
  * (SSOutofResource) ??

  Repair
  ======
  * NoError
  * RecordLocked
  * SSError
  * BadParam
  * (SSOutofResource) ??

  Commit
  ======
  * NoError
  * AlreadyFulfilled
  * NoUncommitted
  * SSError
  * BadParam
  * (SSOutofResource) ??

*/
package proc

import ()

/*
A PrepareUpdate response is considered as SUCCESS if its status is
	NoError,
	Inserting (key not exist or expired. Actual status: Inserting version == 0)
	MarkDelte (key is marked delete. Actual status: Inserting version > 0)
	AlreadyFulfilled
A PrepareUpdate response is considered as FAIL if otherwise

Prepare phase is considered as SUCCESS if
	for client request
		#MarkDelete == 0, and
		#NoError + #AlreadyFulfilled > 0, and
		#NoError + #AlreadyFulfilled + #Inserting >= W
	for replication request
		#MarkDelete == 0, and
		#NoError + #AlreadyFulfilled + #Inserting >= W
  Action upon Prepare phase SUCCESS
    send Commit to NoError and Inserting SSs

Prepare phase is considered as FAIL if
	for client request
		#MarkDelete > 0, or
		#FAIL >= W-1, or
		#Inserting > W
	for replication request
		#MarkDelete > 0, or
		#FAIL >= W-1

  Action upon Prepare phase FAIL
    if #MarkDelete != 0
      send MarkDelete to NoError and AlreadyFulfilled (??) SSs
      send Abort to Inserting SSs
    if #MarkDelete == 0
      send Abort to NoErr, AlreadyFulfilled, Inserting

Update:
  1. Send PrepareUpdate to W SSs.
  2. Wait and process PrepareUpdate responses


*/
