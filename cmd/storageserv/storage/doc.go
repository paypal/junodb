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
Package storage provides Juno Storage Server implementations.

  TODO:
    * StorageEngine.acquireLock() could return AlreadyFulfilled or RecordLocked.
      Double check AlreadyFulfilled for different types of requests that could be seen in phase2
    * PrepareSet replication
    * MarkDelete for replication
    * markDelete if locked by different requestID
    * check if BadRequestID status




  Proxy should pass LastModificationTime to SS in Commit request

  =============================
  PrepareCreate
  =============================
  SHOULD NOT have a PrepareCreate request for replication

  Return status:
    * NoError
    * AlreadyFulfilled
    * Inserting
      key is marked as delete
    * RecordLocked
    * DupKey
    * SSError
      fail to read from DB
    * BadParam
    * SSOutofResource

                         Request       | NoErr Response
                                       |
  ---------------------+---------------+---------------
  OpCode               | MUST          | MUST          |
  Namespace            | MUST          | MUST          |
  Key                  | MUST          | MUST          |
  Value                | OPTIONAL      | SHOULD NOT    |
  ---------------------+
  TimeToLive           | MUST          | SHOULD NOT    |
  ExpirationTime       |
  ---------------------+
  Version              | SHOULD NOT    | SHOULD NOT    |
  CreationTime         | SHOULD        | SHOULD NOT    |
  LastModificationTime | SHOULD NOT    | SHOULD        |
  SourceInfo           | SHOULD NOT    | SHOULD NOT    |
  RequestID            | MUST          | MUST          |
  OriginatorRID        | SHOULD NOT    | SHOULD NOT    |
  CorrelationID        | OPTIONAL      | SHOULD NOT    |


  =============================
  Commit for PrepareCreate
  =============================
  Return status:
    * NoError
    * AlreadyFulfilled
    * NoUncommitted
    * SSError
    * BadParam
    * SSOutofResource
                         Request       | NoErr Response
                                       |
  ---------------------+---------------+---------------
  OpCode               | MUST          | MUST          |
  Namespace            | MUST          | MUST          |
  Key                  | MUST          | MUST          |
  Value                | SHOULD NOT    | SHOULD NOT    |
  ---------------------+
  TimeToLive           | MUST          | MUST          |
  ExpirationTime       |
  ---------------------+
  Version              | MUST          | MUST          |
  CreationTime         | MUST          | MUST          |
  LastModificationTime | SHOULD        | MUST          |
  SourceInfo           | SHOULD NOT    | SHOULD NOT    |
  RequestID            | MUST          | MUST          |
  OriginatorRID        | SHOULD NOT    | MUST          |
  CorrelationID        | OPTIONAL      | SHOULD NOT    |

  =============================
  PrepareUpdate
  =============================

  Return status:
    * NoError
    * AlreadyFulfilled
    * Inserting
    * RecordLocked
	* VersionConflict

    * SSError
      fail to read from DB
    * BadParam
    * SSOutofResource

                        Request        | Replication   |           Response            |
                                       |               +---------------+---------------+
                                       | Request       |  NoError      | Inserting     |
  ---------------------+---------------+---------------+---------------+---------------+
  OpCode               | MUST          | MUST          | MUST          | MUST          |
  Namespace            | MUST          | MUST          | MUST          | MUST          |
  Key                  | MUST          | MUST          | MUST          | MUST          |
  Value                | OPTIONAL      | OPTIONAL      | SHOULD NOT    | SHOULD NOT    |
  ---------------------+
  TimeToLive           | OPTIONAL      | MUST          | MUST          | MUST          |
  ExpirationTime       |
  ---------------------+
  Version              | OPTIONAL      | MUST          | MUST          | OPTIONAL      |
  CreationTime         | SHOULD NOT    | MUST          | MUST          | OPTIONAL      |
  LastModificationTime | SHOULD NOT    | MUST          | MUST          | OPTIONAL      |
  SourceInfo           | SHOULD NOT    | SHOULD NOT    | SHOULD NOT    | SHOULD NOT    |
  RequestID            | MUST          | MUST          | MUST          | MUST          |
  OriginatorRID        | SHOULD NOT    | MUST          | MUST          | OPTIONAL      |
  CorrelationID        | OPTIONAL      | OPTIONAL      | SHOULD NOT    | SHOULD NOT    |


  =============================
  Commit for PrepareUpdate
  =============================
    Return status:
    * NoError
    * AlreadyFulfilled
    * NoUncommitted
    * SSError
    * BadParam
    * SSOutofResource
                         Request       | NoErr Response
                                       |
  ---------------------+---------------+---------------
  OpCode               | MUST          | MUST          |
  Namespace            | MUST          | MUST          |
  Key                  | MUST          | MUST          |
  Value                | SHOULD NOT    | SHOULD NOT    |
  ---------------------+
  TimeToLive           | MUST          |
  ExpirationTime       |
  ---------------------+
  Version              | MUST          |
  CreationTime         | MUST          |
  LastModificationTime | SHOULD        |
  SourceInfo           | SHOULD NOT    |
  RequestID            | MUST          |
  OriginatorRID        | SHOULD        |
  CorrelationID        | OPTIONAL      |


  =============================
  PrepareSet
  =============================

  Return status:
    * NoError
    * AlreadyFulfilled
    * Inserting
    * RecordLocked
	* VersionConflict ????

    * SSError
      fail to read from DB
    * BadParam
    * SSOutofResourcee

                         Request        | Replication   |           Response            |
                                       |               +---------------+---------------+
                                       | Request       |  NoError      | Inserting     |
  ---------------------+---------------+---------------+---------------+---------------+
  OpCode               | MUST          | MUST          | MUST          | MUST          |
  Namespace            | MUST          | MUST          | MUST          | MUST          |
  Key                  | MUST          | MUST          | MUST          | MUST          |
  Value                | OPTIONAL      | OPTIONAL      | SHOULD NOT    | SHOULD NOT    |
  ---------------------+
  TimeToLive           | OPTIONAL      | MUST          | MUST          | MUST          |
  ExpirationTime       |
  ---------------------+
  Version              | MUST NOT      | MUST          | MUST          | OPTIONAL      |
  CreationTime         | SHOULD NOT    | MUST          | MUST          | OPTIONAL      |
  LastModificationTime | SHOULD NOT    | MUST          | MUST          | OPTIONAL      |
  SourceInfo           | SHOULD NOT    | SHOULD NOT    | SHOULD NOT    | SHOULD NOT    |
  RequestID            | MUST          | MUST          | MUST          | MUST          |
  OriginatorRID        | SHOULD NOT    | MUST          | MUST          | OPTIONAL      |
  CorrelationID        | OPTIONAL      | OPTIONAL      | SHOULD NOT    | SHOULD NOT    |

  =============================
  Commit for PrepareSet
  =============================
    Return status:
    * NoError
    * AlreadyFulfilled
    * NoUncommitted
    * SSError
    * BadParam
    * SSOutofResource
                         Request       | NoErr Response
                                       |
  ---------------------+---------------+---------------
  OpCode               | MUST          | MUST          |
  Namespace            | MUST          | MUST          |
  Key                  | MUST          | MUST          |
  Value                | SHOULD NOT    | SHOULD NOT    |
  ---------------------+
  TimeToLive           | MUST          | MUST
  ExpirationTime       |
  ---------------------+
  Version              | MUST          | MUST
  CreationTime         | MUST          | MUST
  LastModificationTime | SHOULD        | MUST
  SourceInfo           | SHOULD NOT    | SHOULD NOT
  RequestID            | MUST          | MUST
  OriginatorRID        | SHOULD        | MUST
  CorrelationID        | OPTIONAL      |


  =============================
  PrepareDelete
  =============================

  Return status:
    * NoError
    * AlreadyFulfilled
    * NoKey
    * RecordLocked
	* VersionConflict ???? not implemented yet, needed????

    * SSError
      fail to read from DB
    * BadParam
    * SSOutofResourcee
                        Request        | Replication   | NoErr Response
                                       | Request       |
  ---------------------+---------------+---------------+--------------
  OpCode               | MUST          | MUST          | MUST          |
  Namespace            | MUST          | MUST          | MUST          |
  Key                  | MUST          | MUST          | MUST          |
  Value                | SHOULD NOT    | SHOULD NOT    | SHOULD NOT    |
  ---------------------+
  TimeToLive           | SHOULD NOT    | MUST          | MUST          |
  ExpirationTime       |
  ---------------------+
  Version              | SHOULD NOT    | MUST          | MUST          |
  CreationTime         | SHOULD NOT    | MUST          | MUST          |
  LastModificationTime | SHOULD NOT    | MUST          | MUST          |
  SourceInfo           | SHOULD NOT    | SHOULD NOT    | SHOULD NOT    |
  RequestID            | MUST          | MUST          | MUST          |
  OriginatorRID        | SHOULD NOT    | MUST          | MUST          |
  CorrelationID        | OPTIONAL      | OPTIONAL      | SHOULD NOT    |

  =============================
  Commit to PrepareDelete
  =============================
    Return status:
    * NoError
    * AlreadyFulfilled
    * NoUncommitted
    * SSError
    * BadParam
    * SSOutofResource

                        Request        | Replication   | NoErr Response| NoErr Response| NoKey Response|
                                       | Request       |               | to replication|               |
  ---------------------+---------------+---------------+---------------+---------------+---------------+
  OpCode               | MUST          | MUST          | MUST          | MUST          | MUST          |
  Namespace            | MUST          | MUST          | MUST          | MUST          | MUST          |
  Key                  | MUST          | MUST          | MUST          | MUST          | MUST          |
  Value                | SHOULD NOT    | SHOULD NOT    | SHOULD NOT    | SHOULD NOT    | SHOULD NOT    |
  ---------------------+
  TimeToLive           | SHOULD NOT    | SHOULD NOT    | SHOUD NOT     | SHOULD NOT    | SHOULD NOT    |
  ExpirationTime       |
  ---------------------+
  Version              | SHOULD NOT    | SHOULD NOT    | SHOULD NOT    |               | SHOULD NOT    |
  CreationTime         | SHOULD NOT    | SHOULD NOT    | SHOULD NOT    |               | SHOULD NOT    |
  LastModificationTime | SHOULD NOT    | SHOULD NOT    | SHOULD NOT    |               | SHOULD NOT    |
  SourceInfo           | SHOULD NOT    | SHOULD NOT    | SHOULD NOT    |               | SHOULD NOT    |
  RequestID            | MUST          | MUST          | MUST          | MUST          | MUST          |
  OriginatorRID        | SHOULD NOT    | SHOULD NOT    | SHOULD NOT    | SHOULD NOT    | SHOULD NOT    |
  CorrelationID        | OPTIONAL      | OPTIONAL      | SHOULD NOT    | SHOULD NOT    | SHOULD NOT    |


  =============================
    MarkDelete
  =============================
  Return status:
    * NoError
    * AlreadyFulfilled
    * SSError
    * BadParam
    * SSOutofResourceee
                         Request       | NoErr Response
                                       |
  ---------------------+---------------+---------------
  OpCode               | MUST          | MUST          |
  Namespace            | MUST          | MUST          |
  Key                  | MUST          | MUST          |
  Value                | SHOULD NOT    | SHOULD NOT    |
  ---------------------+
  TimeToLive           | SHOULD        | SHOULD NOT    |
  ExpirationTime       |
  ---------------------+
  Version              | SHOULD        | SHOULD NOT    |
  CreationTime         | SHOULD        | SHOULD NOT    |
  LastModificationTime | SHOULD        | SHOULD NOT    |
  SourceInfo           |               | SHOULD NOT    |
  RequestID            | MUST          | MUST          |
  OriginatorRID        | SHOULD        | SHOULD NOT    |
  CorrelationID        | OPTIONAL      | SHOULD NOT    |

  =============================
   Read
  =============================
   Return Status:
     NoError
     NoKey
     SSReadTTLExtendErr
	     ??If request.TTL < rec.TTL, should we return error? simply return NoError seems to be OK
     KeyMarkedDelete

	 BadParam
     SSErr
	 SSOutofResource

     To decide: Considering return DataExpired with the meta data of the expired record?
     Note: right now, when handling replication Get, only TTL is used

                         Request       |               | NoErr Response
                                       | Replication   |
  ---------------------+---------------+---------------+---------------
  OpCode               | MUST          | MUST          | MUST          |
  Namespace            | MUST          | MUST          | MUST          |
  Key                  | MUST          | MUST          | MUST          |
  Value                | SHOULD NOT    | SHOULD NOT    | OPTIONAL      |
  ---------------------+
  TimeToLive           | OPTIONAL      | MUST          | MUST          |
  ExpirationTime       |
  ---------------------+
  Version              | SHOULD NOT    | MUST          | MUST          |
  CreationTime         | SHOULD NOT    | MUST          | MUST          |
  LastModificationTime | SHOULD NOT    | MUST          | MUST          |
  SourceInfo           |               | SHOULD NOT    | SHOULD NOT    |
  RequestID            | MUST          | MUST          | MUST          |
  OriginatorRID        | SHOULD NOT    | MUST          | MUST          |
  CorrelationID        | OPTIONAL      | OPTIONAL      | SHOULD NOT    |
*/
package storage
