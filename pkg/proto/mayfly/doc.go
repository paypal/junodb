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
Package mayfly implements Mayfly binary message protocol.

Mayfly Protocol

Message Header
   byte |      0|      1|      2|      3|      4|      5|      6|      7|
  ------+-------------------------------+-------------------------------+
      0 | magic number                  | message size                  |
  ------+-------------------------------+-------------------------------+
      8 | header size                   | op msg size                   |
  ------+-------------------------------+-------------------------------+
     16 | application name size         | sender ip                     |
  ------+-------------------------------+---------------+---------------+
     24 | recipient ip                  | sender port   | recipient port|
  ------+---------------+---------------+---------------+---------------+
     32 | sender type   |direction      | site id                       |
  ------+---------------+---------------+-------------------------------+

Operational Message Header
   byte |          0|          1|          2|          3|
  ------+-----------+-----------+-----------+-----------+
      0 | record info size                              |
  ------+-----------------------------------------------+
      4 | request id size                               |
  ------+-----------------------------------------------+
      8 | optional data size                            |
  ------+-----------------------------------------------+
     12 | payload data size                             |
  ------+-----------------------+-----------+-----------+
     16 | operation type        | op mode   | rep state |
  ------+-----------------------+-----------+-----------+
     20 | operation status      | <padding>             |
  ------+-----------------------+-----------------------+

Record Info Header
   byte |          0|          1|          2|          3|
  ------+-----------+-----------+-----------+-----------+
      0 | creation time                                 |
  ------+-----------------------------------------------+
      4 | life time                                     |
  ------+-----------------------+-----------------------+
      8 | version               | namespace size        |
  ------+-----------------------+-----------------------+
     12 | key size              | <padding>             |
  ------+-----------------------+-----------------------+

RequestID
   byte |   0|   1|   2|   3|   4|   5|   6|   7|   8|   9|  10|  11|  12|  13|  14|  15|
  ------+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+
      0 | size    | ip                | pid               | requesting time   | sequence|
  ------+---------+-------------------+-------------------+-------------------+---------+
*/
package mayfly
