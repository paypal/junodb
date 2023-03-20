# Juno Wire Protocol
## Protocol Header
Juno wire protocol consists of a 12-byte header. Depending on the type, the appropriate message payload follows the fixed header section. Following is the header protocol:

```
         |0|1|2|3|4|5|6|7|0|1|2|3|4|5|6|7|0|1|2|3|4|5|6|7|0|1|2|3|4|5|6|7|
     byte|              0|              1|              2|              3|
   ------+---------------+---------------+---------------+---------------+
       0 | magic                         | version       | msg type  flag|
         |                               |               +-----------+---+
         |                               |               | type      |RQ |
   ------+-------------------------------+---------------+-----------+---+
       4 | message size                                                  |
   ------+---------------------------------------------------------------+
       8 | opaque                                                        |
   ------+---------------------------------------------------------------+

```
Following is the detailed description of each field in the header:

| offset | name                | size (bytes) |         | meaning                                                                         |
|--------|---------------------|--------------|---------|---------------------------------------------------------------------------------|
| 0      | Magic               | 2            |         | Magic number, used to identify Juno message. '0x5050'                           |
| 2      | Version             | 1            |         | Protocol version, current version is 1.                                         |
| 3      | Message Type flag   | 1            | bit 0-5 | Message Type, 0: Operational Message, 1: Admin Message, 2: Cluster Control Message |
|        |                     |              | bit 6-7 | RQ flags, 0: response, 1: two way request, 3: one way request                       |
| 4      | Message size        | 4            |         | Specifies the length of the message                                             |
| 8      | Opaque              | 4            |         | The Opaque data set in the request will be copied back in the response          |


## Operational Message
- Client Info (ip, port, type, application name)
- Request Type: request or response
- Operation Type: Create, Get, Update, Delete
- Request Id
- Request Info (key, ttl, version, namespace)
- Payload data size
- Payload
- Response Info (status/error code, error string)
- Flag

### Operational Message Header 
```

  operational request header
        |0|1|2|3|4|5|6|7|0|1|2|3|4|5|6|7|0|1|2|3|4|5|6|7|0|1|2|3|4|5|6|7|
   byte |              0|              1|              2|              3|
  ------+---------------+---------------+---------------+---------------+
      0 | opcode        |flag           | shard Id                      |
        |               +-+-------------+                               |
        |               |R|             |                               |
  ------+---------------+-+-------------+-------------------------------+

  operational response header
        |0|1|2|3|4|5|6|7|0|1|2|3|4|5|6|7|0|1|2|3|4|5|6|7|0|1|2|3|4|5|6|7|
   byte |              0|              1|              2|              3|
  ------+---------------+---------------+---------------+---------------+
      0 | opcode        |flag           | reserved      | status        |
        |               +-+-------------+               |               |
        |               |R|             |               |               |
  ------+---------------+-+-------------+---------------+---------------+
 
  opcode:
    0x00    Nop
    0x01    Create
    0x02    Get
    0x03    Update
    0x04    Set
    0x05    Destroy
    0x81    PrepareCreate
    0x82    Read
    0x83    PrepareUpdate
    0x84    PrepareSet
    0x85    PrepareDelete
    0x86    Delete
    0xC1    Commit
    0xC2    Abort (Rollback)
    0xC3    Repair
    0xC4    MarkDelete
    0xE1    Clone
    0xFE    MockSetParam
    oxFF    MockReSet
R:
    1 if it is for replication 
shard Id:
    only meaning for request to SS
status:
    1 byte, only meaningful for response
```
### Message body
A message body may be absent, or have a set of Components.

```
** Component **
 
+-----------------------+-------------------------+-----------------+----------------+--------------+
| 4-byte component size | 1 byte component Tag/ID | component header| component body | padding to 8 |
+-----------------------+-------------------------+-----------------+----------------+--------------+
 
** Payload (or KeyValue) Component **
 
A 12-byte header followed by name, key and value
	Tag/ID: 0x01 
* Header *
 
      |0|1|2|3|4|5|6|7|0|1|2|3|4|5|6|7|0|1|2|3|4|5|6|7|0|1|2|3|4|5|6|7|
      |              0|              1|              2|              3|
------+---------------+---------------+---------------+---------------+
    0 | Size                                                          |
------+---------------+---------------+-------------------------------+
    4 | Tag/ID (0x01) | namespace len | key length                    |
------+---------------+---------------+-------------------------------+
    8 | payload length                                                |
------+---------------------------------------------------------------+
 
 (
  The max namespace length: 255
  payload length = 0 if len(payload data) = 0, otherwise,
  payload length = 1 + len(payload data) = len(payload field)
 )
 
 
* Body *
+---------+-----+---------------+-------------------------+
|namespace| key | payload field | Padding to align 8-byte |
+---------+-----+---------------+-------------------------+
 
* Payload field*
+---------------------+--------------+
| 1 byte payload type | Payload data |
+---------------------+--------------+
 
* Payload Type
0: payload data is the actual value passed from client user
1: payload data is encrypted by Juno client library, details not specified
2: payload data is encrypted by Juno proxy with AES-GCM. encryption key length is 256 bits
3: Payload data is compressed by Juno Client library.
 
* Payload data
for payload type 2
+--------------------------------+----------------+----------------+
| 4 bytes encryption key version | 12 bytes nonce | encrypted data | 
+--------------------------------+----------------+----------------+

for payload type 3
+---------------------------------+------------------+----------------+
| 1 byte size of compression type | compression type | compressed data|
+---------------------------------+------------------+----------------+

* compression type
1) snappy (default algorithm)
2) TBD
** MetaData Component **
A variable length header followed by a set of meta data fields
	Tag/ID: 0x02
* Header *
 
    | 0| 1| 2| 3| 4| 5| 6| 7| 
  0 | size                  | 4 bytes
----+-----------------------+---------
  4 | Tag/ID (0x02)         | 1 byte
----+-----------------------+---------
  5 | Number of fields      | 1 byte
----+--------------+--------+---------
  6 | Field tag    |SizeType| 1 byte
----+--------------+--------+---------
    | ...                   |
----+-----------------------+---------
    | padding to 4          |
----+-----------------------+---------
(Don't think we need a header size. )

SizeType:
  0		variable length field, for that case, 
		the first 1 byte of the field MUST be
		the size of the field(padding to 4 byte).
		The max is 255.
  n		Fixed length: 2 ^ (n+1)  bytes
 
 
 
* Body *
----+-----------------------+---------
    | Field data            | defined by Field tag
----+-----------------------+---------
    | ...                   |
----+-----------------------+---------
    | padding to 8          |
----+-----------------------+--------- 
 
* Predefined Field Types *
 
TimeToLive Field
	Tag		: 0x01
	SizeType	: 0x01
Version Field
	Tag		: 0x02
	SizeType	: 0x01
Creation Time Field
	Tag		: 0x03
	SizeType	: 0x01
Expiration Time Field
	Tag		: 0x04
	SizeType	: 0x01
RequestID/UUID Field
	Tag		: 0x05
	SizeType	: 0x03
Source Info Field
	Tag		: 0x06
	SizeType	: 0
Last Modification time (nano second)
	Tag		: 0x07
	SizeType	: 0x02
Originator RequestID Field
	Tag		: 0x08
	SizeType	: 0x03
Correlation ID field
	Tag		: 0x09
	SizeType	: 0x0
Request Handling Time Field
	Tag		: 0x0a
	SizeType	: 0x01

Tag: 0x06 
 
|0|1|2|3|4|5|6|7|0|1|2|3|4|5|6|7|0|1|2|3|4|5|6|7|0|1|2|3|4|5|6|7|
|              0|              1|              2|              3|
+---------------+-------------+-+---------------+---------------+
| size(incl pad)|app name len |T| Port                          |
+---------------+-------------+-+-------------------------------+
| IPv4 address if T is 0 or IPv6 address if T is 1              |
+---------------------------------------------------------------+
| application name, padding to 4-bytes aligned                  |
+---------------------------------------------------------------+
  
Tag: 0x09
+----+-------------------------------------------
|  0 | field size (including padding)
+----+-------------------------------------------
|  1 | octet sequence length
+----+-------------------------------------------
|    | octet sequence, padding to 4-byte aligned
+----+-------------------------------------------
```
### Operational Message Sample


The request is encoded before sending to a connection (for the purpose of creating sample only), so the opaque is not set
Application name and other optional meta data are not encoded for the sample request


```
Create request
--------------
OPaque        : 0x0
OpCode        : 0x1    Create
MsgType       : 0x40    OperationalMessage(Request) 
ShardId       : 0x0
Key           : key [6B6579]
Namespace     : DummyNS [44756D6D794E53]
RequestID     : 51d0f4af-505f-11e7-9176-000c29cadc31
App name     :  DummyAppName
Value         : value to store [76616C756520746F2073746F7265]
Lifetime      : 1800
           0  1  2  3  4  5  6  7  8  9  A  B  C  D  E  F  0123456789ABCDEF
000000000 50 50 01 40 00 00 00 70 00 00 00 00 01 00 00 00  PP.@...p........
000000010 00 00 00 38 02 03 21 65 06 00 00 00 00 00 07 08  ...8..!e........
000000020 51 D0 F4 AF 50 5F 11 E7 91 76 00 0C 29 CA DC 31  QÐô¯P_.ç.v..)ÊÜ1
000000030 14 0C A9 0C 7F 00 00 01 44 75 6D 6D 79 41 70 70  ..©.....DummyApp
000000040 4E 61 6D 65 00 00 00 00 00 00 00 28 01 07 00 03  Name.......(....
000000050 00 00 00 0E 44 75 6D 6D 79 4E 53 6B 65 79 76 61  ....DummyNSkeyva
000000060 6C 75 65 20 74 6F 20 73 74 6F 72 65 00 00 00 00  lue to store....

Create response
---------------
OPaque        : 0x0
OpCode        : 0x1    Create
MsgType       : 0x0    OperationalMessage(Response) 
OpStatus      : 0x0    Ok
Key           : key [6B6579]
Namespace     : DummyNS [44756D6D794E53]
RequestID     : 51d0f4af-505f-11e7-9176-000c29cadc31
Value         : []
Version       : 1
Creation Time : 1497375598
Lifetime      : 1800
           0  1  2  3  4  5  6  7  8  9  A  B  C  D  E  F  0123456789ABCDEF
000000000 50 50 01 00 00 00 00 50 00 00 00 00 01 00 00 00  PP.....P........
000000010 00 00 00 28 02 04 21 22 23 65 00 00 00 00 07 08  ...(..!"#e......
000000020 00 00 00 01 59 40 23 6E 51 D0 F4 AF 50 5F 11 E7  ....Y@#nQÐô¯P_.ç
000000030 91 76 00 0C 29 CA DC 31 00 00 00 18 01 07 00 03  .v..)ÊÜ1........
000000040 00 00 00 00 44 75 6D 6D 79 4E 53 6B 65 79 00 00  ....DummyNSkey..

======================================================================================
Get Request
-----------
OPaque        : 0x0
OpCode        : 0x2    Get
MsgType       : 0x40    OperationalMessage(Request) 
ShardId       : 0x0
Key           : key [6B6579]
Namespace     : DummyNS [44756D6D794E53]
RequestID     : 88f8fbde-505f-11e7-a836-000c29cadc31
App name     :  DummyAppName
Value         : []
           0  1  2  3  4  5  6  7  8  9  A  B  C  D  E  F  0123456789ABCDEF
000000000 50 50 01 40 00 00 00 58 00 00 00 00 02 00 00 00  PP.@...X........
000000010 00 00 00 30 02 02 65 06 88 F8 FB DE 50 5F 11 E7  ...0..e..øûÞP_.ç
000000020 A8 36 00 0C 29 CA DC 31 14 0C A9 1A 7F 00 00 01  ¨6..)ÊÜ1..©.....
000000030 44 75 6D 6D 79 41 70 70 4E 61 6D 65 00 00 00 00  DummyAppName....
000000040 00 00 00 18 01 07 00 03 00 00 00 00 44 75 6D 6D  ............Dumm
000000050 79 4E 53 6B 65 79 00 00                          yNSkey..

Get Response
------------
OPaque        : 0x0
OpCode        : 0x2    Get
MsgType       : 0x0    OperationalMessage(Response) 
OpStatus      : 0x0    Ok
Key           : key [6B6579]
Namespace     : DummyNS [44756D6D794E53]
RequestID     : 88f8fbde-505f-11e7-a836-000c29cadc31
Value         : value to store [76616C756520746F2073746F7265]
Version       : 1
Creation Time : 1497375598
Lifetime      : 1708
           0  1  2  3  4  5  6  7  8  9  A  B  C  D  E  F  0123456789ABCDEF
000000000 50 50 01 00 00 00 00 60 00 00 00 00 02 00 00 00  PP.....`........
000000010 00 00 00 28 02 04 21 22 23 65 00 00 00 00 06 AC  ...(..!"#e.....¬
000000020 00 00 00 01 59 40 23 6E 88 F8 FB DE 50 5F 11 E7  ....Y@#n.øûÞP_.ç
000000030 A8 36 00 0C 29 CA DC 31 00 00 00 28 01 07 00 03  ¨6..)ÊÜ1...(....
000000040 00 00 00 0E 44 75 6D 6D 79 4E 53 6B 65 79 76 61  ....DummyNSkeyva
000000050 6C 75 65 20 74 6F 20 73 74 6F 72 65 00 00 00 00  lue to store....

======================================================================================
Update Request
--------------
OPaque        : 0x0
OpCode        : 0x3    Update
MsgType       : 0x40    OperationalMessage(Request) 
ShardId       : 0x0
Key           : key [6B6579]
Namespace     : DummyNS [44756D6D794E53]
RequestID     : cb475df7-505f-11e7-9926-000c29cadc31
App name     :  DummyAppName
Value         : value to store [76616C756520746F2073746F7265]
           0  1  2  3  4  5  6  7  8  9  A  B  C  D  E  F  0123456789ABCDEF
000000000 50 50 01 40 00 00 00 68 00 00 00 00 03 00 00 00  PP.@...h........
000000010 00 00 00 30 02 02 65 06 CB 47 5D F7 50 5F 11 E7  ...0..e.ËG]÷P_.ç
000000020 99 26 00 0C 29 CA DC 31 14 0C A9 22 7F 00 00 01  .&..)ÊÜ1..©"....
000000030 44 75 6D 6D 79 41 70 70 4E 61 6D 65 00 00 00 00  DummyAppName....
000000040 00 00 00 28 01 07 00 03 00 00 00 0E 44 75 6D 6D  ...(........Dumm
000000050 79 4E 53 6B 65 79 76 61 6C 75 65 20 74 6F 20 73  yNSkeyvalue to s
000000060 74 6F 72 65 00 00 00 00                          tore....

Update Response
---------------
OPaque        : 0x0
OpCode        : 0x3    Update
MsgType       : 0x0    OperationalMessage(Response) 
OpStatus      : 0x0    Ok
Key           : key [6B6579]
Namespace     : DummyNS [44756D6D794E53]
RequestID     : cb475df7-505f-11e7-9926-000c29cadc31
Value         : []
Version       : 2
Creation Time : 1497375598
Lifetime      : 1596
           0  1  2  3  4  5  6  7  8  9  A  B  C  D  E  F  0123456789ABCDEF
000000000 50 50 01 00 00 00 00 50 00 00 00 00 03 00 00 00  PP.....P........
000000010 00 00 00 28 02 04 21 22 23 65 00 00 00 00 06 3C  ...(..!"#e.....<
000000020 00 00 00 02 59 40 23 6E CB 47 5D F7 50 5F 11 E7  ....Y@#nËG]÷P_.ç
000000030 99 26 00 0C 29 CA DC 31 00 00 00 18 01 07 00 03  .&..)ÊÜ1........
000000040 00 00 00 00 44 75 6D 6D 79 4E 53 6B 65 79 00 00  ....DummyNSkey..

======================================================================================
Set Request
-----------
OPaque        : 0x0
OpCode        : 0x4    Set
MsgType       : 0x40    OperationalMessage(Request) 
ShardId       : 0x0
Key           : key [6B6579]
Namespace     : DummyNS [44756D6D794E53]
RequestID     : d91ff0df-505f-11e7-8de8-000c29cadc31
App name     :  DummyAppName
Value         : value to store [76616C756520746F2073746F7265]
           0  1  2  3  4  5  6  7  8  9  A  B  C  D  E  F  0123456789ABCDEF
000000000 50 50 01 40 00 00 00 68 00 00 00 00 04 00 00 00  PP.@...h........
000000010 00 00 00 30 02 02 65 06 D9 1F F0 DF 50 5F 11 E7  ...0..e.Ù.ðßP_.ç
000000020 8D E8 00 0C 29 CA DC 31 14 0C A9 28 7F 00 00 01  .è..)ÊÜ1..©(....
000000030 44 75 6D 6D 79 41 70 70 4E 61 6D 65 00 00 00 00  DummyAppName....
000000040 00 00 00 28 01 07 00 03 00 00 00 0E 44 75 6D 6D  ...(........Dumm
000000050 79 4E 53 6B 65 79 76 61 6C 75 65 20 74 6F 20 73  yNSkeyvalue to s
000000060 74 6F 72 65 00 00 00 00                          tore....

Set Response
------------
OPaque        : 0x0
OpCode        : 0x4    Set
MsgType       : 0x0    OperationalMessage(Response) 
OpStatus      : 0x0    Ok
Key           : key [6B6579]
Namespace     : DummyNS [44756D6D794E53]
RequestID     : d91ff0df-505f-11e7-8de8-000c29cadc31
Value         : []
Version       : 3
Creation Time : 1497375598
Lifetime      : 1573
           0  1  2  3  4  5  6  7  8  9  A  B  C  D  E  F  0123456789ABCDEF
000000000 50 50 01 00 00 00 00 50 00 00 00 00 04 00 00 00  PP.....P........
000000010 00 00 00 28 02 04 21 22 23 65 00 00 00 00 06 25  ...(..!"#e.....%
000000020 00 00 00 03 59 40 23 6E D9 1F F0 DF 50 5F 11 E7  ....Y@#nÙ.ðßP_.ç
000000030 8D E8 00 0C 29 CA DC 31 00 00 00 18 01 07 00 03  .è..)ÊÜ1........
000000040 00 00 00 00 44 75 6D 6D 79 4E 53 6B 65 79 00 00  ....DummyNSkey..

======================================================================================
Destroy Request
---------------
OPaque        : 0x0
OpCode        : 0x5    Destroy
MsgType       : 0x40    OperationalMessage(Request) 
ShardId       : 0x0
Key           : key [6B6579]
Namespace     : DummyNS [44756D6D794E53]
RequestID     : e185f415-505f-11e7-a80b-000c29cadc31
App name     :  DummyAppName
Value         : []
           0  1  2  3  4  5  6  7  8  9  A  B  C  D  E  F  0123456789ABCDEF
000000000 50 50 01 40 00 00 00 58 00 00 00 00 05 00 00 00  PP.@...X........
000000010 00 00 00 30 02 02 65 06 E1 85 F4 15 50 5F 11 E7  ...0..e.á.ô.P_.ç
000000020 A8 0B 00 0C 29 CA DC 31 14 0C A9 2E 7F 00 00 01  ¨...)ÊÜ1..©.....
000000030 44 75 6D 6D 79 41 70 70 4E 61 6D 65 00 00 00 00  DummyAppName....
000000040 00 00 00 18 01 07 00 03 00 00 00 00 44 75 6D 6D  ............Dumm
000000050 79 4E 53 6B 65 79 00 00                          yNSkey..

Destroy Response
----------------
OPaque        : 0x0
OpCode        : 0x5    Destroy
MsgType       : 0x0    OperationalMessage(Response) 
OpStatus      : 0x0    Ok
Key           : key [6B6579]
Namespace     : DummyNS [44756D6D794E53]
RequestID     : e185f415-505f-11e7-a80b-000c29cadc31
Value         : []
           0  1  2  3  4  5  6  7  8  9  A  B  C  D  E  F  0123456789ABCDEF
000000000 50 50 01 00 00 00 00 40 00 00 00 00 05 00 00 00  PP.....@........
000000010 00 00 00 18 02 01 65 00 E1 85 F4 15 50 5F 11 E7  ......e.á.ô.P_.ç
000000020 A8 0B 00 0C 29 CA DC 31 00 00 00 18 01 07 00 03  ¨...)ÊÜ1........
000000030 00 00 00 00 44 75 6D 6D 79 4E 53 6B 65 79 00 00  ....DummyNSkey..
