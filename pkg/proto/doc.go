/*
Package proto implements Juno binary message protocol.

Juno Message

A Juno message looks like
  +------------------------+----------------------------------------------------+
  | 12-byte message header | message body depending message type(can be absent) |
  +------------------------+----------------------------------------------------+

Message header
        | 0| 1| 2| 3| 4| 5| 6| 7| 0| 1| 2| 3| 4| 5| 6| 7| 0| 1| 2| 3| 4| 5| 6| 7| 0| 1| 2| 3| 4| 5| 6| 7|
   byte |                      0|                      1|                      2|                      3|
  ------+-----------------------+-----------------------+-----------------------+-----------------------+
      0 | magic number                                  | protocol version      | message type flag     |
        |                                               |                       +-----------------+-----+
        |                                               |                       | type            | RQ  |
  ------+-----------------------------------------------+-----------------------+-----------------+-----+
      4 | message size                                                                                  |
  ------+-----------------------------------------------------------------------------------------------+
      8 | opaque                                                                                        |
  ------+-----------------------------------------------------------------------------------------------+

  magic number:
    0x5050
  protocol version:
    1

  message type and flag:
    type:
	  0: operational message
	  1: admin message
	  2: cluster control message
    RQ:
      0 response
      1 two way request
      3 one way request

Operational Message

An operational message looks like
  +------------------------+----------------------------------------------------------------------+
  | 12-byte message header |                       message body                                   |
  |                        +----------------------------+-----------------------------------------+
  |                        | operational message header | operational message body(can be absent) |
  +------------------------+----------------------------+-----------------------------------------+

Details
  ==========================
  operational message header
  ==========================

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
    0x00	Nop
    0x01	Create
    0x02	Get
    0x03	Update
    0x04	Set
    0x05	Destroy
    0x81	PrepareCreate
    0x82	Read
    0x83	PrepareUpdate
    0x84	PrepareSet
    0x85	PrepareDelete
    0x86	Delete
    0xC1	Commit
    0xC2	Abort (Rollback)
    0xC3	Repair
    0xC4	MarkDelete
    0xE1	Clone
    0xFE	MockSetParam
    oxFF	MockReSet

  RQ:
	  0 response
	  1 two way request
	  3 one way request
  R:
	  1 if it is for replication
  shard Id
	  only meaning for request to SS
  Status:
	  1 byte, only meaningful for response
  ========================
  Operational message body
  ========================

  An operational message body contains zero or multiple components

  =============
  * Component *
  =============
  +-----------------------+-------------------------+-----------------+----------------+--------------+
  | 4-byte component size | 1 byte component Tag/ID | component header| component body | padding to 8 |
  +-----------------------+-------------------------+-----------------+----------------+--------------+
  In Juno protocol version 1, two types of components are defined
    1. payload component
    2. meta data component

  =====================================
  ** Payload (or KeyValue) Component **
  =====================================

  Tag/ID: 0x01

  A fixed length header followed by the component body defining namespace, key, and value

  payload component header

  ------+------------------+--------
        | namespace length | 1 byte
  ------+------------------+--------
        | key length       | 2 bytes
  ------+------------------+--------
        | value length     | 4 bytes
  ------+------------------+--------

  payload component body
  +---------+-----+-------+-------------------------+
  |namespace| key | value | padding to align 8-byte |
  +---------+-----+-------+-------------------------+

  payload value field

  +---------------------+--------------+
  | 1 byte payload type | Payload data |
  +---------------------+--------------+

  payload Type
    0: payload data is the actual value passed from client user
    1: payload data is encrypted by Juno client library, details not specified
    2: payload data is encrypted by Juno proxy with AES-GCM. encryption key length is 256 bits

  payload data for payload type 2
  +--------------------------------+----------------+----------------+
  | 4 bytes encryption key version | 12 bytes nonce | encrypted data |
  +--------------------------------+----------------+----------------+

  ====================
  ** Meta Component **
  ====================

  Tag/ID: 0x02

  A variable length header followed by a set of meta data fields

  meta component header

      | 0| 1| 2| 3| 4| 5| 6| 7|
  ----+-----------------------+---------
      | number of fields      | 1 byte
  ----+--------------+--------+---------
      | field tag    |sizeType| 1 byte
  ----+--------------+--------+---------
      | ...                   |
  ----+-----------------------+---------
      | padding to 4          |
  ----+-----------------------+---------


  sizeType:
  0		variable length field, for that case,
		the first 1 byte of the field MUST be
		the size of the field(padding to 4 byte).
		The max is 255.
  n		Fixed length: 2 ^ (n+1)  bytes

  meta component body
  ----+-----------------------+---------
      | field data            | defined by Field tag
  ----+-----------------------+---------
      | ...                   |
  ----+-----------------------+---------
      | padding to 8          |
  ----+-----------------------+---------
*/
package proto
