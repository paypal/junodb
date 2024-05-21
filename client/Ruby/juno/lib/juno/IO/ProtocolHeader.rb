# frozen_string_literal: true

module Juno
  # Juno wire protocol consists of a 12-byte header. Depending on the type, the appropriate message payload follows the fixed header section. Following is the header protocol:
  #
  #         | 0| 1| 2| 3| 4| 5| 6| 7| 0| 1| 2| 3| 4| 5| 6| 7| 0| 1| 2| 3| 4| 5| 6| 7| 0| 1| 2| 3| 4| 5| 6| 7|
  #    byte |                      0|                      1|                      2|                      3|
  #   ------+-----------------------+-----------------------+-----------------------+-----------------------+
  #       0 | magic                                         | version               | message type flag     |
  #         |                                               |                       +-----------------+-----+
  #         |                                               |                       | type            | RQ  |
  #   ------+-----------------------------------------------+-----------------------+-----------------+-----+
  #       4 | message size                                                                                  |
  #   ------+-----------------------------------------------------------------------------------------------+
  #       8 | opaque                                                                                        |
  #   ------+-----------------------------------------------------------------------------------------------+
  #
  # Following is the detailed description of each field in the header:
  #
  # offset	name	size (bytes)	meaning
  # 0	Magic	2
  # Magic number, used to identify Juno message.
  #
  # '0x5050'
  #
  # 2	Version	1	Protocol version, current version is 1.
  # 3 	Message Type flag
  #    1 	bit 0-5
  # Message Type
  #
  # 0: Operational Message
  #
  # 1: Admin Message
  #
  # 2: Cluster Control Message
  #
  # bit 6-7
  # RQ flag
  #
  # 0: response
  #
  # 1: two way request
  #
  # 3: one way request
  #
  # 4	Message size	4	Specifies the length of the message
  # 8	Opaque	4	The Opaque data set in the request will be copied back in the response
  # Operational Message
  # Client Info (ip, port, type, application name)
  # Request Type: request or response
  # Operation Type: Create, Get, Update, Delete
  # Request Id
  # Request Info (key, ttl, version, namespace)
  # Payload data size
  # Payload
  # Response Info (status/error code, error string)
  # Flag
  # Before defining the details of the protocol for operational message, we need to review, and finalize somethings at page.
  #
  # Operational Message Header
  #
  #   operational request header
  #         |0|1|2|3|4|5|6|7|0|1|2|3|4|5|6|7|0|1|2|3|4|5|6|7|0|1|2|3|4|5|6|7|
  #    byte |              0|              1|              2|              3|
  #   ------+---------------+---------------+---------------+---------------+
  #       0 | opcode        |flag           | shard Id                      |
  #         |               +-+-------------+                               |
  #         |               |R|             |                               |
  #   ------+---------------+-+-------------+-------------------------------+
  #
  #   operational response header
  #         |0|1|2|3|4|5|6|7|0|1|2|3|4|5|6|7|0|1|2|3|4|5|6|7|0|1|2|3|4|5|6|7|
  #    byte |              0|              1|              2|              3|
  #   ------+---------------+---------------+---------------+---------------+
  #       0 | opcode        |flag           | reserved      | status        |
  #         |               +-+-------------+               |               |
  #         |               |R|             |               |               |
  #   ------+---------------+-+-------------+---------------+---------------+
  #
  #   opcode:
  #     0x00    Nop
  #     0x01    Create
  #     0x02    Get
  #     0x03    Update
  #     0x04    Set
  #     0x05    Destroy
  #     0x81    PrepareCreate
  #     0x82    Read
  #     0x83    PrepareUpdate
  #     0x84    PrepareSet
  #     0x85    PrepareDelete
  #     0x86    Delete
  #     0xC1    Commit
  #     0xC2    Abort (Rollback)
  #     0xC3    Repair
  #     0xC4    MarkDelete
  #     0xE1    Clone
  #     0xFE    MockSetParam
  #     oxFF    MockReSet
  # R:
  #   1 if it is for replication
  # shard Id:
  #     only meaning for request to SS
  # status:
  #     1 byte, only meaningful for response
  #
  module IO
    class ProtocolHeader < BinData::Record
      class MessageTypes
        OperationalMessage = 0
        AdminMessage = 1
        ClusterControlMessage = 2
      end

      class RequestTypes
        Response = 0
        TwoWayRequest = 1
        OneWayRequest = 2
      end

      class OpCodes
        Nop = 0x00
        Create = 0x01
        Get = 0x02
        Update = 0x03
        Set = 0x04
        Destroy = 0x05
        PrepareCreate = 0x81
        Read = 0x82
        PrepareUpdate = 0x83
        PrepareSet = 0x84
        PrepareDelete = 0x85
        Delete = 0x86
        Commit = 0xC1
        Abort = 0xC2
        Repair = 0xC3
        MarkDelete = 0xC4
        Clone = 0xE1
        MockSetParam = 0xFE
        MockReSet = 0xFF

        def self.valid?(opcode)
          constants.each do |constant|
            return true if const_get(constant) == opcode
          end
          false
        end
      end

      class MessageTypeFlag < BinData::Record
        bit2 :message_request_type, initial_value: ProtocolHeader::RequestTypes::TwoWayRequest
        bit6 :message_type, initial_value: ProtocolHeader::MessageTypes::OperationalMessage
      end

      def request?
        message_type_flag.message_request_type != RequestTypes::Response
      end

      endian :big
      uint16 :magic, value: 0x5050
      uint8  :version, value: 1
      message_type_flag :message_type_flag
      uint32 :message_size
      uint32 :opaque, initial_value: 0
      uint8 :opcode, initial_value: OpCodes::Nop
      uint8 :flag, value: 0

      uint16 :shard_id, value: 0, onlyif: -> { request? }
      uint8 :reserved, onlyif: -> { !request? }
      uint8 :status, onlyif: -> { !request? }

      def request_type
        message_type_flag.message_request_type
      end

      def message_type
        message_type_flag.message_type
      end
    end
  end
end
