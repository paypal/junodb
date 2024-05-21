# frozen_string_literal: true

module Juno
  module IO
    # ** Payload (or KeyValue) Component **
    #
    # A 12-byte header followed by name, key and value
    #   Tag/ID: 0x01
    # * Header *
    #
    #       |0|1|2|3|4|5|6|7|0|1|2|3|4|5|6|7|0|1|2|3|4|5|6|7|0|1|2|3|4|5|6|7|
    #       |              0|              1|              2|              3|
    # ------+---------------+---------------+---------------+---------------+
    #     0 | Size                                                          |
    # ------+---------------+---------------+-------------------------------+
    #     4 | Tag/ID (0x01) | namespace len | key length                    |
    # ------+---------------+---------------+-------------------------------+
    #     8 | payload length                                                |
    # ------+---------------------------------------------------------------+
    #
    #  (
    #   The max namespace length: 255
    #   payload length = 0 if len(payload data) = 0, otherwise,
    #   payload length = 1 + len(payload data) = len(payload field)
    #  )
    #
    #
    # * Body *
    # +---------+-----+---------------+-------------------------+
    # |namespace| key | payload field | Padding to align 8-byte |
    # +---------+-----+---------------+-------------------------+
    #
    # * Payload field*
    # +---------------------+--------------+
    # | 1 byte payload type | Payload data |
    # +---------------------+--------------+
    #
    # * Payload Type
    # 0: payload data is the actual value passed from client user
    # 1: payload data is encrypted by Juno client library, details not specified
    # 2: payload data is encrypted by Juno proxy with AES-GCM. encryption key length is 256 bits
    # 3: Payload data is compressed by Juno Client library.
    #
    # * Payload data
    # for payload type 2
    # +--------------------------------+----------------+----------------+
    # | 4 bytes encryption key version | 12 bytes nonce | encrypted data |
    # +--------------------------------+----------------+----------------+
    #
    # for payload type 3
    # +---------------------------------+------------------+----------------+
    # | 1 byte size of compression type | compression type | compressed data|
    # +---------------------------------+------------------+----------------+
    #
    # * compression type
    # 1) snappy (default algorithm)
    # 2) TBD
    class PayloadComponent < BinData::Record
      class EncryptedPayloadData < BinData::Record
        mandatory_parameter :payload_data_length
      end

      class CompressedPayloadData < BinData::Record
        mandatory_parameter :payload_data_length
        def data_length
          eval_parameter(:payload_data_length) - 1 - compression_type
        end

        uint8 :compression_type_size, value: -> { :compression_type.length }
        string :compression_type, read_length: :compression_type_size, initial_value: CompressionType::None
        string :data, read_length: :data_length
      end

      # class PayloadBody < BinData::Record
      #   mandatory_parameter :payload_length
      #   uint8 :payload_type, initial_value: PayloadType::UnCompressed, only_if: -> { :payload_length.positive? } # optional

      #   choice :payload_data, selection: :payload_type, only_if: -> { :payload_length.positive? } do
      #     compressed_payload_data PayloadType::Compressed, payload_data_length: lambda {
      #                                                                             get_payload_data_length
      #                                                                           }

      #     uncompressed_payload_data PayloadType::UnCompressed, payload_data_length: lambda {
      #                                                                                 get_payload_data_length
      #                                                                               }

      #     encrypted_payload_data PayloadType::Encrypted, payload_data_length: lambda {
      #                                                                           get_payload_data_length
      #                                                                         }
      #   end
      # end

      class UncompressedPayloadData < BinData::Record
        mandatory_parameter :payload_data_length
        string :data, read_length: :payload_data_length
      end

      def get_payload_data_length
        (payload_length.positive? ? payload_length - 1 : 0)
      end

      # to prevent stack overflow
      def custom_num_bytes
        size = component_size.num_bytes + tag_id.num_bytes + namespace_length.num_bytes + key_length.num_bytes + payload_length.num_bytes + namespace.num_bytes + payload_key.num_bytes
        size += payload_type.num_bytes + payload_data.num_bytes if payload_length.positive?
        size
      end

      def padding_length
        (8 - custom_num_bytes % 8) % 8
      end

      endian :big
      uint32 :component_size, value: -> { num_bytes }
      uint8 :tag_id, value: 0x01
      uint8 :namespace_length, value: -> { namespace.length }
      uint16 :key_length, value: -> { payload_key.length }
      uint32 :payload_length, value: -> { payload_data.num_bytes.zero? ? 0 : payload_data.num_bytes + 1 }
      string :namespace, read_length: :namespace_length # required
      string :payload_key, read_length: :key_length # required
      uint8 :payload_type, onlyif: lambda {
                                     payload_length.positive?
                                   }, initial_value: PayloadType::UnCompressed # optional

      choice :payload_data, selection: :payload_type, onlyif: -> { payload_length.positive? } do
        compressed_payload_data PayloadType::Compressed, payload_data_length: lambda {
                                                                                get_payload_data_length
                                                                              }

        uncompressed_payload_data PayloadType::UnCompressed, payload_data_length: lambda {
                                                                                    get_payload_data_length
                                                                                  }

        encrypted_payload_data PayloadType::Encrypted, payload_data_length: lambda {
                                                                              get_payload_data_length
                                                                            }
      end
      string :padding, length: :padding_length

      def set_value(input_value, compression_type = CompressionType::None)
        if compression_type != CompressionType::None
          self.payload_type = PayloadType::Compressed
          payload_data.compression_type = compression_type
        else
          self.payload_type = PayloadType::UnCompressed
        end
        payload_data.data = input_value
      end

      def value
        payload_data.data
      end

      def compressed?
        return true if payload_type == PayloadType::Compressed

        false
      end

      def compression_type
        return payload_data.compression_type if compressed?

        CompressionType::None
      end
    end
  end
end
