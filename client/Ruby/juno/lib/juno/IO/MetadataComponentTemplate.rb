# frozen_string_literal: true

module Juno
  module IO
    class MetadataComponentTemplate < BinData::Record
      class MetadataHeaderField < BinData::Record
        bit3 :size_type
        bit5 :field_tag
      end

      class FixedLengthField < BinData::Record
        mandatory_parameter :field_length

        string :data, read_length: :field_length
      end

      class SourceInfoField < BinData::Record
        def field_bytes
          field_length.num_bytes + app_name_length.num_bytes + port.num_bytes + ip.num_bytes + app_name.num_bytes
        end

        def padding_size
          (4 - field_bytes % 4) % 4
        end

        def ipv6?
          IPAddr.new_ntoh(ip).ipv6?
        end

        endian :big
        uint8 :field_length, value: -> { field_bytes + padding.num_bytes }
        uint8 :app_name_length, value: -> { ipv6? ? app_name.length | 128 : app_name.length }
        uint16 :port
        string :ip, read_length: -> { app_name_length & 128 == 1 ? 16 : 4 }
        string :app_name, read_length: :app_name_length
        string :padding, length: :padding_size
      end

      class CorrelationIDField < BinData::Record
        def padding_size
          size = component_size.num_bytes + correlation_id_length.num_bytes + correlation_id.num_bytes
          (4 - size % 4) % 4
        end

        endian :big
        uint8 :component_size, value: -> { num_bytes }
        uint8 :correlation_id_length, value: -> { correlation_id.length }
        string :correlation_id
        string :padding, length: :padding_size
      end

      def header_num_bytes
        component_size.num_bytes + tag_id.num_bytes + number_of_fields.num_bytes + metadata_fields.num_bytes
      end

      def header_padding_length
        (4 - header_num_bytes % 4) % 4
      end

      endian :big
      uint32 :component_size, value: -> { num_bytes }
      uint8 :tag_id, value: 0x02
      uint8 :number_of_fields, value: -> { metadata_fields.length }
      array :metadata_fields, initial_length: :number_of_fields, type: :metadata_header_field
      string :header_padding, length: :header_padding_length # implement padding length

      string :body, read_length: lambda {
                                   component_size - 4 - 1 - 1 - number_of_fields - header_padding.num_bytes
                                 }
    end
  end
end
