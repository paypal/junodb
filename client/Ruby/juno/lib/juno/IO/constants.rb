# frozen_string_literal: true

# Top module for juno client
module Juno
  # Submodule containing modules related to WireProtocol
  module IO
    class OffsetWidth
      # Count can be integer or '*'
      def self.UINT64(count = '')
        "Q#{count}"
      end

      def self.UINT32(count = '')
        "N#{count}"
      end

      def self.UINT16(count = '')
        "n#{count}"
      end

      def self.UINT8(count = '')
        "C#{count}"
      end
    end

    # Class containing constants for CompressionType
    class CompressionType
      None = 'None'
      Snappy = 'Snappy'

      def self.valid?(compression_type)
        constants.each do |constant|
          return true if const_get(constant) == compression_type
        end
        false
      end
    end

    # Class containing constants for PayloadType
    class PayloadType
      UnCompressed = 0x00
      Encrypted = 0x02
      Compressed = 0x03
    end
  end
end
