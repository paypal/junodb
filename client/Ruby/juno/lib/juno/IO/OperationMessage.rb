# frozen_string_literal: true

module Juno
  module IO
    class OperationMessage
      attr_accessor :protocol_header, :metadata_component, :payload_component

      def initialize
        @protocol_header = ProtocolHeader.new
        @metadata_component = nil
        @payload_component = nil
      end

      # Calculates size of message
      def size
        total_size = protocol_header.num_bytes
        total_size += payload_component.num_bytes unless payload_component.nil?
        total_size += metadata_component.num_bytes unless metadata_component.nil?
        total_size
      end

      # Function to serialize message to buffer
      # @param io [StringIO] (required)
      def write(io)
        protocol_header.message_size = size

        protocol_header.write(io)
        metadata_component&.write(io)
        payload_component&.write(io)
        nil
      end

      # Function to de-serialize message to buffer
      # @param io [StringIO] (required)
      def read(io)
        return if io.eof? || (io.size - io.pos) < 16

        @protocol_header = ProtocolHeader.new
        @metadata_component = MetadataComponent.new
        @payload_component = PayloadComponent.new

        @protocol_header.read(io)

        remaining_size = protocol_header.message_size - 16
        prev_position = io.pos

        @metadata_component.read(io) if !io.eof? && (io.size - io.pos) >= remaining_size

        remaining_size -= (io.pos - prev_position)

        @payload_component.read(io) if !io.eof? && (io.size - io.pos) >= remaining_size
        nil
      end
    end
  end
end
