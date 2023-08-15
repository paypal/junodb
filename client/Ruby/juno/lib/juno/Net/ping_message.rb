# frozen_string_literal: true

module Juno
  module Net
    # Module to Create/Decode Ping messages
    class PingMessage
      # Constant Internal app name to check for ping messages
      JUNO_INTERNAL_APPNAME = 'JunoInternal'

      # method to read the operation message
      attr_reader :operation_message

      # @param operation_message [Juno::IO::OperationMessage] (optional, default: Juno::Net::PingMessage::JUNO_INTERNAL_APPNAME)
      # @param opaque [Integer] (optional, default: 0)
      def initialize(app_name = nil, opaque = 0)
        @PROG_NAME = self.class.name
        @LOGGER = Juno::Logger.instance
        app_name = JUNO_INTERNAL_APPNAME if app_name.to_s.empty?

        meta_data_component = Juno::IO::MetadataComponent.new
        meta_data_component.set_request_uuid
        meta_data_component.set_source_info(app_name: app_name, ip: IPAddr.new(Juno::Utils.local_ips[0]), port: 0)

        protocol_header = Juno::IO::ProtocolHeader.new
        protocol_header.opcode = Juno::IO::ProtocolHeader::OpCodes::Nop
        protocol_header.opaque = opaque

        @operation_message = Juno::IO::OperationMessage.new
        @operation_message.metadata_component = meta_data_component
        @operation_message.protocol_header = protocol_header
      end

      # method to check if given operation message is a Ping response
      # Updates ping_ip in processor if it is a ping response
      # @param operation_message [Juno::IO::OperationMessage] (required)
      # @param operation_message [Juno::Net::IOProcessor] (required)
      def self.ping_response?(operation_message, processor)
        return false unless processor.use_ltm?

        opcode = operation_message&.protocol_header&.opcode
        raise 'missing protocol header' if opcode.nil?
        return false if opcode != Juno::IO::ProtocolHeader::OpCodes::Nop
        return false if operation_message&.metadata_component.nil?
        return false if operation_message&.metadata_component&.ip.to_s.empty?
        return false if operation_message&.metadata_component&.app_name != JUNO_INTERNAL_APPNAME

        ping_ip = operation_message.metadata_component.ip.to_s
        if ping_ip.split('.')[0] == '127'
          processor.ping_ip = ''
          return true
        end
        if Juno::Utils.local_ips.include?(ping_ip)
          processor.ping_ip = ''
          return true
        end
        processor.ping_ip = ping_ip
        true
      end

      # Function to serialize Component to buffer
      # @param buff [StringIO] (required)
      def write(buf)
        @operation_message.write(buf)
      end

      # Function to de-serialize Component from buffer
      # @param buff [StringIO] (required)
      def read(buf)
        @operation_message = Juno::IO::OperationMessage.new
        @operation_message.read(buf)
      end

      # Function to read the ping ip
      def ip
        @operation_message&.metadata_component&.ip
      end

      # Function to read the port from metadata component
      def port
        @operation_message&.metadata_component&.port
      end
    end
  end
end
