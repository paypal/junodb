# frozen_string_literal: true

module Juno
  module IO
    # JunoMessage containing all configuration required to create an operation message
    class JunoMessage
      attr_accessor :operation_type, :server_status,
                    :namespace, :key, :value, :is_compressed, :compression_type,
                    :time_to_live_s, :version, :creation_time, :expiration_time, :request_uuid,
                    :ip, :app_name, :port, :last_modification, :originator_request_id, :correlation_id,
                    :request_handling_time, :request_start_time, :message_size, :compression_achieved, :expiry

      def initialize
        @PROG_NAME = self.class.name
        @LOGGER = Juno::Logger.instance
        @operation_type = Juno::IO::JunoMessage::OperationType::NOP
        @server_status = Juno::ServerStatus::SUCCESS
        @compression_type = Juno::IO::CompressionType::None
        @is_compressed = false
        @compression_achieved = 0
        @message_size = 0
        @value = ''
      end

      class OperationType
        NOP = 0
        CREATE = 1
        GET = 2
        UPDATE = 3
        SET = 4
        DESTROY = 5

        @@status_code_map = nil

        def self.initialize_map
          @@status_code_map = {}

          constants.each do |const|
            const_obj = const_get(const)
            @@status_code_map[const_obj.to_i] = const_obj
          end
        end

        def self.get(status_code)
          initialize_map if @@status_code_map.nil?
          return @@status_code_map[status_code.to_i] if @@status_code_map.key?(status_code)

          INTERNAL_ERROR
        end
      end
    end
  end
end
