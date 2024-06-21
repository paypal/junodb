# frozen_string_literal: true

module Juno
  module ClientUtils
    def self.create_operation_message(juno_message, opaque)
      protocol_header = Juno::IO::ProtocolHeader.new
      protocol_header.version = juno_message.version
      protocol_header.opcode = juno_message.operation_type
      protocol_header.opaque = opaque

      metadata_component = Juno::IO::MetadataComponent.new
      if juno_message.time_to_live_s.to_i.positive?
        metadata_component.set_time_to_live(juno_message.time_to_live_s.to_i)
      end
      metadata_component.set_version(juno_message.version)

      if [Juno::IO::JunoMessage::OperationType::CREATE,
          Juno::IO::JunoMessage::OperationType::SET].include?(juno_message.operation_type)
        metadata_component.set_creation_time(Time.now.to_i)
      end

      metadata_component.set_expiration_time((Time.now + juno_message.time_to_live_s).to_i) # what ?
      metadata_component.set_request_uuid(juno_message.request_uuid)
      metadata_component.set_source_info(app_name: juno_message.app_name, ip: juno_message.ip, port: juno_message.port)
      metadata_component.set_originator_request_id # what

      payload_component = Juno::IO::PayloadComponent.new
      payload_component.namespace = juno_message.namespace
      payload_component.payload_key = juno_message.key
      payload_component.set_value(juno_message.value, juno_message.compression_type)

      operation_message = Juno::IO::OperationMessage.new
      operation_message.protocol_header = protocol_header
      operation_message.metadata_component = metadata_component
      operation_message.payload_component = payload_component

      juno_message.message_size = operation_message.size

      operation_message
    end

    def self.compressed_value(value)
      compressed_value = Snappy.deflate(value)
      compression_achieved = 100 - (compressed_value.length * 100) / value.length
      [compressed_value, compression_achieved]
    rescue Exception
      [value, false]
    end

    def self.decompress_value(value)
      Snappy.inflate(value)
    rescue Exception
      # Log failure
      value
    end

    def self.validate!(juno_request)
      return false unless juno_request.is_a?(Juno::Client::JunoRequest)

      raise ArgumentError, 'Juno request key cannot be empty' if juno_request.key.to_s.nil?

      juno_request.time_to_live_s = Juno.juno_config.default_lifetime unless juno_request.time_to_live_s.to_i.positive?

      if juno_request.time_to_live_s > Juno.juno_config.max_lifetime || juno_request.time_to_live_s.negative?
        raise ArgumentError,
              "Record time_to_live_s (#{juno_request.time_to_live_s}s) cannot be greater than  #{Juno.juno_config.max_lifetime}s or negative ."
      end

      if juno_request.key.to_s.size > Juno.juno_config.max_key_size
        raise ArgumentError,
              "Key size cannot be greater than #{Juno.juno_config.max_key_size}"
      end

      if juno_request.key.to_s.size > Juno.juno_config.max_key_size
        raise ArgumentError,
              "Key size cannot be greater than #{Juno.juno_config.max_key_size}"
      end

      juno_message = Juno::IO::JunoMessage.new
      juno_message.key = juno_request.key
      juno_message.version = juno_request.version
      juno_message.operation_type = juno_request.type
      juno_message.time_to_live_s = juno_request.time_to_live_s
      juno_message.creation_time = juno_request.creation_time
      juno_message.namespace = Juno.juno_config.record_namespace
      juno_message.app_name = Juno.juno_config.app_name
      juno_message.request_uuid = UUIDTools::UUID.random_create.to_s
      juno_message.ip = IPAddr.new(Juno::Utils.local_ips[0])
      juno_message.port = 0

      unless [Juno::Client::JunoRequest::Type::GET,
              Juno::Client::JunoRequest::Type::DESTROY].include?(juno_request.type)
        payload_value = juno_request.value
        is_compressed = false
        compression_achieved = 0
        if Juno.juno_config.use_payload_compression && value.length > 1024
          payload_value, compression_achieved = compressed_value(value)
          is_compressed = true if compression_achieved.positive?
        end
        juno_message.is_compressed = is_compressed
        juno_message.value = payload_value
        juno_message.compression_achieved = compression_achieved
        juno_message.compression_type = is_compressed ? Juno::IO::CompressionType::Snappy : Juno::IO::CompressionType::None
      end

      juno_message
    end

    # @params operation_message [Juno::IO::OperationMessage] (required)
    # @returns [Juno::IO::JunoMessage]
    def self.decode_operation_message(operation_message)
      return nil unless operation_message.is_a?(Juno::IO::OperationMessage)

      juno_message = Juno::IO::JunoMessage.new
      opcode = operation_message.protocol_header.opcode.to_i
      juno_message.operation_type = Juno::IO::JunoMessage::OperationType.get(opcode)

      server_status = operation_message.protocol_header.status.to_i
      juno_message.server_status = Juno::ServerStatus.get(server_status)

      juno_message.message_size = operation_message.protocol_header.message_size.to_i

      unless operation_message.metadata_component.nil?
        metadata_component = operation_message.metadata_component
        juno_message.time_to_live_s = metadata_component.time_to_live.to_i
        juno_message.ip = metadata_component.ip
        juno_message.port = metadata_component.port
        juno_message.version = metadata_component.version.to_i
        juno_message.creation_time = metadata_component.creation_time.to_i
        juno_message.expiration_time = metadata_component.expiration_time.to_i
        juno_message.request_uuid = metadata_component.request_uuid
        juno_message.app_name = metadata_component.app_name
        juno_message.last_modification = metadata_component.last_modification.to_i
        juno_message.originator_request_id = metadata_component.originator_request_id.to_s
        juno_message.correlation_id = metadata_component.correlation_id
        juno_message.request_handling_time = metadata_component.request_handling_time.to_i
        # juno_message.request_start_time = metadata_component.
        # expiry
      end

      unless operation_message.payload_component.nil?
        juno_message.namespace = operation_message.payload_component.namespace.to_s
        juno_message.key = operation_message.payload_component.payload_key.to_s
        juno_message.is_compressed = operation_message.payload_component.compressed?
        juno_message.compression_type = operation_message.payload_component.compression_type.to_i
        juno_message.value = if operation_message.payload_component.payload_length.to_i.zero?
                               juno_message.compression_achieved = 0
                               nil
                             elsif juno_message.is_compressed
                               compressed_value = operation_message.payload_component.value.to_s
                               decompressed_value = decompress_value(compressed_value)
                               juno_message.compression_achieved = 100 - (compressed_value.length / decompressed_value.length.to_f) * 100.0
                               decompressed_value
                             else
                               juno_message.compression_achieved = 0
                               operation_message.payload_component.value.to_s
                             end
      end

      juno_message
    end
  end
end
