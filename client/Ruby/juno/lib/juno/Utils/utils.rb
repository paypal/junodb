# frozen_string_literal: true

module Juno
  class Utils
    def self.local_ips
      ip_addresses = Socket.ip_address_list.select do |addr|
        addr.ipv4? && !addr.ipv4_loopback? # && !addr.ipv4_private?
      end
      ip_addresses.map!(&:ip_address)
    rescue StandardError => e
      puts e.message
      ['127.0.0.1']
    end

    def self.create_message(key, op_type, value = '')
      meta_data_component = Juno::IO::MetadataComponent.new
      # ip = IPAddr.new(Socket.ip_address_list.detect(&:ipv4_private?).ip_address)
      meta_data_component.set_time_to_live(Juno.juno_config.default_lifetime)
      meta_data_component.set_version(1)
      meta_data_component.set_creation_time(1000)
      meta_data_component.set_request_uuid
      meta_data_component.set_correlation_id
      meta_data_component.set_originator_request_id
      meta_data_component.set_source_info(app_name: Juno.juno_config.app_name, ip: IPAddr.new(Juno::Utils.local_ips[0]),
                                          port: Juno.juno_config.port)

      payload_component = Juno::IO::PayloadComponent.new
      payload_component.namespace = Juno.juno_config.record_namespace
      payload_component.payload_key = key
      if op_type == Juno::IO::ProtocolHeader::OpCodes::Create && !value.to_s.empty?
        is_compressed = false
        if Juno.juno_config.use_payload_compression && value.length > 1024
          value, compression_achieved = compressed_value(value)
          is_compressed = true if compression_achieved.positive?
        end
        if is_compressed
          puts 'using compression'
          payload_component.set_value(value, Juno::IO::CompressionType::Snappy)
        else
          payload_component.set_value(value)
        end
      end

      protocol_header = Juno::IO::ProtocolHeader.new
      protocol_header.opcode = op_type

      operation_message = Juno::IO::OperationMessage.new
      operation_message.metadata_component = meta_data_component
      operation_message.protocol_header = protocol_header
      operation_message.payload_component = payload_component
      buffer = StringIO.new
      operation_message.write(buffer)
      buffer
    end

    def self.ssl_context
      ssl_context = OpenSSL::SSL::SSLContext.new
      ssl_context.ssl_version = :TLSv1_1
      cert = OpenSSL::X509::Certificate.new(File.open(File.expand_path(File.join(
                                                                         __dir__, '..', '..', 'server.crt'
                                                                       ))))
      key = OpenSSL::PKey::RSA.new(File.open(File.expand_path(File.join(
                                                                __dir__, '..', '..', 'server.pem'
                                                              ))))
      ca_file = OpenSSL::X509::Certificate.new(File.open(File.expand_path(File.join(
                                                                            __dir__, '..', '..', 'myca.crt'
                                                                          ))))
      ssl_context.add_certificate(cert, key, [ca_file])
      # ssl_context.verify_mode = OpenSSL::SSL::VERIFY_PEER
      ssl_context.ssl_timeout = 10
      ssl_context.timeout = 10
      ssl_context
    end

    def self.ssl_request(buffer)
      socket = TCPSocket.open(Juno.juno_config.host, Juno.juno_config.port)
      if Juno.juno_config.use_ssl
        ctx = ssl_context
        socket = OpenSSL::SSL::SSLSocket.new(socket, ctx)
        socket.sync_close = true
        # socket.post_connection_check(Juno.juno_config.host)
        socket.connect
      end

      # puts socket.peer_finished_message.bytes.join(', ')
      # puts socket.verify_result

      socket.write(buffer.string)
      res_buffer = StringIO.new
      header = true

      size = 0
      while (line = socket.sysread(1024 * 16)) # buffer size of OpenSSL library
        if header
          p = Juno::IO::ProtocolHeader.new
          p.read(StringIO.new(line))
          header = false
          size = p.message_size
        end
        res_buffer.write(line)
        break if res_buffer.length == size
      end
      socket.close

      res_buffer.rewind

      res_buffer
    end

    def self.create_and_send_ping_message
      ping_message = Juno::Net::PingMessage.new
      ping_buffer = StringIO.new
      ping_message.write(ping_buffer)
      response = ssl_request(ping_buffer)
      ping_message = Juno::Net::PingMessage.new
      ping_message.read(response)
      ping_message
    end

    def self.time_diff_ms(a, b)
      (b - a).abs * 1000
    end
  end
end
