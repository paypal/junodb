# frozen_string_literal: true

module Juno
  module Net
    # Hanldes connection creation and receiving data asynchronously
    # Managed by EventMachine::Connection
    class ClientHandler < EventMachine::Connection
      # Constant to count messages received
      @@recv_count = Concurrent::AtomicFixnum.new(0)

      def self.received_messages
        @@recv_count.value
      end

      # Constructor
      # @param io_processor (Juno::Net::IOProcessor)
      def initialize(io_processor)
        super
        @PROG_NAME = self.class.name
        @LOGGER = Juno::Logger.instance
        @io_processor = io_processor
        @channel = self
        @connected = Concurrent::AtomicBoolean.new(false)
        @ssl_connected = Concurrent::AtomicBoolean.new(false)
      end

      # Method called once for each instance of Juno::Net::ClientHandler at initialization
      def post_init
        start_tls_connection if use_ssl?
      end

      # starts tls connection once TCP connection is estabilished
      def start_tls_connection
        raise 'SSL Cert file not found' unless File.exist?(Juno.juno_config.ssl_cert_file)
        raise 'SSL Key file not found' unless File.exist?(Juno.juno_config.ssl_key_file)

        @channel.start_tls(
          private_key_file: Juno.juno_config.ssl_key_file, cert: File.read(Juno.juno_config.ssl_cert_file)
        )
        # Timer to check if SSL Handshake was successful
        EventMachine::Timer.new(Juno.juno_config.max_connection_timeout.to_f / 1000) do
          if @ssl_connected.false?
            puts 'SLL Handshake timeout'
            close_connection
          end
        end
      end

      # Method called when TCP connection estabilished. If useSSL is true, it is called after a successfull ssl handshake
      def on_connection_completed
        # puts "completed #{Time.now}"
      end

      # method to check if channel is connected
      def is_connected?
        if use_ssl?
          return false if @ssl_connected.false?
        elsif @connected.false?
          return false
        end

        # get ip and port of server
        # Socket.unpack_sockaddr_in(@channel.get_peername)
        true
      rescue Exception => e
        @LOGGER.error(@PROG_NAME) { e.message }
        false
      end

      def use_ssl?
        Juno.juno_config.use_ssl
      end

      def use_ltm?
        Juno.juno_config.bypass_ltm
      end

      # method called by EventMachine when data is received from server
      # @param data [String] - byte data received from server
      def receive_data(data)
        @@recv_count.increment
        # puts @@recv_count

        EventMachine.defer do
          operation_message = Juno::IO::OperationMessage.new
          operation_message.read(StringIO.new(data))
          @io_processor.put_response(operation_message)
        end
      end

      # Called by EventMachine after TCP Connection estabilished
      def connection_completed
        @connected.value = true
        on_connection_completed unless use_ssl?
      end

      # Called by EventMachine after connection disconnected
      # @param m - Error if disconnected due to an error
      def unbind(error)
        @connected.value = false
        @ssl_connected.value = false
        puts error unless error.nil?
      end

      # Called by EventMachine after ssl handshake
      def ssl_handshake_completed
        @ssl_connected.value = true
        on_connection_completed if use_ssl?

        # puts get_cipher_name
        # puts get_cipher_protocol
        @server_handshake_completed = true
      end
    end
  end
end
