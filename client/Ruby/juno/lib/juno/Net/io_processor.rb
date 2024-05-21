# frozen_string_literal: true

require 'concurrent'
module Juno
  module Net
    # Module to handle connections to server, reading/writing requests from request queue
    class IOProcessor < BaseProcessor
      INITIAL_BYPASSLTM_RETRY_INTERVAL = 337_500
      MAX_BYPASSLTM_RETRY_INTERVAL = 86_400_000
      INIT_WAIT_TIME = 200
      MAX_WAIT_TIME = 60_000
      # class variable to count messages sent using send_data
      @@counter = Concurrent::AtomicFixnum.new(0)
      def self.send_count
        @@counter.value
      end

      # @param request_queue [Juno::Net::RequestQueue]
      # @param opaque_resp_queue_map [Concurrent::Map] - map containing opaque as key and value as Response queue corresponding to opaque
      def initialize(request_queue, opaque_resp_queue_map)
        super()
        @PROG_NAME = self.class.name
        @LOGGER = Juno::Logger.instance
        @stop = false
        @request_queue = request_queue
        @opaque_resp_queue_map = opaque_resp_queue_map
        @ctx = nil ## changed
        @handshake_failed_attempts = 0
        @reconnect_wait_time = INIT_WAIT_TIME
        @shift = 5 # seconds
        @next_bypass_ltm_check_time = Time.now
        @bypass_ltm_retry_interval = INITIAL_BYPASSLTM_RETRY_INTERVAL
        # @config = config
        @channel = nil
        @next_reconnect_due = Float::INFINITY
      end

      def connection_lifetime
        Juno.juno_config.connection_lifetime
      end

      def put_response(operation_message)
        return if operation_message.nil?

        unless Juno::Net::PingMessage.ping_response?(operation_message, self)
          opaque = operation_message&.protocol_header&.opaque
          return if opaque.nil?

          resp_queue = @opaque_resp_queue_map.get_and_set(opaque.to_i, nil)
          if !resp_queue.nil?
            begin
              resp_queue.push(operation_message)
            rescue ThreadError
              @LOGGER.debug(@PROG_NAME) { "response queue for #{opaque.to_i} is full" }
            end
          else
            @LOGGER.debug(@PROG_NAME) { "resp_queue nil for #{opaque.to_i}" }
          end
        end
        nil
      end

      def disconnect_channel(channel)
        EventMachine::Timer.new(2 * Juno.juno_config.max_response_timeout.to_f / 1000) do
          channel&.close_connection_after_writing if !channel.nil? && channel.is_connected?
        end
      end

      def set_recycle_timer
        @recycle_timer = EventMachine::Timer.new(Juno.juno_config.connection_lifetime.to_f / 1000) do
          juno_connect(true)
        end
      end

      def initiate_bypass_ltm
        send_ping_message
        EventMachine::Timer.new(Juno.juno_config.response_timeout.to_f / 1000) do
          ip = ping_ip
          unless ip.nil?
            new_channel = EventMachine.connect(ip.to_s, Juno.juno_config.port, ClientHandler, self)
            EventMachine::Timer.new(Juno.juno_config.connection_timeout.to_f / 1000) do
              if new_channel.is_connected?
                @LOGGER.info(@PROG_NAME) { "conncected to Proxy #{ip}:#{Juno.juno_config.port} " }
                old_channel = @channel
                @channel = new_channel
                disconnect_channel(old_channel)
              else
                @LOGGER.info(@PROG_NAME) { "could not conncect to Proxy #{ip}:#{Juno.juno_config.port} " }
              end
            end
          end
        end
      end

      # Sends ping message to LoadBalancer to get ProxyIP
      # @see Juno::Net::PingMessage
      def send_ping_message
        ping_message = Juno::Net::PingMessage.new
        buff = StringIO.new
        ping_message.write(buff)
        request_uuid = ping_message&.operation_message&.metadata_component&.request_uuid.to_s
        @request_queue.push(buff.string, request_uuid)
      end

      # Method to handle connections creation, re-attempts on failure, initiates connection refresh and connection to Proxy
      # @param recycle [Boolean] - True if connection refresh request (optional, default: false)
      def juno_connect(recycle = false)
        return if !recycle && !@channel.nil? && @channel.is_connected?

        new_channel = EventMachine.connect(Juno.juno_config.host, Juno.juno_config.port, ClientHandler, self)
        new_channel.pending_connect_timeout = Juno.juno_config.connection_lifetime
        EventMachine::Timer.new(Juno.juno_config.connection_timeout.to_f / 1000) do
          if new_channel.is_connected?
            @LOGGER.info(@PROG_NAME) { "conncected to #{Juno.juno_config.host}:#{Juno.juno_config.port} " }
            if recycle
              old_channel = @channel
              @channel = new_channel
              disconnect_channel(old_channel)
            else
              @channel = new_channel
            end
            initiate_bypass_ltm if use_ltm?
            set_recycle_timer
          else
            @recycle_timer&.cancel
            new_channel&.close_connection if !new_channel.nil? && new_channel.is_connected?
            @LOGGER.info(@PROG_NAME) do
              "Could not conncect to #{Juno.juno_config.host}:#{Juno.juno_config.port}\n Retrying in #{@reconnect_wait_time.to_f / 1000}ms "
            end
            EventMachine::Timer.new(@reconnect_wait_time.to_f / 1000) do
              @reconnect_wait_time *= 2
              @reconnect_wait_time = MAX_WAIT_TIME if @reconnect_wait_time > MAX_WAIT_TIME
              @reconnect_wait_time *= (1 + 0.3 * rand)
              juno_connect(recycle)
            end
          end
        end
      end

      def stop
        @stop = true
      end

      # Event loop to continously check for requests in @request_queue
      def run
        EventMachine.run do
          juno_connect
          EventMachine.tick_loop do
            if !@channel.nil? && @channel.is_connected?
              # key = "19key#{rand(100) + rand(1_000_000)}"
              item = @request_queue.pop
              unless item.nil?
                @@counter.increment
                @channel.send_data(item.msg_buffer)
              end
            end
            :stop if @stop == true
          rescue Exception => e
            @LOGGER.error(@PROG_NAME) do
              "Error in tick_loop: #{e.message}. Stopping tick_loop"
            end
            :stop
          end.on_stop do
            @LOGGER.debug(@PROG_NAME) do
              "tick loop stopped. Stop initiated by client #{@stop}"
            end
            reset_connections
            EventMachine::Timer.new(2 * Juno.juno_config.connection_timeout.to_f / 1000) do
              EventMachine.stop
            end
          end
        rescue Exception => e
          @LOGGER.debug(@PROG_NAME) do
            "EventMachine Fatal Exception #{e.message}"
          end
          reset_connections
          EventMachine.stop
        end
      end

      def reset_connections
        @recycle_timer&.cancel
        disconnect_channel(@channel)
      end

      def use_ssl?
        Juno.juno_config.use_ssl
      end

      def host
        Juno.juno_config.host
      end

      def port
        Juno.juno_config.port
      end

      def use_ltm?
        host != '127.0.0.1' && Juno.juno_config.bypass_ltm # boolean
      end

      def bypass_ltm_disabled?
        if Time.now > @next_bypass_ltm_check_time && @bypass_ltm_retry_interval < MAX_BYPASSLTM_RETRY_INTERVAL
          return false
        end

        true
      end
    end
  end
end
