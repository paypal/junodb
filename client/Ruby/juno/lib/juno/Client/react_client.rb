# frozen_string_literal: true

# Top module for juno client
module Juno
  # Module for code exposed to the developer
  module Client
    # Async Client for Juno
    class ReactClient
      # @ Global Opaque generator. Uses ConcurrentFixnum for thread safety
      @@OPAQUE_GENERATOR = Concurrent::AtomicFixnum.new(-1)
      # @ Variable to count failed requests
      @@fail_count = Concurrent::AtomicFixnum.new(0)
      # @ Variable to count responses received
      @@received = Concurrent::AtomicFixnum.new(0)

      # Constants for operation retry
      MAX_OPERATION_RETRY = 1
      MAX_RETRY_INTERVAL = 15 # msec
      MIN_RETRY_INTERVAL = 10 # msec

      def self.op_count
        @@OPAQUE_GENERATOR.value
      end

      def self.failed_count
        @@fail_count.value
      end

      def self.recv
        @@received.value
      end

      def initialize
        @PROG_NAME = self.class.name
        @LOGGER = Juno::Logger.instance
        @request_queue = Juno::Net::RequestQueue.instance
        @executor = Concurrent::ThreadPoolExecutor.new(min_threads: 4, max_threads: 16, max_queue: 10_000)
        @count = 0
      end

      def stop
        @LOGGER.info(@PROG_NAME) { 'stop initiated by client' }
        @request_queue.stop
        @executor.shutdown
        @executor.wait_for_termination
        @executor.kill
      end

      # Function to create new key value pair
      # @param key [String] key for the document (required)
      # @param value [String] value for the document (required)
      # @param ttl [Integer] Time to live for the document (optional, default: read from config file)
      # @return [Boolean] True if operation submited successfully, else false
      # @see #process_single
      # @see Juno::DefaultProperties::DEFAULT_LIFETIME_S
      def create(key, value, ttl: nil)
        juno_request = Juno::Client::JunoRequest.new(key: key,
                                                     value: value,
                                                     version: 0,
                                                     type: Juno::Client::JunoRequest::Type::CREATE,
                                                     time_to_live_s: ttl,
                                                     creation_time: Time.now.to_i)
        process_single(juno_request)
      end

      # Function to set value for given key
      # @param key [String] key for the document (required)
      # @param value [String] value for the document (required)
      # @param ttl [Integer] Time to live for the document (optional, default: read from config file)
      # @return [Boolean] True if operation submited successfully, else false
      # @see #process_single
      # @see Juno::DefaultProperties::DEFAULT_LIFETIME_S
      def set(key, value, ttl: nil)
        juno_request = Juno::Client::JunoRequest.new(key: key,
                                                     value: value,
                                                     version: 0,
                                                     type: Juno::Client::JunoRequest::Type::SET,
                                                     time_to_live_s: ttl,
                                                     creation_time: Time.now.to_i)
        process_single(juno_request)
      end
      # @!method write
      # @see #create
      alias write set

      def compare_and_set(record_context, value, ttl)
        unless record_context.is_a?(Juno::Client::RecordContext)
          raise ArgumentError, 'recird context should be of type Juno::Client::RecordContext'
        end
        raise ArgumentError, 'Version cannot be less than 1' if record_context.version.to_i < 1

        juno_request = Juno::Client::JunoRequest.new(key: record_context.key.to_s,
                                                     value: value,
                                                     version: record_context.version.to_i,
                                                     type: Juno::Client::JunoRequest::Type::UPDATE,
                                                     time_to_live_s: ttl,
                                                     creation_time: Time.now.to_i)
        process_single(juno_request)
      end

      # Function to get existing key value pair
      # @param key [String] key for the document (required)
      # @param value [String] value for the document (required)
      # @param ttl [Integer] Time to live for the document (optional, default: read from config file)
      # @return [Juno::Client::JunoResponse]
      # @see Juno::Client#process_single
      # @see Juno::DefaultProperties::DEFAULT_LIFETIME_S
      def get(key, ttl: nil)
        juno_request = Juno::Client::JunoRequest.new(key: key,
                                                     value: '',
                                                     version: 0,
                                                     type: Juno::Client::JunoRequest::Type::GET,
                                                     time_to_live_s: ttl,
                                                     creation_time: Time.now.to_i)
        process_single(juno_request)
      end
      # @!method read
      # @see #get
      alias read get

      def update(key, value, ttl: nil)
        juno_request = Juno::Client::JunoRequest.new(key: key,
                                                     value: value,
                                                     version: 0,
                                                     type: Juno::Client::JunoRequest::Type::UPDATE,
                                                     time_to_live_s: ttl,
                                                     creation_time: Time.now.to_i)
        process_single(juno_request)
      end

      def destroy(key, ttl: nil)
        juno_request = Juno::Client::JunoRequest.new(key: key,
                                                     value: '',
                                                     version: 0,
                                                     type: Juno::Client::JunoRequest::Type::DESTROY,
                                                     time_to_live_s: ttl,
                                                     creation_time: Time.now.to_i)
        process_single(juno_request)
      end
      # @!method delete
      # @see #destroy
      alias delete destroy

      def exist?(key); end

      private

      # Function to process a request. Set operation retry true in config file
      # @param ttl [Juno::Client::JunoRequest]
      # @return [Boolean] True if operation submited successfully, else false
      # @see Juno::ClientUtils#validate
      # @see Juno::DefaultProperties::DEFAULT_LIFETIME_S
      # @see Juno::Net::RequestQueue#opaque_resp_queue_map
      def process_single(juno_request)
        # rubocop:disable Style/SignalException
        # Concurrent::Future object to execute asynchronously
        Concurrent::Future.execute(executor: @executor) do
          begin
            begin
              juno_message = Juno::ClientUtils.validate!(juno_request)
            rescue ArgumentError => e
              fail(e.message)
            end
            operation_retry = Juno.juno_config.operation_retry
            juno_resp = nil
            opaque = nil
            opaque_resp_queue_map = @request_queue.opaque_resp_queue_map
            (MAX_OPERATION_RETRY + 1).times do
              opaque = @@OPAQUE_GENERATOR.increment
              operation_message = Juno::ClientUtils.create_operation_message(juno_message, opaque)

              resp_queue = SizedQueue.new(1)
              opaque_resp_queue_map[opaque] = resp_queue

              # map to compute response times for requests
              msg_buffer = StringIO.new
              operation_message.write(msg_buffer)

              fail('Queue full') unless @request_queue.push(msg_buffer.string,
                                                            operation_message&.metadata_component&.request_uuid.to_s)

              resp = nil
              begin
                Timeout.timeout(Juno.juno_config.response_timeout.to_f / 1000) do
                  resp = resp_queue.pop
                end
              rescue Timeout::Error
                resp = nil
              end

              if resp.nil?
                fail('Request Timeout') unless operation_retry

                @LOGGER.debug(@PROG_NAME) { "Retrying #{opaque} " }
                operation_retry = false
                # Backoff time for request retry
                sec = rand(MAX_RETRY_INTERVAL - MIN_RETRY_INTERVAL) + MIN_RETRY_INTERVAL
                @LOGGER.debug(@PROG_NAME) { "Backoff time #{sec.to_f / 1000}ms " }
                sleep(sec.to_f / 1000)
                next
              end
              juno_resp_msg = Juno::ClientUtils.decode_operation_message(resp)
              fail('Could not fetch valid response') if juno_resp_msg.nil?

              juno_resp = Juno::Client::JunoResponse.new(key: juno_resp_msg.key, value: juno_resp_msg.value,
                                                         version: juno_resp_msg.version, time_to_live_s: juno_resp_msg.time_to_live_s,
                                                         creation_time: juno_resp_msg.creation_time,
                                                         operation_status: juno_resp_msg.server_status[:client_operation_status])

              fail(juno_resp.status[:error_msg]) unless [Juno::Client::OperationStatus::SUCCESS[:code], Juno::Client::OperationStatus::CONDITION_VIOLATION[:code],
                                                         Juno::Client::OperationStatus::NO_KEY[:code], Juno::Client::OperationStatus::RECORD_LOCKED[:code],
                                                         Juno::Client::OperationStatus::UNIQUE_KEY_VIOLATION[:code], Juno::Client::OperationStatus::TTL_EXTEND_FAILURE[:code]].include?(juno_resp.status[:code])

              break
            end
          rescue StandardError => e
            puts e.backtrace
            fail(e)
          ensure
            opaque_resp_queue_map.delete(opaque)
            @@fail_count.increment if juno_resp.nil?
            @@received.increment unless juno_resp.nil?
            # rubocop:enable Style/SignalException
          end
          juno_resp
        end
      rescue Concurrent::RejectedExecutionError
        @LOGGER.error(@PROG_NAME) { 'Too many requests Concurrent::Future' }
        nil
      end
    end
  end
end
