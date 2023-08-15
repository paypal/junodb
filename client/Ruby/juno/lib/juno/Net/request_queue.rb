# frozen_string_literal: true

module Juno
  module Net
    # DataType of each item in RequestQueue
    class QueueEntry
      attr_accessor :msg_buffer, :req_id, :enqueue_time

      # @param msg_bugger [StringIO] (required)
      # @param id [String] - request UUID (required)
      def initialize(msg_buffer, id)
        @msg_buffer = msg_buffer
        @req_id = id
        @enqueue_time = Time.now
      end
    end

    # Request queue - Singleton
    class RequestQueue
      # mutex to synchronize creation of RequestQueue instance
      @@mutex = Mutex.new

      # Singleton instance
      @@instance = nil

      def self.instance
        return @@instance unless @@instance.nil?

        @@mutex.synchronize do
          @@instance ||= new
        end
      end

      private_class_method :new

      attr_reader :opaque_resp_queue_map

      def initialize
        @PROG_NAME = self.class.name
        @LOGGER = Juno::Logger.instance
        @size = 13_000 # Default request queue size
        @request_queue = SizedQueue.new(@size)
        @opaque_resp_queue_map = Concurrent::Map.new
        @worker_pool = Juno::Net::WorkerPool.new(self)
      end

      def full?
        @request_queue.size == @size
      end

      def size
        @request_queue.size
      end

      def stop
        @worker_pool.stop
        @request_queue.clear
      end

      # @param operation_message [Juno::IO::OperatioinMessage] (required)
      # @return [Boolean] - true if successfully pushed item to queue. Else false
      def push(msg_buffer, request_uuid)
        # buffer = StringIO.new
        # operation_message.write(buffer)
        # request_uuid = operation_message&.metadata_component&.request_uuid.to_s
        request_uuid = 'not_set' if request_uuid.to_s.empty?

        begin
          @request_queue.push(QueueEntry.new(msg_buffer, request_uuid), true)
        rescue ThreadError
          return false
        end
        true
      end

      # @return [QueueEntry] - nil if queue empty
      def pop
        @request_queue.pop(true)
      rescue ThreadError
        # queue empty
        nil
      end
    end
  end
end
