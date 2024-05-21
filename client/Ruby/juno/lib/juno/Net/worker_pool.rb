# frozen_string_literal: true

module Juno
  module Net
    class WorkerPool
      def initialize(request_queue)
        raise 'Request queue cannot be nil' if request_queue.nil?

        @PROG_NAME = self.class.name
        @LOGGER = Juno::Logger.instance
        @request_queue = request_queue
        EventMachine.threadpool_size = 200
        @io_processor = Juno::Net::IOProcessor.new(@request_queue, @request_queue.opaque_resp_queue_map)
        init
      end

      def init
        @worker = Thread.new do
          @io_processor.run
        end
      end

      def active?
        @worker.alive?
      end

      def stop
        @io_processor.stop
      end
    end
  end
end
