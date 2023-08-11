# frozen_string_literal: true

module Juno
  module Net
    # BaseProcessor - base class for IOProcessor
    # Handles logic for reading and writing ping_ip for bypass ltm
    class BaseProcessor
      def initialize(_ = nil)
        @ping_queue = SizedQueue.new(1)
      end

      def ping_ip=(ip)
        @ping_queue.push(ip)
      end

      def ping_ip
        begin
          ip = @ping_queue.pop(true)
        rescue ThreadError
          return nil
        end

        return nil if ip.to_s.empty?

        begin
          IPAddr.new(ip)
        rescue StandardError
          nil
        end
      end

      def clear_ping_queue
        @ping_queue.clear
      end
    end
  end
end
