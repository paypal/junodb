# frozen_string_literal: true

module Juno
  module Client
    # Response sent to the application
    class JunoResponse
      attr_accessor :key, :status, :value, :record_context

      # @param key [String] (required)
      # @param status [Juno::Client::OperationStatus] (required)
      # @param value [String] (required)
      # @param version [Integer] (required)
      # @param creation_time [Integer] (required)
      # @param time_to_live_s [Integer] (required)
      def initialize(key:, value:, version:, time_to_live_s:, creation_time:, operation_status:)
        @PROG_NAME = self.class.name
        @LOGGER = Juno::Logger.instance
        @key = key
        @status = operation_status
        @value = value
        @record_context = Juno::Client::RecordContext.new(key: key, version: version, creation_time: creation_time,
                                                          time_to_live_s: time_to_live_s)
      end
    end
  end
end
