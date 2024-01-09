# frozen_string_literal: true

module Juno
  module Client
    class RecordContext
      attr_reader :key, :version, :creation_time, :time_to_live_s

      # @param key [String] (required)
      # @param version [Integer] (required)
      # @param creation_time [Integer] (required)
      # @param time_to_live_s [Integer] (required)
      def initialize(key:, version:, creation_time:, time_to_live_s:)
        @PROG_NAME = self.class.name
        @LOGGER = Juno::Logger.instance
        @key = key
        @version = version
        @creation_time = creation_time
        @time_to_live_s = time_to_live_s
      end
    end
  end
end
