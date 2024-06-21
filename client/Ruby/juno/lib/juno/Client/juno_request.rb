# frozen_string_literal: true

# Top module for juno client
module Juno
  module Client
    # Request Object created from application request
    class JunoRequest
      class Type
        CREATE = 1
        GET = 2
        UPDATE = 3
        SET = 4
        DESTROY = 5
      end

      attr_accessor :key, :version, :type, :value, :time_to_live_s, :creation_time

      # Constructor for JunoRequest
      # @param key [String] key for the document (required)
      # @param version [Integer] value for the document (required)
      # @param version [Juno::JunoRequest::Type] value for the document (required)
      # @param type [String] value for the document (optional, default: 1 byte string: 0.chr)
      # @param type [Integer] Time to live for the document (optional, default: read from config file)
      # @param type [Integer] Time to live for the document (optional, default: initialized to Time.now in JunoMessage)
      def initialize(key:, version:, type:, value: nil, time_to_live_s: nil, creation_time: nil)
        @PROG_NAME = self.class.name
        @LOGGER = Juno::Logger.instance
        @key = key
        @version = version.to_i
        @type = type
        @value = value.to_s.empty? ? 0.chr : value.to_s
        @time_to_live_s = time_to_live_s.to_i
        @creation_time = creation_time
      end

      def ==(other)
        return false unless other.is_a?(JunoRequest)

        other.key == @key &&
          other.version == @version &&
          other.type == @type &&
          other.value == @value &&
          other.time_to_live_s == @time_to_live_s &&
          other.creation_time == @creation_time
      end

      # Function to serialize JunoRequest
      def to_s
        "JunoRequest key:#{@key} version:#{@version} type:#{@type}, value: #{@value}, time_to_live: #{@time_to_live}, creation_time: #{@creation_time}"
      end
    end
  end
end
