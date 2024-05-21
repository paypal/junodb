# frozen_string_literal: true

# Top module for juno client
module Juno
  # Module for code exposed to the developer
  module Client
    # Async Client for Juno
    class SyncClient
      def initialize
        @react_client = Juno::Client::ReactClient.new
      end

      # Function to create new key value pair
      # @param key [String] key for the document (required)
      # @param value [String] value for the document (required)
      # @param ttl [Integer] Time to live for the document (optional, default: read from config file)
      # @return [Juno::Client::JunoResponse]
      def create(key, value, ttl: nil)
        juno_resp = @react_client.create(key, value, ttl: ttl).wait
        return nil if juno_resp.nil?

        raise juno_resp.reason if juno_resp.rejected?

        juno_resp.value # JunoResponse
      end

      # Function to set value for given key
      # @param key [String] key for the document (required)
      # @param value [String] value for the document (required)
      # @param ttl [Integer] Time to live for the document (optional, default: read from config file)
      # @return [Juno::Client::JunoResponse]
      def set(key, value, ttl: nil)
        juno_resp = @react_client.set(key.to_s, value.to_s, ttl: ttl).wait
        return nil if juno_resp.nil?

        raise juno_resp.reason if juno_resp.rejected?

        juno_resp.value # JunoResponse
      end

      # Function to get existing key value pair
      # @param key [String] key for the document (required)
      # @param value [String] value for the document (required)
      # @param ttl [Integer] Time to live for the document (optional, default: read from config file)
      # @return [Juno::Client::JunoResponse]
      def get(key, ttl: nil)
        juno_resp = @react_client.get(key.to_s, ttl: ttl).wait
        return nil if juno_resp.nil?

        raise juno_resp.reason if juno_resp.rejected?

        juno_resp.value # JunoResponse
      end

      def update(key, value, ttl: nil)
        juno_resp = @react_client.update(key.to_s, value.to_s, ttl: ttl).wait
        return nil if juno_resp.nil?

        raise juno_resp.reason if juno_resp.rejected?

        juno_resp.value # JunoResponse
      end

      def compare_and_set(record_context, value, ttl: nil)
        juno_resp = nil
        begin
          juno_resp = @react_client.compare_and_set(record_context, value, ttl)
        rescue ArgumentError => e
          raise e.message
        end

        return nil if juno_resp.nil?

        raise juno_resp.reason if juno_resp.rejected?

        juno_resp.value # JunoResponse
      end

      def destroy(key, ttl: nil)
        juno_resp = @react_client.destroy(key.to_s, ttl: ttl).wait
        return nil if juno_resp.nil?

        raise juno_resp.reason if juno_resp.rejected?

        juno_resp.value # JunoResponse
      end
    end
  end
end
