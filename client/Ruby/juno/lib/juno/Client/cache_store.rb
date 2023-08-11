# frozen_string_literal: true

# Top module for juno client
module Juno
  # Module for code exposed to the developer
  module Client
    # Cache store for ruby on rails
    class CacheStore
      def initialize
        @react_client = Juno::Client::ReactClient.new
      end

      def write(key, value, _options = {})
        future_obj = @react_client.set(key, value).wait
        return false if future_obj.nil?

        raise future_obj.reason if future_obj.rejected?

        future_obj.value.status[:txnOk]
      end

      def read(key, options = {})
        future_obj = @react_client.get(key).wait
        read_response(future_obj, options[:version])
      end

      def self.supports_cache_versioning?
        true
      end

      def delete(key)
        future_obj = @react_client.destroy(key).wait
        return false if future_obj.nil? || future_obj.rejected?

        future_obj.value.status[:code] == Juno::Client::OperationStatus::SUCCESS[:code]
      end

      def exist?(key)
        !read(key).nil?
      end

      private

      def read_response(future_obj, version)
        return nil if future_obj.nil? || future_obj.rejected?

        return future_obj.value.value if version.nil?

        return future_obj.value.value if future_obj.record_context.version == version

        nil
      end
    end
  end
end
