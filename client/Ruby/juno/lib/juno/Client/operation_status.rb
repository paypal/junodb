# frozen_string_literal: true

# Constant for Operation Status in JunoResponse
module Juno
  module Client
    class OperationStatus
      SUCCESS = { code: 0, error_msg: 'No error', txnOk: true }.freeze
      NO_KEY = { code: 1, error_msg: 'Key not found', txnOk: true }.freeze
      BAD_PARAM = { code: 2, error_msg: 'Bad parameter', txnOk: false }.freeze
      UNIQUE_KEY_VIOLATION = { code: 3, error_msg: 'Duplicate key', txnOk: true }.freeze
      RECORD_LOCKED = { code: 4, error_msg: 'Record Locked', txnOk: true }.freeze
      ILLEGAL_ARGUMENT = { code: 5, error_msg: 'Illegal argument', txnOk: false }.freeze
      CONDITION_VIOLATION = { code: 6, error_msg: 'Condition in the request violated', txnOk: true }.freeze
      INTERNAL_ERROR = { code: 7, error_msg: 'Internal error', txnOk: false }.freeze
      QUEUE_FULL = { code: 8, error_msg: 'Outbound client queue full', txnOk: false }.freeze
      NO_STORAGE = { code: 9, error_msg: 'No storage server running', txnOk: false }.freeze
      TTL_EXTEND_FAILURE = { code: 10, error_msg: 'Failure to extend TTL on get', txnOk: true }.freeze
      RESPONSE_TIMEOUT = { code: 11, error_msg: 'Response Timed out', txnOk: false }.freeze
      CONNECTION_ERROR = { code: 12, error_msg: 'Connection Error', txnOk: false }.freeze
      UNKNOWN_ERROR = { code: 13, error_msg: 'Unknown Error', txnOk: false }.freeze

      @@status_code_map = nil

      def self.initialize_map
        @@status_code_map = {}

        constants.each do |const|
          const_obj = const_get(const)
          @@status_code_map[const_obj[:code]] = const_obj
        end
      end

      def self.get(status_code)
        initialize_map if @@status_code_map.nil?
        return @@status_code_map[status_code] if @@status_code_map.key?(status_code)

        INTERNAL_ERROR
      end
    end
  end
end
